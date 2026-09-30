// Copyright 2023 Sneller, Inc.
// Copyright 2025 Roman Atachiants
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package s3

import (
	"bytes"
	"cmp"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/kelindar/s3/aws"
)

// Prefix implements fs.File, fs.ReadDirFile, and fs.DirEntry, and fs.FS.
type Prefix struct {
	Key    *aws.SigningKey `xml:"-"`      // Key is the signing key used to sign requests.
	Bucket string          `xml:"-"`      // Bucket is the bucket at the root of the "filesystem"
	Path   string          `xml:"Prefix"` // Path is the path of this prefix, should always be a valid path  (see fs.ValidPath) plus a trailing forward slash to indicate that this is a pseudo-directory prefix.
	token  string          `xml:"-"`      // listing token; "" means start from the beginning
	dirEOF bool            `xml:"-"`      // if true, ReadDir returns io.EOF
	ctx    context.Context
}

func (p *Prefix) join(extra string) string {
	if p.Path == "." { // root of bucket
		return extra
	}
	return path.Join(p.Path, extra)
}

func (p *Prefix) sub(name string) *Prefix {
	return &Prefix{
		Key:    p.Key,
		Bucket: p.Bucket,
		Path:   p.join(name),
		ctx:    p.ctx,
	}
}

// Open opens the object or pseudo-directory
// at the provided path.
// The returned fs.File will be a *File if
// the combined Prefix and path lead to an object;
// if the combind prefix and path produce another
// complete object prefix, then a *Prefix will
// be returned. If the combined prefix and path
// do not produce a prefix that is present within
// the target bucket, then an error matching
// fs.ErrNotExist is returned.
func (p *Prefix) Open(file string) (fs.File, error) {
	file = path.Clean(file)
	switch {
	case file == ".":
		return p, nil
	case !fs.ValidPath(file):
		return nil, badpath("open", file)
	}
	return p.sub(file).openDir()
}

func (p *Prefix) openDir() (fs.File, error) {
	return p.openDirContext(p.requestContext())
}

func (p *Prefix) openDirContext(ctx context.Context) (fs.File, error) {
	if p.Path == "" || p.Path == "." {
		// the root directory trivially exists
		return p, nil
	}
	ret, err := p.listContext(ctx, 1, "", "", "")
	switch {
	case err != nil:
		return nil, err
	// if we got anything at all, it exists
	case len(ret.Contents) == 0 && len(ret.CommonPrefixes) == 0:
		return nil, &fs.PathError{Op: "open", Path: p.Path, Err: fs.ErrNotExist}
	case strings.HasSuffix(p.Path, "/"):
		return p, nil
	}
	path := p.Path + "/"
	return &Prefix{
		Key:    p.Key,
		Bucket: p.Bucket,
		Path:   path,
		ctx:    ctx,
	}, nil
}

func (p *Prefix) requestContext() context.Context {
	return cmp.Or(p.ctx, context.Background())
}

// Name implements fs.DirEntry.Name
func (p *Prefix) Name() string {
	return path.Base(p.Path)
}

// Type implements fs.DirEntry.Type
func (p *Prefix) Type() fs.FileMode {
	return fs.ModeDir
}

// Info implements fs.DirEntry.Info
func (p *Prefix) Info() (fs.FileInfo, error) {
	return p.Stat()
}

// IsDir implements fs.FileInfo.IsDir
func (p *Prefix) IsDir() bool { return true }

// ModTime implements fs.FileInfo.ModTime
//
// Note: currently ModTime returns the zero time.Time,
// as S3 prefixes don't have a meaningful modification time.
func (p *Prefix) ModTime() time.Time { return time.Time{} }

// Mode implements fs.FileInfo.Mode
func (p *Prefix) Mode() fs.FileMode { return fs.ModeDir | 0755 }

// Sys implements fs.FileInfo.Sys
func (p *Prefix) Sys() interface{} { return nil }

// Size implements fs.FileInfo.Size
func (p *Prefix) Size() int64 { return 0 }

// Stat implements fs.File.Stat
func (p *Prefix) Stat() (fs.FileInfo, error) {
	return p, nil
}

// Read implements fs.File.Read.
//
// Read always returns an error.
func (p *Prefix) Read(_ []byte) (int, error) {
	return 0, &fs.PathError{
		Op:   "read",
		Path: p.Path,
		Err:  fs.ErrInvalid,
	}
}

// Close implements fs.File.Close
func (p *Prefix) Close() error {
	return nil
}

// ReadDir implements fs.ReadDirFile
//
// Every returned fs.DirEntry will be either
// a Prefix or a File struct.
func (p *Prefix) ReadDir(n int) ([]fs.DirEntry, error) {
	return p.readDirContext(p.requestContext(), n)
}

func (p *Prefix) readDirContext(ctx context.Context, n int) ([]fs.DirEntry, error) {
	if p.dirEOF {
		return nil, io.EOF
	}
	d, next, err := p.readDirAtContext(ctx, n, p.token, "", "")
	if err == io.EOF {
		p.dirEOF = true
		if len(d) > 0 || n < 0 {
			// the spec for fs.ReadDirFile says
			// ReadDir(-1) shouldn't produce an explicit EOF
			err = nil
		}
	}
	if err != nil {
		return nil, &fs.PathError{Op: "readdir", Path: p.Path, Err: err}
	}
	p.token = next
	return d, nil
}

type listResponse struct {
	IsTruncated    bool     `xml:"IsTruncated"`
	Contents       []File   `xml:"Contents"`
	CommonPrefixes []Prefix `xml:"CommonPrefixes"`
	EncodingType   string   `xml:"EncodingType"`
	NextToken      string   `xml:"NextContinuationToken"`
}

type standardListResponse listResponse

var xmlBodies = sync.Pool{New: func() any {
	buffer := new(bytes.Buffer)
	buffer.Grow(32 << 10)
	return buffer
}}

type listingXMLTag struct {
	name []byte
	self bool
}

type listingXML struct {
	data     []byte
	pos      int
	rootSeen bool
	stack    [][]byte
	values   strings.Builder
	ranges   [64]listingXMLRange
	first    int
	count    int
}

type listingXMLRange struct {
	keyStart, keyEnd   int
	etagStart, etagEnd int
}

func decodeListResponse(data []byte) (listResponse, error) {
	if ret, ok := scanListXML(data); ok {
		return ret, nil
	}
	var ret standardListResponse
	err := xml.NewDecoder(bytes.NewReader(data)).Decode(&ret)
	return listResponse(ret), err
}

func scanListXML(data []byte) (listResponse, bool) {
	p := listingXML{data: data}
	if bytes.HasPrefix(data, []byte{0xef, 0xbb, 0xbf}) {
		p.pos = 3
	}
	root, end, ok := p.next()
	if !ok || end || root.name == nil {
		return listResponse{}, false
	}
	var ret listResponse
	if !root.self {
		for {
			tag, end, ok := p.next()
			if !ok || tag.name == nil {
				return listResponse{}, false
			}
			if end {
				if !bytes.Equal(tag.name, root.name) {
					return listResponse{}, false
				}
				break
			}
			switch {
			case listingXMLIs(tag.name, "IsTruncated"):
				text, ok := p.plainText(tag)
				if !ok {
					return listResponse{}, false
				}
				value, err := strconv.ParseBool(string(bytes.TrimSpace(text)))
				if err != nil {
					return listResponse{}, false
				}
				ret.IsTruncated = value
			case listingXMLIs(tag.name, "Contents"):
				if ret.Contents == nil {
					ret.Contents = make([]File, 0, min(bytes.Count(data, []byte("<Contents>")), 1000))
				}
				var file File
				key, etag, ok := p.object(tag, &file)
				if !ok {
					return listResponse{}, false
				}
				ret.Contents = append(ret.Contents, file)
				p.addObject(&ret, key, etag)
			case listingXMLIs(tag.name, "CommonPrefixes"):
				if ret.CommonPrefixes == nil {
					ret.CommonPrefixes = make([]Prefix, 0, min(bytes.Count(data, []byte("<CommonPrefixes>")), 1000))
				}
				var prefix Prefix
				if !p.commonPrefix(tag, &prefix) {
					return listResponse{}, false
				}
				ret.CommonPrefixes = append(ret.CommonPrefixes, prefix)
			case listingXMLIs(tag.name, "EncodingType"):
				ret.EncodingType, ok = p.text(tag)
				if !ok {
					return listResponse{}, false
				}
			case listingXMLIs(tag.name, "NextContinuationToken"):
				ret.NextToken, ok = p.text(tag)
				if !ok {
					return listResponse{}, false
				}
			default:
				if !p.skip(tag) {
					return listResponse{}, false
				}
			}
		}
	}
	trailing, end, ok := p.next()
	if !ok || end || trailing.name != nil {
		return listResponse{}, false
	}
	p.flushObjects(&ret)
	return ret, true
}

func listingXMLIs(name []byte, want string) bool { return bytes.Equal(name, []byte(want)) }

func (p *listingXML) addObject(ret *listResponse, key, etag []byte) {
	if p.count == 0 {
		p.first = len(ret.Contents) - 1
		p.values.Grow(min(16<<10, min(len(p.ranges), cap(ret.Contents)-p.first)*(len(key)+len(etag))))
	}
	part := &p.ranges[p.count]
	part.keyStart = p.values.Len()
	appendListingXMLText(&p.values, key)
	part.keyEnd = p.values.Len()
	part.etagStart = p.values.Len()
	appendListingXMLText(&p.values, etag)
	part.etagEnd = p.values.Len()
	p.count++
	if p.count == len(p.ranges) {
		p.flushObjects(ret)
	}
}

func (p *listingXML) flushObjects(ret *listResponse) {
	if p.count == 0 {
		return
	}
	value := p.values.String()
	for i := range p.count {
		part := p.ranges[i]
		file := &ret.Contents[p.first+i]
		file.Reader.Path = value[part.keyStart:part.keyEnd]
		file.ETag = value[part.etagStart:part.etagEnd]
	}
	p.values.Reset()
	p.count = 0
}

func (p *listingXML) object(tag listingXMLTag, file *File) ([]byte, []byte, bool) {
	if tag.self {
		return nil, nil, true
	}
	var key, etag []byte
	for {
		child, end, ok := p.next()
		switch {
		case !ok || child.name == nil:
			return nil, nil, false
		case end:
			if !bytes.Equal(child.name, tag.name) || bytes.IndexByte(key, '\r') >= 0 || bytes.IndexByte(etag, '\r') >= 0 || !validListingXMLText(key) || !validListingXMLText(etag) {
				return nil, nil, false
			}
			return key, etag, true
		}
		switch {
		case listingXMLIs(child.name, "Key"):
			key, ok = p.textRaw(child)
		case listingXMLIs(child.name, "ETag"):
			etag, ok = p.textRaw(child)
		case listingXMLIs(child.name, "LastModified"):
			var value []byte
			value, ok = p.plainText(child)
			if ok {
				file.LastModified, _ = time.Parse(time.RFC3339Nano, string(value))
				ok = !file.LastModified.IsZero()
			}
		case listingXMLIs(child.name, "Size"):
			var value []byte
			value, ok = p.plainText(child)
			if ok {
				var err error
				file.Reader.Size, err = strconv.ParseInt(string(bytes.TrimSpace(value)), 10, 64)
				ok = err == nil
			}
		default:
			ok = p.skip(child)
		}
		if !ok {
			return nil, nil, false
		}
	}
}

func (p *listingXML) commonPrefix(tag listingXMLTag, prefix *Prefix) bool {
	if tag.self {
		return true
	}
	for {
		child, end, ok := p.next()
		switch {
		case !ok || child.name == nil:
			return false
		case end:
			return bytes.Equal(child.name, tag.name)
		case listingXMLIs(child.name, "Prefix"):
			prefix.Path, ok = p.text(child)
		default:
			ok = p.skip(child)
		}
		if !ok {
			return false
		}
	}
}

func (p *listingXML) skip(tag listingXMLTag) bool {
	if tag.self {
		return true
	}
	p.stack = append(p.stack[:0], tag.name)
	for len(p.stack) > 0 {
		child, end, ok := p.next()
		switch {
		case !ok || child.name == nil:
			return false
		case end:
			if !bytes.Equal(child.name, p.stack[len(p.stack)-1]) {
				return false
			}
			p.stack = p.stack[:len(p.stack)-1]
		case !child.self:
			if len(p.stack) == 10000 {
				return false
			}
			p.stack = append(p.stack, child.name)
		}
	}
	return true
}

func (p *listingXML) text(tag listingXMLTag) (string, bool) {
	value, ok := p.textRaw(tag)
	if !ok {
		return "", false
	}
	return listingXMLText(value)
}

func (p *listingXML) plainText(tag listingXMLTag) ([]byte, bool) {
	value, ok := p.textRaw(tag)
	if !ok || bytes.IndexByte(value, '&') >= 0 || bytes.IndexByte(value, '\r') >= 0 || !validListingXMLText(value) {
		return nil, false
	}
	return value, true
}

func (p *listingXML) textRaw(tag listingXMLTag) ([]byte, bool) {
	if tag.self {
		return nil, true
	}
	start := p.pos
	for p.pos < len(p.data) {
		relative := bytes.IndexByte(p.data[p.pos:], '<')
		if relative < 0 {
			return nil, false
		}
		p.pos += relative
		if p.pos+1 >= len(p.data) || p.data[p.pos+1] != '/' {
			return nil, false
		}
		value := p.data[start:p.pos]
		closing, ok := p.close()
		if !ok || !bytes.Equal(closing, tag.name) {
			return nil, false
		}
		return value, true
	}
	return nil, false
}

func (p *listingXML) next() (listingXMLTag, bool, bool) {
	for p.pos < len(p.data) {
		if p.data[p.pos] != '<' {
			next := bytes.IndexByte(p.data[p.pos:], '<')
			if next < 0 {
				if !validListingXMLText(p.data[p.pos:]) {
					return listingXMLTag{}, false, false
				}
				p.pos = len(p.data)
				break
			}
			if !validListingXMLText(p.data[p.pos : p.pos+next]) {
				return listingXMLTag{}, false, false
			}
			p.pos += next
		}
		switch {
		case bytes.HasPrefix(p.data[p.pos:], []byte("<?xml")) && !p.rootSeen && (p.pos == 0 || p.pos == 3):
			end := bytes.Index(p.data[p.pos+5:], []byte("?>"))
			if end < 0 {
				return listingXMLTag{}, false, false
			}
			declaration := p.data[p.pos : p.pos+5+end+2]
			if !validListingXMLDeclaration(declaration) {
				return listingXMLTag{}, false, false
			}
			p.pos += 5 + end + 2
			continue
		case bytes.HasPrefix(p.data[p.pos:], []byte("</")):
			name, ok := p.close()
			return listingXMLTag{name: name}, true, ok
		case p.pos+1 >= len(p.data) || p.data[p.pos+1] == '!' || p.data[p.pos+1] == '?':
			return listingXMLTag{}, false, false
		}
		return p.open()
	}
	return listingXMLTag{}, false, true
}

func (p *listingXML) open() (listingXMLTag, bool, bool) {
	p.pos++
	start := p.pos
	for p.pos < len(p.data) && listingXMLNameByte(p.data[p.pos], p.pos == start) {
		p.pos++
	}
	name := p.data[start:p.pos]
	if len(name) == 0 || bytes.IndexByte(name, ':') >= 0 {
		return listingXMLTag{}, false, false
	}
	hasNamespace := false
	for p.pos < len(p.data) {
		for p.pos < len(p.data) && isListingXMLSpace(p.data[p.pos]) {
			p.pos++
		}
		if p.pos >= len(p.data) {
			return listingXMLTag{}, false, false
		}
		switch p.data[p.pos] {
		case '>':
			p.pos++
			p.rootSeen = true
			return listingXMLTag{name: name}, false, true
		case '/':
			if p.pos+1 >= len(p.data) || p.data[p.pos+1] != '>' {
				return listingXMLTag{}, false, false
			}
			p.pos += 2
			p.rootSeen = true
			return listingXMLTag{name: name, self: true}, false, true
		default:
			attrStart := p.pos
			for p.pos < len(p.data) && p.data[p.pos] != '=' && p.data[p.pos] != '>' && !isListingXMLSpace(p.data[p.pos]) {
				p.pos++
			}
			attr := p.data[attrStart:p.pos]
			if !bytes.Equal(attr, []byte("xmlns")) || hasNamespace {
				return listingXMLTag{}, false, false
			}
			hasNamespace = true
			for p.pos < len(p.data) && isListingXMLSpace(p.data[p.pos]) {
				p.pos++
			}
			if p.pos >= len(p.data) || p.data[p.pos] != '=' {
				return listingXMLTag{}, false, false
			}
			p.pos++
			for p.pos < len(p.data) && isListingXMLSpace(p.data[p.pos]) {
				p.pos++
			}
			if p.pos >= len(p.data) || (p.data[p.pos] != '\'' && p.data[p.pos] != '"') {
				return listingXMLTag{}, false, false
			}
			quote := p.data[p.pos]
			p.pos++
			valueStart := p.pos
			for p.pos < len(p.data) && p.data[p.pos] != quote {
				if p.data[p.pos] == '<' {
					return listingXMLTag{}, false, false
				}
				p.pos++
			}
			if p.pos >= len(p.data) || bytes.IndexByte(p.data[valueStart:p.pos], '\r') >= 0 || !validListingXMLText(p.data[valueStart:p.pos]) {
				return listingXMLTag{}, false, false
			}
			p.pos++
		}
	}
	return listingXMLTag{}, false, false
}

func (p *listingXML) close() ([]byte, bool) {
	p.pos += 2
	start := p.pos
	for p.pos < len(p.data) && listingXMLNameByte(p.data[p.pos], p.pos == start) {
		p.pos++
	}
	name := p.data[start:p.pos]
	if len(name) == 0 || bytes.IndexByte(name, ':') >= 0 {
		return nil, false
	}
	for p.pos < len(p.data) && isListingXMLSpace(p.data[p.pos]) {
		p.pos++
	}
	if p.pos >= len(p.data) || p.data[p.pos] != '>' {
		return nil, false
	}
	p.pos++
	return name, true
}

func listingXMLNameByte(value byte, first bool) bool {
	return value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z' || value == '_' || !first && (value >= '0' && value <= '9' || value == '-' || value == '.')
}

func validListingXMLDeclaration(declaration []byte) bool {
	for _, valid := range [][]byte{
		[]byte(`<?xml version="1.0"?>`),
		[]byte(`<?xml version="1.0" encoding="UTF-8"?>`),
		[]byte(`<?xml version="1.0" encoding="utf-8"?>`),
		[]byte(`<?xml version='1.0'?>`),
		[]byte(`<?xml version='1.0' encoding='UTF-8'?>`),
		[]byte(`<?xml version='1.0' encoding='utf-8'?>`),
	} {
		if bytes.Equal(declaration, valid) {
			return true
		}
	}
	return false
}

func isListingXMLSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\n' || value == '\r'
}

func validListingXMLRune(value rune) bool {
	return value == '\t' || value == '\n' || value == '\r' || value >= 0x20 && value <= 0xD7FF || value >= 0xE000 && value <= 0xFFFD || value >= 0x10000 && value <= 0x10FFFF
}

func validListingXMLText(src []byte) bool {
	if bytes.Contains(src, []byte("]]>")) {
		return false
	}
	for i := 0; i < len(src); {
		if src[i] == '&' {
			_, size, ok := listingXMLEntity(src[i:])
			if !ok {
				return false
			}
			i += size
			continue
		}
		r, size := utf8.DecodeRune(src[i:])
		if size == 1 && r == utf8.RuneError || !validListingXMLRune(r) {
			return false
		}
		i += size
	}
	return true
}

func listingXMLText(src []byte) (string, bool) {
	switch {
	case bytes.IndexByte(src, '\r') >= 0 || !validListingXMLText(src):
		return "", false
	case bytes.IndexByte(src, '&') < 0:
		return string(src), true
	}
	var text strings.Builder
	text.Grow(len(src))
	appendListingXMLText(&text, src)
	return text.String(), true
}

func appendListingXMLText(dst *strings.Builder, src []byte) {
	for len(src) > 0 {
		index := bytes.IndexByte(src, '&')
		if index < 0 {
			dst.Write(src)
			return
		}
		dst.Write(src[:index])
		r, size, _ := listingXMLEntity(src[index:])
		dst.WriteRune(r)
		src = src[index+size:]
	}
}

func listingXMLEntity(src []byte) (rune, int, bool) {
	end := bytes.IndexByte(src, ';')
	if end < 2 {
		return 0, 0, false
	}
	entity := src[1:end]
	switch string(entity) {
	case "amp":
		return '&', end + 1, true
	case "lt":
		return '<', end + 1, true
	case "gt":
		return '>', end + 1, true
	case "apos":
		return '\'', end + 1, true
	case "quot":
		return '"', end + 1, true
	}
	if entity[0] != '#' {
		return 0, 0, false
	}
	base, digits := 10, entity[1:]
	if len(digits) > 1 && digits[0] == 'x' {
		base, digits = 16, digits[1:]
	}
	value, err := strconv.ParseUint(string(digits), base, 32)
	r := rune(value)
	return r, end + 1, err == nil && validListingXMLRune(r)
}

func (p *Prefix) list(n int, token, seek, prefix string) (*listResponse, error) {
	return p.listContext(p.requestContext(), n, token, seek, prefix)
}

func (p *Prefix) listContext(ctx context.Context, n int, token, seek, prefix string) (*listResponse, error) {
	if !ValidBucket(p.Bucket) {
		return nil, badBucket(p.Bucket)
	}
	// make sure there's a '/' at the end and
	// append the prefix
	path := p.Path
	switch {
	case path == "" || path == ".":
		// NOTE: if p.Path was "." this will replace
		// it with prefix which may be ""; this is
		// the intended behavior
		path = prefix
	case !strings.HasSuffix(path, "/"):
		path += "/" + prefix
	default:
		path += prefix
	}
	// the seek parameter is only meaningful
	// if it is "larger" than the prefix being listed;
	// otherwise we should reject it
	// (AWS S3 accepts redundant start-after params,
	// but Minio rejects them)
	if seek != "" && (seek < prefix || !strings.HasPrefix(seek, prefix)) {
		return nil, fmt.Errorf("seek %q not compatible with prefix %q", seek, prefix)
	}
	var query strings.Builder
	query.Grow(96 + len(path) + len(token) + len(seek))
	query.WriteByte('?')
	if token != "" {
		query.WriteString("continuation-token=")
		query.WriteString(url.QueryEscape(token))
		query.WriteByte('&')
	}
	query.WriteString("delimiter=%2F&list-type=2")
	if n > 0 {
		query.WriteString("&max-keys=")
		query.WriteString(strconv.Itoa(n))
	}
	if path != "" {
		query.WriteString("&prefix=")
		query.WriteString(queryEscape(path))
	}
	if seek != "" {
		query.WriteString("&start-after=")
		query.WriteString(queryEscape(p.join(seek)))
	}
	res, err := doSigned(ctx, p.Key, "GET", rawURI(p.Key, p.Bucket, query.String()), nil)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case 200:
		// ok
	case 403:
		return nil, fs.ErrPermission
	case 404:
		// this can actually mean the bucket doesn't exist,
		// but for practical purposes we can treat it
		// as an empty filesystem
		return nil, fs.ErrNotExist
	default:
		return nil, fmt.Errorf("s3 list objects s3://%s/%s: %s", p.Bucket, p.Path, res.status())
	}

	body := xmlBodies.Get().(*bytes.Buffer)
	body.Reset()
	defer func() {
		if body.Cap() <= 256<<10 {
			body.Reset()
			xmlBodies.Put(body)
		}
	}()
	_, err = body.ReadFrom(res.Body)
	if err != nil {
		return nil, fmt.Errorf("xml decoding response: %w", err)
	}
	ret, err := decodeListResponse(body.Bytes())
	if err != nil {
		return nil, fmt.Errorf("xml decoding response: %w", err)
	}
	return &ret, nil
}

func patmatch(pattern, name string) (bool, error) {
	if pattern == "" {
		return true, nil
	}
	return path.Match(pattern, name)
}

func ignoreKey(key string, dirOK bool) bool {
	name := path.Base(key)
	return key == "" ||
		!dirOK && key[len(key)-1] == '/' ||
		name == "." || name == ".."
}

// readDirAt reads n entries (or all if n < 0)
// from a directory using the given continuation
// token, returning the directory entries, the
// next continuation token, and any error.
//
// If seek is provided, this will be appended to
// the prefix path passed as the start-after
// parameter to the list call.
//
// If pattern is provided, the returned entries
// will be filtered against this pattern, and
// the prefix before the first meta-character
// will be used to determine a prefix that will
// be appended to the path passed as the prefix
// parameter to the list call.
//
// If the full directory listing was read in one
// call, this returns the list of directory
// entries, an empty continuation token, and
// io.EOF. Note that this behavior differs from
// fs.ReadDirFile.ReadDir.
func (p *Prefix) readDirAt(n int, token, seek, pattern string) (d []fs.DirEntry, next string, err error) {
	return p.readDirAtContext(p.requestContext(), n, token, seek, pattern)
}

func (p *Prefix) readDirAtContext(ctx context.Context, n int, token, seek, pattern string) (d []fs.DirEntry, next string, err error) {
	prefix, _ := splitMeta(pattern)
	ret, err := p.listContext(ctx, n, token, seek, prefix)
	if err != nil {
		return nil, "", err
	}
	out := make([]fs.DirEntry, 0, len(ret.Contents)+len(ret.CommonPrefixes))
	for i := range ret.Contents {
		if ignoreKey(ret.Contents[i].Path(), false) {
			continue
		}
		switch match, err := patmatch(pattern, ret.Contents[i].Name()); {
		case err != nil:
			return nil, "", err
		case !match:
			continue
		}
		ret.Contents[i].Key = p.Key
		ret.Contents[i].Bucket = p.Bucket
		ret.Contents[i].Reader.ctx = ctx
		out = append(out, &ret.Contents[i])
	}
	for i := range ret.CommonPrefixes {
		if ignoreKey(ret.CommonPrefixes[i].Path, true) {
			continue
		}
		switch match, err := patmatch(pattern, ret.CommonPrefixes[i].Name()); {
		case err != nil:
			return nil, "", err
		case !match:
			continue
		}
		ret.CommonPrefixes[i].Key = p.Key
		ret.CommonPrefixes[i].Bucket = p.Bucket
		ret.CommonPrefixes[i].ctx = ctx
		out = append(out, &ret.CommonPrefixes[i])
	}
	slices.SortFunc(out, func(a, b fs.DirEntry) int {
		return strings.Compare(a.Name(), b.Name())
	})
	if !ret.IsTruncated {
		err = io.EOF
	}
	return out, ret.NextToken, err
}
