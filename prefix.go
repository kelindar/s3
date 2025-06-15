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
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/kelindar/s3/aws"
)

// Prefix implements fs.File, fs.ReadDirFile,
// and fs.DirEntry, and fs.FS.
type Prefix struct {
	// Key is the signing key used to sign requests.
	Key *aws.SigningKey `xml:"-"`
	// Bucket is the bucket at the root of the "filesystem"
	Bucket string `xml:"-"`
	// Path is the path of this prefix.
	// The value of Path should always be
	// a valid path (see fs.ValidPath) plus
	// a trailing forward slash to indicate
	// that this is a pseudo-directory prefix.
	Path   string       `xml:"Prefix"`
	Client *http.Client `xml:"-"`

	// listing token;
	// "" means start from the beginning
	token string
	// if true, ReadDir returns io.EOF
	dirEOF bool
}

func (p *Prefix) join(extra string) string {
	if p.Path == "." {
		// root of bucket
		return extra
	}
	return path.Join(p.Path, extra)
}

func (p *Prefix) sub(name string) *Prefix {
	return &Prefix{
		Key:    p.Key,
		Client: p.Client,
		Bucket: p.Bucket,
		Path:   p.join(name),
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
	if file == "." {
		return p, nil
	}
	if !fs.ValidPath(file) {
		return nil, badpath("open", file)
	}
	return p.sub(file).openDir()
}

func (p *Prefix) openDir() (fs.File, error) {
	if p.Path == "" || p.Path == "." {
		// the root directory trivially exists
		return p, nil
	}
	ret, err := p.list(1, "", "", "")
	if err != nil {
		return nil, err
	}
	// if we got anything at all, it exists
	if len(ret.Contents) == 0 && len(ret.CommonPrefixes) == 0 {
		return nil, &fs.PathError{Op: "open", Path: p.Path, Err: fs.ErrNotExist}
	}
	if strings.HasSuffix(p.Path, "/") {
		return p, nil
	}
	path := p.Path + "/"
	return &Prefix{
		Key:    p.Key,
		Bucket: p.Bucket,
		Client: p.Client,
		Path:   path,
	}, nil
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
	if p.dirEOF {
		return nil, io.EOF
	}
	d, next, err := p.readDirAt(n, p.token, "", "")
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

func (p *Prefix) list(n int, token, seek, prefix string) (*listResponse, error) {
	if !ValidBucket(p.Bucket) {
		return nil, badBucket(p.Bucket)
	}
	parts := []string{
		"delimiter=%2F",
		"list-type=2",
	}
	// make sure there's a '/' at the end and
	// append the prefix
	path := p.Path
	if path != "" && path != "." {
		if !strings.HasSuffix(path, "/") {
			path += "/" + prefix
		} else {
			path += prefix
		}
	} else {
		// NOTE: if p.Path was "." this will replace
		// it with prefix which may be ""; this is
		// the intended behavior
		path = prefix
	}
	if path != "" {
		parts = append(parts, "prefix="+queryEscape(path))
	}
	// the seek parameter is only meaningful
	// if it is "larger" than the prefix being listed;
	// otherwise we should reject it
	// (AWS S3 accepts redundant start-after params,
	// but Minio rejects them)
	if seek != "" && (seek < prefix || !strings.HasPrefix(seek, prefix)) {
		return nil, fmt.Errorf("seek %q not compatible with prefix %q", seek, prefix)
	}
	if seek != "" {
		parts = append(parts, "start-after="+queryEscape(p.join(seek)))
	}
	if n > 0 {
		parts = append(parts, fmt.Sprintf("max-keys=%d", n))
	}
	if token != "" {
		parts = append(parts, "continuation-token="+url.QueryEscape(token))
	}
	sort.Strings(parts)
	query := "?" + strings.Join(parts, "&")
	req, err := http.NewRequestWithContext(context.TODO(), http.MethodGet, rawURI(p.Key, p.Bucket, query), nil)
	if err != nil {
		return nil, fmt.Errorf("creating http request: %w", err)
	}
	p.Key.SignV4(req, nil)
	res, err := flakyDo(p.client(), req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		if res.StatusCode == 403 {
			return nil, fs.ErrPermission
		}
		if res.StatusCode == 404 {
			// this can actually mean the bucket doesn't exist,
			// but for practical purposes we can treat it
			// as an empty filesystem
			return nil, fs.ErrNotExist
		}
		return nil, fmt.Errorf("s3 list objects s3://%s/%s: %s", p.Bucket, p.Path, res.Status)
	}
	ret := &listResponse{}
	err = xml.NewDecoder(res.Body).Decode(ret)
	if err != nil {
		return nil, fmt.Errorf("xml decoding response: %w", err)
	}
	return ret, nil
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
	prefix, _ := splitMeta(pattern)
	ret, err := p.list(n, token, seek, prefix)
	if err != nil {
		return nil, "", err
	}
	out := make([]fs.DirEntry, 0, len(ret.Contents)+len(ret.CommonPrefixes))
	for i := range ret.Contents {
		if ignoreKey(ret.Contents[i].Path(), false) {
			continue
		}
		name := ret.Contents[i].Name()
		match, err := patmatch(pattern, name)
		if err != nil {
			return nil, "", err
		} else if !match {
			continue
		}
		ret.Contents[i].Key = p.Key
		ret.Contents[i].Client = p.client()
		ret.Contents[i].Bucket = p.Bucket
		// FIXME: we're using the "wrong" context here
		// because we really just wanted to use the
		// embedded context for limiting the time spent
		// scanning and not the time spent reading the
		// input file...
		ret.Contents[i].ctx = context.Background()
		out = append(out, &ret.Contents[i])
	}
	for i := range ret.CommonPrefixes {
		if ignoreKey(ret.CommonPrefixes[i].Path, true) {
			continue
		}
		name := ret.CommonPrefixes[i].Name()
		match, err := patmatch(pattern, name)
		if err != nil {
			return nil, "", err
		} else if !match {
			continue
		}
		ret.CommonPrefixes[i].Key = p.Key
		ret.CommonPrefixes[i].Bucket = p.Bucket
		ret.CommonPrefixes[i].Client = p.Client
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

func (p *Prefix) client() *http.Client {
	if p.Client == nil {
		return &DefaultClient
	}
	return p.Client
}
