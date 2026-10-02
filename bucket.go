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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"net/http"
	"path"
	"slices"
	"strings"

	"github.com/kelindar/s3/aws"
	"github.com/kelindar/s3/fsutil"
	"github.com/valyala/fasthttp"
)

// Bucket implements fs.FS, fs.ReadDirFS, and fs.SubFS.
type Bucket struct {
	key  *aws.SigningKey // signing key
	bkt  string          // bucket name
	Lazy bool            // If true, causes the initial Open call to use a HEAD operation rather than a GET operation.
}

// NewBucket creates a new Bucket instance.
func NewBucket(key *aws.SigningKey, bucket string) *Bucket {
	return &Bucket{
		key: key,
		bkt: bucket,
	}
}

func (b *Bucket) sub(name string) *Prefix {
	return &Prefix{
		Key:    b.key,
		Bucket: b.bkt,
		Path:   name,
	}
}

func (b *Bucket) subContext(ctx context.Context, name string) *Prefix {
	p := b.sub(name)
	p.ctx = ctx
	return p
}

func badpath(op, name string) error {
	return &fs.PathError{
		Op:   op,
		Path: name,
		Err:  fs.ErrInvalid,
	}
}

// Write performs a PutObject operation at the object key 'key' and returns the ETag of the newly-created object.
func (b *Bucket) Write(ctx context.Context, key string, contents []byte) (string, error) {
	etag, _, err := b.write(ctx, key, contents, nil)
	return etag, err
}

// Condition adds HTTP conditional request headers to a write.
// Implementations must set one or more If-* headers.
type Condition func(http.Header) error

func validateCondition(header http.Header) error {
	if len(header) == 0 {
		return errors.New("s3 PUT: condition set no headers")
	}
	for name, values := range header {
		switch {
		case http.CanonicalHeaderKey(name) != name:
			return fmt.Errorf("s3 PUT: condition set non-canonical header %q", name)
		case !strings.HasPrefix(name, "If-"):
			return fmt.Errorf("s3 PUT: condition set non-conditional header %q", name)
		case len(values) != 1:
			return fmt.Errorf("s3 PUT: condition set multiple values for header %q", name)
		case values[0] == "":
			return fmt.Errorf("s3 PUT: condition set empty header %q", name)
		case name == "If-None-Match" && values[0] != "*":
			return errors.New(`s3 PUT: If-None-Match must be "*"`)
		}
	}
	return nil
}

// IfMatch writes only while the object's ETag matches etag.
func IfMatch(etag string) Condition {
	return func(header http.Header) error {
		if etag == "" {
			return errors.New("empty ETag")
		}
		header.Set("If-Match", etag)
		return nil
	}
}

// IfNoneMatch writes only when the object does not exist. S3 PutObject requires
// etag to be "*".
func IfNoneMatch(etag string) Condition {
	return func(header http.Header) error {
		if etag == "" {
			return errors.New("empty ETag")
		}
		header.Set("If-None-Match", etag)
		return nil
	}
}

// WriteIf performs a PutObject only when condition's HTTP preconditions hold.
// It returns applied=false and nil error when S3 rejects the precondition.
// Conditional requests are not retried; after a transport error the write may
// have committed, so callers must resolve its outcome before retrying.
func (b *Bucket) WriteIf(ctx context.Context, key string, contents []byte, condition Condition) (etag string, applied bool, err error) {
	if condition == nil {
		return "", false, errors.New("s3 PUT: missing condition")
	}
	return b.write(ctx, key, contents, condition)
}

func (b *Bucket) write(ctx context.Context, key string, contents []byte, condition Condition) (string, bool, error) {
	key = path.Clean(key)
	_, base := path.Split(key)
	switch {
	case !fs.ValidPath(key):
		return "", false, badpath("s3 PUT", key)
	case base == ".":
		// Don't allow a path that is nominally a directory
		return "", false, badpath("s3 PUT", key)
	}

	var res *response
	var err error
	var ifMatch string
	switch {
	case condition == nil:
		res, err = doObject(ctx, b.key, http.MethodPut, b.bkt, key, contents)
	case ctx == nil:
		return "", false, errors.New("s3 request: nil context")
	case ctx.Err() != nil:
		return "", false, ctx.Err()
	default:
		header := make(http.Header)
		if err := condition(header); err != nil {
			return "", false, fmt.Errorf("s3 PUT condition: %w", err)
		}
		if err := validateCondition(header); err != nil {
			return "", false, err
		}
		ifMatch = header.Get("If-Match")
		req := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(req)
		setURI(req, b.key, b.bkt, key, "")
		req.Header.SetMethod(fasthttp.MethodPut)
		var storage [8][2]string
		headers := storage[:0]
		for name, values := range header {
			headers = append(headers, [2]string{strings.ToLower(name), values[0]})
		}
		signRequest(b.key, req, contents, headers...)

		// A lost response may follow a committed write. Retrying can turn that
		// uncertainty into a false precondition failure.
		res, err = doFastRequest(ctx, req)
	}
	if err != nil {
		return "", false, err
	}
	defer res.Body.Close()
	switch {
	case res.StatusCode == http.StatusPreconditionFailed && condition != nil:
		return "", false, nil
	case res.StatusCode != http.StatusOK:
		code, message := extractS3Error(res.Body)
		if res.StatusCode == http.StatusNotFound && ifMatch != "" && code == "NoSuchKey" {
			return "", false, nil
		}
		return "", false, fmt.Errorf("s3 PUT: %s %s", res.status(), message)
	default:
		return res.Header.Get("ETag"), true, nil
	}
}

// Sub implements fs.SubFS.Sub.
func (b *Bucket) Sub(dir string) (fs.FS, error) {
	dir = path.Clean(dir)
	switch {
	case !fs.ValidPath(dir):
		return nil, badpath("sub", dir)
	case dir == ".":
		return b, nil
	}
	return b.sub(dir + "/"), nil
}

// Open implements fs.FS.Open
//
// The returned fs.File will be either a *File
// or a *Prefix depending on whether name refers
// to an object or a common path prefix that
// leads to multiple objects.
// If name does not refer to an object or a path prefix,
// then Open returns an error matching fs.ErrNotExist.
func (b *Bucket) Open(name string) (fs.File, error) {
	return b.OpenContext(context.Background(), name)
}

// OpenContext opens name using ctx for object reads and directory listing.
func (b *Bucket) OpenContext(ctx context.Context, name string) (fs.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// interpret a trailing / to mean
	// a directory
	isDir := strings.HasSuffix(name, "/")
	name = path.Clean(name)
	switch {
	case !fs.ValidPath(name):
		return nil, badpath("open", name)
	// opening the "root directory"
	case name == ".":
		return b.subContext(ctx, "."), nil
	}
	if !isDir {
		// try a HEAD or GET operation; these
		// are cheaper and faster than
		// full listing operations
		f, err := openContext(ctx, b.key, b.bkt, name, !b.Lazy)
		if err == nil || !errors.Is(err, fs.ErrNotExist) {
			return f, err
		}
	}

	return b.subContext(ctx, name).openDirContext(ctx)
}

// OpenRange produces an [io.ReadCloser] that reads data from
// the file given by [name] with the etag given by [etag]
// starting at byte [start] and continuing for [width] bytes.
// If [etag] does not match the ETag of the object, then
// [ErrETagChanged] will be returned.
func (b *Bucket) OpenRange(name, etag string, start, width int64) (io.ReadCloser, error) {
	return b.OpenRangeContext(context.Background(), name, etag, start, width)
}

// OpenRangeContext reads a byte range using ctx.
func (b *Bucket) OpenRangeContext(ctx context.Context, name, etag string, start, width int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	name = path.Clean(name)
	if !fs.ValidPath(name) || name == "." {
		return nil, badpath("OpenRange", name)
	}
	r := Reader{
		Key:    b.key,
		Bucket: b.bkt,
		Path:   name,
		ETag:   etag,
		ctx:    ctx,
	}
	return r.RangeReader(start, width)
}

// VisitDir implements fs.VisitDirFS
func (b *Bucket) VisitDir(name, seek, pattern string, walk fsutil.VisitDirFn) error {
	name = path.Clean(name)
	switch {
	case !fs.ValidPath(name):
		return badpath("visitdir", name)
	case name == ".":
		return b.sub(".").VisitDir(".", seek, pattern, walk)
	}
	return b.sub(name+"/").VisitDir(".", seek, pattern, walk)
}

// ReadDir implements fs.ReadDirFS
func (b *Bucket) ReadDir(name string) ([]fs.DirEntry, error) {
	name = path.Clean(name)
	if !fs.ValidPath(name) {
		return nil, badpath("readdir", name)
	}
	prefix := b.sub(".")
	if name != "." {
		prefix = b.sub(name + "/")
	}

	var entries []fs.DirEntry
	for token := ""; ; {
		page, next, err := prefix.readDirAtContext(context.Background(), -1, token, "", "")
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, &fs.PathError{Op: "readdir", Path: prefix.Path, Err: err}
		}
		if len(entries) == 0 && len(page) > 0 {
			entries = page
		} else {
			entries = append(entries, page...)
		}
		if errors.Is(err, io.EOF) || next == "" {
			break
		}
		token = next
	}

	if len(entries) == 0 && name != "." {
		// An empty listing usually means the directory does not exist.
		f, err := b.sub(name + "/").openDir()
		if err != nil {
			return nil, err
		}
		f.Close()
	}
	slices.SortFunc(entries, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	return entries, nil
}

// List lazily lists a prefix. It yields the first error and stops.
func (b *Bucket) List(ctx context.Context, name string) iter.Seq2[fs.DirEntry, error] {
	return func(yield func(fs.DirEntry, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(nil, err)
			return
		}
		name = path.Clean(name)
		if !fs.ValidPath(name) {
			yield(nil, badpath("readdir", name))
			return
		}

		prefix := b.sub(".")
		if name != "." {
			prefix = b.sub(name + "/")
		}

		var token string
		for {
			entries, next, err := prefix.readDirAtContext(ctx, -1, token, "", "")
			for _, entry := range entries {
				if !yield(entry, nil) {
					return
				}
			}
			switch {
			case errors.Is(err, io.EOF):
				return
			case err != nil:
				yield(nil, &fs.PathError{Op: "readdir", Path: prefix.Path, Err: err})
				return
			case next == "":
				return
			}
			token = next
		}
	}
}

// Delete removes the object at fullpath.
func (b *Bucket) Delete(ctx context.Context, fullpath string) error {
	fullpath = path.Clean(fullpath)
	if !fs.ValidPath(fullpath) {
		return fmt.Errorf("%s: %s", fullpath, fs.ErrInvalid)
	}
	res, err := doObject(ctx, b.key, http.MethodDelete, b.bkt, fullpath, nil)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 204 {
		return fmt.Errorf("s3 DELETE: %s %s", res.status(), extractMessage(res.Body))
	}
	return nil
}

// WriteFrom performs a multipart upload of data from an io.ReaderAt to the specified key.
func (b *Bucket) WriteFrom(ctx context.Context, key string, r io.ReaderAt, size int64) error {
	key = path.Clean(key)
	_, base := path.Split(key)
	switch err := ctx.Err(); {
	case !fs.ValidPath(key):
		return badpath("s3 Upload", key)
	case base == ".":
		return badpath("s3 Upload", key)
	case size < 0:
		return fmt.Errorf("size must be non-negative, got %d", size)
	case err != nil:
		return err
	}
	if size < MinPartSize {
		contents := make([]byte, int(size))
		if size > 0 {
			if _, err := io.ReadFull(io.NewSectionReader(r, 0, size), contents); err != nil {
				return fmt.Errorf("reading s3 object: %w", err)
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := b.Write(ctx, key, contents)
		return err
	}

	uploader := &uploader{
		Key:    b.key,
		Bucket: b.bkt,
		Object: key,
	}

	// Start multipart upload
	if err := uploader.Start(ctx); err != nil {
		return fmt.Errorf("starting multipart upload: %w", err)
	}
	defer func() {
		if !uploader.finished {
			_ = uploader.Abort(context.WithoutCancel(ctx))
		}
	}()

	return uploader.UploadFrom(ctx, r, size)
}
