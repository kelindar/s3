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
	"net/http"
	"path"
	"strings"

	"github.com/kelindar/s3/aws"
	"github.com/kelindar/s3/fsutil"
)

// Bucket implements fs.FS, fs.ReadDirFS, and fs.SubFS.
type Bucket struct {
	key    *aws.SigningKey // signing key
	bkt    string          // bucket name
	Client *http.Client    // HTTP client used for requests, if nil then DefaultClient is used
	Lazy   bool            // If true, causes the initial Open call to use a HEAD operation rather than a GET operation.
}

// NewBucket creates a new Bucket instance.
func NewBucket(key *aws.SigningKey, bucket string) *Bucket {
	return &Bucket{
		key: key,
		bkt: bucket,
	}
}

func (b *Bucket) client() *http.Client {
	if b.Client == nil {
		return &DefaultClient
	}
	return b.Client
}

func (b *Bucket) sub(name string) *Prefix {
	return &Prefix{
		Key:    b.key,
		Client: b.Client,
		Bucket: b.bkt,
		Path:   name,
	}
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
	if key = path.Clean(key); !fs.ValidPath(key) {
		return "", badpath("s3 PUT", key)
	}

	// Don't allow a path that is nominally a directory
	if _, base := path.Split(key); base == "." {
		return "", badpath("s3 PUT", key)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, uri(b.key, b.bkt, key), nil)
	if err != nil {
		return "", err
	}

	b.key.SignV4(req, contents)
	res, err := flakyDo(b.client(), req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return "", fmt.Errorf("s3 PUT: %s %s", res.Status, extractMessage(res.Body))
	}
	etag := res.Header.Get("ETag")
	return etag, nil
}

// Sub implements fs.SubFS.Sub.
func (b *Bucket) Sub(dir string) (fs.FS, error) {
	dir = path.Clean(dir)
	if !fs.ValidPath(dir) {
		return nil, badpath("sub", dir)
	}
	if dir == "." {
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
	// interpret a trailing / to mean
	// a directory
	isDir := strings.HasSuffix(name, "/")
	name = path.Clean(name)
	if !fs.ValidPath(name) {
		return nil, badpath("open", name)
	}
	// opening the "root directory"
	if name == "." {
		return b.sub("."), nil
	}
	if !isDir {
		// try a HEAD or GET operation; these
		// are cheaper and faster than
		// full listing operations
		f, err := Open(b.key, b.bkt, name, !b.Lazy)
		if err == nil || !errors.Is(err, fs.ErrNotExist) {
			return f, err
		}
	}

	return b.sub(name).openDir()
}

// OpenRange produces an [io.ReadCloser] that reads data from
// the file given by [name] with the etag given by [etag]
// starting at byte [start] and continuing for [width] bytes.
// If [etag] does not match the ETag of the object, then
// [ErrETagChanged] will be returned.
func (b *Bucket) OpenRange(name, etag string, start, width int64) (io.ReadCloser, error) {
	name = path.Clean(name)
	if !fs.ValidPath(name) || name == "." {
		return nil, badpath("OpenRange", name)
	}
	r := Reader{
		Client: b.Client,
		Key:    b.key,
		Bucket: b.bkt,
		Path:   name,
		ETag:   etag,
	}
	return r.RangeReader(start, width)
}

// VisitDir implements fs.VisitDirFS
func (b *Bucket) VisitDir(name, seek, pattern string, walk fsutil.VisitDirFn) error {
	name = path.Clean(name)
	if !fs.ValidPath(name) {
		return badpath("visitdir", name)
	}
	if name == "." {
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
	if name == "." {
		return b.sub(".").ReadDir(-1)
	}
	ret, err := b.sub(name + "/").ReadDir(-1)
	if err != nil {
		return ret, err
	}
	if len(ret) == 0 {
		// *almost always* because name doesn't actually exist;
		// we should double-check
		f, err := b.sub(name + "/").openDir()
		if err != nil {
			return nil, err
		}
		f.Close()
	}
	return ret, nil
}

// Delete removes the object at fullpath.
func (b *Bucket) Delete(ctx context.Context, fullpath string) error {
	fullpath = path.Clean(fullpath)
	if !fs.ValidPath(fullpath) {
		return fmt.Errorf("%s: %s", fullpath, fs.ErrInvalid)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, uri(b.key, b.bkt, fullpath), nil)
	if err != nil {
		return err
	}
	b.key.SignV4(req, nil)
	res, err := flakyDo(b.client(), req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 204 {
		return fmt.Errorf("s3 DELETE: %s %s", res.Status, extractMessage(res.Body))
	}
	return nil
}

// WriteFrom performs a multipart upload of data from an io.ReaderAt to the specified key.
func (b *Bucket) WriteFrom(ctx context.Context, key string, r io.ReaderAt, size int64) error {
	if key = path.Clean(key); !fs.ValidPath(key) {
		return badpath("s3 Upload", key)
	}

	if _, base := path.Split(key); base == "." {
		return badpath("s3 Upload", key)
	}

	if size < 0 {
		return fmt.Errorf("size must be non-negative, got %d", size)
	}

	uploader := &uploader{
		Key:    b.key,
		Client: b.Client,
		Bucket: b.bkt,
		Object: key,
	}

	// Start multipart upload
	if err := uploader.Start(); err != nil {
		return fmt.Errorf("starting multipart upload: %w", err)
	}

	return uploader.UploadFrom(ctx, r, size)
}
