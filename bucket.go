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
	Key    *aws.SigningKey
	Bucket string
	Client *http.Client
	Ctx    context.Context

	// DelayGet, if true, causes the
	// Open call to use a HEAD operation
	// rather than a GET operation.
	// The first call to fs.File.Read will
	// cause the full GET to be performed.
	DelayGet bool
}

// NewBucket creates a new Bucket instance.
func NewBucket(ctx context.Context, key *aws.SigningKey, bucket string) *Bucket {
	return &Bucket{
		Key:    key,
		Bucket: bucket,
		Ctx:    ctx,
	}
}

func (b *Bucket) sub(name string) *Prefix {
	return b.subctx(b.Ctx, name)
}

func (b *Bucket) subctx(ctx context.Context, name string) *Prefix {
	return &Prefix{
		Key:    b.Key,
		Client: b.Client,
		Bucket: b.Bucket,
		Path:   name,
		Ctx:    ctx,
	}
}

// WithContext implements db.ContextFS
func (b *Bucket) WithContext(ctx context.Context) fs.FS {
	return b.subctx(ctx, ".")
}

func badpath(op, name string) error {
	return &fs.PathError{
		Op:   op,
		Path: name,
		Err:  fs.ErrInvalid,
	}
}

// Put performs a PutObject operation at the object key 'where'
// and returns the ETag of the newly-created object.
func (b *Bucket) Put(where string, contents []byte) (string, error) {
	where = path.Clean(where)
	if !fs.ValidPath(where) {
		return "", badpath("s3 PUT", where)
	}
	_, base := path.Split(where)
	if base == "." {
		// don't allow a path that is
		// nominally a directory
		return "", badpath("s3 PUT", where)
	}
	return b.put(where, contents)
}

func (b *Bucket) put(where string, contents []byte) (string, error) {
	req, err := http.NewRequestWithContext(b.Ctx, http.MethodPut, uri(b.Key, b.Bucket, where), nil)
	if err != nil {
		return "", err
	}
	b.Key.SignV4(req, contents)
	client := b.Client
	if client == nil {
		client = &DefaultClient
	}
	res, err := flakyDo(client, req)
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
		Key:    b.Key,
		Bucket: b.Bucket,
		Path:   name,
		ETag:   etag,
	}
	return r.RangeReader(start, width)
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
		f, err := Open(b.Key, b.Bucket, name, !b.DelayGet)
		if err == nil || !errors.Is(err, fs.ErrNotExist) {
			return f, err
		}
	}

	return b.sub(name).openDir()
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

// Remove removes the object at fullpath.
func (b *Bucket) Remove(fullpath string) error {
	fullpath = path.Clean(fullpath)
	if !fs.ValidPath(fullpath) {
		return fmt.Errorf("%s: %s", fullpath, fs.ErrInvalid)
	}
	req, err := http.NewRequestWithContext(b.Ctx, http.MethodDelete, uri(b.Key, b.Bucket, fullpath), nil)
	if err != nil {
		return err
	}
	b.Key.SignV4(req, nil)
	client := b.Client
	if client == nil {
		client = &DefaultClient
	}
	res, err := flakyDo(client, req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 204 {
		return fmt.Errorf("s3 DELETE: %s %s", res.Status, extractMessage(res.Body))
	}
	return nil
}
