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
	"path"
	"strings"
	"time"

	"github.com/kelindar/s3/fsutil"
)

// File implements fs.File
type File struct {
	// Reader is a reader that points to
	// the associated s3 object.
	Reader

	ctx  context.Context // from parent bucket
	body io.ReadCloser   // actual body; populated lazily
	pos  int64           // current read offset
}

// Name implements fs.FileInfo.Name
func (f *File) Name() string {
	return path.Base(f.Reader.Path)
}

// Path returns the full path to the
// S3 object within its bucket.
// See also blockfmt.NamedFile
func (f *File) Path() string {
	return f.Reader.Path
}

// Mode implements fs.FileInfo.Mode
func (f *File) Mode() fs.FileMode { return 0644 }

// Open implements fsutil.Opener
func (f *File) Open() (fs.File, error) { return f, nil }

// Read implements fs.File.Read
//
// Note: Read is not safe to call from
// multiple goroutines simultaneously.
// Use ReadAt for parallel reads.
//
// Also note: the first call to Read performs
// an HTTP request to S3 to read the entire
// contents of the object starting at the
// current read offset (zero by default, or
// another offset set via Seek).
// If you need to read a sub-range of the
// object, consider using f.Reader.RangeReader
func (f *File) Read(p []byte) (int, error) {
	if f.body != nil {
		n, err := f.body.Read(p)
		f.pos += int64(n)
		if n > 0 || errors.Is(err, io.EOF) {
			return n, err
		}
		// fall through here and re-try the request;
		// occasionally S3 will send us an RST for not
		// reading the response fast enough
		f.body.Close()
		f.body = nil
	}

	err := f.ctx.Err()
	if err != nil {
		return 0, err
	}

	f.body, err = f.Reader.RangeReader(f.pos, f.Size()-f.pos)
	if err != nil {
		return 0, err
	}

	n, err := f.body.Read(p)
	f.pos += int64(n)
	return n, err
}

// Info implements fs.DirEntry.Info
//
// Info returns exactly the same thing as f.Stat
func (f *File) Info() (fs.FileInfo, error) {
	return f.Stat()
}

// Type implements fs.DirEntry.Type
//
// Type returns exactly the same thing as f.Mode
func (f *File) Type() fs.FileMode { return f.Mode() }

// Close implements fs.File.Close
func (f *File) Close() error {
	if f.body == nil {
		return nil
	}
	err := f.body.Close()
	f.body = nil
	f.pos = 0
	return err
}

// Seek implements io.Seeker
//
// Seek rejects offsets that are beyond
// the size of the underlying object.
func (f *File) Seek(offset int64, whence int) (int64, error) {
	var newpos int64
	switch whence {
	case io.SeekStart:
		newpos = offset
	case io.SeekCurrent:
		newpos = f.pos + offset
	case io.SeekEnd:
		newpos = f.Reader.Size + offset
	default:
		panic("invalid seek whence")
	}
	if newpos < 0 || newpos > f.Reader.Size {
		return f.pos, fmt.Errorf("invalid seek offset %d", newpos)
	}
	// current data is invalid
	// if the position has changed
	if newpos != f.pos && f.body != nil {
		f.body.Close()
		f.body = nil
	}
	f.pos = newpos
	return f.pos, nil
}

func (f *File) Size() int64 {
	return f.Reader.Size
}

// IsDir implements fs.DirEntry.IsDir.
// IsDir always returns false.
func (f *File) IsDir() bool { return false }

// ModTime implements fs.DirEntry.ModTime.
// This returns the same value as f.Reader.LastModified.
func (f *File) ModTime() time.Time { return f.Reader.LastModified }

// Sys implements fs.FileInfo.Sys.
func (f *File) Sys() interface{} { return nil }

// Stat implements fs.File.Stat
func (f *File) Stat() (fs.FileInfo, error) {
	return f, nil
}

// split a glob pattern on the first meta-character
// so that we can list from the most specific prefix
func splitMeta(pattern string) (string, string) {
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '*', '?', '\\', '[':
			return pattern[:i], pattern[i:]
		default:
		}
	}
	return pattern, ""
}

// VisitDir implements fs.VisitDirFS
func (p *Prefix) VisitDir(name, seek, pattern string, walk fsutil.VisitDirFn) error {
	if !ValidBucket(p.Bucket) {
		return badBucket(p.Bucket)
	}
	subp := p
	if name != "." {
		subp = p.sub(name)
		if !strings.HasSuffix(subp.Path, "/") {
			subp.Path += "/"
		}
	}
	token := ""
	for {
		d, tok, err := subp.readDirAt(-1, token, seek, pattern)
		if err != nil && err != io.EOF {
			return &fs.PathError{Op: "visit", Path: subp.Path, Err: err}
		}

		// despite being called "start-after", the
		// S3 API includes the seek key in the list
		// response, which is not consistent with
		// fsutil.VisitDir, so filter it out here...
		if len(d) > 0 && d[0].Name() == seek {
			d = d[1:]
		}

		for i := range d {
			err := walk(d[i])
			if err != nil {
				return err
			}
		}
		if err == io.EOF {
			return nil
		}
		token = tok
	}
}
