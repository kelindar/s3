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
	"math/rand"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/kelindar/s3/aws"
	"github.com/kelindar/s3/fsutil"
	"github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/assert"
)

// If you have AWS credentials available and a test bucket set up, you can run
// this "integration test"
func TestAWS(t *testing.T) {
	bucket := os.Getenv("AWS_TEST_BUCKET")
	if testing.Short() || bucket == "" {
		t.Skip("skipping AWS-specific test")
	}

	r := rand.New(rand.NewSource(time.Now().Unix()))
	prefix := fmt.Sprintf("go-test-%d", r.Int())
	key, err := aws.AmbientKey("b2", DeriveForBucket(bucket))
	if err != nil {
		t.Skipf("skipping; couldn't derive key: %s", err)
	}

	testIntegration(t, bucket, prefix, key)
}

func TestBucket(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().Unix()))

	bucket := "test-bucket"
	prefix := fmt.Sprintf("go-test-%d", r.Int())

	// Create mock server
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	// Create S3 client pointing to mock server
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	testIntegration(t, bucket, prefix, key)
}

func testIntegration(t *testing.T, bucket, prefix string, key *aws.SigningKey) {
	b := &BucketFS{
		Key:      key,
		Bucket:   bucket,
		DelayGet: true,
		Ctx:      context.Background(),
	}

	tests := []struct {
		name string
		run  func(t *testing.T, b *BucketFS, prefix string)
	}{
		{
			"BasicCRUD",
			testBasicCrud,
		},
		{
			"WalkGlob",
			testWalkGlob,
		},
		{
			"WalkGlobRoot",
			testWalkGlobRoot,
		},
		{
			"ReadDir",
			testReadDir,
		},
	}
	for _, tr := range tests {
		t.Run(tr.name, func(t *testing.T) {
			tr.run(t, b, prefix)
		})
	}

	rm := func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if p == prefix && errors.Is(err, fs.ErrNotExist) {
				// everthing already cleaned up
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil // cannot rm
		}
		t.Logf("remove %s", p)
		return b.Remove(p)
	}
	// remove everything left under the prefix
	assert.NoError(t, fs.WalkDir(b, prefix, rm))
}

func testReadDir(t *testing.T, b *BucketFS, prefix string) {
	fullp := path.Join(prefix, "xyz-does-not-exist")
	items, err := fs.ReadDir(b, fullp)
	assert.Empty(t, items, "expected no items for non-existent directory")
	assert.ErrorIs(t, err, fs.ErrNotExist)
}

// write an object, read it back, and delete it
func testBasicCrud(t *testing.T, b *BucketFS, prefix string) {
	contents := []byte("here are some object contents")
	fullp := path.Join(prefix, "foo/bar/filename-with:chars= space")

	etag, err := b.Put(fullp, contents)
	assert.NoError(t, err)

	f, err := b.Open(fullp)
	assert.NoError(t, err)
	defer f.Close()

	s3f, ok := f.(*File)
	assert.True(t, ok, "Open should return *File, got %T", f)
	assert.Equal(t, etag, s3f.ETag)
	assert.Equal(t, fullp, s3f.Path())

	mem, err := io.ReadAll(f)
	assert.NoError(t, err, "reading contents")
	assert.Equal(t, contents, mem)

	assert.NoError(t, f.Close(), "Close")
	assert.NoError(t, b.Remove(s3f.Path()))

	// should get fs.ErrNotExist on another get;
	// this path exercises the list-for-get path:
	f, err = b.Open(fullp)
	assert.ErrorIs(t, err, fs.ErrNotExist, "open non-existent file should yield fs.ErrNotExist")
	if f != nil {
		f.Close()
	}
}

func testWalkGlob(t *testing.T, b *BucketFS, prefix string) {
	// dirs to create; create some placeholder
	// dirs with bad names to test that those are
	// ignored in listing
	dirs := []string{
		"a/b/c",
		"x/", // ignored
		"x/b/c",
		"x/y/",  // ignored
		"x/y/.", // ignored
		"x/y/a",
		"x/y/z",
		"y/",      // ignored
		"y/bc/..", // ignored
		"y/bc/a",
		"y/bc/b",
		"z/#.txt", // exercises sorting
		"z/#/b",
	}
	cases := []struct {
		seek, pattern string
		results       []string
	}{
		{"", "x/?/?", []string{"x/b/c", "x/y/a", "x/y/z"}},
		{"x/y", "?/?/?", []string{"x/y/a", "x/y/z", "z/#/b"}},
		{"x/y", "x/*y/*", []string{"x/y/a", "x/y/z"}},
		{"", "x/[by]/c", []string{"x/b/c"}},
		{"x/y/a", "?/?/?", []string{"x/y/z", "z/#/b"}},
		{"", "?/b*/?", []string{"a/b/c", "x/b/c", "y/bc/a", "y/bc/b"}},
		{"", "z/#*", []string{"z/#.txt"}},
	}
	for _, full := range dirs {
		// NOTE: don't use path.Join, it will remove
		// the trailing '/'
		if prefix[len(prefix)-1] != '/' {
			full = prefix + "/" + full
		} else {
			full = prefix + full
		}
		_, err := b.put(full, []byte(fmt.Sprintf("contents of %q", full)))
		assert.NoError(t, err)
	}

	dir, err := b.Open(prefix)
	assert.NoError(t, err)

	pre, ok := dir.(*Prefix)
	assert.True(t, ok, "BucketFS.Open(%s) should return *Prefix, got %T", prefix, dir)
	for i := range cases {
		seek := cases[i].seek
		pattern := cases[i].pattern
		want := cases[i].results
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			var got []string
			err := fsutil.WalkGlob(pre, seek, pattern, func(p string, f fs.File, err error) error {
				assert.NoError(t, err, "error walking %s", p)
				f.Close()
				p = strings.TrimPrefix(p, prefix+"/")
				got = append(got, p)
				return nil
			})
			assert.NoError(t, err, "fsutil.WalkGlob")
			assert.Equal(t, want, got)
		})
	}
}

func testWalkGlobRoot(t *testing.T, b *BucketFS, prefix string) {
	name := prefix + ".txt"
	_, err := b.put(name, nil)
	assert.NoError(t, err, "creating test file")
	t.Cleanup(func() {
		t.Log("remove", name)
		b.Remove(name)
	})

	root, err := b.Open(".")
	assert.NoError(t, err)

	pre, ok := root.(*Prefix)
	assert.True(t, ok, `BucketFS.Open(".") should return *Prefix, got %T`, root)

	// look for the prefix in the root
	found := false
	err = fsutil.WalkGlob(pre, "", "go-test-*.txt", func(p string, f fs.File, err error) error {
		t.Log("visiting:", p)
		assert.NoError(t, err, "error walking %s", p)
		f.Close()
		if p == name {
			found = true
		}
		return nil
	})

	assert.NoError(t, err, "fsutil.WalkGlob")
	assert.True(t, found, "could not find %q in the bucket", name)
}
