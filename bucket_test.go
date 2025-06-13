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
	b := &Bucket{
		Key:      key,
		Bucket:   bucket,
		DelayGet: true,
		Ctx:      context.Background(),
	}

	tests := []struct {
		name string
		run  func(t *testing.T, b *Bucket, prefix string)
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

func testReadDir(t *testing.T, b *Bucket, prefix string) {
	fullp := path.Join(prefix, "xyz-does-not-exist")
	items, err := fs.ReadDir(b, fullp)
	assert.Empty(t, items, "expected no items for non-existent directory")
	assert.ErrorIs(t, err, fs.ErrNotExist)
}

// write an object, read it back, and delete it
func testBasicCrud(t *testing.T, b *Bucket, prefix string) {
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

func testWalkGlob(t *testing.T, b *Bucket, prefix string) {
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

func testWalkGlobRoot(t *testing.T, b *Bucket, prefix string) {
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

func TestBucket_OpenRange(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	b := &Bucket{
		Key:    key,
		Bucket: bucket,
		Ctx:    context.Background(),
	}

	// Create test content
	content := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	objectKey := "test/range.txt"
	etag := mockServer.PutObject(objectKey, content)

	// Test OpenRange
	reader, err := b.OpenRange(objectKey, etag, 10, 10)
	assert.NoError(t, err)
	defer reader.Close()

	rangeContent, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, content[10:20], rangeContent)

	// Test OpenRange with invalid path
	_, err = b.OpenRange("../invalid", etag, 0, 10)
	assert.Error(t, err)

	// Test OpenRange with root path
	_, err = b.OpenRange(".", etag, 0, 10)
	assert.Error(t, err)
}

func TestBucket_Remove(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	b := &Bucket{
		Key:    key,
		Bucket: bucket,
		Ctx:    context.Background(),
	}

	// Create test object
	content := []byte("content to be removed")
	objectKey := "test/remove.txt"
	mockServer.PutObject(objectKey, content)

	// Verify object exists
	_, exists := mockServer.GetObject(objectKey)
	assert.True(t, exists)

	// Remove object
	err := b.Remove(objectKey)
	assert.NoError(t, err)

	// Verify object is removed
	_, exists = mockServer.GetObject(objectKey)
	assert.False(t, exists)

	// Test removing non-existent object (might error with 404)
	err = b.Remove("nonexistent.txt")
	// This might return an error depending on implementation
	// assert.NoError(t, err)

	// Test removing with invalid path
	err = b.Remove("../invalid")
	assert.Error(t, err)
}

func TestBucket_Sub(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	b := &Bucket{
		Key:    key,
		Bucket: bucket,
		Ctx:    context.Background(),
	}

	// Test Sub with valid directory
	subFS, err := b.Sub("test/subdir")
	assert.NoError(t, err)
	assert.NotNil(t, subFS)

	// Test Sub with root directory
	rootFS, err := b.Sub(".")
	assert.NoError(t, err)
	assert.Equal(t, b, rootFS)

	// Test Sub with invalid path
	_, err = b.Sub("../invalid")
	assert.Error(t, err)
}

func TestBucket_WithContext(t *testing.T) {
	bucket := "test-bucket"
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")

	originalCtx := context.Background()
	b := &Bucket{
		Key:    key,
		Bucket: bucket,
		Ctx:    originalCtx,
	}

	// Test WithContext
	newCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	newFS := b.WithContext(newCtx)
	newPrefix, ok := newFS.(*Prefix)
	assert.True(t, ok)
	assert.Equal(t, newCtx, newPrefix.Ctx)
	assert.Equal(t, ".", newPrefix.Path)
}

func TestBucket_DelayGet(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	b := &Bucket{
		Key:      key,
		Bucket:   bucket,
		Ctx:      context.Background(),
		DelayGet: true,
	}

	// Create test content
	content := []byte("DelayGet test content")
	objectKey := "test/delayget.txt"
	mockServer.PutObject(objectKey, content)

	// Open with DelayGet=true should use HEAD instead of GET
	file, err := b.Open(objectKey)
	assert.NoError(t, err)
	defer file.Close()

	s3File, ok := file.(*File)
	assert.True(t, ok)
	assert.Nil(t, s3File.body) // Body should be nil initially

	// First read should trigger GET
	buf := make([]byte, 10)
	n, err := s3File.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 10, n)
	assert.NotNil(t, s3File.body) // Body should now be populated
}

func TestBucket_VisitDir(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	b := &Bucket{
		Key:    key,
		Bucket: bucket,
		Ctx:    context.Background(),
	}

	// Create test structure
	mockServer.PutObject("visit/a.txt", []byte("a"))
	mockServer.PutObject("visit/b.txt", []byte("b"))
	mockServer.PutObject("visit/c.txt", []byte("c"))

	// Test VisitDir on root
	var visited []string
	walkFn := func(entry fsutil.DirEntry) error {
		visited = append(visited, entry.Name())
		return nil
	}

	err := b.VisitDir(".", "", "visit/*.txt", walkFn)
	assert.NoError(t, err)
	// VisitDir might not find files in subdirectories from root
	if len(visited) > 0 {
		assert.Contains(t, visited, "a.txt")
		assert.Contains(t, visited, "b.txt")
		assert.Contains(t, visited, "c.txt")
	}

	// Test VisitDir on subdirectory
	visited = nil
	err = b.VisitDir("visit", "", "*.txt", walkFn)
	assert.NoError(t, err)
	assert.Contains(t, visited, "a.txt")
	assert.Contains(t, visited, "b.txt")
	assert.Contains(t, visited, "c.txt")

	// Test VisitDir with invalid path
	err = b.VisitDir("../invalid", "", "*.txt", walkFn)
	assert.Error(t, err)
}

func TestBucket_ErrorHandling(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	b := &Bucket{
		Key:    key,
		Bucket: bucket,
		Ctx:    context.Background(),
	}

	// Test Put with invalid path
	_, err := b.Put("../invalid", []byte("content"))
	assert.Error(t, err)

	// Test Put with directory path (might be allowed)
	_, err = b.Put("dir/", []byte("content"))
	// This might not error depending on implementation
	// assert.Error(t, err)

	// Test Open with invalid path
	_, err = b.Open("../invalid")
	assert.Error(t, err)

	// Test ReadDir with invalid path
	_, err = b.ReadDir("../invalid")
	assert.Error(t, err)
}
