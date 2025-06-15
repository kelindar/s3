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
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"testing"
	"time"

	"github.com/kelindar/s3/aws"
	"github.com/kelindar/s3/fsutil"
	"github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/assert"
)

func TestPrefix_BasicProperties(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Create test structure
	mockServer.PutObject("dir1/file1.txt", []byte("content1"))
	mockServer.PutObject("dir1/file2.txt", []byte("content2"))
	mockServer.PutObject("dir1/subdir/file3.txt", []byte("content3"))

	b := NewBucket(key, bucket)

	// Open directory
	dir, err := b.Open("dir1")
	assert.NoError(t, err)
	defer dir.Close()

	prefix, ok := dir.(*Prefix)
	assert.True(t, ok)

	// Test basic properties
	assert.Equal(t, "dir1", prefix.Name())
	assert.True(t, prefix.IsDir())
	assert.Equal(t, fs.ModeDir|0755, prefix.Mode())
	assert.Equal(t, fs.ModeDir, prefix.Type())
	assert.Equal(t, int64(0), prefix.Size())
	assert.Equal(t, time.Time{}, prefix.ModTime()) // Prefixes don't have mod times
	assert.Nil(t, prefix.Sys())

	// Test Stat
	info, err := prefix.Stat()
	assert.NoError(t, err)
	assert.Equal(t, prefix, info)

	// Test Info (fs.DirEntry interface)
	dirInfo, err := prefix.Info()
	assert.NoError(t, err)
	assert.Equal(t, info, dirInfo)
}

func TestPrefix_ReadDir(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Create test structure
	mockServer.PutObject("testdir/file1.txt", []byte("content1"))
	mockServer.PutObject("testdir/file2.txt", []byte("content2"))
	mockServer.PutObject("testdir/subdir/file3.txt", []byte("content3"))
	mockServer.PutObject("testdir/another/file4.txt", []byte("content4"))

	b := NewBucket(key, bucket)

	// Open directory
	dir, err := b.Open("testdir")
	assert.NoError(t, err)
	defer dir.Close()

	prefix, ok := dir.(*Prefix)
	assert.True(t, ok)

	// Test ReadDir(-1) - read all entries
	entries, err := prefix.ReadDir(-1)
	assert.NoError(t, err)
	assert.Len(t, entries, 4) // 2 files + 2 subdirs

	// Verify entries are sorted
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	assert.Equal(t, []string{"another", "file1.txt", "file2.txt", "subdir"}, names)

	// Test ReadDir(2) - read limited entries
	prefix.token = ""
	prefix.dirEOF = false
	entries, err = prefix.ReadDir(2)
	assert.NoError(t, err)
	assert.Len(t, entries, 2)
	assert.Equal(t, "another", entries[0].Name())
	assert.Equal(t, "file1.txt", entries[1].Name())

	// Test ReadDir after EOF
	prefix.dirEOF = true
	entries, err = prefix.ReadDir(-1)
	assert.Empty(t, entries)
	assert.ErrorIs(t, err, io.EOF)
}

func TestPrefix_Open(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Create test structure
	mockServer.PutObject("parent/child/file.txt", []byte("content"))
	mockServer.PutObject("parent/file2.txt", []byte("content2"))

	b := NewBucket(key, bucket)

	// Open parent directory
	parent, err := b.Open("parent")
	assert.NoError(t, err)
	defer parent.Close()

	prefix, ok := parent.(*Prefix)
	assert.True(t, ok)

	// Test opening current directory
	current, err := prefix.Open(".")
	assert.NoError(t, err)
	assert.Equal(t, prefix, current)

	// Test opening subdirectory
	child, err := prefix.Open("child")
	assert.NoError(t, err)
	defer child.Close()

	childPrefix, ok := child.(*Prefix)
	assert.True(t, ok)
	assert.Equal(t, "child", childPrefix.Name())

	// Test opening non-existent directory
	_, err = prefix.Open("nonexistent")
	assert.Error(t, err)
	assert.ErrorIs(t, err, fs.ErrNotExist)

	// Test opening with invalid path
	_, err = prefix.Open("../invalid")
	assert.Error(t, err)
}

func TestPrefix_Read(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	mockServer.PutObject("dir/file.txt", []byte("content"))

	b := NewBucket(key, bucket)

	dir, err := b.Open("dir")
	assert.NoError(t, err)
	defer dir.Close()

	prefix, ok := dir.(*Prefix)
	assert.True(t, ok)

	// Reading from a directory should always return an error
	buf := make([]byte, 10)
	n, err := prefix.Read(buf)
	assert.Equal(t, 0, n)
	assert.Error(t, err)
	assert.ErrorIs(t, err, fs.ErrInvalid)
}

func TestPrefix_Close(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	mockServer.PutObject("dir/file.txt", []byte("content"))

	b := NewBucket(key, bucket)

	dir, err := b.Open("dir")
	assert.NoError(t, err)

	prefix, ok := dir.(*Prefix)
	assert.True(t, ok)

	// Close should always succeed for directories
	err = prefix.Close()
	assert.NoError(t, err)
}

func TestPrefix_VisitDir(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Create test structure
	mockServer.PutObject("visit/a.txt", []byte("a"))
	mockServer.PutObject("visit/b.txt", []byte("b"))
	mockServer.PutObject("visit/c.txt", []byte("c"))
	mockServer.PutObject("visit/sub/d.txt", []byte("d"))

	prefix := &Prefix{
		Key:    key,
		Bucket: bucket,
		Path:   "visit/",
	}

	// Test VisitDir with pattern
	var visited []string
	walkFn := func(entry fsutil.DirEntry) error {
		visited = append(visited, entry.Name())
		return nil
	}

	err := prefix.VisitDir(".", "", "*.txt", walkFn)
	assert.NoError(t, err)
	assert.Contains(t, visited, "a.txt")
	assert.Contains(t, visited, "b.txt")
	assert.Contains(t, visited, "c.txt")

	// Test VisitDir with seek
	visited = nil
	err = prefix.VisitDir(".", "b.txt", "*.txt", walkFn)
	assert.NoError(t, err)
	// Should start after b.txt
	assert.Contains(t, visited, "c.txt")
	assert.NotContains(t, visited, "a.txt")
	assert.NotContains(t, visited, "b.txt")
}

func TestPrefix_RootDirectory(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Create files in root
	mockServer.PutObject("root1.txt", []byte("content1"))
	mockServer.PutObject("root2.txt", []byte("content2"))

	b := NewBucket(key, bucket)

	// Open root directory
	root, err := b.Open(".")
	assert.NoError(t, err)
	defer root.Close()

	prefix, ok := root.(*Prefix)
	assert.True(t, ok)
	assert.Equal(t, ".", prefix.Name())

	// Test reading root directory
	entries, err := prefix.ReadDir(-1)
	assert.NoError(t, err)
	assert.Len(t, entries, 2)
}

func TestPrefix_EmptyDirectory(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	b := NewBucket(key, bucket)

	// Try to open non-existent directory
	_, err := b.Open("nonexistent")
	assert.Error(t, err)
	assert.ErrorIs(t, err, fs.ErrNotExist)
}

func TestPrefix_SubPrefixCreation(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	prefix := &Prefix{
		Key:    key,
		Bucket: bucket,
		Path:   "parent/",
	}

	// Test sub method
	sub := prefix.sub("child")
	assert.Equal(t, "parent/child", sub.Path)
	assert.Equal(t, key, sub.Key)
	assert.Equal(t, bucket, sub.Bucket)

	// Test join method
	joined := prefix.join("extra")
	assert.Equal(t, "parent/extra", joined)

	// Test join with root prefix
	rootPrefix := &Prefix{Path: "."}
	rootJoined := rootPrefix.join("test")
	assert.Equal(t, "test", rootJoined)
}

func TestPrefix_OpenDir_ErrorCases(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Test openDir with invalid bucket
	invalidPrefix := &Prefix{
		Key:    key,
		Bucket: "invalid_bucket",
		Path:   "test/",
	}

	_, err := invalidPrefix.openDir()
	assert.Error(t, err)
}

func TestPrefix_List_ErrorCases(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Test list with invalid bucket
	invalidPrefix := &Prefix{
		Key:    key,
		Bucket: "invalid_bucket",
		Path:   "test/",
	}

	_, err := invalidPrefix.list(100, "", "", "")
	assert.Error(t, err)
}

func TestPrefix_ReadDirAt_Comprehensive(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Create test structure with many files
	for i := 0; i < 10; i++ {
		mockServer.PutObject(fmt.Sprintf("test/file%02d.txt", i), []byte(fmt.Sprintf("content%d", i)))
	}

	prefix := &Prefix{
		Key:    key,
		Bucket: bucket,
		Path:   "test/",
	}

	// Test readDirAt with different parameters
	entries, token, err := prefix.readDirAt(5, "", "", "")
	assert.NoError(t, err)
	assert.Len(t, entries, 5)
	_ = token // Ignore token for this test

	// Test readDirAt with seek
	entries, _, err = prefix.readDirAt(3, "", "file05.txt", "")
	assert.NoError(t, err)
	// Should return files after file05.txt
	if len(entries) > 0 {
		assert.True(t, entries[0].Name() > "file05.txt")
	}

	// Test readDirAt with pattern
	entries, _, err = prefix.readDirAt(10, "", "", "file0[0-2].txt")
	if err != io.EOF {
		assert.NoError(t, err)
		// Should only return files matching pattern
		for _, entry := range entries {
			matched, _ := patmatch("file0[0-2].txt", entry.Name())
			assert.True(t, matched)
		}
	}
}

func TestPrefix_Client_Method(t *testing.T) {
	bucket := "test-bucket"
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")

	// Test with nil client
	prefix := &Prefix{
		Key:    key,
		Bucket: bucket,
		Path:   "test/",
		Client: nil,
	}

	client := prefix.client()
	assert.Equal(t, &DefaultClient, client)

	// Test with custom client
	customClient := &http.Client{}
	prefix.Client = customClient
	client = prefix.client()
	assert.Equal(t, customClient, client)
}
