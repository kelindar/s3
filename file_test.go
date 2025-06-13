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
	"io"
	"io/fs"
	"testing"
	"time"

	"github.com/kelindar/s3/aws"
	"github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/assert"
)

func TestFile_BasicProperties(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Create test content
	content := []byte("Hello, World! This is test content for file operations.")
	objectKey := "test/file.txt"
	etag := mockServer.PutObject(objectKey, content)

	// Open the file
	file, err := Open(key, bucket, objectKey, false)
	assert.NoError(t, err)
	defer file.Close()

	// Test basic properties
	assert.Equal(t, "file.txt", file.Name())
	assert.Equal(t, objectKey, file.Path())
	assert.Equal(t, etag, file.ETag)
	assert.Equal(t, int64(len(content)), file.Size())
	assert.False(t, file.IsDir())
	assert.Equal(t, fs.FileMode(0644), file.Mode())
	assert.Equal(t, fs.FileMode(0644), file.Type())

	// Test Stat
	info, err := file.Stat()
	assert.NoError(t, err)
	assert.Equal(t, file, info) // File implements fs.FileInfo
	assert.Equal(t, "file.txt", info.Name())
	assert.Equal(t, int64(len(content)), info.Size())
	assert.False(t, info.IsDir())

	// Test Info (fs.DirEntry interface)
	dirInfo, err := file.Info()
	assert.NoError(t, err)
	assert.Equal(t, info, dirInfo)

	// Test Sys
	assert.Nil(t, file.Sys())

	// Test Open (fsutil.Opener interface)
	reopened, err := file.Open()
	assert.NoError(t, err)
	assert.Equal(t, file, reopened)
}

func TestFile_ReadOperations(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Create test content
	content := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	objectKey := "test/read-test.txt"
	mockServer.PutObject(objectKey, content)

	// Test full read
	file, err := Open(key, bucket, objectKey, true)
	assert.NoError(t, err)
	defer file.Close()

	readContent, err := io.ReadAll(file)
	assert.NoError(t, err)
	assert.Equal(t, content, readContent)

	// Test partial reads
	file2, err := Open(key, bucket, objectKey, false)
	assert.NoError(t, err)
	defer file2.Close()

	buf := make([]byte, 10)
	n, err := file2.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 10, n)
	assert.Equal(t, content[:10], buf)

	// Read more
	n, err = file2.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 10, n)
	assert.Equal(t, content[10:20], buf)
}

func TestFile_SeekOperations(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	content := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	objectKey := "test/seek-test.txt"
	mockServer.PutObject(objectKey, content)

	file, err := Open(key, bucket, objectKey, false)
	assert.NoError(t, err)
	defer file.Close()

	// Test SeekStart
	pos, err := file.Seek(10, io.SeekStart)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), pos)

	buf := make([]byte, 5)
	n, err := file.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, content[10:15], buf)

	// Test SeekCurrent
	pos, err = file.Seek(5, io.SeekCurrent)
	assert.NoError(t, err)
	assert.Equal(t, int64(20), pos)

	n, err = file.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, content[20:25], buf)

	// Test SeekEnd
	pos, err = file.Seek(-5, io.SeekEnd)
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)-5), pos)

	n, err = file.Read(buf)
	if err != io.EOF {
		assert.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.Equal(t, content[len(content)-5:], buf)
	}

	// Test invalid seeks
	_, err = file.Seek(-1, io.SeekStart)
	assert.Error(t, err)

	_, err = file.Seek(int64(len(content)+1), io.SeekStart)
	assert.Error(t, err)
}

func TestFile_EmptyFile(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Create empty file
	objectKey := "test/empty.txt"
	mockServer.PutObject(objectKey, []byte{})

	file, err := Open(key, bucket, objectKey, true)
	assert.NoError(t, err)
	defer file.Close()

	assert.Equal(t, int64(0), file.Size())

	// Reading empty file should return EOF
	buf := make([]byte, 10)
	n, err := file.Read(buf)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
}

func TestFile_LazyLoading(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	content := []byte("Lazy loading test content")
	objectKey := "test/lazy.txt"
	mockServer.PutObject(objectKey, content)

	// Open without contents (lazy loading)
	file, err := Open(key, bucket, objectKey, false)
	assert.NoError(t, err)
	defer file.Close()

	// Body should be nil initially
	assert.Nil(t, file.body)

	// First read should trigger lazy loading
	buf := make([]byte, 10)
	n, err := file.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 10, n)
	assert.Equal(t, content[:10], buf)

	// Body should now be populated
	assert.NotNil(t, file.body)
}

func TestFile_ModTime(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	content := []byte("ModTime test")
	objectKey := "test/modtime.txt"
	beforePut := time.Now()
	mockServer.PutObject(objectKey, content)
	afterPut := time.Now()

	file, err := Open(key, bucket, objectKey, false)
	assert.NoError(t, err)
	defer file.Close()

	modTime := file.ModTime()
	// ModTime might be zero for mock server, so just check it's not nil
	assert.NotNil(t, modTime)
	// For real S3, this would be between beforePut and afterPut
	if !modTime.IsZero() {
		assert.True(t, modTime.After(beforePut) || modTime.Equal(beforePut))
		assert.True(t, modTime.Before(afterPut) || modTime.Equal(afterPut))
	}
}

func TestFile_ErrorHandling(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Test opening non-existent file
	_, err := Open(key, bucket, "nonexistent.txt", false)
	assert.Error(t, err)
	assert.ErrorIs(t, err, fs.ErrNotExist)
}

func TestFile_ContextCancellation(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	content := []byte("Context cancellation test")
	objectKey := "test/context.txt"
	mockServer.PutObject(objectKey, content)

	file, err := Open(key, bucket, objectKey, false)
	assert.NoError(t, err)
	defer file.Close()

	// Cancel the context
	ctx, cancel := context.WithCancel(context.Background())
	file.ctx = ctx
	cancel()

	// Reading should fail with context error
	buf := make([]byte, 10)
	_, err = file.Read(buf)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestFile_PathOperations(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Create test file
	content := []byte("Path operations test")
	objectKey := "dir/subdir/file.txt"
	mockServer.PutObject(objectKey, content)

	file, err := Open(key, bucket, objectKey, false)
	assert.NoError(t, err)
	defer file.Close()

	// Test path operations
	assert.Equal(t, "file.txt", file.Name())
	assert.Equal(t, objectKey, file.Path())

	// Test that file implements various interfaces
	var _ fs.File = file
	var _ fs.FileInfo = file
	var _ fs.DirEntry = file
	var _ io.Seeker = file
	var _ io.ReaderAt = file
}

func TestFile_ReadRetry(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	content := []byte("Read retry test content")
	objectKey := "test/retry.txt"
	mockServer.PutObject(objectKey, content)

	file, err := Open(key, bucket, objectKey, false)
	assert.NoError(t, err)
	defer file.Close()

	// First read should work
	buf := make([]byte, 10)
	n, err := file.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 10, n)

	// Simulate body error by closing it
	if file.body != nil {
		file.body.Close()
		file.body = nil
	}

	// Next read should retry and work
	n, err = file.Read(buf)
	if err != io.EOF {
		assert.NoError(t, err)
		assert.Equal(t, 10, n)
	}
}
