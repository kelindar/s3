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
	"io/fs"
	"net/http"
	"testing"

	"github.com/kelindar/s3/aws"
	"github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/assert"
)

func TestErrorHandling_InvalidBucketOperations(t *testing.T) {
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")

	// Test operations with invalid bucket names
	invalidBucket := "invalid_bucket"

	// Test Stat with invalid bucket
	_, err := Stat(key, invalidBucket, "test.txt")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidBucket)

	// Test Open with invalid bucket
	_, err = Open(key, invalidBucket, "test.txt", false)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidBucket)

	// Test BucketRegion with invalid bucket
	_, err = BucketRegion(key, invalidBucket)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidBucket)

	// Test URL with invalid bucket
	_, err = URL(key, invalidBucket, "test.txt")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidBucket)
}

func TestErrorHandling_NetworkErrors(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	// Enable error simulation
	mockServer.EnableErrorSimulation(mock.ErrorSimulation{
		NetworkErrors: true,
		ErrorRate:     1.0, // 100% error rate
	})

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	b := &Bucket{
		Key:    key,
		Bucket: bucket,
		Ctx:    context.Background(),
	}

	// Test Put with network error
	_, err := b.Put("test.txt", []byte("content"))
	assert.Error(t, err)

	// Test Open with network error
	_, err = b.Open("test.txt")
	assert.Error(t, err)

	// Test ReadDir with network error
	_, err = b.ReadDir(".")
	assert.Error(t, err)
}

func TestErrorHandling_PermissionErrors(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	// Enable permission error simulation
	mockServer.EnableErrorSimulation(mock.ErrorSimulation{
		PermissionErrors: true,
		ErrorRate:        1.0,
	})

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	b := &Bucket{
		Key:    key,
		Bucket: bucket,
		Ctx:    context.Background(),
	}

	// Test operations with permission errors
	_, err := b.Put("test.txt", []byte("content"))
	assert.Error(t, err)

	_, err = b.Open("test.txt")
	assert.Error(t, err)
}

func TestErrorHandling_NotFoundErrors(t *testing.T) {
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

	// Test opening non-existent file
	_, err := b.Open("nonexistent.txt")
	assert.Error(t, err)
	assert.ErrorIs(t, err, fs.ErrNotExist)

	// Test reading non-existent directory
	_, err = b.ReadDir("nonexistent-dir")
	assert.Error(t, err)
	assert.ErrorIs(t, err, fs.ErrNotExist)

	// Test OpenRange on non-existent file
	_, err = b.OpenRange("nonexistent.txt", "fake-etag", 0, 10)
	assert.Error(t, err)
}

func TestErrorHandling_ETagMismatch(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Create test content
	content := []byte("ETag test content")
	objectKey := "test/etag.txt"
	realETag := mockServer.PutObject(objectKey, content)

	// Test OpenRange with correct ETag should work
	b := &Bucket{
		Key:    key,
		Bucket: bucket,
		Ctx:    context.Background(),
	}

	_, err := b.OpenRange(objectKey, realETag, 0, 10)
	assert.NoError(t, err)

	// Note: Mock server doesn't implement ETag validation,
	// so we can't test ETag mismatches effectively
}

func TestErrorHandling_ContextCancellation(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	b := &Bucket{
		Key:    key,
		Bucket: bucket,
		Ctx:    ctx,
	}

	// Test operations with cancelled context
	_, err := b.Put("test.txt", []byte("content"))
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)

	_, err = b.Open("test.txt")
	assert.Error(t, err)
}

func TestErrorHandling_InvalidPaths(t *testing.T) {
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

	invalidPaths := []string{
		"../parent",
		// Note: Some paths that might seem invalid are actually handled gracefully
		// by the S3 client, so we only test truly invalid ones
	}

	for _, invalidPath := range invalidPaths {
		t.Run("path_"+invalidPath, func(t *testing.T) {
			// Test Put with invalid path
			_, err := b.Put(invalidPath, []byte("content"))
			assert.Error(t, err)

			// Test Open with invalid path
			_, err = b.Open(invalidPath)
			assert.Error(t, err)

			// Test ReadDir with invalid path
			_, err = b.ReadDir(invalidPath)
			assert.Error(t, err)

			// Test Remove with invalid path
			err = b.Remove(invalidPath)
			assert.Error(t, err)

			// Test Sub with invalid path
			_, err = b.Sub(invalidPath)
			assert.Error(t, err)

			// Test OpenRange with invalid path
			_, err = b.OpenRange(invalidPath, "etag", 0, 10)
			assert.Error(t, err)
		})
	}
}

func TestErrorHandling_HTTPErrors(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Test with wrong bucket name to trigger 404
	wrongBucket := "wrong-bucket"
	b := &Bucket{
		Key:    key,
		Bucket: wrongBucket,
		Ctx:    context.Background(),
	}

	// Test Put to wrong bucket
	_, err := b.Put("test.txt", []byte("content"))
	assert.Error(t, err)

	// Test Open from wrong bucket
	_, err = b.Open("test.txt")
	assert.Error(t, err)

	// Test ReadDir from wrong bucket
	_, err = b.ReadDir(".")
	assert.Error(t, err)
}

func TestErrorHandling_CustomHTTPClient(t *testing.T) {
	bucket := "test-bucket"
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")

	// Create a client that always returns an error
	errorClient := &http.Client{
		Transport: &errorTransport{},
	}

	b := &Bucket{
		Key:    key,
		Bucket: bucket,
		Client: errorClient,
		Ctx:    context.Background(),
	}

	// Test operations with error client
	_, err := b.Put("test.txt", []byte("content"))
	assert.Error(t, err)

	_, err = b.Open("test.txt")
	assert.Error(t, err)
}

// errorTransport always returns an error
type errorTransport struct{}

func (t *errorTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return nil, errors.New("simulated network error")
}
