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
	"errors"
	"net/http"
	"testing"

	"github.com/kelindar/s3/aws"
	"github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/assert"
)

// Test an upload session using the mock S3 server
func TestUpload(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	up := Uploader{
		Key:    key,
		Client: &http.Client{},
		Bucket: bucket,
		Object: "test-object",
	}

	// Test Start
	err := up.Start()
	assert.NoError(t, err)
	assert.NotEmpty(t, up.ID(), "upload ID should be set")

	// Upload two parts in reverse order to test that the final
	// POST merges the parts in-order
	part := make([]byte, MinPartSize+1)
	for i := range part {
		part[i] = byte(i % 256)
	}

	// Upload part 2 first
	err = up.Upload(2, part)
	assert.NoError(t, err)
	assert.Equal(t, 1, up.CompletedParts())

	// Upload part 1 second
	err = up.Upload(1, part)
	assert.NoError(t, err)
	assert.Equal(t, 2, up.CompletedParts())

	// Complete the upload
	err = up.Close(nil)
	assert.NoError(t, err)
	assert.NotEmpty(t, up.ETag(), "final ETag should be set")

	// Abort shouldn't do anything after completion
	assert.NoError(t, up.Abort(), "abort after completion should not error")

	// Test a new upload for error case
	up2 := Uploader{
		Key:    key,
		Client: &http.Client{},
		Bucket: bucket,
		Object: "test-object-2",
	}

	err = up2.Start()
	assert.NoError(t, err)

	// Upload a part
	err = up2.Upload(1, part)
	assert.NoError(t, err)

	// Test Abort
	err = up2.Abort()
	assert.NoError(t, err, "abort should work")
}

func TestUploader_NextPart(t *testing.T) {
	up := &Uploader{}

	// Test NextPart increments correctly
	part1 := up.NextPart()
	assert.Equal(t, int64(1), part1)

	part2 := up.NextPart()
	assert.Equal(t, int64(2), part2)

	part3 := up.NextPart()
	assert.Equal(t, int64(3), part3)
}

func TestUploader_MinPartSize(t *testing.T) {
	up := &Uploader{}
	assert.Equal(t, MinPartSize, up.MinPartSize())
	assert.Equal(t, 5*1024*1024, up.MinPartSize())
}

func TestUploader_ErrorHandling(t *testing.T) {
	up := &Uploader{}

	// Test Upload before Start
	assert.Panics(t, func() {
		up.Upload(1, make([]byte, MinPartSize))
	})

	// Test Close before Start
	assert.Panics(t, func() {
		up.Close(nil)
	})

	// Test CopyFrom before Start
	reader := &Reader{}
	assert.Panics(t, func() {
		up.CopyFrom(1, reader, 0, 0)
	})
}

func TestUploader_PartSizeValidation(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	up := Uploader{
		Key:    key,
		Client: &http.Client{},
		Bucket: bucket,
		Object: "test-object",
	}

	err := up.Start()
	assert.NoError(t, err)

	// Test Upload with part too small
	smallPart := make([]byte, MinPartSize-1)
	err = up.Upload(1, smallPart)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "below min part size")
}

func TestUploader_CopyFromValidation(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	up := Uploader{
		Key:    key,
		Client: &http.Client{},
		Bucket: bucket,
		Object: "test-object",
	}

	err := up.Start()
	assert.NoError(t, err)

	reader := &Reader{
		Size: int64(MinPartSize * 2),
	}

	// Test CopyFrom with negative start
	err = up.CopyFrom(1, reader, -1, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "start and end values must be positive")

	// Test CopyFrom with negative end
	err = up.CopyFrom(1, reader, 0, -1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "start and end values must be positive")

	// Test CopyFrom with end greater than size
	err = up.CopyFrom(1, reader, 0, reader.Size+1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "end value")

	// Test CopyFrom with size too small
	err = up.CopyFrom(1, reader, 0, MinPartSize-1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "below min part size")
}

func TestUploader_StateChecks(t *testing.T) {
	up := &Uploader{}

	// Test initial state
	assert.False(t, up.Closed())
	assert.Equal(t, 0, up.CompletedParts())
	assert.Equal(t, "", up.ID())
	assert.Equal(t, "", up.ETag())
	assert.Equal(t, int64(0), up.Size())
}

func TestUploader_CopyFrom_Comprehensive(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Create source content in the mock server
	sourceContent := make([]byte, MinPartSize*2)
	for i := range sourceContent {
		sourceContent[i] = byte(i % 256)
	}
	sourceETag := mockServer.PutObject("source-object", sourceContent)

	up := Uploader{
		Key:    key,
		Client: &http.Client{},
		Bucket: bucket,
		Object: "test-object",
	}

	err := up.Start()
	assert.NoError(t, err)

	reader := &Reader{
		Size:   int64(len(sourceContent)),
		ETag:   sourceETag,
		Bucket: bucket,
		Path:   "source-object",
	}

	// Test successful copy
	err = up.CopyFrom(1, reader, 0, MinPartSize)
	assert.NoError(t, err)

	// Wait for background copy to complete
	up.bg.Wait()

	// Verify part was added
	assert.Equal(t, 1, up.CompletedParts())
}

func TestUploader_Start_ErrorCases(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Test with error simulation
	mockServer.EnableErrorSimulation(mock.ErrorSimulation{
		InternalErrors: true,
		ErrorRate:      1.0, // 100% error rate
	})

	up := Uploader{
		Key:    key,
		Client: &http.Client{},
		Bucket: bucket,
		Object: "test-object",
	}

	err := up.Start()
	assert.Error(t, err, "should get error with error simulation enabled")

	// Disable error simulation for other tests
	mockServer.DisableErrorSimulation()

	// Test with wrong bucket (this should work with mock server,
	// but we can test validation in the uploader)
	up2 := Uploader{
		Key:    key,
		Client: &http.Client{},
		Bucket: "wrong-bucket", // Different bucket than mock server
		Object: "test-object",
	}

	err = up2.Start()
	assert.Error(t, err, "should get error with wrong bucket")
}

func TestUploader_Close_ErrorCases(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	up := Uploader{
		Key:    key,
		Client: &http.Client{},
		Bucket: bucket,
		Object: "test-object",
	}

	err := up.Start()
	assert.NoError(t, err)

	// Upload a part first
	part := make([]byte, MinPartSize+1)
	err = up.Upload(1, part)
	assert.NoError(t, err)

	// Enable error simulation for close operation
	mockServer.EnableErrorSimulation(mock.ErrorSimulation{
		InternalErrors: true,
		ErrorRate:      1.0, // 100% error rate
	})

	err = up.Close(nil)
	assert.Error(t, err, "should get error with error simulation enabled")
}

func TestUploader_Size_WithParts(t *testing.T) {
	up := &Uploader{}

	// Add some parts
	up.parts = []tagpart{
		{Num: 1, ETag: "etag1", size: 1000},
		{Num: 2, ETag: "etag2", size: 2000},
		{Num: 3, ETag: "etag3", size: 1500},
	}
	up.finished = true // Size() only returns non-zero when finished

	expectedSize := int64(1000 + 2000 + 1500)
	assert.Equal(t, expectedSize, up.Size())
}

func TestUploader_UploadReaderAt(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	up := Uploader{
		Key:    key,
		Client: &http.Client{},
		Bucket: bucket,
		Object: "test-object",
	}

	err := up.Start()
	assert.NoError(t, err)

	// Test UploadReaderAt with invalid reader
	invalidReader := &errorReaderAt{}
	content := make([]byte, MinPartSize*2)
	err = up.UploadReaderAt(invalidReader, int64(len(content)))
	assert.Error(t, err)
}

// errorReaderAt always returns an error
type errorReaderAt struct{}

func (r *errorReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, errors.New("simulated read error")
}
