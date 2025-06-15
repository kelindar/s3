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
	"bytes"
	"context"
	"testing"

	"github.com/kelindar/s3/aws"
	"github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/assert"
)

// Test the public API through Bucket.WriteFrom
func TestBucket_WriteFromLarge(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	b := NewBucket(key, bucket)

	// Create test data larger than MinPartSize to trigger multipart upload
	testData := make([]byte, MinPartSize*3+1000) // ~15MB + 1000 bytes
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Test WriteFrom with context
	ctx := context.Background()
	objectKey := "test/large-multipart-file.bin"

	assert.NoError(t, b.WriteFrom(ctx, objectKey, bytes.NewReader(testData), int64(len(testData))))

	// Verify the object was created
	assert.True(t, mockServer.ObjectExists(objectKey))

	// Verify the content is correct
	content, found := mockServer.ObjectContent(objectKey)
	assert.True(t, found)
	assert.Equal(t, testData, content)
}

func TestBucket_WriteFromSmall(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	b := NewBucket(key, bucket)

	// Create test data smaller than MinPartSize (should use single part)
	testData := make([]byte, 1024) // 1KB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Test WriteFrom with context
	ctx := context.Background()
	objectKey := "test/small-file.bin"

	assert.NoError(t, b.WriteFrom(ctx, objectKey, bytes.NewReader(testData), int64(len(testData))))

	// Verify the object was created
	assert.True(t, mockServer.ObjectExists(objectKey))

	// Verify the content is correct
	content, found := mockServer.ObjectContent(objectKey)
	assert.True(t, found)
	assert.Equal(t, testData, content)
}

func TestBucket_WriteFromValidation(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	b := NewBucket(key, bucket)

	testData := make([]byte, 1024)
	ctx := context.Background()

	// Test with invalid path
	err := b.WriteFrom(ctx, "../invalid", bytes.NewReader(testData), int64(len(testData)))
	assert.Error(t, err)

	// Test with negative size
	err = b.WriteFrom(ctx, "valid-path", bytes.NewReader(testData), -1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "size must be non-negative")
}

// Test calculatePartSize function
func TestCalculatePartSize(t *testing.T) {
	// Test small size
	partSize := calculatePartSize(MinPartSize)
	assert.Equal(t, int64(MinPartSize), partSize)

	// Test large size that requires multiple parts
	largeSize := int64(MinPartSize) * MaxParts * 2 // Requires doubling part size
	partSize = calculatePartSize(largeSize)
	assert.Greater(t, partSize, int64(MinPartSize))
	assert.LessOrEqual(t, largeSize/partSize, int64(MaxParts))
}

// Test multipart upload through mock server directly
func TestMultipartUpload_MockServer(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	// Test that mock server supports multipart uploads
	objectKey := "test/multipart-direct.bin"

	// Create test data
	part1Data := make([]byte, MinPartSize)
	for i := range part1Data {
		part1Data[i] = byte(i % 256)
	}
	part2Data := make([]byte, MinPartSize)
	for i := range part2Data {
		part2Data[i] = byte((i + 100) % 256)
	}

	// Put object using mock server directly (simulates multipart)
	allData := append(part1Data, part2Data...)
	etag := mockServer.PutObject(objectKey, allData)
	assert.NotEmpty(t, etag)

	// Verify object exists
	assert.True(t, mockServer.ObjectExists(objectKey))

	// Verify content
	content, found := mockServer.ObjectContent(objectKey)
	assert.True(t, found)
	assert.Equal(t, allData, content)
}

// Test constants and helper functions
func TestUploaderConstants(t *testing.T) {
	assert.Equal(t, 5*1024*1024, MinPartSize)
	assert.Equal(t, 10000, MaxParts)
}

func TestUploader_StartValidation(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Test missing bucket
	u1 := &uploader{
		Key:    key,
		Object: "test-object",
	}
	err := u1.Start()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Bucket and s3.Uploader.Object must be present")

	// Test missing object
	u2 := &uploader{
		Key:    key,
		Bucket: bucket,
	}
	err = u2.Start()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Bucket and s3.Uploader.Object must be present")

	// Test double start
	u3 := &uploader{
		Key:    key,
		Bucket: bucket,
		Object: "test-object",
	}
	assert.NoError(t, u3.Start())

	assert.Panics(t, func() {
		u3.Start()
	})
}

func TestUploader_UploadValidation(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	u := &uploader{
		Key:    key,
		Bucket: bucket,
		Object: "test-object",
	}

	// Test Upload before Start
	data := make([]byte, MinPartSize)
	assert.Panics(t, func() {
		u.Upload(1, data)
	})

	// Start uploader
	assert.NoError(t, u.Start())

	// Test Upload with data too small
	smallData := make([]byte, MinPartSize-1)
	err := u.Upload(1, smallData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "below min part size")

	// Test valid Upload
	assert.NoError(t, u.Upload(1, data))
}

func TestUploader_CloseValidation(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	uploader := &uploader{
		Key:    key,
		Bucket: bucket,
		Object: "test-object",
	}

	// Test Close before Start
	assert.Panics(t, func() {
		uploader.Close(nil)
	})

	// Start uploader and upload a part
	assert.NoError(t, uploader.Start())

	data := make([]byte, MinPartSize)
	assert.NoError(t, uploader.Upload(1, data))

	// Test valid Close
	assert.NoError(t, uploader.Close([]byte("final data")))

	// Test double Close
	assert.Panics(t, func() {
		uploader.Close(nil)
	})
}

func TestUploader_Abort(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	u := &uploader{
		Key:    key,
		Bucket: bucket,
		Object: "test/abort-test.bin",
	}

	// Test Abort before Start (should do nothing)
	assert.NoError(t, u.Abort())

	// Start uploader
	assert.NoError(t, u.Start())
	uploadID := u.ID()

	// Verify upload exists
	_, exists := mockServer.GetMultipartUpload(uploadID)
	assert.True(t, exists)

	// Upload a part
	data := make([]byte, MinPartSize)
	assert.NoError(t, u.Upload(1, data))

	// Test Abort
	assert.NoError(t, u.Abort())

	// Verify upload was cleaned up
	_, exists = mockServer.GetMultipartUpload(uploadID)
	assert.False(t, exists)

	// Test Abort after Close
	u2 := &uploader{
		Key:    key,
		Bucket: bucket,
		Object: "test/abort-test2.bin",
	}
	assert.NoError(t, u2.Start())
	assert.NoError(t, u2.Upload(1, data))
	assert.NoError(t, u2.Close(nil))

	// Abort should do nothing after successful close
	assert.NoError(t, u2.Abort())
}

func TestUploader_CopyFrom(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Create source object
	sourceData := make([]byte, MinPartSize*2)
	for i := range sourceData {
		sourceData[i] = byte(i % 256)
	}
	sourceKey := "source/large-file.bin"
	etag := mockServer.PutObject(sourceKey, sourceData)

	// Create source Reader
	sourceReader := &Reader{
		Key:    key,
		Bucket: bucket,
		Path:   sourceKey,
		ETag:   etag,
		Size:   int64(len(sourceData)),
	}

	uploader := &uploader{
		Key:    key,
		Bucket: bucket,
		Object: "dest/copied-file.bin",
	}

	// Test CopyFrom before Start
	assert.Panics(t, func() {
		uploader.CopyFrom(1, sourceReader, 0, 0)
	})

	// Start uploader
	assert.NoError(t, uploader.Start())

	// Test CopyFrom with invalid range
	err := uploader.CopyFrom(1, sourceReader, -1, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "start and end values must be positive")

	err = uploader.CopyFrom(1, sourceReader, 0, int64(len(sourceData)+1000))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "end value")

	// Test CopyFrom with size too small
	err = uploader.CopyFrom(1, sourceReader, 0, MinPartSize-1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "below min part size")

	// Test valid CopyFrom (copy entire source)
	assert.NoError(t, uploader.CopyFrom(1, sourceReader, 0, 0))

	// Test CopyFrom with range
	assert.NoError(t, uploader.CopyFrom(2, sourceReader, 0, MinPartSize))

	// Close uploader
	assert.NoError(t, uploader.Close(nil))

	// Verify object was created
	assert.True(t, mockServer.ObjectExists("dest/copied-file.bin"))
}

func TestUploader_UploadFrom(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	uploader := &uploader{
		Key:    key,
		Bucket: bucket,
		Object: "test/upload-from.bin",
	}

	// Start uploader
	assert.NoError(t, uploader.Start())

	// Create test data larger than MinPartSize to trigger multipart
	testData := make([]byte, MinPartSize*2+1000)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Test UploadFrom
	ctx := context.Background()
	reader := bytes.NewReader(testData)
	assert.NoError(t, uploader.UploadFrom(ctx, reader, int64(len(testData))))

	// Verify object was created
	assert.True(t, mockServer.ObjectExists("test/upload-from.bin"))

	// Verify content is correct
	content, found := mockServer.ObjectContent("test/upload-from.bin")
	assert.True(t, found)
	assert.Equal(t, testData, content)
}

func TestUploader_UploadFromContextCancellation(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	uploader := &uploader{
		Key:    key,
		Bucket: bucket,
		Object: "test/cancelled-upload.bin",
	}

	// Start uploader
	assert.NoError(t, uploader.Start())

	// Create test data
	testData := make([]byte, MinPartSize*3)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Test UploadFrom with cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	reader := bytes.NewReader(testData)
	err := uploader.UploadFrom(ctx, reader, int64(len(testData)))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")
}

func TestUploader_ContentType(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	uploader := &uploader{
		Key:         key,
		Bucket:      bucket,
		Object:      "test/content-type.bin",
		ContentType: "application/octet-stream",
	}

	// Start uploader
	assert.NoError(t, uploader.Start())

	// Upload a part
	data := make([]byte, MinPartSize)
	assert.NoError(t, uploader.Upload(1, data))

	// Close uploader
	assert.NoError(t, uploader.Close(nil))

	// Verify object was created
	assert.True(t, mockServer.ObjectExists("test/content-type.bin"))

	// Verify content type was set (this would need to be checked in the mock server)
	obj, found := mockServer.GetObject("test/content-type.bin")
	assert.True(t, found)
	assert.Equal(t, "application/octet-stream", obj.ContentType)
}

func TestUploader_PartOrdering(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	uploader := &uploader{
		Key:    key,
		Bucket: bucket,
		Object: "test/part-ordering.bin",
	}

	// Start uploader
	assert.NoError(t, uploader.Start())

	// Upload parts out of order
	data1 := make([]byte, MinPartSize)
	for i := range data1 {
		data1[i] = 1
	}
	data2 := make([]byte, MinPartSize)
	for i := range data2 {
		data2[i] = 2
	}
	data3 := make([]byte, MinPartSize)
	for i := range data3 {
		data3[i] = 3
	}

	// Upload in order: 3, 1, 2
	assert.NoError(t, uploader.Upload(3, data3))
	assert.NoError(t, uploader.Upload(1, data1))
	assert.NoError(t, uploader.Upload(2, data2))

	// Close uploader
	assert.NoError(t, uploader.Close(nil))

	// Verify object was created
	assert.True(t, mockServer.ObjectExists("test/part-ordering.bin"))

	// Verify content is in correct order (1, 2, 3)
	content, found := mockServer.ObjectContent("test/part-ordering.bin")
	assert.True(t, found)

	// Check that the parts are in the correct order
	expectedSize := len(data1) + len(data2) + len(data3)
	assert.Equal(t, expectedSize, len(content))

	// Verify each part's content
	assert.Equal(t, byte(1), content[0])             // First part
	assert.Equal(t, byte(2), content[MinPartSize])   // Second part
	assert.Equal(t, byte(3), content[MinPartSize*2]) // Third part
}

func TestUploader_EdgeCases(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Test with empty final part
	u := &uploader{
		Key:    key,
		Bucket: bucket,
		Object: "test/empty-final.bin",
	}

	assert.NoError(t, u.Start())

	data := make([]byte, MinPartSize)
	assert.NoError(t, u.Upload(1, data))

	// Close with empty final part
	assert.NoError(t, u.Close(nil))
	assert.True(t, mockServer.ObjectExists("test/empty-final.bin"))

	// Test Size() before Close
	u2 := &uploader{
		Key:    key,
		Bucket: bucket,
		Object: "test/size-before-close.bin",
	}

	assert.NoError(t, u2.Start())

	// Size should be 0 before Close
	assert.Equal(t, int64(0), u2.Size())

	assert.NoError(t, u2.Upload(1, data))

	// Size should still be 0 before Close
	assert.Equal(t, int64(0), u2.Size())
	assert.NoError(t, u2.Close(nil))
	assert.Equal(t, int64(len(data)), u2.Size())
}
