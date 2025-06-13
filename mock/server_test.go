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

package mock

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/kelindar/s3"
	"github.com/kelindar/s3/aws"
	"github.com/stretchr/testify/assert"
)

func TestMockS3ServerBasicOperations(t *testing.T) {
	// Create mock server
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Create S3 client pointing to mock server
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	bucket := &s3.Bucket{
		Key:    key,
		Bucket: "test-bucket",
		Ctx:    context.Background(),
	}

	// Test PUT operation
	testContent := []byte("Hello, World!")
	etag, err := bucket.Put("test/file.txt", testContent)
	assert.NoError(t, err)
	assert.NotEmpty(t, etag)

	// Verify object exists in mock server
	assert.True(t, mockServer.ObjectExists("test/file.txt"))

	// Test GET operation
	file, err := bucket.Open("test/file.txt")
	assert.NoError(t, err)
	defer file.Close()

	content, err := io.ReadAll(file)
	assert.NoError(t, err)
	assert.Equal(t, testContent, content)

	// Test HEAD operation (file info)
	s3File := file.(*s3.File)
	assert.Equal(t, etag, s3File.ETag)
	assert.Equal(t, "test/file.txt", s3File.Path())

	// Verify request logging
	assert.True(t, mockServer.HasRequestWithMethod("PUT"))
	assert.True(t, mockServer.HasRequestWithMethod("GET"))
}

func TestMockS3ServerListOperations(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Populate test data
	testData := map[string][]byte{
		"dir1/file1.txt": []byte("content1"),
		"dir1/file2.txt": []byte("content2"),
		"dir2/file3.txt": []byte("content3"),
		"root.txt":       []byte("root content"),
	}
	mockServer.PopulateTestData(testData)

	// Test listing all objects
	allObjects := mockServer.ListObjects("")
	assert.Len(t, allObjects, 4)

	// Test listing with prefix
	dir1Objects := mockServer.ListObjects("dir1/")
	assert.Len(t, dir1Objects, 2)
	assert.Contains(t, dir1Objects, "dir1/file1.txt")
	assert.Contains(t, dir1Objects, "dir1/file2.txt")
}

func TestMockS3ServerRangeRequests(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Create large test content
	testContent := make([]byte, 1000)
	for i := range testContent {
		testContent[i] = byte(i % 256)
	}
	mockServer.PutObject("large-file.bin", testContent)

	// Create S3 client
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	bucket := &s3.Bucket{
		Key:    key,
		Bucket: "test-bucket",
		Ctx:    context.Background(),
	}

	// Test range read (start=100, width=101 to read bytes 100-200 inclusive)
	reader, err := bucket.OpenRange("large-file.bin", "", 100, 101)
	assert.NoError(t, err)
	defer reader.Close()

	rangeContent, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, testContent[100:201], rangeContent)
}

func TestMockS3ServerMultipartUpload(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Create S3 client
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	uploader := &s3.Uploader{
		Key:    key,
		Bucket: "test-bucket",
		Object: "multipart-file.bin",
	}

	// Start multipart upload
	err := uploader.Start()
	assert.NoError(t, err)
	assert.NotEmpty(t, uploader.ID())

	// Verify upload exists in mock server
	_, exists := mockServer.GetMultipartUpload(uploader.ID())
	assert.True(t, exists)

	// Upload parts (minimum 5MB each for real S3)
	part1 := make([]byte, 5*1024*1024) // 5MB
	part2 := make([]byte, 5*1024*1024) // 5MB

	err = uploader.Upload(1, part1)
	assert.NoError(t, err)

	err = uploader.Upload(2, part2)
	assert.NoError(t, err)

	// Complete upload
	err = uploader.Close(nil) // No final part
	assert.NoError(t, err)

	finalETag := uploader.ETag()
	assert.NotEmpty(t, finalETag)

	// Verify final object exists
	assert.True(t, mockServer.ObjectExists("multipart-file.bin"))

	// Verify upload was cleaned up
	_, exists = mockServer.GetMultipartUpload(uploader.ID())
	assert.False(t, exists)
}

func TestMockS3ServerCopyPartOperations(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Create source content
	sourceContent := make([]byte, 1024) // 1KB for testing
	for i := range sourceContent {
		sourceContent[i] = byte(i % 256)
	}
	sourceETag := mockServer.PutObject("source-object", sourceContent)

	// Start a multipart upload using S3 client
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	uploader := &s3.Uploader{
		Key:    key,
		Bucket: "test-bucket",
		Object: "copy-destination",
	}

	err := uploader.Start()
	assert.NoError(t, err)
	uploadID := uploader.ID()

	// Test copy part operation using direct HTTP request
	url := fmt.Sprintf("%s/test-bucket/copy-destination?partNumber=1&uploadId=%s", mockServer.URL(), uploadID)
	req, err := http.NewRequest("PUT", url, nil)
	assert.NoError(t, err)

	// Set copy source headers (use the same bucket name as the mock server)
	req.Header.Set("x-amz-copy-source", "/test-bucket/source-object")
	req.Header.Set("x-amz-copy-source-if-match", sourceETag)
	req.Header.Set("x-amz-copy-source-range", "bytes=0-511") // Copy first 512 bytes

	// Make the request
	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()

	// Verify successful response
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotEmpty(t, resp.Header.Get("ETag"))

	// Verify the response contains copy part result XML
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Contains(t, string(body), "<CopyPartResult>")
	assert.Contains(t, string(body), "<ETag>")

	// Verify the part was added to the upload
	upload, exists := mockServer.GetMultipartUpload(uploadID)
	assert.True(t, exists)
	assert.Len(t, upload.Parts, 1)
	assert.Equal(t, 1, upload.Parts[1].PartNumber)
	assert.Equal(t, int64(512), upload.Parts[1].Size)
	assert.Equal(t, sourceContent[0:512], upload.Parts[1].Content)
}

func TestMockS3ServerCopyPartErrorCases(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Create source content
	sourceContent := []byte("test content")
	sourceETag := mockServer.PutObject("source-object", sourceContent)

	// Start a multipart upload using S3 client
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	uploader := &s3.Uploader{
		Key:    key,
		Bucket: "test-bucket",
		Object: "copy-destination",
	}

	err := uploader.Start()
	assert.NoError(t, err)
	uploadID := uploader.ID()

	// Test 1: Copy from non-existent source
	url := fmt.Sprintf("%s/test-bucket/copy-destination?partNumber=1&uploadId=%s", mockServer.URL(), uploadID)
	req, err := http.NewRequest("PUT", url, nil)
	assert.NoError(t, err)

	req.Header.Set("x-amz-copy-source", "/test-bucket/non-existent-object")
	req.Header.Set("x-amz-copy-source-if-match", "fake-etag")

	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// Test 2: Copy with ETag mismatch (precondition failed)
	req2, err := http.NewRequest("PUT", url, nil)
	assert.NoError(t, err)

	req2.Header.Set("x-amz-copy-source", "/test-bucket/source-object")
	req2.Header.Set("x-amz-copy-source-if-match", "wrong-etag")

	resp2, err := http.DefaultClient.Do(req2)
	assert.NoError(t, err)
	defer resp2.Body.Close()

	assert.Equal(t, http.StatusPreconditionFailed, resp2.StatusCode)

	// Test 3: Copy with invalid range
	req3, err := http.NewRequest("PUT", url, nil)
	assert.NoError(t, err)

	req3.Header.Set("x-amz-copy-source", "/test-bucket/source-object")
	req3.Header.Set("x-amz-copy-source-if-match", sourceETag)
	// Try to copy beyond the source object size
	req3.Header.Set("x-amz-copy-source-range", fmt.Sprintf("bytes=0-%d", len(sourceContent)+100))

	resp3, err := http.DefaultClient.Do(req3)
	assert.NoError(t, err)
	defer resp3.Body.Close()

	assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, resp3.StatusCode)

	// Test 4: Invalid copy source format
	req4, err := http.NewRequest("PUT", url, nil)
	assert.NoError(t, err)

	req4.Header.Set("x-amz-copy-source", "invalid-format")

	resp4, err := http.DefaultClient.Do(req4)
	assert.NoError(t, err)
	defer resp4.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp4.StatusCode)
}

func TestMockS3ServerErrorSimulation(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Enable error simulation
	mockServer.EnableErrorSimulation(ErrorSimulation{
		InternalErrors: true,
		ErrorRate:      1.0, // 100% error rate
	})

	// Create S3 client
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	bucket := &s3.Bucket{
		Key:    key,
		Bucket: "test-bucket",
		Ctx:    context.Background(),
	}

	// This should fail due to error simulation
	_, err := bucket.Put("test-file.txt", []byte("test"))
	assert.Error(t, err)

	// Disable error simulation
	mockServer.DisableErrorSimulation()

	// This should succeed
	_, err = bucket.Put("test-file.txt", []byte("test"))
	assert.NoError(t, err)
}
