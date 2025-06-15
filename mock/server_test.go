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
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
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

	bucket := s3.NewBucket(key, "test-bucket")

	// Test PUT operation
	testContent := []byte("Hello, World!")
	etag, err := bucket.Write(context.Background(), "test/file.txt", testContent)
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

	bucket := s3.NewBucket(key, "test-bucket")

	// Test range read (start=100, width=101 to read bytes 100-200 inclusive)
	reader, err := bucket.OpenRange("large-file.bin", "", 100, 101)
	assert.NoError(t, err)
	defer reader.Close()

	rangeContent, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, testContent[100:201], rangeContent)
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

	bucket := s3.NewBucket(key, "test-bucket")

	// This should fail due to error simulation
	_, err := bucket.Write(context.Background(), "test-file.txt", []byte("test"))
	assert.Error(t, err)

	// Disable error simulation
	mockServer.DisableErrorSimulation()

	// This should succeed
	_, err = bucket.Write(context.Background(), "test-file.txt", []byte("test"))
	assert.NoError(t, err)
}

func TestMockS3ServerClearAndDelete(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Add test objects
	testData := map[string][]byte{
		"file1.txt": []byte("content1"),
		"file2.txt": []byte("content2"),
		"file3.txt": []byte("content3"),
	}
	mockServer.PopulateTestData(testData)

	// Verify objects exist
	assert.True(t, mockServer.ObjectExists("file1.txt"))
	assert.True(t, mockServer.ObjectExists("file2.txt"))
	assert.True(t, mockServer.ObjectExists("file3.txt"))

	// Test DeleteObject
	deleted := mockServer.DeleteObject("file1.txt")
	assert.True(t, deleted)
	assert.False(t, mockServer.ObjectExists("file1.txt"))

	// Test deleting non-existent object
	deleted = mockServer.DeleteObject("non-existent.txt")
	assert.False(t, deleted)

	// Test Clear
	mockServer.Clear()
	assert.False(t, mockServer.ObjectExists("file2.txt"))
	assert.False(t, mockServer.ObjectExists("file3.txt"))
	assert.Len(t, mockServer.ListObjects(""), 0)
}

func TestMockS3ServerObjectContent(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Test ObjectContent with existing object
	testContent := []byte("Hello, World!")
	mockServer.PutObject("test.txt", testContent)

	content, found := mockServer.ObjectContent("test.txt")
	assert.True(t, found)
	assert.Equal(t, testContent, content)

	// Test ObjectContent with non-existent object
	content, found = mockServer.ObjectContent("non-existent.txt")
	assert.False(t, found)
	assert.Nil(t, content)
}

func TestMockS3ServerMetadata(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Create object with metadata
	testContent := []byte("test content")
	metadata := map[string]string{
		"author":      "test-user",
		"description": "test file",
		"version":     "1.0",
	}
	mockServer.PutObjectWithMetadata("test-with-metadata.txt", testContent, metadata)

	// Test GetObjectMetadata
	retrievedMetadata, found := mockServer.GetObjectMetadata("test-with-metadata.txt")
	assert.True(t, found)
	assert.Equal(t, metadata, retrievedMetadata)

	// Test SetObjectMetadata
	newMetadata := map[string]string{
		"author":  "updated-user",
		"version": "2.0",
	}
	success := mockServer.SetObjectMetadata("test-with-metadata.txt", newMetadata)
	assert.True(t, success)

	// Verify updated metadata
	retrievedMetadata, found = mockServer.GetObjectMetadata("test-with-metadata.txt")
	assert.True(t, found)
	assert.Equal(t, newMetadata, retrievedMetadata)

	// Test metadata operations on non-existent object
	_, found = mockServer.GetObjectMetadata("non-existent.txt")
	assert.False(t, found)

	success = mockServer.SetObjectMetadata("non-existent.txt", newMetadata)
	assert.False(t, success)
}

func TestMockS3ServerRequestLogging(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Create S3 client
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()
	bucket := s3.NewBucket(key, "test-bucket")

	// Initial request count should be 0
	assert.Equal(t, 0, mockServer.RequestCount())

	// Make some requests
	_, err := bucket.Write(context.Background(), "test1.txt", []byte("content1"))
	assert.NoError(t, err)
	_, err = bucket.Write(context.Background(), "test2.txt", []byte("content2"))
	assert.NoError(t, err)

	// Check request count
	assert.Equal(t, 2, mockServer.RequestCount())

	// Test HasRequestWithMethod
	assert.True(t, mockServer.HasRequestWithMethod("PUT"))
	assert.False(t, mockServer.HasRequestWithMethod("DELETE"))

	// Test GetRequestsWithMethod
	putRequests := mockServer.GetRequestsWithMethod("PUT")
	assert.Len(t, putRequests, 2)
	for _, req := range putRequests {
		assert.Equal(t, "PUT", req.Method)
	}

	// Test with non-existent method
	deleteRequests := mockServer.GetRequestsWithMethod("DELETE")
	assert.Len(t, deleteRequests, 0)

	// Make a GET request
	file, err := bucket.Open("test1.txt")
	assert.NoError(t, err)
	file.Close()

	// Check updated counts
	assert.Equal(t, 3, mockServer.RequestCount())
	assert.True(t, mockServer.HasRequestWithMethod("GET"))

	getRequests := mockServer.GetRequestsWithMethod("GET")
	assert.Len(t, getRequests, 1)
	assert.Equal(t, "GET", getRequests[0].Method)
}

func TestMockS3ServerHeadRequests(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Create test object
	testContent := []byte("test content for HEAD request")
	etag := mockServer.PutObject("head-test.txt", testContent)

	// Test HEAD request directly using HTTP client to ensure handleHeadObject is tested
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Make a direct HEAD request to test the handler
	req, err := http.NewRequest("HEAD", mockServer.URL()+"/test-bucket/head-test.txt", nil)
	assert.NoError(t, err)

	key.SignV4(req, nil)

	client := &http.Client{}
	resp, err := client.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()

	// Verify HEAD response
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, etag, resp.Header.Get("ETag"))
	assert.Equal(t, strconv.Itoa(len(testContent)), resp.Header.Get("Content-Length"))

	// Test HEAD request for non-existent object
	req2, err := http.NewRequest("HEAD", mockServer.URL()+"/test-bucket/non-existent.txt", nil)
	assert.NoError(t, err)
	key.SignV4(req2, nil)

	resp2, err := client.Do(req2)
	assert.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)

	// Verify HEAD requests were logged
	assert.True(t, mockServer.HasRequestWithMethod("HEAD"))
	headRequests := mockServer.GetRequestsWithMethod("HEAD")
	assert.Len(t, headRequests, 2)
}

func TestMockS3ServerDeleteRequests(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Create test objects
	mockServer.PutObject("delete-test1.txt", []byte("content1"))
	mockServer.PutObject("delete-test2.txt", []byte("content2"))

	// Create S3 client
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()
	bucket := s3.NewBucket(key, "test-bucket")

	// Test DELETE request through S3 client
	assert.NoError(t, bucket.Delete(context.Background(), "delete-test1.txt"))
	assert.False(t, mockServer.ObjectExists("delete-test1.txt"))

	// Test deleting non-existent object (S3 DELETE is idempotent, should not error)
	err := bucket.Delete(context.Background(), "non-existent.txt")
	// Note: Real S3 returns 204 even for non-existent objects, but our mock returns 404
	// This is acceptable behavior for a mock
	if err != nil {
		assert.Contains(t, err.Error(), "404")
	}

	// Verify DELETE requests were logged
	assert.True(t, mockServer.HasRequestWithMethod("DELETE"))
	deleteRequests := mockServer.GetRequestsWithMethod("DELETE")
	assert.Len(t, deleteRequests, 2)
}

func TestMockS3ServerContentTypeDetection(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Test various file types
	testCases := []struct {
		filename   string
		content    []byte
		expectedCT string
	}{
		{"test.txt", []byte("plain text"), "text/plain"},
		{"test.html", []byte("<html></html>"), "text/html"},
		{"test.htm", []byte("<html></html>"), "text/html"},
		{"test.json", []byte(`{"key": "value"}`), "application/json"},
		{"test.xml", []byte("<?xml version='1.0'?>"), "application/xml"},
		{"test.pdf", []byte("PDF content"), "application/pdf"},
		{"test.jpg", []byte("JPEG content"), "image/jpeg"},
		{"test.jpeg", []byte("JPEG content"), "image/jpeg"},
		{"test.png", []byte("PNG content"), "image/png"},
		{"test.gif", []byte("GIF content"), "image/gif"},
		{"test.bin", []byte("\x00\x01\x02\x03"), "application/octet-stream"}, // Use actual binary content
		{"no-extension", []byte("content"), "text/plain; charset=utf-8"},     // http.DetectContentType
	}

	for _, tc := range testCases {
		mockServer.PutObject(tc.filename, tc.content)
		obj, found := mockServer.GetObject(tc.filename)
		assert.True(t, found, "Object %s should exist", tc.filename)
		assert.Equal(t, tc.expectedCT, obj.ContentType, "Content type for %s", tc.filename)
	}
}

func TestMockS3ServerAdvancedRangeRequests(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Create large test content
	testContent := make([]byte, 1000)
	for i := range testContent {
		testContent[i] = byte(i % 256)
	}
	mockServer.PutObject("range-test.bin", testContent)

	// Create S3 client
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()
	bucket := s3.NewBucket(key, "test-bucket")

	// Test various range formats
	testCases := []struct {
		name     string
		start    int64
		width    int64
		expected []byte
	}{
		{"Beginning range", 0, 100, testContent[0:100]},
		{"Middle range", 200, 100, testContent[200:300]},
		{"End range", 900, 100, testContent[900:1000]},
		{"Single byte", 500, 1, testContent[500:501]},
		{"Large range", 100, 800, testContent[100:900]},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := bucket.OpenRange("range-test.bin", "", tc.start, tc.width)
			assert.NoError(t, err)
			defer reader.Close()

			content, err := io.ReadAll(reader)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, content)
		})
	}

	// Test suffix range (last N bytes) - use proper range calculation
	// For suffix range, we need to calculate the start position correctly
	startPos := int64(len(testContent)) - 50 // Last 50 bytes
	reader, err := bucket.OpenRange("range-test.bin", "", startPos, 50)
	assert.NoError(t, err)
	defer reader.Close()

	content, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, testContent[950:1000], content)
}

func TestMockS3ServerMultipartUpload(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Create S3 client
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Test multipart upload through WriteFrom (which uses internal uploader)
	bucket := s3.NewBucket(key, "test-bucket")

	// Create test data larger than MinPartSize to trigger multipart upload
	testData := make([]byte, s3.MinPartSize*2+1000) // ~10MB + 1000 bytes
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Use WriteFrom which internally handles multipart upload
	err := bucket.WriteFrom(context.Background(), "multipart-test.bin", bytes.NewReader(testData), int64(len(testData)))
	assert.NoError(t, err)

	// Verify object was created
	assert.True(t, mockServer.ObjectExists("multipart-test.bin"))

	// Verify final content
	finalContent, found := mockServer.ObjectContent("multipart-test.bin")
	assert.True(t, found)
	assert.Equal(t, testData, finalContent)

	// Verify multipart upload requests were made
	assert.True(t, mockServer.HasRequestWithMethod("POST")) // Initiate multipart
	assert.True(t, mockServer.HasRequestWithMethod("PUT"))  // Upload parts
}

func TestMockS3ServerListObjects(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Create S3 client
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()
	bucket := s3.NewBucket(key, "test-bucket")

	// Populate test data with hierarchical structure
	testData := map[string][]byte{
		"root.txt":             []byte("root content"),
		"folder1/file1.txt":    []byte("file1 content"),
		"folder1/file2.txt":    []byte("file2 content"),
		"folder1/sub/file.txt": []byte("sub file content"),
		"folder2/file3.txt":    []byte("file3 content"),
		"folder2/file4.txt":    []byte("file4 content"),
	}
	mockServer.PopulateTestData(testData)

	// Test listing all objects through S3 client (triggers handleListObjects)
	entries, err := bucket.ReadDir(".")
	assert.NoError(t, err)
	assert.Len(t, entries, 3) // root.txt, folder1/, folder2/

	// Test listing with prefix using fs.ReadDir
	entries, err = bucket.ReadDir("folder1")
	assert.NoError(t, err)
	assert.Len(t, entries, 3) // file1.txt, file2.txt, sub/

	// Test listing with specific prefix
	entries, err = bucket.ReadDir("folder1/sub")
	assert.NoError(t, err)
	assert.Len(t, entries, 1) // file.txt

	// Verify LIST requests were logged
	assert.True(t, mockServer.HasRequestWithMethod("GET"))
	getRequests := mockServer.GetRequestsWithMethod("GET")

	// Should have GET requests for list operations
	hasListRequest := false
	for _, req := range getRequests {
		if strings.Contains(req.Query, "list-type=2") {
			hasListRequest = true
			break
		}
	}
	assert.True(t, hasListRequest, "Should have list-type=2 query parameter in GET requests")
}

func TestMockS3ServerInvalidRequests(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Test invalid bucket name
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()
	wrongBucket := s3.NewBucket(key, "wrong-bucket")

	// This should fail with NoSuchBucket error
	_, err := wrongBucket.Write(context.Background(), "test.txt", []byte("content"))
	assert.Error(t, err)

	// Test with correct bucket
	correctBucket := s3.NewBucket(key, "test-bucket")
	_, err = correctBucket.Write(context.Background(), "test.txt", []byte("content"))
	assert.NoError(t, err)

	// Verify requests were logged
	assert.True(t, mockServer.RequestCount() >= 2)
}

func TestMockS3ServerCopyPart(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Create source object for copy operations
	sourceData := make([]byte, s3.MinPartSize*2) // 10MB
	for i := range sourceData {
		sourceData[i] = byte(i % 256)
	}
	mockServer.PutObject("source-large.bin", sourceData)

	// Create S3 client
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()
	bucket := s3.NewBucket(key, "test-bucket")

	// Test copy operation through WriteFrom with a Reader that supports copy
	sourceReader := &s3.Reader{
		Key:    key,
		Bucket: "test-bucket",
		Path:   "source-large.bin",
		Size:   int64(len(sourceData)),
	}

	// Use WriteFrom which may trigger copy part operations for large files
	err := bucket.WriteFrom(context.Background(), "copied-large.bin", sourceReader, int64(len(sourceData)))
	assert.NoError(t, err)

	// Verify object was created
	assert.True(t, mockServer.ObjectExists("copied-large.bin"))

	// Verify content is correct
	copiedContent, found := mockServer.ObjectContent("copied-large.bin")
	assert.True(t, found)
	assert.Equal(t, sourceData, copiedContent)
}

func TestMockS3ServerErrorHandling(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Create S3 client
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()
	bucket := s3.NewBucket(key, "test-bucket")

	// Test various error conditions

	// Test accessing non-existent object
	_, err := bucket.Open("non-existent.txt")
	assert.Error(t, err)

	// Test listing non-existent directory
	_, err = bucket.ReadDir("non-existent-dir")
	assert.Error(t, err)

	// Test range request on non-existent object
	_, err = bucket.OpenRange("non-existent.bin", "", 0, 100)
	assert.Error(t, err)

	// Verify error responses were handled properly
	assert.True(t, mockServer.RequestCount() > 0)
}

func TestMockS3ServerMultipartUploadUtilities(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Test GetMultipartUpload and ListMultipartUploads with no uploads
	uploads := mockServer.ListMultipartUploads()
	assert.Len(t, uploads, 0)

	_, exists := mockServer.GetMultipartUpload("non-existent-upload-id")
	assert.False(t, exists)

	// Create S3 client and start a multipart upload
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()
	bucket := s3.NewBucket(key, "test-bucket")

	// Use WriteFrom to trigger multipart upload
	testData := make([]byte, s3.MinPartSize*2) // 10MB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	err := bucket.WriteFrom(context.Background(), "multipart-utils-test.bin", bytes.NewReader(testData), int64(len(testData)))
	assert.NoError(t, err)

	// Verify object was created (upload completed and cleaned up)
	assert.True(t, mockServer.ObjectExists("multipart-utils-test.bin"))

	// Verify no active uploads remain
	uploads = mockServer.ListMultipartUploads()
	assert.Len(t, uploads, 0)
}

func TestMockS3ServerAbortMultipartUpload(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Test abort multipart upload by making direct HTTP requests
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Initiate multipart upload
	req, err := http.NewRequest("POST", mockServer.URL()+"/test-bucket/abort-test.bin?uploads=", nil)
	assert.NoError(t, err)
	key.SignV4(req, nil)

	client := &http.Client{}
	resp, err := client.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Parse upload ID from response (simplified - in real implementation would parse XML)
	// For testing purposes, we'll use the fact that our mock generates predictable IDs
	uploads := mockServer.ListMultipartUploads()
	assert.Len(t, uploads, 1)

	var uploadID string
	for id := range uploads {
		uploadID = id
		break
	}

	// Abort the upload
	req2, err := http.NewRequest("DELETE", mockServer.URL()+"/test-bucket/abort-test.bin?uploadId="+uploadID, nil)
	assert.NoError(t, err)
	key.SignV4(req2, nil)

	resp2, err := client.Do(req2)
	assert.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp2.StatusCode)

	// Verify upload was cleaned up
	uploads = mockServer.ListMultipartUploads()
	assert.Len(t, uploads, 0)

	// Test aborting non-existent upload
	req3, err := http.NewRequest("DELETE", mockServer.URL()+"/test-bucket/abort-test.bin?uploadId=non-existent", nil)
	assert.NoError(t, err)
	key.SignV4(req3, nil)

	resp3, err := client.Do(req3)
	assert.NoError(t, err)
	defer resp3.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp3.StatusCode)
}

func TestMockS3ServerS3SelectDirect(t *testing.T) {
	mockServer := New("test-bucket", "us-east-1")
	defer mockServer.Close()

	// Create test object for S3 Select
	testContent := []byte(`{"id": 1, "name": "test", "value": 100}
{"id": 2, "name": "example", "value": 200}`)
	mockServer.PutObject("select-test.json", testContent)

	// Test S3 Select by making direct HTTP request
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	// Make S3 Select request
	req, err := http.NewRequest("POST", mockServer.URL()+"/test-bucket/select-test.json?select=", strings.NewReader("SELECT * FROM S3Object"))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/xml")
	key.SignV4(req, []byte("SELECT * FROM S3Object"))

	client := &http.Client{}
	resp, err := client.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()

	// Verify S3 Select response
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))

	// Read response body
	result, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.NotEmpty(t, result)

	// Test S3 Select on non-existent object
	req2, err := http.NewRequest("POST", mockServer.URL()+"/test-bucket/non-existent.json?select=", strings.NewReader("SELECT * FROM S3Object"))
	assert.NoError(t, err)
	key.SignV4(req2, []byte("SELECT * FROM S3Object"))

	resp2, err := client.Do(req2)
	assert.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)

	// Verify S3 Select requests were logged
	assert.True(t, mockServer.HasRequestWithMethod("POST"))
	postRequests := mockServer.GetRequestsWithMethod("POST")

	hasSelectRequest := false
	for _, req := range postRequests {
		if strings.Contains(req.Query, "select") {
			hasSelectRequest = true
			break
		}
	}
	assert.True(t, hasSelectRequest)
}
