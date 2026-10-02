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
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/kelindar/s3"
	"github.com/kelindar/s3/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func TestWriteMultipartXML(t *testing.T) {
	var body bytes.Buffer
	require.NoError(t, writeXMLFields(&body, "InitiateMultipartUploadResult",
		"Bucket", "test", "Key", "a&b<key>", "UploadId", "id-1"))
	var got InitiateMultipartUploadResponse
	require.NoError(t, xml.Unmarshal(body.Bytes(), &got))
	assert.Equal(t, "test", got.Bucket)
	assert.Equal(t, "a&b<key>", got.Key)
	assert.Equal(t, "id-1", got.UploadId)
}

func TestRequestLogging(t *testing.T) {
	server := New("test-bucket", "us-east-1")
	defer server.Close()
	put := func(content string) {
		req := httptest.NewRequest(http.MethodPut, "/test-bucket/object", strings.NewReader(content))
		server.ServeHTTP(httptest.NewRecorder(), req)
	}

	put("logged")
	logs := server.GetRequestLog()
	assert.Len(t, logs, 1)
	assert.Equal(t, []byte("logged"), logs[0].Body)

	server.SetRequestLogging(false)
	put("not logged")
	assert.Equal(t, 1, server.RequestCount())
	content, ok := server.ObjectContent("object")
	assert.True(t, ok)
	assert.Equal(t, []byte("not logged"), content)

	server.SetRequestLogging(true)
	req := httptest.NewRequest(http.MethodGet, "/test-bucket/object", nil)
	server.ServeHTTP(httptest.NewRecorder(), req)
	assert.Equal(t, 2, server.RequestCount())
}

func TestFastMockParity(t *testing.T) {
	server := New("test-bucket", "us-east-1")
	defer server.Close()
	server.SetRequestLogging(false)
	server.PutObject("dir/key.txt", []byte("payload"))
	server.PutObject("dir/sub/key.txt", []byte("nested"))
	server.PutObject("dir/../odd.txt", []byte("dots"))

	for _, tc := range []struct {
		name, method, target string
		headers              map[string]string
	}{
		{"list", http.MethodGet, "/test-bucket/?list-type=2&prefix=dir%2F&delimiter=%2F", nil},
		{"page", http.MethodGet, "/test-bucket/?list-type=2&prefix=dir%2F&max-keys=1", nil},
		{"get", http.MethodGet, "/test-bucket/dir/key.txt", nil},
		{"head", http.MethodHead, "/test-bucket/dir/key.txt", nil},
		{"range", http.MethodGet, "/test-bucket/dir/key.txt", map[string]string{"Range": "bytes=1-3"}},
		{"if-match", http.MethodGet, "/test-bucket/dir/key.txt", map[string]string{"If-Match": "mismatch"}},
		{"missing", http.MethodGet, "/test-bucket/missing", nil},
		{"dot-segment", http.MethodGet, "/test-bucket/dir/../odd.txt", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, tc.target, nil)
			var fastRequest fasthttp.Request
			fastRequest.SetRequestURI(tc.target)
			fastRequest.Header.SetMethod(tc.method)
			for key, value := range tc.headers {
				request.Header.Set(key, value)
				fastRequest.Header.Set(key, value)
			}
			standard := httptest.NewRecorder()
			server.ServeHTTP(standard, request)
			var ctx fasthttp.RequestCtx
			ctx.Init(&fastRequest, nil, nil)
			server.fastHandler()(&ctx)
			assert.Equal(t, standard.Code, ctx.Response.StatusCode())
			assert.Equal(t, standard.Body.Bytes(), ctx.Response.Body())
			for _, header := range []string{"Content-Type", "Content-Range", "Content-Length", "ETag", "Last-Modified"} {
				if value := standard.Header().Get(header); value != "" {
					assert.Equal(t, value, string(ctx.Response.Header.Peek(header)), header)
				}
			}
		})
	}
	response, err := http.Get(server.URL() + "/test-bucket/dir/../odd.txt")
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.StatusCode)
	assert.Equal(t, []byte("dots"), body)
}

func TestServer(t *testing.T) {
	t.Run("basic operations", func(t *testing.T) {
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
	})

	t.Run("list operations", func(t *testing.T) {
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
	})

	t.Run("error simulation", func(t *testing.T) {
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
	})

	t.Run("utilities", func(t *testing.T) {
		mockServer := New("test-bucket", "us-east-1")
		defer mockServer.Close()

		// Test multiple utility functions in one comprehensive test
		testData := map[string][]byte{
			"file1.txt": []byte("content1"),
			"file2.txt": []byte("content2"),
		}
		mockServer.PopulateTestData(testData)

		// Test ObjectExists and ObjectContent
		assert.True(t, mockServer.ObjectExists("file1.txt"))
		content, found := mockServer.ObjectContent("file1.txt")
		assert.True(t, found)
		assert.Equal(t, []byte("content1"), content)

		// Test metadata operations
		metadata := map[string]string{"author": "test", "version": "1.0"}
		mockServer.PutObjectWithMetadata("meta-test.txt", []byte("content"), metadata)

		retrievedMeta, found := mockServer.GetObjectMetadata("meta-test.txt")
		assert.True(t, found)
		assert.Equal(t, metadata, retrievedMeta)

		// Test SetObjectMetadata
		newMeta := map[string]string{"author": "updated"}
		assert.True(t, mockServer.SetObjectMetadata("meta-test.txt", newMeta))

		// Test DeleteObject
		assert.True(t, mockServer.DeleteObject("file1.txt"))
		assert.False(t, mockServer.ObjectExists("file1.txt"))
		assert.False(t, mockServer.DeleteObject("non-existent.txt"))

		// Test Clear
		mockServer.Clear()
		assert.False(t, mockServer.ObjectExists("file2.txt"))
		assert.Len(t, mockServer.ListObjects(""), 0)

		// Test error cases
		_, found = mockServer.ObjectContent("non-existent.txt")
		assert.False(t, found)
		_, found = mockServer.GetObjectMetadata("non-existent.txt")
		assert.False(t, found)
		assert.False(t, mockServer.SetObjectMetadata("non-existent.txt", newMeta))
	})

	t.Run("head requests", func(t *testing.T) {
		mockServer := New("test-bucket", "us-east-1")
		defer mockServer.Close()

		// Create test object
		testContent := []byte("test content for HEAD request")
		etag := mockServer.PutObject("head-test.txt", testContent)

		// Test HEAD request directly using HTTP client to ensure handleHeadObject is tested
		// Make a direct HEAD request to test the handler
		req, err := http.NewRequest("HEAD", mockServer.URL()+"/test-bucket/head-test.txt", nil)
		assert.NoError(t, err)

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

		resp2, err := client.Do(req2)
		assert.NoError(t, err)
		defer resp2.Body.Close()
		assert.Equal(t, http.StatusNotFound, resp2.StatusCode)

		// Verify HEAD requests were logged
		assert.True(t, mockServer.HasRequestWithMethod("HEAD"))
		headRequests := mockServer.GetRequestsWithMethod("HEAD")
		assert.Len(t, headRequests, 2)
	})

	t.Run("content type detection", func(t *testing.T) {
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
	})

	t.Run("range requests", func(t *testing.T) {
		mockServer := New("test-bucket", "us-east-1")
		defer mockServer.Close()

		// Create test content
		testContent := make([]byte, 1000)
		for i := range testContent {
			testContent[i] = byte(i % 256)
		}
		etag := mockServer.PutObject("range-test.bin", testContent)

		// Create S3 client
		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()
		bucket := s3.NewBucket(key, "test-bucket")

		// Test key range scenarios
		testCases := []struct {
			start    int64
			width    int64
			expected []byte
		}{
			{0, 100, testContent[0:100]},      // Beginning
			{200, 100, testContent[200:300]},  // Middle
			{900, 100, testContent[900:1000]}, // End
			{950, 50, testContent[950:1000]},  // Last 50 bytes
		}

		for _, tc := range testCases {
			reader, err := bucket.OpenRange("range-test.bin", etag, tc.start, tc.width)
			assert.NoError(t, err)
			defer reader.Close()

			content, err := io.ReadAll(reader)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, content)
		}

		_, err := bucket.OpenRange("range-test.bin", "stale-etag", 0, 100)
		assert.ErrorIs(t, err, s3.ErrETagChanged)
	})

	t.Run("multipart upload", func(t *testing.T) {
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
		var partHashes []byte
		for start := 0; start < len(testData); start += s3.MinPartSize {
			sum := md5.Sum(testData[start:min(start+s3.MinPartSize, len(testData))])
			partHashes = append(partHashes, sum[:]...)
		}
		object, found := mockServer.GetObject("multipart-test.bin")
		require.True(t, found)
		assert.Equal(t, fmt.Sprintf(`"%x-%d"`, md5.Sum(partHashes), len(partHashes)/md5.Size), object.ETag)

		// Verify multipart upload requests were made
		assert.True(t, mockServer.HasRequestWithMethod("POST")) // Initiate multipart
		assert.True(t, mockServer.HasRequestWithMethod("PUT"))  // Upload parts
	})

	t.Run("error handling", func(t *testing.T) {
		mockServer := New("test-bucket", "us-east-1")
		defer mockServer.Close()

		// Create S3 client
		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		// Test invalid bucket name
		wrongBucket := s3.NewBucket(key, "wrong-bucket")
		_, err := wrongBucket.Write(context.Background(), "test.txt", []byte("content"))
		assert.Error(t, err)

		// Test with correct bucket for various error conditions
		bucket := s3.NewBucket(key, "test-bucket")

		// Test accessing non-existent object
		_, err = bucket.Open("non-existent.txt")
		assert.Error(t, err)

		// Test range request on non-existent object
		_, err = bucket.OpenRange("non-existent.bin", "", 0, 100)
		assert.Error(t, err)

		// Test listing non-existent directory
		_, err = bucket.ReadDir("non-existent-dir")
		assert.Error(t, err)

		// Test DELETE on non-existent object
		err = bucket.Delete(context.Background(), "non-existent.txt")
		if err != nil {
			assert.Contains(t, err.Error(), "404")
		}

		// Verify requests were logged
		assert.True(t, mockServer.RequestCount() > 0)
	})

	t.Run("advanced operations", func(t *testing.T) {
		mockServer := New("test-bucket", "us-east-1")
		defer mockServer.Close()

		// Test multipart upload utilities
		uploads := mockServer.ListMultipartUploads()
		assert.Len(t, uploads, 0)
		_, exists := mockServer.GetMultipartUpload("non-existent")
		assert.False(t, exists)

		// Test S3 Select and multipart abort via direct HTTP
		client := &http.Client{}

		// Test S3 Select
		mockServer.PutObject("test.json", []byte(`{"id": 1}`))
		req, _ := http.NewRequest("POST", mockServer.URL()+"/test-bucket/test.json?select=", strings.NewReader("SELECT * FROM S3Object"))
		resp, err := client.Do(req)
		assert.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		// Test multipart initiate and abort
		req2, _ := http.NewRequest("POST", mockServer.URL()+"/test-bucket/test.bin?uploads=", nil)
		resp2, err := client.Do(req2)
		assert.NoError(t, err)
		defer resp2.Body.Close()
		assert.Equal(t, http.StatusOK, resp2.StatusCode)

		// Get upload ID and abort
		uploads = mockServer.ListMultipartUploads()
		assert.Len(t, uploads, 1)
		var uploadID string
		for id := range uploads {
			uploadID = id
			break
		}

		req3, _ := http.NewRequest("DELETE", mockServer.URL()+"/test-bucket/test.bin?uploadId="+uploadID, nil)
		resp3, err := client.Do(req3)
		assert.NoError(t, err)
		defer resp3.Body.Close()
		assert.Equal(t, http.StatusNoContent, resp3.StatusCode)

		// Verify cleanup
		uploads = mockServer.ListMultipartUploads()
		assert.Len(t, uploads, 0)

		// Test request logging
		assert.True(t, mockServer.RequestCount() > 0)
		assert.True(t, mockServer.HasRequestWithMethod("POST"))
		assert.True(t, mockServer.HasRequestWithMethod("DELETE"))

		postRequests := mockServer.GetRequestsWithMethod("POST")
		assert.True(t, len(postRequests) >= 2) // S3 Select + multipart initiate
	})
}
