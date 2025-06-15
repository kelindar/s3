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
	"io"
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
