# Mock S3 Server

A comprehensive mock implementation of the AWS S3 API for testing purposes. This mock server implements all S3 operations used by the S3 client library, providing a complete testing environment without requiring actual AWS infrastructure.

## Features

### Core S3 Operations
- **GET Object** - Retrieve objects with full range request support
- **HEAD Object** - Get object metadata without content
- **PUT Object** - Upload objects with automatic ETag generation
- **DELETE Object** - Remove objects from storage
- **LIST Objects v2** - List objects with prefix, delimiter, and pagination support

### Advanced Features
- **Multipart Uploads** - Complete workflow (initiate, upload parts, complete, abort)
- **S3 Select** - Basic Parquet query simulation with streaming response
- **Range Requests** - HTTP range header support with proper Content-Range responses
- **Error Simulation** - Configurable error injection for testing resilience
- **Request Logging** - Capture and inspect all requests for debugging

### Testing Integration
- **Thread-safe** - Concurrent operations supported
- **Easy Setup/Teardown** - Simple lifecycle management
- **Test Data Population** - Bulk data loading utilities
- **Assertion Helpers** - Convenient verification methods
- **AWS-Compatible** - Proper XML response formats and HTTP status codes

## Quick Start

```go
package main

import (
    "context"
    "io"
    "testing"
    
    "github.com/kelindar/s3"
    "github.com/kelindar/s3/aws"
    "github.com/kelindar/s3/mock"
    "github.com/stretchr/testify/assert"
)

func TestS3Operations(t *testing.T) {
    // Create mock server
    mockServer := mock.NewMockS3Server("test-bucket", "us-east-1")
    defer mockServer.Close()
    
    // Create S3 client pointing to mock server
    key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
    key.BaseURI = mockServer.URL()
    
    bucket := &s3.BucketFS{
        Key:    key,
        Bucket: "test-bucket",
        Ctx:    context.Background(),
    }
    
    // Test operations
    etag, err := bucket.Put("test.txt", []byte("Hello, World!"))
    assert.NoError(t, err)
    
    file, err := bucket.Open("test.txt")
    assert.NoError(t, err)
    defer file.Close()
    
    content, err := io.ReadAll(file)
    assert.NoError(t, err)
    assert.Equal(t, []byte("Hello, World!"), content)
}
```

## API Reference

### MockS3Server

#### Constructor
```go
func NewMockS3Server(bucket, region string) *MockS3Server
```

#### Lifecycle Methods
```go
func (m *MockS3Server) URL() string           // Get server URL
func (m *MockS3Server) Close()                // Shutdown server
func (m *MockS3Server) Clear()                // Clear all data
```

#### Object Management
```go
func (m *MockS3Server) PutObject(key string, content []byte) string
func (m *MockS3Server) PutObjectWithMetadata(key string, content []byte, metadata map[string]string) string
func (m *MockS3Server) GetObject(key string) (*MockObject, bool)
func (m *MockS3Server) DeleteObject(key string) bool
func (m *MockS3Server) ListObjects(prefix string) []string
```

#### Test Utilities
```go
func (m *MockS3Server) PopulateTestData(data map[string][]byte)
func (m *MockS3Server) ObjectExists(key string) bool
func (m *MockS3Server) ObjectContent(key string) ([]byte, bool)
func (m *MockS3Server) RequestCount() int
func (m *MockS3Server) HasRequestWithMethod(method string) bool
func (m *MockS3Server) GetRequestLog() []RequestLog
```

#### Error Simulation
```go
func (m *MockS3Server) EnableErrorSimulation(config ErrorSimulation)
func (m *MockS3Server) DisableErrorSimulation()

type ErrorSimulation struct {
    NetworkErrors    bool
    NotFoundErrors   bool
    PermissionErrors bool
    InternalErrors   bool
    ErrorRate        float64 // 0.0 to 1.0
}
```

#### Multipart Upload Support
```go
func (m *MockS3Server) GetMultipartUpload(uploadID string) (*MockMultipartUpload, bool)
func (m *MockS3Server) ListMultipartUploads() map[string]*MockMultipartUpload
```

## Usage Examples

### Basic Object Operations
```go
mockServer := mock.NewMockS3Server("test-bucket", "us-east-1")
defer mockServer.Close()

// Put object
etag := mockServer.PutObject("path/to/file.txt", []byte("content"))

// Check existence
exists := mockServer.ObjectExists("path/to/file.txt")

// Get content
content, found := mockServer.ObjectContent("path/to/file.txt")

// List objects
objects := mockServer.ListObjects("path/")
```

### Bulk Data Population
```go
testData := map[string][]byte{
    "dir1/file1.txt": []byte("content1"),
    "dir1/file2.txt": []byte("content2"),
    "dir2/file3.txt": []byte("content3"),
}
mockServer.PopulateTestData(testData)
```

### Request Verification
```go
// Check request count
count := mockServer.RequestCount()

// Check for specific methods
hasPut := mockServer.HasRequestWithMethod("PUT")
hasGet := mockServer.HasRequestWithMethod("GET")

// Get detailed request logs
logs := mockServer.GetRequestLog()
for _, log := range logs {
    fmt.Printf("%s %s at %v\n", log.Method, log.Path, log.Timestamp)
}
```

### Error Simulation
```go
// Enable 50% error rate
mockServer.EnableErrorSimulation(mock.ErrorSimulation{
    InternalErrors: true,
    ErrorRate:      0.5,
})

// Test error handling
_, err := bucket.Put("test.txt", []byte("content"))
// err may or may not be nil depending on simulation

// Disable errors
mockServer.DisableErrorSimulation()
```

### Range Requests
```go
// Create large file
largeContent := make([]byte, 10000)
mockServer.PutObject("large-file.bin", largeContent)

// Test range read through client
reader, err := bucket.OpenRange("large-file.bin", "", 1000, 2000)
// Returns bytes 1000-2000
```

### Multipart Upload Testing
```go
uploader := &s3.Uploader{
    Key:    key,
    Bucket: "test-bucket",
    Object: "large-file.bin",
}

err := uploader.Start()
// Upload ID available in mock server

err = uploader.Upload(1, part1Data)
err = uploader.Upload(2, part2Data)

finalETag, err := uploader.Close()
// Object now exists in mock server
```

## Thread Safety

The mock server is fully thread-safe and supports concurrent operations:

```go
// Safe to use from multiple goroutines
go func() { mockServer.PutObject("file1.txt", data1) }()
go func() { mockServer.PutObject("file2.txt", data2) }()
go func() { content, _ := mockServer.ObjectContent("file1.txt") }()
```

## AWS Compatibility

The mock server provides AWS-compatible responses:

- **XML Response Formats** - Matches AWS S3 XML schemas
- **HTTP Status Codes** - Proper 200, 404, 403, 400, 206 responses
- **Headers** - ETag, Content-Type, Last-Modified, Content-Range
- **Error Responses** - AWS-style error XML with proper error codes
- **Query Parameters** - Supports all S3 query parameters (prefix, delimiter, max-keys, etc.)

This ensures that tests using the mock server will behave identically to tests against real AWS S3.
