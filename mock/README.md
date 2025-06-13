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
- **Content Type Detection** - Automatic MIME type detection based on file extensions
- **Metadata Support** - Store and retrieve custom object metadata

### Testing Integration
- **Thread-safe** - Concurrent operations supported with proper mutex protection
- **Easy Setup/Teardown** - Simple lifecycle management with `New()` and `Close()`
- **Test Data Population** - Bulk data loading utilities with `PopulateTestData()`
- **Request Verification** - Comprehensive request logging and inspection methods
- **AWS-Compatible** - Proper XML response formats and HTTP status codes
- **testify/assert Integration** - All examples use `github.com/stretchr/testify/assert`

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
    mockServer := mock.New("test-bucket", "us-east-1")
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
    assert.NotEmpty(t, etag)

    // Verify object exists in mock server
    assert.True(t, mockServer.ObjectExists("test.txt"))

    file, err := bucket.Open("test.txt")
    assert.NoError(t, err)
    defer file.Close()

    content, err := io.ReadAll(file)
    assert.NoError(t, err)
    assert.Equal(t, []byte("Hello, World!"), content)

    // Verify request logging
    assert.True(t, mockServer.HasRequestWithMethod("PUT"))
    assert.True(t, mockServer.HasRequestWithMethod("GET"))
}
```

## API Reference

### Server

#### Constructor
```go
func New(bucket, region string) *Server
```

#### Lifecycle Methods
```go
func (m *Server) URL() string           // Get server URL
func (m *Server) Close()                // Shutdown server
func (m *Server) Clear()                // Clear all data
```

#### Object Management
```go
func (m *Server) PutObject(key string, content []byte) string
func (m *Server) PutObjectWithMetadata(key string, content []byte, metadata map[string]string) string
func (m *Server) GetObject(key string) (*Object, bool)
func (m *Server) DeleteObject(key string) bool
func (m *Server) ListObjects(prefix string) []string
func (m *Server) SetObjectMetadata(key string, metadata map[string]string) bool
func (m *Server) GetObjectMetadata(key string) (map[string]string, bool)
```

#### Test Utilities
```go
func (m *Server) PopulateTestData(data map[string][]byte)
func (m *Server) ObjectExists(key string) bool
func (m *Server) ObjectContent(key string) ([]byte, bool)
func (m *Server) RequestCount() int
func (m *Server) HasRequestWithMethod(method string) bool
func (m *Server) GetRequestsWithMethod(method string) []RequestLog
func (m *Server) GetRequestLog() []RequestLog
```

#### Error Simulation
```go
func (m *Server) EnableErrorSimulation(config ErrorSimulation)
func (m *Server) DisableErrorSimulation()

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
func (m *Server) GetMultipartUpload(uploadID string) (*Multipart, bool)
func (m *Server) ListMultipartUploads() map[string]*Multipart
```

#### Data Types
```go
type Object struct {
    Content      []byte
    ETag         string
    LastModified time.Time
    ContentType  string
    Metadata     map[string]string
}

type Multipart struct {
    ID       string
    Bucket   string
    Key      string
    Parts    map[int]*PartInfo
    Created  time.Time
    Metadata map[string]string
}

type RequestLog struct {
    Method    string
    Path      string
    Query     string
    Headers   map[string]string
    Body      []byte
    Timestamp time.Time
}
```

## Usage Examples

### Basic Object Operations
```go
mockServer := mock.New("test-bucket", "us-east-1")
defer mockServer.Close()

// Put object
etag := mockServer.PutObject("path/to/file.txt", []byte("content"))

// Put object with metadata
metadata := map[string]string{"author": "test", "version": "1.0"}
etag = mockServer.PutObjectWithMetadata("path/to/file.txt", []byte("content"), metadata)

// Check existence
exists := mockServer.ObjectExists("path/to/file.txt")

// Get content
content, found := mockServer.ObjectContent("path/to/file.txt")

// Get object with metadata
obj, found := mockServer.GetObject("path/to/file.txt")
if found {
    fmt.Printf("ETag: %s, Content-Type: %s\n", obj.ETag, obj.ContentType)
}

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

// Get requests by method
putRequests := mockServer.GetRequestsWithMethod("PUT")
for _, req := range putRequests {
    fmt.Printf("PUT %s with body size: %d\n", req.Path, len(req.Body))
}

// Get detailed request logs
logs := mockServer.GetRequestLog()
for _, log := range logs {
    fmt.Printf("%s %s?%s at %v\n", log.Method, log.Path, log.Query, log.Timestamp)
}
```

### Error Simulation
```go
// Enable 50% error rate for internal errors
mockServer.EnableErrorSimulation(mock.ErrorSimulation{
    InternalErrors: true,
    ErrorRate:      0.5,
})

// Test error handling
_, err := bucket.Put("test.txt", []byte("content"))
// err may or may not be nil depending on simulation

// Enable different types of errors
mockServer.EnableErrorSimulation(mock.ErrorSimulation{
    NetworkErrors:    true,
    NotFoundErrors:   true,
    PermissionErrors: true,
    ErrorRate:        0.3, // 30% error rate
})

// Disable all error simulation
mockServer.DisableErrorSimulation()
```

### Range Requests
```go
// Create large file
largeContent := make([]byte, 10000)
for i := range largeContent {
    largeContent[i] = byte(i % 256)
}
mockServer.PutObject("large-file.bin", largeContent)

// Test range read through client (if supported by client)
// The mock server automatically handles HTTP Range headers
// and returns proper 206 Partial Content responses
```

### Multipart Upload Testing
```go
uploader := &s3.Uploader{
    Key:    key,
    Bucket: "test-bucket",
    Object: "large-file.bin",
}

err := uploader.Start()
assert.NoError(t, err)

// Upload ID is now available in mock server
uploadID := uploader.ID()
upload, exists := mockServer.GetMultipartUpload(uploadID)
assert.True(t, exists)
assert.Equal(t, "large-file.bin", upload.Key)

// Upload parts
part1Data := make([]byte, s3.MinPartSize)
part2Data := make([]byte, s3.MinPartSize)

err = uploader.Upload(1, part1Data)
assert.NoError(t, err)
err = uploader.Upload(2, part2Data)
assert.NoError(t, err)

// Complete upload
err = uploader.Close(nil)
assert.NoError(t, err)

// Verify object exists in mock server
assert.True(t, mockServer.ObjectExists("large-file.bin"))

// Verify upload was cleaned up
_, exists = mockServer.GetMultipartUpload(uploadID)
assert.False(t, exists)
```

## Thread Safety

The mock server is fully thread-safe and supports concurrent operations with proper mutex protection:

```go
// Safe to use from multiple goroutines
var wg sync.WaitGroup
data1 := []byte("content1")
data2 := []byte("content2")

wg.Add(3)
go func() {
    defer wg.Done()
    mockServer.PutObject("file1.txt", data1)
}()
go func() {
    defer wg.Done()
    mockServer.PutObject("file2.txt", data2)
}()
go func() {
    defer wg.Done()
    content, _ := mockServer.ObjectContent("file1.txt")
    fmt.Printf("Read %d bytes\n", len(content))
}()
wg.Wait()

// All operations are guaranteed to be thread-safe
assert.Equal(t, 2, len(mockServer.ListObjects("")))
```

## AWS Compatibility

The mock server provides AWS-compatible responses:

- **XML Response Formats** - Matches AWS S3 XML schemas for all operations
- **HTTP Status Codes** - Proper 200, 201, 204, 206, 400, 403, 404, 500 responses
- **Headers** - ETag, Content-Type, Last-Modified, Content-Range, Content-Length
- **Error Responses** - AWS-style error XML with proper error codes and messages
- **Query Parameters** - Supports all S3 query parameters (prefix, delimiter, max-keys, continuation-token, etc.)
- **Multipart Upload Flow** - Complete AWS-compatible multipart upload workflow
- **Content Type Detection** - Automatic MIME type detection based on file extensions
- **Range Requests** - HTTP Range header support with proper 206 Partial Content responses

This ensures that tests using the mock server will behave identically to tests against real AWS S3.

## Integration with testify/assert

All examples in this documentation use `github.com/stretchr/testify/assert` for assertions, which is the recommended testing approach for this codebase:

```go
import (
    "testing"
    "github.com/stretchr/testify/assert"
    "github.com/kelindar/s3/mock"
)

func TestExample(t *testing.T) {
    mockServer := mock.New("test-bucket", "us-east-1")
    defer mockServer.Close()

    etag := mockServer.PutObject("test.txt", []byte("content"))
    assert.NotEmpty(t, etag)
    assert.True(t, mockServer.ObjectExists("test.txt"))

    content, found := mockServer.ObjectContent("test.txt")
    assert.True(t, found)
    assert.Equal(t, []byte("content"), content)
}
```
