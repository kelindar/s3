<p align="center">
<img width="200" height="110" src=".github/logo.png" border="0" alt="kelindar/s3">
<br>
<img src="https://img.shields.io/github/go-mod/go-version/kelindar/s3" alt="Go Version">
<a href="https://pkg.go.dev/github.com/kelindar/s3"><img src="https://pkg.go.dev/badge/github.com/kelindar/s3" alt="PkgGoDev"></a>
<a href="https://goreportcard.com/report/github.com/kelindar/s3"><img src="https://goreportcard.com/badge/github.com/kelindar/s3" alt="Go Report Card"></a>
<a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License"></a>
<a href="https://coveralls.io/github/kelindar/s3"><img src="https://coveralls.io/repos/github/kelindar/s3/badge.svg" alt="Coverage"></a>
</p>

# Slim AWS S3 (and compatible) client

A lightweight, high-performance AWS S3 client library for Go that implements the standard `fs.FS` interface, allowing you to work with S3 buckets as if they were local filesystems.

> **Attribution**: This library is extracted from [Sneller's lightweight S3 client](https://github.com/SnellerInc/sneller/tree/main/aws/s3). Most of the credit goes to the Sneller team for the original implementation and design.

## Features

- **Standard `fs.FS` Interface**: Compatible with any Go code that accepts `fs.FS`
- **Lightweight**: Minimal dependencies, focused on performance
- **Range Reads**: Efficient partial file reading with HTTP range requests
- **Multi-part Uploads**: Support for large file uploads
- **Pattern Matching**: Built-in glob pattern support for file listing
- **Context Support**: Full context cancellation support
- **Lazy Loading**: Optional HEAD-only requests until actual read
- **Multiple Auth Methods**: Environment variables, IAM roles, manual keys

**Use When:**
- ✅ Building applications that need to treat S3 as a filesystem (compatible with `fs.FS`)
- ✅ Requiring lightweight, minimal-dependency S3 operations
- ✅ Working with large files that benefit from range reads and multipart uploads

**Not For:**
- ❌ Applications requiring the full AWS SDK feature set (SQS, DynamoDB, etc.)
- ❌ Use cases requiring advanced S3 features (bucket policies, lifecycle rules, object locking, versioning management, etc.)
- ❌ Projects that need official AWS support and enterprise features

## Installation

```bash
go get github.com/kelindar/s3
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "io"
    "io/fs"
    
    "github.com/kelindar/s3"
    "github.com/kelindar/s3/aws"
)

func main() {
    // Create signing key from ambient credentials
    key, err := aws.AmbientKey("s3", s3.DeriveForBucket("my-bucket"))
    if err != nil {
        panic(err)
    }

    // Create Bucket instance
    bucket := s3.NewBucket(context.Background(), key, "my-bucket")
    
    // Upload a file
    etag, err := bucket.Put("hello.txt", []byte("Hello, World!"))
    if err != nil {
        panic(err)
    }
    fmt.Printf("Uploaded with ETag: %s\n", etag)
    
    // Read the file back
    file, err := bucket.Open("hello.txt")
    if err != nil {
        panic(err)
    }
    defer file.Close()
    
    content, err := io.ReadAll(file)
    if err != nil {
        panic(err)
    }
    fmt.Printf("Content: %s\n", content)
}
```

## Authentication

### Ambient Credentials (Recommended)

The library automatically discovers credentials from:
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- IAM roles (EC2, ECS, Lambda)
- AWS credentials file (`~/.aws/credentials`)
- Web identity tokens

```go
key, err := aws.AmbientKey("s3", s3.DeriveForBucket("my-bucket"))
```

### Manual Credentials

```go
key := aws.DeriveKey(
    "",                    // baseURI (empty for AWS S3)
    "your-access-key",     // AWS Access Key ID
    "your-secret-key",     // AWS Secret Key
    "us-east-1",          // AWS Region
    "s3",                 // Service
)
```

## Usage Examples

### File Operations

```go
import (
    "errors"
    "io"
    "io/fs"
)

// Upload a file
etag, err := bucket.Put("path/to/file.txt", []byte("content"))

// Read a file
file, err := bucket.Open("path/to/file.txt")
if err != nil {
    panic(err)
}
defer file.Close()
content, err := io.ReadAll(file)

// Check if file exists
_, err := bucket.Open("path/to/file.txt")
if errors.Is(err, fs.ErrNotExist) {
    fmt.Println("File does not exist")
}
```

### Directory Operations

```go
import (
    "fmt"
    "io/fs"
)

// List directory contents
entries, err := fs.ReadDir(bucket, "path/to/directory")
for _, entry := range entries {
    fmt.Printf("%s (dir: %t)\n", entry.Name(), entry.IsDir())
}

// Walk directory tree
err = fs.WalkDir(bucket, ".", func(path string, d fs.DirEntry, err error) error {
    if err != nil {
        return err
    }
    fmt.Printf("Found: %s\n", path)
    return nil
})
```

### Pattern Matching

```go
import (
    "fmt"
    "io/fs"
    "github.com/kelindar/s3/fsutil"
)

// Find all .txt files
err := fsutil.WalkGlob(bucket, "", "*.txt", func(path string, f fs.File, err error) error {
    if err != nil {
        return err
    }
    defer f.Close()
    fmt.Printf("Text file: %s\n", path)
    return nil
})
```

### Range Reads

```go
import "io"

// Read first 1KB of a file
reader, err := bucket.OpenRange("large-file.dat", "", 0, 1024)
if err != nil {
    panic(err)
}
defer reader.Close()

data, err := io.ReadAll(reader)
```

### Multi-part Upload

```go
import "github.com/kelindar/s3"

uploader := &s3.Uploader{
    Key:         key,
    Bucket:      "my-bucket",
    Object:      "large-file.dat",
    ContentType: "application/octet-stream",
}

// Start upload
err := uploader.Start()
if err != nil {
    panic(err)
}

// Upload parts (minimum 5MB each, except last)
err = uploader.Upload(1, part1Data) // []byte with len >= 5MB
err = uploader.Upload(2, part2Data)

// Complete upload
err = uploader.Close(nil)
```

### Bucket Options

```go
bucket := s3.NewBucket(ctx, key, "my-bucket")
bucket.Client = httpClient   // Optional: Custom HTTP client
bucket.Lazy = true           // Optional: Use HEAD instead of GET for Open()
```

### Working with Subdirectories

```go
import "io/fs"

// Create a sub-filesystem for a specific prefix
subFS, err := bucket.Sub("data/2023/")
if err != nil {
    panic(err)
}

// Now work within that prefix
files, err := fs.ReadDir(subFS, ".")
```

## Error Handling

The library uses standard Go `fs` package errors:

```go
import (
    "errors"
    "fmt"
    "io/fs"
)

file, err := bucket.Open("nonexistent.txt")
if errors.Is(err, fs.ErrNotExist) {
    fmt.Println("File not found")
} else if errors.Is(err, fs.ErrPermission) {
    fmt.Println("Access denied")
}
```

## Testing

Set environment variables for integration tests:

```bash
export AWS_TEST_BUCKET=your-test-bucket
go test ./...
```

## License

Licensed under the Apache License, Version 2.0. See LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
