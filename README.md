<p align="center">
<img width="300" height="100" src=".github/logo.png" border="0" alt="kelindar/s3">
<br>
<img src="https://img.shields.io/github/go-mod/go-version/kelindar/s3" alt="Go Version">
<a href="https://pkg.go.dev/github.com/kelindar/s3"><img src="https://pkg.go.dev/badge/github.com/kelindar/s3" alt="PkgGoDev"></a>
<a href="https://goreportcard.com/report/github.com/kelindar/s3"><img src="https://goreportcard.com/badge/github.com/kelindar/s3" alt="Go Report Card"></a>
<a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License"></a>
<a href="https://coveralls.io/github/kelindar/s3"><img src="https://coveralls.io/repos/github/kelindar/s3/badge.svg" alt="Coverage"></a>
</p>

## Slim AWS S3 client

A lightweight, high-performance AWS S3 client library for Go that implements the standard `fs.FS` interface, allowing you to work with S3 buckets as if they were local filesystems.

> **Attribution**: This library is extracted from [Sneller's lightweight S3 client](https://github.com/SnellerInc/sneller/tree/main/aws/s3). Most of the credit goes to the Sneller team for the original implementation and design.

### Features

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
- ❌ Requiring advanced S3 features (bucket policies, lifecycle, object locking, versioning, etc.)
- ❌ Projects that need official AWS support and enterprise features


### Quick Start

```go
package main

import (
    "context"
    "fmt"
    "io"
    
    "github.com/kelindar/s3"
    "github.com/kelindar/s3/aws"
)

func main() {
    // Create signing key from ambient credentials
    key, err := aws.AmbientKey("s3", "", s3.DeriveForBucket("my-bucket"))
    if err != nil {
        panic(err)
    }

    // Create Bucket instance
    bucket := s3.NewBucket(key, "my-bucket")
    
    // Upload a file
    etag, err := bucket.Write(context.Background(), "hello.txt", []byte("Hello, World!"))
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

### Ambient Credentials (Recommended)

`AmbientKey` discovers credentials from the following sources:
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- AWS credentials file (`~/.aws/credentials`)
- Web identity tokens

EC2 instance role credentials can be loaded explicitly with `aws.EC2Role`. `AmbientKey` does not query EC2 or ECS metadata.

```go
key, err := aws.AmbientKey("s3", "", s3.DeriveForBucket("my-bucket"))
```

### Manual Credentials

If you prefer to manage credentials manually, you can derive a signing key directly:

```go
key := aws.DeriveKey(
    "",                    // baseURI (empty for AWS S3)
    "your-access-key",     // AWS Access Key ID
    "your-secret-key",     // AWS Secret Key
    "us-east-1",          // AWS Region
    "s3",                 // Service
)
```

### Bucket Options

You can customize the behavior of the bucket by setting options:

```go
bucket := s3.NewBucket(key, "my-bucket")
bucket.Lazy = true           // Optional: Use HEAD instead of GET for Open()
```

### File Operations

If you need to work with files, the library provides standard `fs.FS` operations. Here's an example of uploading, reading, and checking for file existence:

```go
// Upload a file
etag, err := bucket.Write(context.Background(), "path/to/file.txt", []byte("content"))

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

If you need to work with directories, the library provides standard `fs.ReadDirFS` operations. Here's an example of listing directory contents and walking the directory tree:

```go
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

The library supports pattern matching using the `fsutil.WalkGlob` function. Here's an example of finding all `.txt` files:

```go
import (
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

If you need to read a specific range of bytes from a file, you can use the `OpenRange` function. In the following example, we read the first 1KB of a file:

```go
// Read first 1KB of a file
reader, err := bucket.OpenRange("large-file.dat", "", 0, 1024)
if err != nil {
    panic(err)
}
defer reader.Close()

data, err := io.ReadAll(reader)
```

### Multi-part Upload

`WriteFrom` accepts an `io.ReaderAt` and the object size. It uses a single PUT below `s3.MinPartSize` and multipart upload at or above that size:

```go
// Open a large file
file, err := os.Open("large-file.dat")
if err != nil {
    panic(err)
}
defer file.Close()

// Get file size
stat, err := file.Stat()
if err != nil {
    panic(err)
}

// Upload with WriteFrom; this file uses multipart upload.
err = bucket.WriteFrom(context.Background(), "large-file.dat", file, stat.Size())
if err != nil {
    panic(err)
}
```

The `WriteFrom` method automatically:
- Determines optimal part size based on file size
- Uploads parts in parallel for better performance
- Handles multipart upload initialization and completion
- Respects context cancellation for upload control

### Compose Objects

`Compose` joins byte ranges from existing objects with server-side copy. Each `s3.CopyPart` names the source key, its ETag, the offset, and the size. Every part must be at least `s3.MinPartSize`; `Compose` returns the new object's ETag.

```go
ctx := context.Background()
part := make([]byte, s3.MinPartSize)
sourceETag, err := bucket.Write(ctx, "part-1", part)
if err != nil {
    panic(err)
}

etag, err := bucket.Compose(ctx, "combined", []s3.CopyPart{{
    SourceKey: "part-1",
    ETag:      sourceETag,
    Offset:    0,
    Size:      int64(len(part)),
}})
if err != nil {
    panic(err)
}
fmt.Printf("Composed with ETag: %s\n", etag)
```

### Working with Subdirectories

You can work with subdirectories by creating a sub-filesystem using the `Sub` method. In the following example, we create a sub-filesystem for the `data/2023/` prefix and list all files within that prefix:

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

The library uses standard Go `fs` package errors. You can check for specific errors using the `errors.Is` function:

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
