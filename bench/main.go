// Run with `go run .` from this directory. Use `-bench s3/` to select a prefix.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"time"

	"github.com/kelindar/bench"
	"github.com/kelindar/s3"
	"github.com/kelindar/s3/aws"
	"github.com/kelindar/s3/fsutil"
	"github.com/kelindar/s3/mock"
)

const (
	bucketName = "bench-bucket"
	region     = "us-east-1"
)

func main() {
	bench.Run(func(b *bench.B) {
		benchSign(b)
		benchWrite(b)
		benchRead(b)
		benchList(b)
		benchGlob(b)
		benchUpload(b)
		benchCompose(b)
	}, bench.WithFile("bench.gob"))
}

func benchSign(b *bench.B) {
	key := aws.DeriveKey("", "bench-access", "bench-secret", region, "s3")
	req, err := http.NewRequest(http.MethodPut, "https://bench-bucket.s3.us-east-1.amazonaws.com/object", nil)
	check(err)
	body := []byte("benchmark payload")

	b.Run("sign/v4", func(int) { key.SignV4(req, nil) })
	b.Run("sign/body", func(int) { key.SignV4(req, body) })
	b.Run("sign/raw", func(int) {
		key.SignV4Raw(http.MethodPut, "/object", "", "bench-bucket.s3.us-east-1.amazonaws.com", nil)
	})
	var storage [512]byte
	b.Run("sign/into", func(int) {
		key.SignV4Into(storage[:0], []byte(http.MethodPut), []byte("/object"), nil, []byte("bench-bucket.s3.us-east-1.amazonaws.com"), nil)
	})
	b.Run("sign/url", func(int) {
		_, err := key.SignURL("https://bench-bucket.s3.us-east-1.amazonaws.com/object", time.Hour)
		check(err)
	})
}

func benchWrite(b *bench.B) {
	bucket, server := mockBucket()
	defer server.Close()
	ctx := context.Background()
	payload := []byte("benchmark payload")

	b.Run("s3/put", func(int) {
		_, err := bucket.Write(ctx, "put/object", payload)
		check(err)
	})

	etag := server.PutObject("put-if/object", payload)
	condition := s3.IfMatch(etag)
	b.Run("s3/put-if", func(int) {
		_, applied, err := bucket.WriteIf(ctx, "put-if/object", payload, condition)
		check(err)
		if !applied {
			fail("conditional write was not applied")
		}
	})

	b.Run("s3/delete", func(int) {
		server.PutObject("delete/object", payload)
		check(bucket.Delete(ctx, "delete/object"))
	})
}

func benchRead(b *bench.B) {
	bucket, server := mockBucket()
	defer server.Close()
	payload := bytes.Repeat([]byte("x"), 64<<10)
	etag := server.PutObject("read/object", payload)
	var buf [32 << 10]byte

	b.Run("s3/get", func(int) {
		file, err := bucket.Open("read/object")
		check(err)
		var read int
		for {
			n, err := file.Read(buf[:])
			read += n
			if err == io.EOF {
				break
			}
			check(err)
		}
		if read != len(payload) {
			fail(fmt.Sprintf("s3/get: read %d bytes, want %d", read, len(payload)))
		}
		check(file.Close())
	})

	b.Run("s3/read-copy", func(int) {
		file, err := bucket.Open("read/object")
		check(err)
		_, err = io.Copy(io.Discard, file)
		check(err)
		check(file.Close())
	})

	bucket.Lazy = true
	b.Run("s3/head", func(int) {
		file, err := bucket.Open("read/object")
		check(err)
		check(file.Close())
	})

	b.Run("s3/range", func(int) {
		reader, err := bucket.OpenRange("read/object", etag, 0, 1024)
		check(err)
		_, err = io.Copy(io.Discard, reader)
		check(err)
		check(reader.Close())
	})
}

func benchList(b *bench.B) {
	benchListing(b, "s3/list/100", "listing", 100)
	benchListing(b, "s3/list/1k", "large", 1000)
}

func benchListing(b *bench.B, name, prefix string, count int) {
	bucket, server := mockBucket()
	defer server.Close()
	server.PopulateTestData(listingData(prefix, count))

	b.Run(name, func(int) {
		entries, err := bucket.ReadDir(prefix)
		check(err)
		if len(entries) != count {
			fail(fmt.Sprintf("%s: got %d entries, want %d", name, len(entries), count))
		}
	})
}

func benchGlob(b *bench.B) {
	bucket, server := mockBucket()
	defer server.Close()
	data := make(map[string][]byte, 32)
	for i := range 32 {
		data[fmt.Sprintf("glob-%04d.txt", i)] = []byte("x")
	}
	server.PopulateTestData(data)

	b.Run("fs/glob", func(int) {
		check(fsutil.WalkGlob(bucket, "", "glob-*.txt", func(_ string, file fs.File, err error) error {
			if err != nil {
				return err
			}
			return file.Close()
		}))
	})
}

func benchUpload(b *bench.B) {
	bucket, server := mockBucket()
	defer server.Close()
	ctx := context.Background()
	payload := bytes.Repeat([]byte("x"), 128<<10)
	reader := bytes.NewReader(payload)

	b.Run("s3/upload-small", func(int) {
		check(bucket.WriteFrom(ctx, "upload/object", reader, int64(len(payload))))
	})

	large := bytes.Repeat([]byte("x"), 2*s3.MinPartSize+(128<<10))
	largeReader := bytes.NewReader(large)
	b.Run("s3/upload-multipart", func(int) {
		check(bucket.WriteFrom(ctx, "upload/multipart", largeReader, int64(len(large))))
	})
}

func benchCompose(b *bench.B) {
	bucket, server := mockBucket()
	defer server.Close()
	ctx := context.Background()
	payload := bytes.Repeat([]byte("x"), s3.MinPartSize)
	etag := server.PutObject("source/object", payload)
	parts := []s3.CopyPart{{SourceKey: "source/object", ETag: etag, Size: int64(len(payload))}}

	b.Run("s3/compose", func(int) {
		_, err := bucket.Compose(ctx, "composed/object", parts)
		check(err)
	})
}

func mockBucket() (*s3.Bucket, *mock.Server) {
	server := mock.New(bucketName, region)
	server.SetRequestLogging(false)
	key := aws.DeriveKey("", "bench-access", "bench-secret", region, "s3")
	key.BaseURI = server.URL()
	return s3.NewBucket(key, bucketName), server
}

func listingData(prefix string, count int) map[string][]byte {
	data := make(map[string][]byte, count)
	for i := range count {
		data[fmt.Sprintf("%s/file-%04d.txt", prefix, i)] = []byte("x")
	}
	return data
}

func check(err error) {
	if err != nil {
		fail(err.Error())
	}
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
