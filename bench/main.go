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
	"path"
	"time"

	sdkaws "github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
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
	bench.Run(suite, bench.WithFile("bench.gob"), bench.WithConfidence(95), bench.WithReference())
}

func suite(b *bench.B) {
	benchSign(b)
	benchWrite(b)
	benchRead(b)
	benchList(b)
	benchGlob(b)
	benchUpload(b)
	benchCompose(b)
}

func benchSign(b *bench.B) {
	key := aws.DeriveKey("", "bench-access", "bench-secret", region, "s3")
	method := []byte("PUT")
	path := []byte("/object")
	host := []byte("bench-bucket.s3.us-east-1.amazonaws.com")
	body := []byte("benchmark payload")
	var storage [512]byte
	ctx := context.Background()
	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	credentials := sdkaws.Credentials{AccessKeyID: key.AccessKey, SecretAccessKey: key.Secret}
	target := "https://bench-bucket.s3.us-east-1.amazonaws.com/object"

	for _, test := range []struct {
		name string
		body []byte
	}{{"sign/v4", nil}, {"sign/body", body}} {
		stamp, hash, auth := key.Sign(storage[:0], method, path, nil, host, test.body)
		when, err := time.Parse("20060102T150405Z", string(stamp))
		check(err)
		req, err := http.NewRequest(http.MethodPut, target, nil)
		check(err)
		req.Header.Set("X-Amz-Content-Sha256", hash)
		check(signer.SignHTTP(ctx, credentials, req, hash, "s3", region, when))
		if req.Header.Get("Authorization") != string(auth) {
			fail(test.name + ": SDK authorization differs")
		}
		b.Run(test.name, func(int) {
			key.Sign(storage[:0], method, path, nil, host, test.body)
		}, func(int) {
			check(signer.SignHTTP(ctx, credentials, req, hash, "s3", region, time.Now()))
		})
	}
	req, err := http.NewRequest(http.MethodGet, target+"?X-Amz-Expires=3600", nil)
	check(err)
	b.Run("sign/url", func(int) {
		_, err := key.SignURL(target, time.Hour)
		check(err)
	}, func(int) {
		_, _, err := signer.PresignHTTP(ctx, credentials, req, "UNSIGNED-PAYLOAD", "s3", region, time.Now())
		check(err)
	})
}

func benchWrite(b *bench.B) {
	bucket, server := mockBucket()
	defer server.Close()
	ref, close := reference(server.URL())
	defer close()
	ctx := context.Background()
	payload := []byte("benchmark payload")
	wantETag := server.PutObject("put/object", payload)

	b.Run("s3/put", func(int) {
		etag, err := bucket.Write(ctx, "put/object", payload)
		check(err)
		if etag != wantETag {
			fail("s3/put: unexpected ETag")
		}
	}, func(int) {
		out, err := ref.PutObject(ctx, &sdk.PutObjectInput{Bucket: sdkaws.String(bucketName), Key: sdkaws.String("put/object"), Body: bytes.NewReader(payload)})
		check(err)
		if sdkaws.ToString(out.ETag) != wantETag {
			fail("s3/put: unexpected SDK ETag")
		}
	})

	etag := server.PutObject("put-if/object", payload)
	condition := s3.IfMatch(etag)
	b.Run("s3/put-if", func(int) {
		_, applied, err := bucket.WriteIf(ctx, "put-if/object", payload, condition)
		check(err)
		if !applied {
			fail("conditional write was not applied")
		}
	}, func(int) {
		_, err := ref.PutObject(ctx, &sdk.PutObjectInput{Bucket: sdkaws.String(bucketName), Key: sdkaws.String("put-if/object"), Body: bytes.NewReader(payload), IfMatch: sdkaws.String(etag)})
		check(err)
	})

	b.Run("s3/delete", func(int) {
		server.PutObject("delete/object", payload)
		check(bucket.Delete(ctx, "delete/object"))
	}, func(int) {
		server.PutObject("delete/object", payload)
		_, err := ref.DeleteObject(ctx, &sdk.DeleteObjectInput{Bucket: sdkaws.String(bucketName), Key: sdkaws.String("delete/object")})
		check(err)
	})
}

func benchRead(b *bench.B) {
	bucket, server := mockBucket()
	defer server.Close()
	ref, close := reference(server.URL())
	defer close()
	ctx := context.Background()
	payload := bytes.Repeat([]byte("x"), 64<<10)
	etag := server.PutObject("read/object", payload)
	var buf [32 << 10]byte

	b.Run("s3/get", func(int) {
		file, err := bucket.Open("read/object")
		check(err)
		n, err := read(file, buf[:])
		check(err)
		if n != len(payload) {
			fail(fmt.Sprintf("s3/get: read %d bytes, want %d", n, len(payload)))
		}
		check(file.Close())
	}, func(int) {
		out, err := ref.GetObject(ctx, &sdk.GetObjectInput{Bucket: sdkaws.String(bucketName), Key: sdkaws.String("read/object")})
		check(err)
		n, err := read(out.Body, buf[:])
		check(err)
		if n != len(payload) {
			fail(fmt.Sprintf("s3/get: SDK read %d bytes, want %d", n, len(payload)))
		}
		check(out.Body.Close())
	})

	b.Run("s3/read-copy", func(int) {
		file, err := bucket.Open("read/object")
		check(err)
		n, err := io.Copy(io.Discard, file)
		check(err)
		if n != int64(len(payload)) {
			fail("s3/read-copy: unexpected byte count")
		}
		check(file.Close())
	}, func(int) {
		out, err := ref.GetObject(ctx, &sdk.GetObjectInput{Bucket: sdkaws.String(bucketName), Key: sdkaws.String("read/object")})
		check(err)
		n, err := io.Copy(io.Discard, out.Body)
		check(err)
		if n != int64(len(payload)) {
			fail("s3/read-copy: unexpected SDK byte count")
		}
		check(out.Body.Close())
	})

	bucket.Lazy = true
	b.Run("s3/head", func(int) {
		file, err := bucket.Open("read/object")
		check(err)
		info, err := file.Stat()
		check(err)
		if info.Size() != int64(len(payload)) {
			fail("s3/head: unexpected content length")
		}
		check(file.Close())
	}, func(int) {
		out, err := ref.HeadObject(ctx, &sdk.HeadObjectInput{Bucket: sdkaws.String(bucketName), Key: sdkaws.String("read/object")})
		check(err)
		if sdkaws.ToInt64(out.ContentLength) != int64(len(payload)) {
			fail("s3/head: unexpected SDK content length")
		}
	})

	b.Run("s3/range", func(int) {
		reader, err := bucket.OpenRange("read/object", etag, 0, 1024)
		check(err)
		n, err := io.Copy(io.Discard, reader)
		check(err)
		if n != 1024 {
			fail("s3/range: unexpected byte count")
		}
		check(reader.Close())
	}, func(int) {
		out, err := ref.GetObject(ctx, &sdk.GetObjectInput{Bucket: sdkaws.String(bucketName), Key: sdkaws.String("read/object"), IfMatch: sdkaws.String(etag), Range: sdkaws.String("bytes=0-1023")})
		check(err)
		n, err := io.Copy(io.Discard, out.Body)
		check(err)
		if n != 1024 {
			fail("s3/range: unexpected SDK byte count")
		}
		check(out.Body.Close())
	})
}

func benchList(b *bench.B) {
	benchListing(b, "s3/list/100", "listing", 100)
	benchListing(b, "s3/list/1k", "large", 1000)
}

func benchListing(b *bench.B, name, prefix string, count int) {
	bucket, server := mockBucket()
	defer server.Close()
	ref, close := reference(server.URL())
	defer close()
	server.PopulateTestData(listingData(prefix, count))

	b.Run(name, func(int) {
		entries, err := bucket.ReadDir(prefix)
		check(err)
		if len(entries) != count {
			fail(fmt.Sprintf("%s: got %d entries, want %d", name, len(entries), count))
		}
	}, func(int) {
		entries, err := referenceList(context.Background(), ref, prefix+"/")
		check(err)
		if len(entries) != count {
			fail(fmt.Sprintf("%s: SDK got %d entries, want %d", name, len(entries), count))
		}
	})
}

func benchGlob(b *bench.B) {
	bucket, server := mockBucket()
	defer server.Close()
	ref, close := reference(server.URL())
	defer close()
	data := make(map[string][]byte, 32)
	for i := range 32 {
		data[fmt.Sprintf("glob-%04d.txt", i)] = []byte("x")
	}
	server.PopulateTestData(data)

	b.Run("fs/glob", func(int) {
		count := 0
		check(fsutil.WalkGlob(bucket, "", "glob-*.txt", func(_ string, file fs.File, err error) error {
			if err != nil {
				return err
			}
			count++
			return file.Close()
		}))
		if count != len(data) {
			fail("fs/glob: unexpected match count")
		}
	}, func(int) {
		objects, err := referenceList(context.Background(), ref, "glob-")
		check(err)
		count := 0
		for _, object := range objects {
			match, err := path.Match("glob-*.txt", sdkaws.ToString(object.Key))
			check(err)
			if match {
				count++
			}
		}
		if count != len(data) {
			fail("fs/glob: unexpected SDK match count")
		}
	})
}

func benchUpload(b *bench.B) {
	bucket, server := mockBucket()
	defer server.Close()
	ref, close := reference(server.URL())
	defer close()
	ctx := context.Background()
	payload := bytes.Repeat([]byte("x"), 128<<10)
	reader := bytes.NewReader(payload)

	b.Run("s3/upload-small", func(int) {
		check(bucket.WriteFrom(ctx, "upload/object", reader, int64(len(payload))))
	}, func(int) {
		check(referenceUpload(ctx, ref, "upload/object", reader, int64(len(payload))))
	})

	large := bytes.Repeat([]byte("x"), 2*s3.MinPartSize+(128<<10))
	largeReader := bytes.NewReader(large)
	b.Run("s3/upload-multipart", func(int) {
		check(bucket.WriteFrom(ctx, "upload/multipart", largeReader, int64(len(large))))
	}, func(int) {
		check(referenceUpload(ctx, ref, "upload/multipart", largeReader, int64(len(large))))
	})
}

func benchCompose(b *bench.B) {
	bucket, server := mockBucket()
	defer server.Close()
	ref, close := reference(server.URL())
	defer close()
	ctx := context.Background()
	payload := bytes.Repeat([]byte("x"), s3.MinPartSize)
	etag := server.PutObject("source/object", payload)
	parts := []s3.CopyPart{{SourceKey: "source/object", ETag: etag, Size: int64(len(payload))}}

	b.Run("s3/compose", func(int) {
		_, err := bucket.Compose(ctx, "composed/object", parts)
		check(err)
	}, func(int) {
		_, err := referenceCompose(ctx, ref, "composed/object", parts)
		check(err)
	})
}

func read(reader io.Reader, buf []byte) (int, error) {
	total := 0
	for {
		n, err := reader.Read(buf)
		total += n
		switch err {
		case io.EOF:
			return total, nil
		case nil:
		default:
			return total, err
		}
	}
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
