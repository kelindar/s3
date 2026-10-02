package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	sdkaws "github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/kelindar/bench"
	"github.com/kelindar/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSuite(t *testing.T) {
	file := filepath.Join(t.TempDir(), "bench.json")
	bench.Run(suite, bench.WithFile(file), bench.WithReference(), bench.WithSamples(2), bench.WithDuration(time.Nanosecond), bench.WithBootstrap(100))
	data, err := os.ReadFile(file)
	require.NoError(t, err)
	var results map[string]bench.Result
	require.NoError(t, json.Unmarshal(data, &results))
	assert.Len(t, results, 16)
	for _, name := range []string{
		"sign/v4", "sign/body", "sign/url", "s3/put", "s3/put-if", "s3/delete", "s3/get", "s3/read-copy", "s3/head", "s3/range",
		"s3/list/100", "s3/list/1k", "fs/glob", "s3/upload-small", "s3/upload-multipart", "s3/compose",
	} {
		require.Contains(t, results, name)
		assert.Len(t, results[name].Samples, 2)
	}
}

func TestListingData(t *testing.T) {
	data := listingData("listing", 2)
	require.Len(t, data, 2)
	require.Contains(t, data, "listing/file-0000.txt")
	require.Contains(t, data, "listing/file-0001.txt")
}

func BenchmarkWriteContext(b *testing.B) {
	for _, name := range []string{"background", "cancel", "deadline"} {
		b.Run(name, func(b *testing.B) {
			bucket, server := mockBucket()
			defer server.Close()
			ref, close := reference(server.URL())
			defer close()
			ctx := context.Background()
			switch name {
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
			case "deadline":
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Hour)
				defer cancel()
			}
			payload := []byte("payload")
			compare(b, int64(len(payload)), func() error {
				_, err := bucket.Write(ctx, "object", payload)
				return err
			}, func() error {
				_, err := ref.PutObject(ctx, &sdk.PutObjectInput{Bucket: sdkaws.String(bucketName), Key: sdkaws.String("object"), Body: bytes.NewReader(payload)})
				return err
			})
		})
	}
}

func BenchmarkUploadMultipart(b *testing.B) {
	bucket, server := mockBucket()
	defer server.Close()
	ref, close := reference(server.URL())
	defer close()
	payload := bytes.Repeat([]byte("x"), 2*s3.MinPartSize+(128<<10))
	reader := bytes.NewReader(payload)
	compare(b, int64(len(payload)), func() error {
		return bucket.WriteFrom(context.Background(), "upload/multipart", reader, int64(len(payload)))
	}, func() error {
		return referenceUpload(context.Background(), ref, "upload/multipart", reader, int64(len(payload)))
	})
}

func BenchmarkList(b *testing.B) {
	for _, count := range []int{100, 1000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			bucket, server := mockBucket()
			defer server.Close()
			ref, close := reference(server.URL())
			defer close()
			server.PopulateTestData(listingData("listing", count))
			compare(b, 0, func() error {
				entries, err := bucket.ReadDir("listing")
				if err == nil && len(entries) != count {
					return fmt.Errorf("got %d entries, want %d", len(entries), count)
				}
				return err
			}, func() error {
				entries, err := referenceList(context.Background(), ref, "listing/")
				if err == nil && len(entries) != count {
					return fmt.Errorf("SDK got %d entries, want %d", len(entries), count)
				}
				return err
			})
		})
	}
}

func BenchmarkCompose(b *testing.B) {
	bucket, server := mockBucket()
	defer server.Close()
	ref, close := reference(server.URL())
	defer close()
	data := bytes.Repeat([]byte("x"), s3.MinPartSize)
	etag := server.PutObject("source/object", data)
	parts := []s3.CopyPart{{SourceKey: "source/object", ETag: etag, Size: int64(len(data))}}
	compare(b, int64(len(data)), func() error {
		_, err := bucket.Compose(context.Background(), "composed/object", parts)
		return err
	}, func() error {
		_, err := referenceCompose(context.Background(), ref, "composed/object", parts)
		return err
	})
}

func compare(b *testing.B, size int64, own, reference func() error) {
	for _, test := range []struct {
		name string
		run  func() error
	}{{"s3", own}, {"sdk", reference}} {
		b.Run(test.name, func(b *testing.B) {
			require.NoError(b, test.run())
			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := test.run(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
