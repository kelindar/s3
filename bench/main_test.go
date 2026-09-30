package main

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/kelindar/s3"
	"github.com/stretchr/testify/require"
)

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
			_, err := bucket.Write(ctx, "object", payload)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := bucket.Write(ctx, "object", payload); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkUploadMultipart(b *testing.B) {
	bucket, server := mockBucket()
	defer server.Close()
	payload := bytes.Repeat([]byte("x"), 2*s3.MinPartSize+(128<<10))
	reader := bytes.NewReader(payload)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := bucket.WriteFrom(context.Background(), "upload/multipart", reader, int64(len(payload))); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkList(b *testing.B) {
	for _, count := range []int{100, 1000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			bucket, server := mockBucket()
			defer server.Close()
			server.PopulateTestData(listingData("listing", count))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				entries, err := bucket.ReadDir("listing")
				if err != nil || len(entries) != count {
					b.Fatalf("got %d entries: %v", len(entries), err)
				}
			}
		})
	}
}

func BenchmarkCompose(b *testing.B) {
	bucket, server := mockBucket()
	defer server.Close()
	data := bytes.Repeat([]byte("x"), s3.MinPartSize)
	etag := server.PutObject("source/object", data)
	parts := []s3.CopyPart{{SourceKey: "source/object", ETag: etag, Size: int64(len(data))}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := bucket.Compose(context.Background(), "composed/object", parts); err != nil {
			b.Fatal(err)
		}
	}
}
