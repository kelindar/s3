package s3

import (
	"context"
	"testing"

	"github.com/kelindar/s3/aws"
	"github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompose(t *testing.T) {
	t.Run("escaped source key", func(t *testing.T) {
		server := mock.New("test-bucket", "us-east-1")
		defer server.Close()
		key := aws.DeriveKey(server.URL(), "access", "secret", "us-east-1", "s3")
		bucket := NewBucket(key, "test-bucket")
		contents := make([]byte, MinPartSize)
		source := "folder/100% a+&☃"
		etag := server.PutObject(source, contents)

		_, err := bucket.Compose(context.Background(), "output", []CopyPart{{SourceKey: source, ETag: etag, Size: int64(len(contents))}})
		require.NoError(t, err)
		stored, found := server.ObjectContent("output")
		require.True(t, found)
		assert.Equal(t, contents, stored)
		requests := server.GetRequestsWithMethod("PUT")
		require.Len(t, requests, 1)
		assert.Equal(t, "/test-bucket/folder/100%25%20a%2B%26%E2%98%83", requests[0].Headers["X-Amz-Copy-Source"])
	})

	t.Run("merges parts", func(t *testing.T) {
		server := mock.New("test-bucket", "us-east-1")
		defer server.Close()
		key := aws.DeriveKey("", "test", "test", "us-east-1", "s3")
		key.BaseURI = server.URL()
		bucket := NewBucket(key, "test-bucket")
		ctx := context.Background()

		a := make([]byte, MinPartSize)
		b := make([]byte, MinPartSize)
		for i := range a {
			a[i], b[i] = 'a', 'b'
		}
		eta, err := bucket.Write(ctx, "a.log", a)
		require.NoError(t, err)
		etb, err := bucket.Write(ctx, "b.log", b)
		require.NoError(t, err)

		etag, err := bucket.Compose(ctx, "out.log", []CopyPart{
			{SourceKey: "a.log", ETag: eta, Size: int64(len(a))},
			{SourceKey: "b.log", ETag: etb, Size: int64(len(b))},
		})
		require.NoError(t, err)
		require.NotEmpty(t, etag)
		obj, ok := server.GetObject("out.log")
		require.True(t, ok)
		require.Equal(t, append(a, b...), obj.Content)
	})

	t.Run("rejects changed source", func(t *testing.T) {
		server := mock.New("test-bucket", "us-east-1")
		defer server.Close()
		key := aws.DeriveKey("", "test", "test", "us-east-1", "s3")
		key.BaseURI = server.URL()
		bucket := NewBucket(key, "test-bucket")
		ctx := context.Background()

		data := make([]byte, MinPartSize)
		_, err := bucket.Write(ctx, "source.log", data)
		require.NoError(t, err)
		_, err = bucket.Compose(ctx, "out.log", []CopyPart{{SourceKey: "source.log", ETag: "stale", Size: int64(len(data))}})
		require.Error(t, err)
		require.Empty(t, server.ListMultipartUploads())
	})

	t.Run("validates range before starting upload", func(t *testing.T) {
		server := mock.New("test-bucket", "us-east-1")
		defer server.Close()
		key := aws.DeriveKey("", "test", "test", "us-east-1", "s3")
		key.BaseURI = server.URL()
		bucket := NewBucket(key, "test-bucket")
		part := CopyPart{SourceKey: "source.log", ETag: "etag", Offset: int64(^uint64(0) >> 1), Size: MinPartSize}
		_, err := bucket.Compose(context.Background(), "out.log", []CopyPart{part})
		require.Error(t, err)
		require.Zero(t, server.RequestCount())
	})
}
