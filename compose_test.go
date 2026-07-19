package s3

import (
	"context"
	"testing"

	"github.com/kelindar/s3/aws"
	"github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/require"
)

func TestCompose(t *testing.T) {
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
}
