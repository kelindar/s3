package s3

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kelindar/s3/aws"
	"github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComposeConcurrency(t *testing.T) {
	previous := defaultClient
	client := newClient(previous.Dial)
	client.MaxConnsPerHost = 64
	defaultClient = client
	defer func() { client.CloseIdleConnections(); defaultClient = previous }()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
			_, _ = io.WriteString(w, `<InitiateMultipartUploadResult><Bucket>test-bucket</Bucket><Key>out</Key><UploadId>id</UploadId></InitiateMultipartUploadResult>`)
		case r.Method == http.MethodPut:
			time.Sleep(100 * time.Millisecond)
			_, _ = io.WriteString(w, `<CopyPartResult><ETag>etag</ETag></CopyPartResult>`)
		case r.Method == http.MethodGet:
			_, _ = io.WriteString(w, "warm")
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			_, _ = io.WriteString(w, `<CompleteMultipartUploadResult><ETag>final</ETag></CompleteMultipartUploadResult>`)
		}
	}))
	defer server.Close()
	key := aws.DeriveKey(server.URL, "access", "secret", "us-east-1", "s3")

	// Establish the pool sequentially to avoid the Windows TCP accept backlog.
	warm := make([]*response, client.MaxConnsPerHost)
	for i := range warm {
		var err error
		warm[i], err = doObject(context.Background(), key, http.MethodGet, "test-bucket", "warm", nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = warm[i].Body.Close() })
	}
	for _, res := range warm {
		_, err := io.Copy(io.Discard, res.Body)
		require.NoError(t, err)
		require.NoError(t, res.Body.Close())
	}
	parts := make([]CopyPart, 128)
	for i := range parts {
		parts[i] = CopyPart{SourceKey: "source", ETag: "etag", Size: MinPartSize}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	etag, err := NewBucket(key, "test-bucket").Compose(ctx, "out", parts)
	assert.NoError(t, err)
	assert.Equal(t, "final", etag)
}

func TestCompose(t *testing.T) {
	t.Run("cancelled copies abort", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var aborted atomic.Bool
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
				_, _ = io.WriteString(w, `<InitiateMultipartUploadResult><Bucket>test-bucket</Bucket><Key>out</Key><UploadId>id</UploadId></InitiateMultipartUploadResult>`)
			case r.Method == http.MethodPut:
				cancel()
				<-r.Context().Done()
			case r.Method == http.MethodDelete:
				aborted.Store(true)
				w.WriteHeader(http.StatusNoContent)
			default:
				assert.Fail(t, "cancelled upload must not complete")
			}
		}))
		defer server.Close()
		key := aws.DeriveKey(server.URL, "access", "secret", "us-east-1", "s3")
		parts := make([]CopyPart, 128)
		for i := range parts {
			parts[i] = CopyPart{SourceKey: "source", ETag: "etag", Size: MinPartSize}
		}
		_, err := NewBucket(key, "test-bucket").Compose(ctx, "out", parts)
		assert.ErrorIs(t, err, context.Canceled)
		assert.True(t, aborted.Load())
	})

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
