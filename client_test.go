package s3

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kelindar/s3/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	t.Run("signed cancellation", func(t *testing.T) {
		key := aws.DeriveKey("", "access", "secret", "us-east-1", "s3")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		res, err := doSigned(ctx, key, http.MethodGet, "http://127.0.0.1:1", nil)
		assert.Nil(t, res)
		assert.ErrorIs(t, err, context.Canceled)
		res, err = doSigned(nil, key, http.MethodGet, "http://127.0.0.1:1", nil)
		assert.Nil(t, res)
		assert.Error(t, err)
	})

	t.Run("signed request", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "/a/../b%2Fc%2B%26", r.URL.EscapedPath())
			assert.Equal(t, "a=1&b=%2B", r.URL.RawQuery)
			assert.Equal(t, http.MethodPut, r.Method)
			assert.Equal(t, "session-token", r.Header.Get("X-Amz-Security-Token"))
			assert.Equal(t, "UNSIGNED-PAYLOAD", r.Header.Get("X-Amz-Content-Sha256"))
			assert.Contains(t, r.Header.Get("Authorization"), "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token")
			_, err := time.Parse("20060102T150405Z", r.Header.Get("X-Amz-Date"))
			assert.NoError(t, err)
			body, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			assert.Equal(t, []byte("payload"), body)
			_, _ = w.Write([]byte("response"))
		}))
		defer server.Close()
		key := aws.DeriveKey(server.URL, "access", "secret", "us-east-1", "s3")
		key.Token = "session-token"
		res, err := doSigned(context.Background(), key, http.MethodPut, server.URL+"/a/../b%2Fc%2B%26?a=1&b=%2B", []byte("payload"))
		require.NoError(t, err)
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		assert.Equal(t, []byte("response"), body)
	})

	t.Run("path and response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "/a/../b%2Fc", r.URL.EscapedPath())
			assert.Equal(t, "match", r.Header.Get("If-Match"))
			w.Header().Set("ETag", `"tag"`)
			_, _ = w.Write([]byte("payload"))
		}))
		defer server.Close()
		req, err := http.NewRequest(http.MethodGet, server.URL+"/a/../b%2Fc", nil)
		require.NoError(t, err)
		req.Header.Set("If-Match", "match")
		res, err := doFast(req, nil)
		require.NoError(t, err)
		defer res.Body.Close()
		assert.Equal(t, http.StatusOK, res.StatusCode)
		assert.Equal(t, int64(7), res.ContentLength)
		assert.Equal(t, `"tag"`, res.Header.Get("ETag"))
		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		assert.Equal(t, []byte("payload"), body)
		assert.NoError(t, res.Body.Close())
		_, err = res.Body.Read(make([]byte, 1))
		assert.ErrorIs(t, err, io.ErrClosedPipe)
	})

	t.Run("retry body", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			assert.Equal(t, []byte("payload"), body)
			if attempts.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()
		req, err := http.NewRequest(http.MethodPut, server.URL, bytes.NewReader([]byte("payload")))
		require.NoError(t, err)
		res, err := flakyDo(req, []byte("payload"))
		require.NoError(t, err)
		defer res.Body.Close()
		assert.Equal(t, http.StatusOK, res.StatusCode)
		assert.Equal(t, int32(2), attempts.Load())
	})

	t.Run("native retry body", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			assert.Equal(t, []byte("payload"), body)
			if attempts.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
		}))
		defer server.Close()
		key := aws.DeriveKey(server.URL, "access", "secret", "us-east-1", "s3")
		res, err := doSigned(context.Background(), key, http.MethodPut, server.URL, []byte("payload"))
		require.NoError(t, err)
		defer res.Body.Close()
		assert.Equal(t, http.StatusOK, res.StatusCode)
		assert.Equal(t, int32(2), attempts.Load())
	})

	t.Run("deadline", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(30 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		res, err := doFast(req, nil)
		assert.Nil(t, res)
		require.Error(t, err)
		assert.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	})

	t.Run("cancelled body", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("payload"))
		}))
		defer server.Close()
		ctx, cancel := context.WithCancel(context.Background())
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		res, err := doFast(req, nil)
		require.NoError(t, err)
		cancel()
		var one [1]byte
		_, err = res.Body.Read(one[:])
		assert.ErrorIs(t, err, context.Canceled)
		assert.NoError(t, res.Body.Close())
	})

	t.Run("cancel in flight", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(started)
			<-release
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()
		defer close(release)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		finished := make(chan error, 1)
		go func() {
			res, err := doFast(req, nil)
			if res != nil {
				_ = res.Body.Close()
			}
			finished <- err
		}()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("request did not start")
		}
		cancel()
		select {
		case err := <-finished:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("request did not stop after cancellation")
		}
	})
}
