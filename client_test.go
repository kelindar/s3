package s3

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kelindar/s3/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func TestClient(t *testing.T) {
	t.Run("conditional retry", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "match", r.Header.Get("If-Match"))
			assert.Equal(t, "Tue, 15 Nov 1994 08:12:31 GMT", r.Header.Get("If-Unmodified-Since"))
			assert.Contains(t, r.Header.Get("Authorization"), "SignedHeaders=host;if-match;if-unmodified-since;x-amz-content-sha256;x-amz-date")
			body, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			assert.Equal(t, "payload", string(body))
			if attempts.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, "retry")
				return
			}
			w.Header().Set("ETag", "updated")
		}))
		defer server.Close()
		key := aws.DeriveKey(server.URL, "access", "secret", "us-east-1", "s3")
		etag, applied, err := NewBucket(key, "bucket").WriteIf(context.Background(), "object", []byte("payload"), func(header http.Header) error {
			header.Set("If-Match", "match")
			header.Set("If-Unmodified-Since", "Tue, 15 Nov 1994 08:12:31 GMT")
			return nil
		})
		require.NoError(t, err)
		assert.True(t, applied)
		assert.Equal(t, "updated", etag)
		assert.EqualValues(t, 2, attempts.Load())
	})

	t.Run("range headers", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "bytes=2-4", r.Header.Get("Range"))
			assert.Equal(t, "match", r.Header.Get("If-Match"))
			assert.Contains(t, r.Header.Get("Authorization"), "SignedHeaders=host;if-match;x-amz-content-sha256;x-amz-date")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = io.WriteString(w, "cde")
		}))
		defer server.Close()
		key := aws.DeriveKey(server.URL, "access", "secret", "us-east-1", "s3")
		reader := &Reader{Key: key, Bucket: "bucket", Path: "object", ETag: "match"}
		body, err := reader.RangeReader(2, 3)
		require.NoError(t, err)
		defer body.Close()
		contents, err := io.ReadAll(body)
		require.NoError(t, err)
		assert.Equal(t, "cde", string(contents))
	})

	t.Run("copy headers and retry", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "id+/=", r.URL.Query().Get("uploadId"))
			assert.Equal(t, "/bucket/a%20b%25", r.Header.Get("X-Amz-Copy-Source"))
			assert.Equal(t, "match", r.Header.Get("X-Amz-Copy-Source-If-Match"))
			assert.Equal(t, "bytes=1-5242880", r.Header.Get("X-Amz-Copy-Source-Range"))
			assert.Contains(t, r.Header.Get("Authorization"), "SignedHeaders=host;x-amz-content-sha256;x-amz-copy-source;x-amz-copy-source-if-match;x-amz-copy-source-range;x-amz-date")
			if attempts.Add(1) == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = io.WriteString(w, "retry")
				return
			}
			_, _ = io.WriteString(w, `<CopyPartResult><ETag>copied</ETag></CopyPartResult>`)
		}))
		defer server.Close()
		key := aws.DeriveKey(server.URL, "access", "secret", "us-east-1", "s3")
		u := &uploader{Key: key, Bucket: "bucket", Object: "object", Scheme: "http", Host: strings.TrimPrefix(server.URL, "http://"), id: "id+/=", started: true}
		require.NoError(t, u.CopyFrom(context.Background(), 1, &Reader{Bucket: "bucket", Path: "a b%", ETag: "match", Size: MinPartSize + 1}, 1, MinPartSize+1))
		u.bg.Wait()
		require.NoError(t, u.asyncerr)
		require.Len(t, u.parts, 1)
		assert.Equal(t, "copied", u.parts[0].ETag)
		assert.EqualValues(t, 2, attempts.Load())
	})

	t.Run("abort does not retry", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			assert.Equal(t, http.MethodDelete, r.Method)
			assert.Equal(t, "id+/=", r.URL.Query().Get("uploadId"))
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer server.Close()
		key := aws.DeriveKey(server.URL, "access", "secret", "us-east-1", "s3")
		u := &uploader{Key: key, Bucket: "bucket", Object: "object", Scheme: "http", Host: strings.TrimPrefix(server.URL, "http://"), id: "id+/=", started: true}
		assert.Error(t, u.Abort(context.Background()))
		assert.EqualValues(t, 1, attempts.Load())
		assert.True(t, u.started)
	})

	t.Run("connection reuse", func(t *testing.T) {
		for _, name := range []string{"background", "cancel", "deadline"} {
			t.Run(name, func(t *testing.T) {
				var connections atomic.Int32
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.WriteString(w, "body")
				}))
				server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
					if state == http.StateNew {
						connections.Add(1)
					}
				}
				server.Start()
				defer server.Close()

				ctx := context.Background()
				switch name {
				case "cancel":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					defer cancel()
				case "deadline":
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, time.Second)
					defer cancel()
				}
				key := aws.DeriveKey(server.URL, "access", "secret", "us-east-1", "s3")
				for range 2 {
					res, err := doSigned(ctx, key, http.MethodGet, server.URL, nil)
					require.NoError(t, err)
					_, err = io.Copy(io.Discard, res.Body)
					closeErr := res.Body.Close()
					require.NoError(t, err)
					require.NoError(t, closeErr)
				}
				assert.EqualValues(t, 1, connections.Load(), "a live context must not disable connection reuse")
			})
		}
	})

	t.Run("header timeout excludes body", func(t *testing.T) {
		// Scale the default header limit down to avoid waiting a minute in the test.
		const headerTimeout = 100 * time.Millisecond
		previous := defaultClient
		client := newClient(previous.Dial)
		client.ReadTimeout = headerTimeout
		defaultClient = client
		defer func() {
			client.CloseIdleConnections()
			defaultClient = previous
		}()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "4")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			time.Sleep(3 * headerTimeout)
			_, _ = io.WriteString(w, "body")
		}))
		defer server.Close()
		key := aws.DeriveKey(server.URL, "access", "secret", "us-east-1", "s3")
		res, err := doSigned(context.Background(), key, http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		assert.NoError(t, err)
		assert.Equal(t, "body", string(body))
	})

	t.Run("header timeout enforced", func(t *testing.T) {
		previous := defaultClient
		client := newClient(previous.Dial)
		client.ReadTimeout = 20 * time.Millisecond
		defaultClient = client
		defer func() {
			client.CloseIdleConnections()
			defaultClient = previous
		}()
		release := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-release
		}))
		defer server.Close()
		defer close(release)
		req := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(req)
		req.SetRequestURI(server.URL)
		res, err := doFastRequest(context.Background(), req)
		assert.Nil(t, res)
		assert.ErrorIs(t, err, fasthttp.ErrTimeout)
	})

	t.Run("released cancellation", func(t *testing.T) {
		var connections atomic.Int32
		var requests atomic.Int32
		release := make(chan struct{})
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "4")
			if requests.Add(1) == 2 {
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				<-release
			}
			_, _ = io.WriteString(w, "body")
		}))
		server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
			if state == http.StateNew {
				connections.Add(1)
			}
		}
		server.Start()
		defer server.Close()
		defer close(release)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		key := aws.DeriveKey(server.URL, "access", "secret", "us-east-1", "s3")
		first, err := doSigned(ctx, key, http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, first.Body)
		closeErr := first.Body.Close()
		require.NoError(t, err)
		require.NoError(t, closeErr)
		second, err := doSigned(context.Background(), key, http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		defer second.Body.Close()
		cancel()
		time.Sleep(20 * time.Millisecond)
		release <- struct{}{}
		body, err := io.ReadAll(second.Body)
		assert.NoError(t, err)
		assert.Equal(t, "body", string(body))
		assert.EqualValues(t, 1, connections.Load())
	})

	t.Run("cancel during dial", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		peers := make(chan net.Conn, 1)
		previous := defaultClient
		client := newClient(func(string) (net.Conn, error) {
			close(started)
			<-release
			conn, peer := net.Pipe()
			_ = peer.SetReadDeadline(time.Now().Add(time.Second))
			peers <- peer
			return conn, nil
		})
		defaultClient = client
		defer func() {
			client.CloseIdleConnections()
			defaultClient = previous
		}()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		key := aws.DeriveKey("", "access", "secret", "us-east-1", "s3")
		finished := make(chan error, 1)
		go func() {
			res, err := doSigned(ctx, key, http.MethodGet, "http://127.0.0.1:1", nil)
			if res != nil {
				_ = res.Body.Close()
			}
			finished <- err
		}()
		<-started
		cancel()
		select {
		case err := <-finished:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Error("request did not stop during dialing")
		}
		close(release)
		peer := <-peers
		defer peer.Close()
		var data [1]byte
		_, err := peer.Read(data[:])
		assert.ErrorIs(t, err, io.EOF, "a lease acquired after cancellation must be closed")
	})

	t.Run("close blocked read", func(t *testing.T) {
		release := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "4")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-release
		}))
		defer server.Close()
		defer close(release)
		key := aws.DeriveKey(server.URL, "access", "secret", "us-east-1", "s3")
		res, err := doSigned(context.Background(), key, http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		defer res.Body.Close()
		finished := make(chan error, 1)
		go func() {
			_, err := io.ReadAll(res.Body)
			finished <- err
		}()
		assert.Eventually(t, func() bool {
			if res.Body.readLock.TryLock() {
				res.Body.readLock.Unlock()
				return false
			}
			return true
		}, time.Second, time.Millisecond)
		assert.NoError(t, res.Body.Close())
		select {
		case err := <-finished:
			assert.Error(t, err)
		case <-time.After(time.Second):
			t.Error("body read did not stop after closing")
		}
	})

	t.Run("blocked body cancellation", func(t *testing.T) {
		for _, test := range []struct {
			name string
			err  error
		}{
			{name: "cancel", err: context.Canceled},
			{name: "deadline", err: context.DeadlineExceeded},
		} {
			t.Run(test.name, func(t *testing.T) {
				release := make(chan struct{})
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Length", "4")
					w.WriteHeader(http.StatusOK)
					w.(http.Flusher).Flush()
					<-release
				}))
				defer server.Close()
				defer close(release)
				ctx, cancel := context.WithCancel(context.Background())
				if test.err == context.DeadlineExceeded {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
				}
				defer cancel()
				key := aws.DeriveKey(server.URL, "access", "secret", "us-east-1", "s3")
				res, err := doSigned(ctx, key, http.MethodGet, server.URL, nil)
				require.NoError(t, err)
				defer res.Body.Close()
				if test.err == context.Canceled {
					stop := time.AfterFunc(20*time.Millisecond, cancel)
					defer stop.Stop()
				}
				_, err = io.ReadAll(res.Body)
				assert.ErrorIs(t, err, test.err)
			})
		}
	})

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
		req := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(req)
		req.SetRequestURI(server.URL + "/a/../b%2Fc")
		req.Header.Set("If-Match", "match")
		res, err := doFastRequest(context.Background(), req)
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
		req := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(req)
		req.SetRequestURI(server.URL)
		res, err := doFastRequest(ctx, req)
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
		req := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(req)
		req.SetRequestURI(server.URL)
		res, err := doFastRequest(ctx, req)
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
		req := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(req)
		req.SetRequestURI(server.URL)
		finished := make(chan error, 1)
		go func() {
			res, err := doFastRequest(ctx, req)
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
