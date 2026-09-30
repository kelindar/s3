package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/kelindar/s3"
	"github.com/kelindar/s3/aws"
	"github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
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

func BenchmarkUploadTransport(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		switch {
		case r.Method == http.MethodPost && query.Has("uploads"):
			_, _ = io.WriteString(w, `<InitiateMultipartUploadResult><Bucket>`+bucketName+`</Bucket><Key>upload/multipart</Key><UploadId>id</UploadId></InitiateMultipartUploadResult>`)
		case r.Method == http.MethodPut && query.Has("partNumber"):
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
		case r.Method == http.MethodPost && query.Has("uploadId"):
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = io.WriteString(w, `<CompleteMultipartUploadResult><ETag>complete</ETag></CompleteMultipartUploadResult>`)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	key := aws.DeriveKey(server.URL, "bench-access", "bench-secret", region, "s3")
	bucket := s3.NewBucket(key, bucketName)
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

func BenchmarkStaticListTransport(b *testing.B) {
	seed := mock.New(bucketName, region)
	seed.PopulateTestData(listingData("listing", 1000))
	response, err := http.Get(seed.URL() + "/" + bucketName + "?list-type=2&prefix=listing%2F")
	require.NoError(b, err)
	body, err := io.ReadAll(response.Body)
	require.NoError(b, err)
	require.NoError(b, response.Body.Close())
	seed.Close()

	run := func(b *testing.B, serverURL string) {
		key := aws.DeriveKey("", "bench-access", "bench-secret", region, "s3")
		key.BaseURI = serverURL
		bucket := s3.NewBucket(key, bucketName)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			entries, err := bucket.ReadDir("listing")
			if err != nil || len(entries) != 1000 {
				b.Fatalf("got %d entries: %v", len(entries), err)
			}
		}
	}

	standard := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	b.Run("nethttp", func(b *testing.B) { run(b, standard.URL) })
	standard.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(b, err)
	fast := &fasthttp.Server{Handler: func(ctx *fasthttp.RequestCtx) {
		ctx.SetContentType("application/xml")
		ctx.Response.SetBodyRaw(body)
	}}
	done := make(chan error, 1)
	go func() { done <- fast.Serve(listener) }()
	b.Run("fasthttp", func(b *testing.B) { run(b, "http://"+listener.Addr().String()) })
	require.NoError(b, fast.Shutdown())
	require.NoError(b, <-done)
}

func BenchmarkAdaptedPutTransport(b *testing.B) {
	server := mock.New(bucketName, region)
	defer server.Close()
	standard := httptest.NewServer(server)
	defer standard.Close()
	server.SetRequestLogging(false)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(b, err)
	fast := &fasthttp.Server{Handler: fasthttpadaptor.NewFastHTTPHandler(server)}
	done := make(chan error, 1)
	go func() { done <- fast.Serve(listener) }()
	defer func() {
		require.NoError(b, fast.Shutdown())
		require.NoError(b, <-done)
	}()

	for _, endpoint := range []struct{ name, url string }{
		{"nethttp", standard.URL},
		{"fasthttp-adapter", "http://" + listener.Addr().String()},
	} {
		b.Run(endpoint.name, func(b *testing.B) {
			key := aws.DeriveKey("", "bench-access", "bench-secret", region, "s3")
			key.BaseURI = endpoint.url
			bucket := s3.NewBucket(key, bucketName)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := bucket.Write(context.Background(), "put/object", []byte("payload")); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRawClientTransport(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("payload"))
	}))
	defer server.Close()
	b.Run("nethttp", func(b *testing.B) {
		client := &http.Client{}
		b.ReportAllocs()
		for range b.N {
			request, err := http.NewRequest(http.MethodGet, server.URL, nil)
			if err != nil {
				b.Fatal(err)
			}
			response, err := client.Do(request)
			if err != nil {
				b.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			if err := response.Body.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("fasthttp", func(b *testing.B) {
		client := &fasthttp.Client{}
		request := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(request)
		response := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseResponse(response)
		request.SetRequestURI(server.URL)
		request.Header.SetMethod(fasthttp.MethodGet)
		b.ReportAllocs()
		for range b.N {
			if err := client.Do(request, response); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMockClientPair(b *testing.B) {
	server := mock.New(bucketName, region)
	defer server.Close()
	server.SetRequestLogging(false)
	server.PutObject("object", []byte("payload"))
	uri := server.URL() + "/" + bucketName + "/object"

	b.Run("nethttp", func(b *testing.B) {
		client := &http.Client{}
		b.ReportAllocs()
		for range b.N {
			response, err := client.Get(uri)
			if err != nil {
				b.Fatal(err)
			}
			_, err = io.Copy(io.Discard, response.Body)
			if err != nil {
				b.Fatal(err)
			}
			if err = response.Body.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("fasthttp", func(b *testing.B) {
		client := &fasthttp.Client{}
		request := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(request)
		response := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseResponse(response)
		request.SetRequestURI(uri)
		request.Header.SetMethod(http.MethodGet)
		b.ReportAllocs()
		for range b.N {
			if err := client.Do(request, response); err != nil {
				b.Fatal(err)
			}
			if !bytes.Equal(response.Body(), []byte("payload")) {
				b.Fatal("unexpected body")
			}
		}
	})
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
