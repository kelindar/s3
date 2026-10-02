package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	sdkaws "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/kelindar/s3"
	"github.com/kelindar/s3/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSigningURL(t *testing.T) {
	key := aws.DeriveKey("", "bench-access", "bench-secret", region, "s3")
	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	for _, target := range []string{
		"https://bench-bucket.s3.us-east-1.amazonaws.com/object",
		"https://bench-bucket.s3.us-east-1.amazonaws.com/a%20b%2B%E2%98%83?prefix=a%2Bb%20c",
	} {
		t.Run(target, func(t *testing.T) {
			signed, err := key.SignURL(target, time.Hour)
			require.NoError(t, err)
			own, err := url.Parse(signed)
			require.NoError(t, err)
			when, err := time.Parse("20060102T150405Z", own.Query().Get("X-Amz-Date"))
			require.NoError(t, err)
			req, err := http.NewRequest(http.MethodGet, target, nil)
			require.NoError(t, err)
			query := req.URL.Query()
			query.Set("X-Amz-Expires", "3600")
			req.URL.RawQuery = query.Encode()
			signed, _, err = signer.PresignHTTP(context.Background(), sdkaws.Credentials{AccessKeyID: key.AccessKey, SecretAccessKey: key.Secret}, req, "UNSIGNED-PAYLOAD", "s3", region, when)
			require.NoError(t, err)
			reference, err := url.Parse(signed)
			require.NoError(t, err)
			assert.Equal(t, own.EscapedPath(), reference.EscapedPath())
			assert.Equal(t, own.Query(), reference.Query())
		})
	}
}

func TestReference(t *testing.T) {
	bucket, server := mockBucket()
	t.Cleanup(server.Close)
	server.SetRequestLogging(true)
	client, close := reference(server.URL())
	t.Cleanup(close)
	ctx := context.Background()

	t.Run("upload", func(t *testing.T) {
		for _, test := range []struct {
			name           string
			size, requests int
		}{{"empty", 0, 1}, {"small", 128 << 10, 1}, {"exact", s3.MinPartSize, 3}, {"multipart", 2*s3.MinPartSize + (128 << 10), 5}} {
			t.Run(test.name, func(t *testing.T) {
				server.Clear()
				data := make([]byte, test.size)
				for i := range data {
					data[i] = byte(i / s3.MinPartSize)
				}
				require.NoError(t, referenceUpload(ctx, client, "upload/object", bytes.NewReader(data), int64(test.size)))
				requests := server.GetRequestLog()
				require.Len(t, requests, test.requests)
				for i, request := range requests {
					assert.Equal(t, "/"+bucketName+"/upload/object", request.Path)
					if test.size < s3.MinPartSize || i > 0 {
						assert.Equal(t, "UNSIGNED-PAYLOAD", request.Headers["X-Amz-Content-Sha256"])
					}
				}
				if test.size >= s3.MinPartSize {
					assert.Equal(t, http.MethodPost, requests[0].Method)
					assert.Equal(t, http.MethodPost, requests[len(requests)-1].Method)
					if test.name == "multipart" {
						query, err := url.ParseQuery(requests[3].Query)
						require.NoError(t, err)
						assert.Equal(t, "3", query.Get("partNumber"))
					}
				}
				stored, exists := server.ObjectContent("upload/object")
				assert.True(t, exists)
				assert.True(t, bytes.Equal(data, stored))
				file, err := bucket.Open("upload/object")
				require.NoError(t, err)
				got, err := io.ReadAll(file)
				closeErr := file.Close()
				require.NoError(t, err)
				assert.NoError(t, closeErr)
				assert.True(t, bytes.Equal(data, got))
				require.NoError(t, bucket.WriteFrom(ctx, "upload/library", bytes.NewReader(data), int64(len(data))))
				stored, exists = server.ObjectContent("upload/library")
				assert.True(t, exists)
				assert.True(t, bytes.Equal(data, stored))
			})
		}
	})

	t.Run("compose", func(t *testing.T) {
		server.Clear()
		data := bytes.Repeat([]byte("a"), s3.MinPartSize+17)
		etag := server.PutObject("source/a b+☃", data)
		parts := []s3.CopyPart{{SourceKey: "source/a b+☃", ETag: etag, Offset: 17, Size: s3.MinPartSize}}
		_, err := referenceCompose(ctx, client, "composed/object", parts)
		require.NoError(t, err)
		requests := server.GetRequestLog()
		require.Len(t, requests, 3)
		assert.Equal(t, http.MethodPost, requests[0].Method)
		assert.Equal(t, http.MethodPut, requests[1].Method)
		assert.Equal(t, etag, requests[1].Headers["X-Amz-Copy-Source-If-Match"])
		assert.Equal(t, "/"+bucketName+"/"+url.PathEscape(parts[0].SourceKey), requests[1].Headers["X-Amz-Copy-Source"])
		assert.Equal(t, "bytes=17-5242896", requests[1].Headers["X-Amz-Copy-Source-Range"])
		assert.Equal(t, "UNSIGNED-PAYLOAD", requests[2].Headers["X-Amz-Content-Sha256"])
		stored, exists := server.ObjectContent("composed/object")
		assert.True(t, exists)
		assert.Equal(t, data[17:], stored)
		_, err = bucket.Compose(ctx, "composed/library", parts)
		require.NoError(t, err)
		got, exists := server.ObjectContent("composed/library")
		assert.True(t, exists)
		assert.Equal(t, stored, got)
	})

	t.Run("listing", func(t *testing.T) {
		server.Clear()
		server.PopulateTestData(listingData("listing", 1201))
		objects, err := referenceList(ctx, client, "listing/")
		require.NoError(t, err)
		requests := server.GetRequestLog()
		require.Len(t, requests, 2)
		query, err := url.ParseQuery(requests[0].Query)
		require.NoError(t, err)
		assert.Equal(t, "listing/", query.Get("prefix"))
		assert.Equal(t, "/", query.Get("delimiter"))
		assert.Equal(t, "url", query.Get("encoding-type"))
		entries, err := bucket.ReadDir("listing")
		require.NoError(t, err)
		require.Len(t, objects, 1201)
		require.Len(t, entries, len(objects))
		for i, object := range objects {
			file := entries[i].(*s3.File)
			assert.Equal(t, sdkaws.ToString(object.Key), file.Path())
			assert.Equal(t, sdkaws.ToString(object.ETag), file.ETag)
			assert.Equal(t, sdkaws.ToInt64(object.Size), file.Size())
		}
	})

	t.Run("conditions", func(t *testing.T) {
		server.Clear()
		etag := server.PutObject("conditional/object", []byte("before"))
		_, err := client.PutObject(ctx, &sdk.PutObjectInput{Bucket: sdkaws.String(bucketName), Key: sdkaws.String("conditional/object"), IfMatch: sdkaws.String(etag), Body: bytes.NewReader([]byte("after"))})
		require.NoError(t, err)
		_, applied, err := bucket.WriteIf(ctx, "conditional/object", []byte("wrong"), s3.IfMatch(etag))
		require.NoError(t, err)
		assert.False(t, applied)
		stored, exists := server.ObjectContent("conditional/object")
		assert.True(t, exists)
		assert.Equal(t, []byte("after"), stored)
	})

	t.Run("reads", func(t *testing.T) {
		server.Clear()
		data := bytes.Repeat([]byte("0123456789abcdef"), 4096)
		etag := server.PutObject("read/object", data)
		head, err := client.HeadObject(ctx, &sdk.HeadObjectInput{Bucket: sdkaws.String(bucketName), Key: sdkaws.String("read/object")})
		require.NoError(t, err)
		assert.Equal(t, int64(len(data)), sdkaws.ToInt64(head.ContentLength))
		assert.Equal(t, etag, sdkaws.ToString(head.ETag))
		requests := server.GetRequestLog()
		require.Len(t, requests, 1)
		assert.Equal(t, http.MethodHead, requests[0].Method)
		out, err := client.GetObject(ctx, &sdk.GetObjectInput{Bucket: sdkaws.String(bucketName), Key: sdkaws.String("read/object"), IfMatch: sdkaws.String(etag), Range: sdkaws.String("bytes=23-1046")})
		require.NoError(t, err)
		got, err := io.ReadAll(out.Body)
		closeErr := out.Body.Close()
		require.NoError(t, err)
		assert.NoError(t, closeErr)
		assert.Equal(t, data[23:1047], got)
		requests = server.GetRequestLog()
		require.Len(t, requests, 2)
		assert.Equal(t, http.MethodGet, requests[1].Method)
		assert.Equal(t, "bytes=23-1046", requests[1].Headers["Range"])
		assert.Equal(t, etag, requests[1].Headers["If-Match"])
		body, err := bucket.OpenRange("read/object", etag, 23, 1024)
		require.NoError(t, err)
		own, err := io.ReadAll(body)
		closeErr = body.Close()
		require.NoError(t, err)
		assert.NoError(t, closeErr)
		assert.Equal(t, got, own)
	})
}
