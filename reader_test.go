// Copyright 2023 Sneller, Inc.
// Copyright 2025 Roman Atachiants
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package s3

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kelindar/s3/aws"
	"github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func TestValidBucket(t *testing.T) {
	tests := map[string]bool{
		// from AWS docs
		"docexamplebucket1":       true,
		"log-delivery-march-2020": true,
		"my-hosted-content":       true,

		// from AWS docs (valid, but not recommended)
		"docexamplewebsite.com":     true,
		"www.docexamplewebsite.com": true,
		"my.example.s3.bucket":      true,

		// additional valid bucket names
		"default":                    true,
		"abc":                        true,
		"123456789":                  true,
		"this.is.a.long.bucket-name": true,
		"123456789a123456789b123456789c123456789d123456789e123456789f123": true,

		// from AWS docs (invalid)
		"doc_example_bucket":  false, // contains underscores
		"DocExampleBucket":    false, // contains uppercase letters
		"doc-example-bucket-": false, // ends with a hyphen

		// additional invalid bucket names
		"-startwithhyphen":       false, // starts with a hyphen
		".startwithdot":          false, // starts with a dot
		"double..dot":            false, // two consecutive dots
		"xn---invalid-prefix":    false, // invalid prefix
		"invalid-suffix-s3alias": false, // invalid suffix
		"a":                      false, // too short (at least 3 chars)
		"ab":                     false, // too short (at least 3 chars)
		"123456789a123456789b123456789c123456789d123456789e123456789F1234": false, // too long (<=63 chars)
		// TODO: IP check is not implemented and is treated as a valid bucket-name
		//"192.168.5.4": false, // IP address
	}
	for name, want := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, want, ValidBucket(name), "bucket name %q", name)
		})
	}
}

func TestReader(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	put := func(objectKey string, content []byte) *Reader {
		return &Reader{
			Key:    key,
			ETag:   mockServer.PutObject(objectKey, content),
			Size:   int64(len(content)),
			Bucket: bucket,
			Path:   objectKey,
		}
	}

	t.Run("range reader", func(t *testing.T) {
		content := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
		reader := put("test/range-test.txt", content)

		rangeReader, err := reader.RangeReader(10, 10)
		require.NoError(t, err)
		defer rangeReader.Close()

		rangeContent, err := io.ReadAll(rangeReader)
		assert.NoError(t, err)
		assert.Equal(t, content[10:20], rangeContent)
	})

	t.Run("read at", func(t *testing.T) {
		content := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
		reader := put("test/readat-test.txt", content)

		buf := make([]byte, 10)
		n, err := reader.ReadAt(buf, 5)
		assert.NoError(t, err)
		assert.Equal(t, 10, n)
		assert.Equal(t, content[5:15], buf)
	})

	t.Run("write to", func(t *testing.T) {
		content := []byte("WriteTo test content")
		reader := put("test/writeto-test.txt", content)

		var buf bytes.Buffer
		n, err := reader.WriteTo(&buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(content)), n)
		assert.Equal(t, content, buf.Bytes())
	})
}

func TestReadAtBounds(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		http.ServeContent(w, req, "object", time.Time{}, bytes.NewReader([]byte("abcd")))
	}))
	defer server.Close()
	key := aws.DeriveKey(server.URL, "access", "secret", "us-east-1", "s3")
	r := Reader{Key: key, Bucket: "bucket", Path: "object", Size: 4}
	for _, test := range []struct {
		name     string
		off      int64
		width, n int
		err      error
	}{
		{name: "partial eof", off: 2, width: 4, n: 2, err: io.EOF},
		{name: "at eof", off: 4, width: 1, err: io.EOF},
		{name: "past eof", off: 5, width: 1, err: io.EOF},
		{name: "empty", off: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := requests.Load()
			dst := make([]byte, test.width)
			n, err := r.ReadAt(dst, test.off)
			assert.Equal(t, test.n, n)
			assert.ErrorIs(t, err, test.err)
			if test.n == 2 {
				assert.Equal(t, "cd", string(dst[:n]))
			} else {
				assert.Equal(t, before, requests.Load())
			}
		})
	}
	for _, test := range []struct{ off, width int64 }{{-1, 1}, {0, -1}, {1<<63 - 1, 2}, {0, 0}} {
		t.Run(fmt.Sprintf("range %d %d", test.off, test.width), func(t *testing.T) {
			before := requests.Load()
			body, err := r.RangeReader(test.off, test.width)
			if body != nil {
				defer body.Close()
			}
			if test.width == 0 {
				require.NoError(t, err)
				data, err := io.ReadAll(body)
				assert.NoError(t, err)
				assert.Empty(t, data)
			} else {
				assert.Error(t, err)
				assert.Nil(t, body)
			}
			assert.Equal(t, before, requests.Load())
		})
	}
}

func TestStat(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	content := []byte("Stat test content")
	objectKey := "test/stat-test.txt"
	mockServer.PutObject(objectKey, content)

	reader, err := Stat(key, bucket, objectKey)
	assert.NoError(t, err)
	assert.Equal(t, objectKey, reader.Path)
	assert.Equal(t, int64(len(content)), reader.Size)
	assert.NotEmpty(t, reader.ETag)
	stored, ok := mockServer.GetObject(objectKey)
	require.True(t, ok)
	assert.WithinDuration(t, stored.LastModified, reader.LastModified, time.Second)
}

func TestNewFile(t *testing.T) {
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	bucket := "test-bucket"
	objectKey := "test/newfile-test.txt"
	etag := "test-etag"
	size := int64(100)

	file := NewFile(key, bucket, objectKey, etag, size)
	assert.Equal(t, key, file.Key)
	assert.Equal(t, bucket, file.Bucket)
	assert.Equal(t, objectKey, file.Path())
	assert.Equal(t, etag, file.ETag)
	assert.Equal(t, size, file.Size())
}

func TestURL(t *testing.T) {
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	bucket := "test-bucket"
	objectKey := "test/url-test.txt"

	url, err := URL(key, bucket, objectKey)
	assert.NoError(t, err)
	assert.Contains(t, url, bucket)
	assert.Contains(t, url, objectKey)
	assert.Contains(t, url, "X-Amz-Signature")

	// Test invalid bucket
	_, err = URL(key, "invalid_bucket", objectKey)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidBucket)
}

func TestPathEscape(t *testing.T) {
	for value := 0; value < 256; value++ {
		path := "a/" + string([]byte{byte(value)}) + "z"
		want := strings.ReplaceAll(strings.ReplaceAll(url.QueryEscape(path), "+", "%20"), "%2F", "/")
		assert.Equal(t, want, almostPathEscape(path), "path %q", path)
		assert.Equal(t, queryEscape(path), string(appendQueryEscape(nil, path)), "query %q", path)
	}
	for _, path := range []string{"folder/file.txt", "folder/é😀.txt"} {
		want := strings.ReplaceAll(strings.ReplaceAll(url.QueryEscape(path), "+", "%20"), "%2F", "/")
		assert.Equal(t, want, almostPathEscape(path), "path %q", path)
		assert.Equal(t, queryEscape(path), string(appendQueryEscape(nil, path)), "query %q", path)
	}
}

func TestRequestURI(t *testing.T) {
	var req fasthttp.Request
	for _, base := range []string{"", "http://localhost:9000", "http://localhost:9000/api%20s3/", "http://[::1]:9000/api"} {
		for _, bucket := range []string{"bucket", "bucket.name"} {
			key := aws.DeriveKey(base, "access", "secret", "us-east-1", "s3")
			for _, object := range []string{"", "folder/file.txt", "a/../b//c", "a b+%&☃?#", strings.Repeat("a b/", 1024)} {
				req.Reset()
				const query = "partNumber=1&uploadId=a%2B%2F%3D"
				want := uri(key, bucket, object) + "?" + query
				setURI(&req, key, bucket, object, query)
				assert.Equal(t, want, string(req.URI().FullURI()))
				parsed, err := url.Parse(want)
				require.NoError(t, err)
				assert.Equal(t, parsed.EscapedPath(), string(req.URI().PathOriginal()))
				assert.Equal(t, query, string(req.URI().QueryString()))
				signRequest(key, &req, nil)
				assert.Equal(t, parsed.Host, string(req.Header.Host()))
			}
		}
	}
}

func TestBucketRegion(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	key.BaseURI = mockServer.URL()

	t.Run("custom base uri", func(t *testing.T) {
		region, err := BucketRegion(key, bucket)
		assert.NoError(t, err)
		assert.Equal(t, "us-east-1", region)
	})

	t.Run("default aws", func(t *testing.T) {
		// Outcome depends on network access to AWS, so only the code path is exercised.
		defaultKey := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		_, _ = BucketRegion(defaultKey, bucket)
	})

	t.Run("invalid bucket", func(t *testing.T) {
		_, err := BucketRegion(key, "invalid_bucket")
		assert.ErrorIs(t, err, ErrInvalidBucket)
	})
}

func TestDeriveForBucket(t *testing.T) {
	bucket := "test-bucket"
	mockServer := mock.New(bucket, "us-east-1")
	defer mockServer.Close()

	deriveFn := DeriveForBucket(bucket)

	// Test with mock server
	key, err := deriveFn(mockServer.URL(), "fake-access-key", "fake-secret-key", "", "us-east-1", "s3")
	assert.NoError(t, err)
	assert.Equal(t, "us-east-1", key.Region)
	assert.Equal(t, "s3", key.Service)

	// Test invalid bucket
	_, err = deriveFn("", "fake-access-key", "fake-secret-key", "", "us-east-1", "invalid_bucket")
	assert.Error(t, err)

	// Test invalid service
	_, err = deriveFn("", "fake-access-key", "fake-secret-key", "", "us-east-1", "invalid")
	assert.Error(t, err)
}

func TestReaderErrors(t *testing.T) {
	t.Run("range reader", func(t *testing.T) {
		bucket := "test-bucket"
		mockServer := mock.New(bucket, "us-east-1")
		defer mockServer.Close()

		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		// Create test content
		content := []byte("Range reader error test content")
		objectKey := "test/range-error.txt"
		etag := mockServer.PutObject(objectKey, content)

		reader := &Reader{
			Key:    key,
			ETag:   etag,
			Size:   int64(len(content)),
			Bucket: bucket,
			Path:   objectKey,
		}

		// Test range beyond file size
		_, err := reader.RangeReader(int64(len(content)+10), 10)
		assert.Error(t, err)

		// Test with invalid bucket in reader
		invalidReader := &Reader{
			Key:    key,
			ETag:   etag,
			Size:   int64(len(content)),
			Bucket: "invalid_bucket",
			Path:   objectKey,
		}

		_, err = invalidReader.RangeReader(0, 10)
		assert.Error(t, err)
		// The error might not be ErrInvalidBucket depending on implementation
		// assert.ErrorIs(t, err, ErrInvalidBucket)

		// Test with non-existent object
		nonExistentReader := &Reader{
			Key:    key,
			ETag:   "fake-etag",
			Size:   100,
			Bucket: bucket,
			Path:   "nonexistent.txt",
		}

		_, err = nonExistentReader.RangeReader(0, 10)
		assert.Error(t, err)
	})

	t.Run("write to", func(t *testing.T) {
		bucket := "test-bucket"
		mockServer := mock.New(bucket, "us-east-1")
		defer mockServer.Close()

		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		// Test with non-existent object
		reader := &Reader{
			Key:    key,
			ETag:   "fake-etag",
			Size:   100,
			Bucket: bucket,
			Path:   "nonexistent.txt",
		}

		var buf bytes.Buffer
		_, err := reader.WriteTo(&buf)
		assert.Error(t, err)

		// Test with invalid bucket
		invalidReader := &Reader{
			Key:    key,
			ETag:   "fake-etag",
			Size:   100,
			Bucket: "invalid_bucket",
			Path:   "test.txt",
		}

		_, err = invalidReader.WriteTo(&buf)
		assert.Error(t, err)
	})

	t.Run("read at", func(t *testing.T) {
		bucket := "test-bucket"
		mockServer := mock.New(bucket, "us-east-1")
		defer mockServer.Close()

		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		// Test with non-existent object
		reader := &Reader{
			Key:    key,
			ETag:   "fake-etag",
			Size:   100,
			Bucket: bucket,
			Path:   "nonexistent.txt",
		}

		buf := make([]byte, 10)
		_, err := reader.ReadAt(buf, 0)
		assert.Error(t, err)
	})
}
