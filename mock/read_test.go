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

package mock

import (
	"bytes"
	"encoding/xml"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kelindar/s3"
	"github.com/kelindar/s3/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func TestRangeBounds(t *testing.T) {
	for _, test := range []struct {
		header             string
		length, start, end int64
		invalid            bool
	}{
		{header: "bytes=1-99", length: 4, start: 1, end: 3},
		{header: "bytes=-99", length: 4, end: 3},
		{header: "bytes=-0", length: 4, invalid: true},
		{header: "bytes=4-99", length: 4, invalid: true},
		{header: "bytes=-1", invalid: true},
	} {
		t.Run(test.header, func(t *testing.T) {
			start, end, err := parseRange(test.header, test.length)
			if test.invalid {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.start, start)
			assert.Equal(t, test.end, end)
		})
	}
}

func TestListPagination(t *testing.T) {
	server := New("test-bucket", "us-east-1")
	defer server.Close()
	server.SetRequestLogging(false)
	server.PutObject("dir/a", nil)
	server.PutObject("dir/b", nil)
	server.PutObject("root.txt", nil)

	list := func(query url.Values) ListObjectsV2Response {
		req := httptest.NewRequest(http.MethodGet, "/test-bucket/", nil)
		response := httptest.NewRecorder()
		server.handleListObjects(response, req, query)
		var result ListObjectsV2Response
		assert.NoError(t, xml.Unmarshal(response.Body.Bytes(), &result))
		return result
	}

	first := list(url.Values{"delimiter": {"/"}, "max-keys": {"1"}})
	assert.Equal(t, []CommonPrefix{{Prefix: "dir/"}}, first.CommonPrefixes)
	assert.True(t, first.IsTruncated)
	assert.Equal(t, "dir/b", first.NextContinuationToken)

	second := list(url.Values{"delimiter": {"/"}, "max-keys": {"1"}, "continuation-token": {first.NextContinuationToken}})
	assert.Len(t, second.Contents, 1)
	assert.Equal(t, "root.txt", second.Contents[0].Key)
	assert.Empty(t, second.CommonPrefixes)
	assert.False(t, second.IsTruncated)

	last := list(url.Values{"continuation-token": {"zzzz"}})
	assert.Empty(t, last.Contents)
	assert.False(t, last.IsTruncated)
}

func TestListSnapshotInvalidation(t *testing.T) {
	server := New("test-bucket", "us-east-1")
	defer server.Close()
	server.SetRequestLogging(false)
	list := func() []ObjectInfo {
		response := httptest.NewRecorder()
		server.handleListObjects(response, httptest.NewRequest(http.MethodGet, "/test-bucket/", nil), nil)
		var result ListObjectsV2Response
		require.NoError(t, xml.Unmarshal(response.Body.Bytes(), &result))
		return result.Contents
	}

	server.PutObject("a", []byte("1"))
	require.Len(t, list(), 1)
	server.PutObject("a", []byte("12"))
	contents := list()
	require.Len(t, contents, 1)
	assert.Equal(t, int64(2), contents[0].Size)
	server.PutObject("b", nil)
	contents = list()
	require.Len(t, contents, 2)
	assert.Equal(t, []string{"a", "b"}, []string{contents[0].Key, contents[1].Key})

	put := httptest.NewRequest(http.MethodPut, "/test-bucket/c", strings.NewReader("new"))
	server.ServeHTTP(httptest.NewRecorder(), put)
	assert.Len(t, list(), 3)
	server.DeleteObject("a")
	contents = list()
	require.Len(t, contents, 2)
	assert.Equal(t, []string{"b", "c"}, []string{contents[0].Key, contents[1].Key})
	server.Clear()
	assert.Empty(t, list())
}

func TestListSnapshotConcurrent(t *testing.T) {
	server := New("test-bucket", "us-east-1")
	defer server.Close()
	server.SetRequestLogging(false)
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		for i := range 100 {
			server.PutObject("key-"+strconv.Itoa(i), nil)
		}
	}()
	go func() {
		defer group.Done()
		request := httptest.NewRequest(http.MethodGet, "/test-bucket/", nil)
		for range 100 {
			response := httptest.NewRecorder()
			server.handleListObjects(response, request, nil)
			var result ListObjectsV2Response
			if err := xml.Unmarshal(response.Body.Bytes(), &result); err != nil {
				t.Errorf("decode listing: %v", err)
			}
		}
	}()
	group.Wait()
	assert.Len(t, server.listingSnapshot(), 100)
}

func BenchmarkListingHandler(b *testing.B) {
	for _, count := range []int{100, 1000} {
		server := New("bench-bucket", "us-east-1")
		server.SetRequestLogging(false)
		for i := range count {
			server.PutObject("objects/"+strconv.Itoa(i), nil)
		}
		query := url.Values{"prefix": {"objects/"}, "delimiter": {"/"}, "max-keys": {"1000"}}
		request := httptest.NewRequest(http.MethodGet, "/bench-bucket/", nil)
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				response := httptest.NewRecorder()
				server.handleListObjects(response, request, query)
			}
		})
		server.Close()
	}
}

func BenchmarkListingHandlerDiscard(b *testing.B) {
	server := New("bench-bucket", "us-east-1")
	defer server.Close()
	for i := range 1000 {
		server.PutObject("objects/"+strconv.Itoa(i), nil)
	}
	query := url.Values{"prefix": {"objects/"}, "delimiter": {"/"}}
	request := httptest.NewRequest(http.MethodGet, "/bench-bucket/", nil)
	response := httptest.NewRecorder()
	response.Body = nil
	server.listingSnapshot()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		server.handleListObjects(response, request, query)
	}
}

func BenchmarkFastMockList(b *testing.B) {
	server := New("bench-bucket", "us-east-1")
	defer server.Close()
	standard := httptest.NewServer(server)
	defer standard.Close()
	server.SetRequestLogging(false)
	for i := range 1000 {
		server.PutObject("objects/"+strconv.Itoa(i), nil)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(b, err)
	fast := &fasthttp.Server{Handler: server.fastHandler()}
	done := make(chan error, 1)
	go func() { done <- fast.Serve(listener) }()
	defer func() {
		require.NoError(b, fast.Shutdown())
		require.NoError(b, <-done)
	}()

	for _, endpoint := range []struct{ name, url string }{
		{"nethttp", standard.URL},
		{"fasthttp", "http://" + listener.Addr().String()},
	} {
		b.Run(endpoint.name, func(b *testing.B) {
			key := aws.DeriveKey("", "bench-access", "bench-secret", "us-east-1", "s3")
			key.BaseURI = endpoint.url
			bucket := s3.NewBucket(key, "bench-bucket")
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				entries, err := bucket.ReadDir("objects")
				if err != nil || len(entries) != 1000 {
					b.Fatalf("got %d entries: %v", len(entries), err)
				}
			}
		})
	}
}

func TestListXML(t *testing.T) {
	for _, response := range []ListObjectsV2Response{
		{},
		{
			Name: "bucket&name", Prefix: "folder/<", Delimiter: "/", MaxKeys: 1000, IsTruncated: true,
			Contents:              []ObjectInfo{{Key: "folder/a<&.txt", LastModified: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), ETag: `"tag"`, Size: 12}},
			CommonPrefixes:        []CommonPrefix{{Prefix: "folder/sub&dir/"}},
			NextContinuationToken: "next&token",
		},
		{
			Name:     "bucket😀",
			Contents: []ObjectInfo{{Key: "key😀\t\n\r\x00", LastModified: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}},
		},
	} {
		want, err := xml.Marshal(response)
		require.NoError(t, err)
		var got bytes.Buffer
		require.NoError(t, response.writeTo(&got))
		assert.Equal(t, string(want), got.String())
	}
}

func FuzzListXML(f *testing.F) {
	f.Add("plain/key", `"etag"`)
	f.Add("unicode😀\t\n\r<&", "token'\x00")
	f.Fuzz(func(t *testing.T, key, etag string) {
		response := ListObjectsV2Response{
			Name: "bucket", Prefix: key, NextContinuationToken: etag,
			Contents: []ObjectInfo{{Key: key, ETag: etag, LastModified: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}},
		}
		want, err := xml.Marshal(response)
		require.NoError(t, err)
		var got bytes.Buffer
		require.NoError(t, response.writeTo(&got))
		assert.Equal(t, string(want), got.String())
	})
}

func BenchmarkListObjects(b *testing.B) {
	server := New("bench-bucket", "us-east-1")
	defer server.Close()
	server.SetRequestLogging(false)
	for i := range 1000 {
		server.PutObject("listing/object-"+strconv.Itoa(i), []byte("x"))
	}
	req := httptest.NewRequest(http.MethodGet, "/bench-bucket?list-type=2&prefix=listing%2F", nil)
	b.ReportAllocs()
	for range b.N {
		res := httptest.NewRecorder()
		server.ServeHTTP(res, req)
		if res.Code != http.StatusOK {
			b.Fatal(res.Code)
		}
	}
}
