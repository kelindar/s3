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
	"context"
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"io"
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
	"github.com/valyala/fasthttp/fasthttpadaptor"
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

func TestFastUploadPart(t *testing.T) {
	m := New("test-bucket", "us-east-1")
	defer m.Close()
	m.SetRequestLogging(false)
	m.uploads["active"] = &Multipart{Parts: make(map[int]*PartInfo)}

	for _, test := range []struct {
		name, query string
		status      int
	}{
		{"valid", "partNumber=1&uploadId=active", http.StatusOK},
		{"invalid part", "partNumber=0&uploadId=active", http.StatusBadRequest},
		{"missing upload", "partNumber=1&uploadId=missing", http.StatusNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPut, m.URL()+"/test-bucket/object?"+test.query, strings.NewReader("payload"))
			require.NoError(t, err)
			res, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer res.Body.Close()
			assert.Equal(t, test.status, res.StatusCode)
		})
	}
	assert.Equal(t, []byte("payload"), m.uploads["active"].Parts[1].Content)
}

func TestScanCompleteMultipart(t *testing.T) {
	body := []byte(`<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Part><PartNumber>2</PartNumber><ETag>&#34;aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&#34;</ETag></Part></CompleteMultipartUpload>`)
	parts, ok := scanCompleteMultipart(body)
	assert.True(t, ok)
	require.Len(t, parts, 1)
	assert.Equal(t, 2, parts[0].number)
	assert.True(t, parts[0].etagValid)
	_, ok = scanCompleteMultipart(bytes.Replace(body, []byte("&#34;"), []byte("&quot;"), 1))
	assert.False(t, ok)
}

func TestCompleteMultipartFallback(t *testing.T) {
	m := New("test-bucket", "us-east-1")
	defer m.Close()
	m.SetRequestLogging(false)
	content := []byte("payload")
	m.uploads["active"] = &Multipart{Parts: map[int]*PartInfo{
		1: {PartNumber: 1, ETag: generateETag(content), Content: content},
	}}
	body := fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, generateETag(content))
	res, err := http.Post(m.URL()+"/test-bucket/object?uploadId=active", "application/xml", strings.NewReader(body))
	require.NoError(t, err)
	defer res.Body.Close()
	assert.Equal(t, http.StatusOK, res.StatusCode)
	stored, ok := m.ObjectContent("object")
	assert.True(t, ok)
	assert.Equal(t, content, stored)
}

func TestCompleteETag(t *testing.T) {
	content := []byte("payload")
	wrongETag := generateETag([]byte("different payload"))
	for _, test := range []struct {
		name string
		fast bool
	}{
		{name: "fast", fast: true},
		{name: "fallback"},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := New("test-bucket", "us-east-1")
			defer m.Close()
			m.SetRequestLogging(false)
			m.uploads["active"] = &Multipart{Parts: map[int]*PartInfo{
				1: {PartNumber: 1, ETag: generateETag(content), Content: content},
			}}

			var body string
			if test.fast {
				body = fmt.Sprintf(`<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Part><PartNumber>1</PartNumber><ETag>&#34;%s&#34;</ETag></Part></CompleteMultipartUpload>`, wrongETag[1:len(wrongETag)-1])
			} else {
				body = fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, wrongETag)
			}
			res, err := http.Post(m.URL()+"/test-bucket/object?uploadId=active", "application/xml", strings.NewReader(body))
			require.NoError(t, err)
			defer res.Body.Close()
			assert.Equal(t, http.StatusBadRequest, res.StatusCode)
			var failure struct {
				Code string `xml:"Code"`
			}
			require.NoError(t, xml.NewDecoder(res.Body).Decode(&failure))
			assert.Equal(t, "InvalidPart", failure.Code)
			_, exists := m.GetMultipartUpload("active")
			assert.True(t, exists)
			_, exists = m.GetObject("object")
			assert.False(t, exists)
		})
	}
}

func TestFastMultipart(t *testing.T) {
	for _, test := range []struct {
		name    string
		logging bool
	}{
		{name: "direct"},
		{name: "logged", logging: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := New("test-bucket", "us-east-1")
			defer m.Close()
			m.SetRequestLogging(test.logging)
			send := func(method, target string, body []byte) *http.Response {
				req, err := http.NewRequest(method, m.URL()+target, bytes.NewReader(body))
				require.NoError(t, err)
				res, err := http.DefaultClient.Do(req)
				require.NoError(t, err)
				return res
			}
			read := func(res *http.Response) []byte {
				defer res.Body.Close()
				body, err := io.ReadAll(res.Body)
				require.NoError(t, err)
				return body
			}
			start := func() InitiateMultipartUploadResponse {
				res := send(http.MethodPost, "/test-bucket/object?uploads=", nil)
				require.Equal(t, http.StatusOK, res.StatusCode)
				var result InitiateMultipartUploadResponse
				require.NoError(t, xml.Unmarshal(read(res), &result))
				assert.Equal(t, "test-bucket", result.Bucket)
				assert.Equal(t, "object", result.Key)
				return result
			}

			started := start()
			res := send(http.MethodPost, "/test-bucket/object?uploadId="+started.UploadId, []byte("<bad"))
			assert.Equal(t, http.StatusBadRequest, res.StatusCode)
			var malformed struct {
				Code string `xml:"Code"`
			}
			require.NoError(t, xml.Unmarshal(read(res), &malformed))
			assert.Equal(t, "MalformedXML", malformed.Code)
			_, exists := m.GetMultipartUpload(started.UploadId)
			assert.True(t, exists)

			content := []byte("payload")
			res = send(http.MethodPut, "/test-bucket/object?partNumber=1&uploadId="+started.UploadId, content)
			assert.Equal(t, http.StatusOK, res.StatusCode)
			partETag := generateETag(content)
			assert.Equal(t, partETag, res.Header.Get("ETag"))
			read(res)

			complete := fmt.Sprintf(`<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Part><PartNumber>1</PartNumber><ETag>&#34;%s&#34;</ETag></Part></CompleteMultipartUpload>`, partETag[1:len(partETag)-1])
			res = send(http.MethodPost, "/test-bucket/object?uploadId="+started.UploadId, []byte(complete))
			require.Equal(t, http.StatusOK, res.StatusCode)
			var completed CompleteMultipartUploadResponse
			require.NoError(t, xml.Unmarshal(read(res), &completed))
			partHash := md5.Sum(content)
			assert.Equal(t, fmt.Sprintf(`"%x-1"`, md5.Sum(partHash[:])), completed.ETag)
			object, exists := m.GetObject("object")
			require.True(t, exists)
			assert.Equal(t, content, object.Content)
			assert.Equal(t, completed.ETag, object.ETag)

			aborted := start()
			res = send(http.MethodDelete, "/test-bucket/object?uploadId="+aborted.UploadId, nil)
			assert.Equal(t, http.StatusNoContent, res.StatusCode)
			read(res)
			_, exists = m.GetMultipartUpload(aborted.UploadId)
			assert.False(t, exists)

			res = send(http.MethodDelete, "/test-bucket/object?uploadId=missing", nil)
			assert.Equal(t, http.StatusNotFound, res.StatusCode)
			var missing struct {
				Code string `xml:"Code"`
			}
			require.NoError(t, xml.Unmarshal(read(res), &missing))
			assert.Equal(t, "NoSuchUpload", missing.Code)
			if test.logging {
				assert.Equal(t, 7, m.RequestCount())
				return
			}
			assert.Zero(t, m.RequestCount())
		})
	}
}

func TestCopyPartChecksum(t *testing.T) {
	for _, test := range []struct {
		name, rangeHeader string
		multipart         bool
	}{
		{name: "whole object"},
		{name: "whole object range", rangeHeader: "bytes=0-6"},
		{name: "partial range", rangeHeader: "bytes=1-3"},
		{name: "multipart source", multipart: true},
		{name: "multipart source range", rangeHeader: "bytes=0-6", multipart: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			content := []byte("payload")
			source := &Object{Content: content, ETag: generateETag(content)}
			if test.multipart {
				sum := md5.Sum(content)
				source.ETag = fmt.Sprintf(`"%x-1"`, md5.Sum(sum[:]))
			}
			m := &Server{bucket: "test", objects: map[string]*Object{"source": source}}
			upload := &Multipart{Parts: make(map[int]*PartInfo)}
			req := httptest.NewRequest(http.MethodPut, "/test/target", nil)
			req.Header.Set("x-amz-copy-source-if-match", source.ETag)
			req.Header.Set("x-amz-copy-source-range", test.rangeHeader)
			res := httptest.NewRecorder()
			m.handleCopyPart(res, req, upload, 1, "/test/source")
			require.Equal(t, http.StatusOK, res.Code)
			if test.rangeHeader == "bytes=1-3" {
				content = content[1:4]
			}
			expected := fmt.Sprintf(`"%x"`, md5.Sum(content))
			assert.Equal(t, expected, res.Header().Get("ETag"))
			require.Contains(t, upload.Parts, 1)
			assert.Equal(t, expected, upload.Parts[1].ETag)
			assert.Equal(t, content, upload.Parts[1].Content)
		})
	}
}

func TestCopySource(t *testing.T) {
	for _, key := range []string{"folder/a b+&☃", "folder/literal%2Fkey"} {
		t.Run(key, func(t *testing.T) {
			content := []byte("payload")
			m := &Server{bucket: "test", objects: map[string]*Object{key: {Content: content, ETag: generateETag(content)}}}
			upload := &Multipart{Parts: make(map[int]*PartInfo)}
			res := httptest.NewRecorder()
			m.handleCopyPart(res, httptest.NewRequest(http.MethodPut, "/test/target", nil), upload, 1, "/test/"+url.PathEscape(key))
			require.Equal(t, http.StatusOK, res.Code)
			require.Contains(t, upload.Parts, 1)
			assert.Equal(t, content, upload.Parts[1].Content)
		})
	}
	t.Run("invalid escape", func(t *testing.T) {
		m := &Server{bucket: "test"}
		upload := &Multipart{Parts: make(map[int]*PartInfo)}
		res := httptest.NewRecorder()
		m.handleCopyPart(res, httptest.NewRequest(http.MethodPut, "/test/target", nil), upload, 1, "/test/invalid%zz")
		assert.Equal(t, http.StatusBadRequest, res.Code)
		assert.Empty(t, upload.Parts)
	})
}

func TestWriteMultipartXML(t *testing.T) {
	var body bytes.Buffer
	require.NoError(t, writeXMLFields(&body, "InitiateMultipartUploadResult",
		"Bucket", "test", "Key", "a&b<key>", "UploadId", "id-1"))
	var got InitiateMultipartUploadResponse
	require.NoError(t, xml.Unmarshal(body.Bytes(), &got))
	assert.Equal(t, "test", got.Bucket)
	assert.Equal(t, "a&b<key>", got.Key)
	assert.Equal(t, "id-1", got.UploadId)
}

func TestUploadIDConcurrent(t *testing.T) {
	const count = 100
	ids := make(chan string, count)
	var group sync.WaitGroup
	for range count {
		group.Add(1)
		go func() {
			defer group.Done()
			ids <- generateUploadID()
		}()
	}
	group.Wait()
	close(ids)

	seen := make(map[string]struct{}, count)
	for id := range ids {
		assert.NotContains(t, seen, id)
		seen[id] = struct{}{}
	}
}

func TestRequestLogging(t *testing.T) {
	server := New("test-bucket", "us-east-1")
	defer server.Close()
	put := func(content string) {
		req := httptest.NewRequest(http.MethodPut, "/test-bucket/object", strings.NewReader(content))
		server.ServeHTTP(httptest.NewRecorder(), req)
	}

	put("logged")
	logs := server.GetRequestLog()
	assert.Len(t, logs, 1)
	assert.Equal(t, []byte("logged"), logs[0].Body)

	server.SetRequestLogging(false)
	put("not logged")
	assert.Equal(t, 1, server.RequestCount())
	content, ok := server.ObjectContent("object")
	assert.True(t, ok)
	assert.Equal(t, []byte("not logged"), content)

	server.SetRequestLogging(true)
	req := httptest.NewRequest(http.MethodGet, "/test-bucket/object", nil)
	server.ServeHTTP(httptest.NewRecorder(), req)
	assert.Equal(t, 2, server.RequestCount())
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

func TestFastMockParity(t *testing.T) {
	server := New("test-bucket", "us-east-1")
	defer server.Close()
	server.SetRequestLogging(false)
	server.PutObject("dir/key.txt", []byte("payload"))
	server.PutObject("dir/sub/key.txt", []byte("nested"))
	server.PutObject("dir/../odd.txt", []byte("dots"))

	for _, tc := range []struct {
		name, method, target string
		headers              map[string]string
	}{
		{"list", http.MethodGet, "/test-bucket/?list-type=2&prefix=dir%2F&delimiter=%2F", nil},
		{"page", http.MethodGet, "/test-bucket/?list-type=2&prefix=dir%2F&max-keys=1", nil},
		{"get", http.MethodGet, "/test-bucket/dir/key.txt", nil},
		{"head", http.MethodHead, "/test-bucket/dir/key.txt", nil},
		{"range", http.MethodGet, "/test-bucket/dir/key.txt", map[string]string{"Range": "bytes=1-3"}},
		{"if-match", http.MethodGet, "/test-bucket/dir/key.txt", map[string]string{"If-Match": "mismatch"}},
		{"missing", http.MethodGet, "/test-bucket/missing", nil},
		{"dot-segment", http.MethodGet, "/test-bucket/dir/../odd.txt", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, tc.target, nil)
			var fastRequest fasthttp.Request
			fastRequest.SetRequestURI(tc.target)
			fastRequest.Header.SetMethod(tc.method)
			for key, value := range tc.headers {
				request.Header.Set(key, value)
				fastRequest.Header.Set(key, value)
			}
			standard := httptest.NewRecorder()
			server.ServeHTTP(standard, request)
			var ctx fasthttp.RequestCtx
			ctx.Init(&fastRequest, nil, nil)
			server.fastHandler()(&ctx)
			assert.Equal(t, standard.Code, ctx.Response.StatusCode())
			assert.Equal(t, standard.Body.Bytes(), ctx.Response.Body())
			for _, header := range []string{"Content-Type", "Content-Range", "Content-Length", "ETag", "Last-Modified"} {
				if value := standard.Header().Get(header); value != "" {
					assert.Equal(t, value, string(ctx.Response.Header.Peek(header)), header)
				}
			}
		})
	}
	response, err := http.Get(server.URL() + "/test-bucket/dir/../odd.txt")
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.StatusCode)
	assert.Equal(t, []byte("dots"), body)
}

func TestFastWrites(t *testing.T) {
	server := New("test-bucket", "us-east-1")
	defer server.Close()
	server.SetRequestLogging(false)
	var request fasthttp.Request
	request.SetRequestURI("/test-bucket/file")
	request.Header.SetMethod(http.MethodPut)
	request.SetBodyString("first")
	var ctx fasthttp.RequestCtx
	ctx.Init(&request, nil, nil)
	server.fastHandler()(&ctx)
	assert.Equal(t, http.StatusOK, ctx.Response.StatusCode())
	assert.NotEmpty(t, ctx.Response.Header.Peek("ETag"))

	request.SetRequestURI("/test-bucket/xxxx")
	request.SetBodyString("other")
	content, found := server.ObjectContent("file")
	require.True(t, found)
	assert.Equal(t, []byte("first"), content)

	request.Reset()
	request.SetRequestURI("/test-bucket/file")
	request.Header.SetMethod(http.MethodDelete)
	ctx.Init(&request, nil, nil)
	server.fastHandler()(&ctx)
	assert.Equal(t, http.StatusNoContent, ctx.Response.StatusCode())
	_, found = server.ObjectContent("file")
	assert.False(t, found)
}

func TestFastPutConditions(t *testing.T) {
	for _, test := range []struct {
		name, target, match, noneMatch string
		status                         int
	}{
		{"match", "file", generateETag([]byte("before")), "", http.StatusOK},
		{"stale", "file", "stale", "", http.StatusPreconditionFailed},
		{"missing", "missing", "stale", "", http.StatusNotFound},
		{"create", "missing", "", "*", http.StatusOK},
		{"exists", "file", "", "*", http.StatusPreconditionFailed},
		{"invalid", "file", "", "etag", http.StatusBadRequest},
		{"both", "file", "stale", "*", http.StatusPreconditionFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := &Server{bucket: "test", objects: make(map[string]*Object), errors: &ErrorSimulation{}}
			server.noLogging.Store(true)
			server.PutObject("file", []byte("before"))
			request := httptest.NewRequest(http.MethodPut, "/test/"+test.target, strings.NewReader("after"))
			request.Header.Set("If-Match", test.match)
			request.Header.Set("If-None-Match", test.noneMatch)
			standard := httptest.NewRecorder()
			server.handlePutObject(standard, request, test.target)
			server.DeleteObject("missing")
			server.PutObject("file", []byte("before"))
			var fastRequest fasthttp.Request
			fastRequest.SetRequestURI("/test/" + test.target)
			fastRequest.Header.SetMethod(http.MethodPut)
			fastRequest.Header.Set("If-Match", test.match)
			fastRequest.Header.Set("If-None-Match", test.noneMatch)
			fastRequest.SetBodyString("after")
			var ctx fasthttp.RequestCtx
			ctx.Init(&fastRequest, nil, nil)
			server.fastHandler()(&ctx)
			assert.Equal(t, test.status, standard.Code)
			assert.Equal(t, standard.Code, ctx.Response.StatusCode())
			assert.Equal(t, standard.Body.Bytes(), ctx.Response.Body())
			assert.Equal(t, standard.Header().Get("ETag"), string(ctx.Response.Header.Peek("ETag")))
			content, exists := server.ObjectContent(test.target)
			switch {
			case test.status == http.StatusOK:
				assert.True(t, exists)
				assert.Equal(t, []byte("after"), content)
			case test.target == "file":
				assert.True(t, exists)
				assert.Equal(t, []byte("before"), content)
			default:
				assert.False(t, exists)
			}
		})
	}

	t.Run("concurrent create", func(t *testing.T) {
		server := &Server{bucket: "test", objects: make(map[string]*Object), errors: &ErrorSimulation{}}
		server.noLogging.Store(true)
		handler := server.fastHandler()
		statuses := make(chan int, 16)
		for range cap(statuses) {
			go func() {
				var request fasthttp.Request
				request.SetRequestURI("/test/file")
				request.Header.SetMethod(http.MethodPut)
				request.Header.Set("If-None-Match", "*")
				request.SetBodyString("after")
				var ctx fasthttp.RequestCtx
				ctx.Init(&request, nil, nil)
				handler(&ctx)
				statuses <- ctx.Response.StatusCode()
			}()
		}
		var applied int
		for range cap(statuses) {
			if status := <-statuses; status == http.StatusOK {
				applied++
			} else {
				assert.Equal(t, http.StatusPreconditionFailed, status)
			}
		}
		assert.Equal(t, 1, applied)
	})
}

func BenchmarkMultipart(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 2*s3.MinPartSize+(128<<10))
	contents := [][]byte{payload[:s3.MinPartSize], payload[s3.MinPartSize : 2*s3.MinPartSize], payload[2*s3.MinPartSize:]}
	b.Run("receive", func(b *testing.B) {
		var ctx fasthttp.RequestCtx
		var reader bytes.Reader
		b.SetBytes(int64(len(payload)))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			for _, part := range contents {
				reader.Reset(part)
				ctx.Request.SetBodyStream(&reader, len(part))
				stored, err := readFastContent(&ctx)
				if err != nil || len(stored) != len(part) {
					b.Fatalf("read %d bytes: %v", len(stored), err)
				}
			}
		}
	})
	b.Run("checksum", func(b *testing.B) {
		b.SetBytes(int64(len(payload)))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			for _, part := range contents {
				if generateETag(part) == "" {
					b.Fatal("missing ETag")
				}
			}
		}
	})
	b.Run("assemble", func(b *testing.B) {
		server := &Server{bucket: "bench-bucket", objects: make(map[string]*Object), uploads: make(map[string]*Multipart)}
		upload := &Multipart{Parts: make(map[int]*PartInfo)}
		var body strings.Builder
		body.WriteString(`<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		for i, content := range contents {
			etag := generateETag(content)
			upload.Parts[i+1] = &PartInfo{PartNumber: i + 1, ETag: etag, Content: content}
			fmt.Fprintf(&body, `<Part><PartNumber>%d</PartNumber><ETag>&#34;%s&#34;</ETag></Part>`, i+1, etag[1:len(etag)-1])
		}
		body.WriteString(`</CompleteMultipartUpload>`)
		encoded := []byte(body.String())
		b.SetBytes(int64(len(payload)))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			server.uploads["active"] = upload
			if _, failure := server.completeMultipart("active", "object", encoded); failure != nil {
				b.Fatal(failure)
			}
		}
	})
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

func BenchmarkConditionalPutHandler(b *testing.B) {
	server := &Server{bucket: "test", objects: make(map[string]*Object), errors: &ErrorSimulation{}}
	server.noLogging.Store(true)
	etag := server.PutObject("file", []byte("payload"))
	for _, handler := range []struct {
		name string
		run  fasthttp.RequestHandler
	}{
		{"adapter", fasthttpadaptor.NewFastHTTPHandler(server)},
		{"native", server.fastHandler()},
	} {
		b.Run(handler.name, func(b *testing.B) {
			var request fasthttp.Request
			request.SetRequestURI("/test/file")
			request.Header.SetMethod(http.MethodPut)
			request.Header.Set("If-Match", etag)
			request.SetBodyString("payload")
			var ctx fasthttp.RequestCtx
			ctx.Init(&request, nil, nil)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				ctx.Response.Reset()
				handler.run(&ctx)
				if ctx.Response.StatusCode() != http.StatusOK {
					b.Fatal(ctx.Response.StatusCode())
				}
			}
		})
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

func TestServer(t *testing.T) {
	t.Run("basic operations", func(t *testing.T) {
		// Create mock server
		mockServer := New("test-bucket", "us-east-1")
		defer mockServer.Close()

		// Create S3 client pointing to mock server
		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		bucket := s3.NewBucket(key, "test-bucket")

		// Test PUT operation
		testContent := []byte("Hello, World!")
		etag, err := bucket.Write(context.Background(), "test/file.txt", testContent)
		assert.NoError(t, err)
		assert.NotEmpty(t, etag)

		// Verify object exists in mock server
		assert.True(t, mockServer.ObjectExists("test/file.txt"))

		// Test GET operation
		file, err := bucket.Open("test/file.txt")
		assert.NoError(t, err)
		defer file.Close()

		content, err := io.ReadAll(file)
		assert.NoError(t, err)
		assert.Equal(t, testContent, content)

		// Test HEAD operation (file info)
		s3File := file.(*s3.File)
		assert.Equal(t, etag, s3File.ETag)
		assert.Equal(t, "test/file.txt", s3File.Path())

		// Verify request logging
		assert.True(t, mockServer.HasRequestWithMethod("PUT"))
		assert.True(t, mockServer.HasRequestWithMethod("GET"))
	})

	t.Run("list operations", func(t *testing.T) {
		mockServer := New("test-bucket", "us-east-1")
		defer mockServer.Close()

		// Populate test data
		testData := map[string][]byte{
			"dir1/file1.txt": []byte("content1"),
			"dir1/file2.txt": []byte("content2"),
			"dir2/file3.txt": []byte("content3"),
			"root.txt":       []byte("root content"),
		}
		mockServer.PopulateTestData(testData)

		// Test listing all objects
		allObjects := mockServer.ListObjects("")
		assert.Len(t, allObjects, 4)

		// Test listing with prefix
		dir1Objects := mockServer.ListObjects("dir1/")
		assert.Len(t, dir1Objects, 2)
		assert.Contains(t, dir1Objects, "dir1/file1.txt")
		assert.Contains(t, dir1Objects, "dir1/file2.txt")
	})

	t.Run("error simulation", func(t *testing.T) {
		mockServer := New("test-bucket", "us-east-1")
		defer mockServer.Close()

		// Enable error simulation
		mockServer.EnableErrorSimulation(ErrorSimulation{
			InternalErrors: true,
			ErrorRate:      1.0, // 100% error rate
		})

		// Create S3 client
		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		bucket := s3.NewBucket(key, "test-bucket")

		// This should fail due to error simulation
		_, err := bucket.Write(context.Background(), "test-file.txt", []byte("test"))
		assert.Error(t, err)

		// Disable error simulation
		mockServer.DisableErrorSimulation()

		// This should succeed
		_, err = bucket.Write(context.Background(), "test-file.txt", []byte("test"))
		assert.NoError(t, err)
	})

	t.Run("utilities", func(t *testing.T) {
		mockServer := New("test-bucket", "us-east-1")
		defer mockServer.Close()

		// Test multiple utility functions in one comprehensive test
		testData := map[string][]byte{
			"file1.txt": []byte("content1"),
			"file2.txt": []byte("content2"),
		}
		mockServer.PopulateTestData(testData)

		// Test ObjectExists and ObjectContent
		assert.True(t, mockServer.ObjectExists("file1.txt"))
		content, found := mockServer.ObjectContent("file1.txt")
		assert.True(t, found)
		assert.Equal(t, []byte("content1"), content)

		// Test metadata operations
		metadata := map[string]string{"author": "test", "version": "1.0"}
		mockServer.PutObjectWithMetadata("meta-test.txt", []byte("content"), metadata)

		retrievedMeta, found := mockServer.GetObjectMetadata("meta-test.txt")
		assert.True(t, found)
		assert.Equal(t, metadata, retrievedMeta)

		// Test SetObjectMetadata
		newMeta := map[string]string{"author": "updated"}
		assert.True(t, mockServer.SetObjectMetadata("meta-test.txt", newMeta))

		// Test DeleteObject
		assert.True(t, mockServer.DeleteObject("file1.txt"))
		assert.False(t, mockServer.ObjectExists("file1.txt"))
		assert.False(t, mockServer.DeleteObject("non-existent.txt"))

		// Test Clear
		mockServer.Clear()
		assert.False(t, mockServer.ObjectExists("file2.txt"))
		assert.Len(t, mockServer.ListObjects(""), 0)

		// Test error cases
		_, found = mockServer.ObjectContent("non-existent.txt")
		assert.False(t, found)
		_, found = mockServer.GetObjectMetadata("non-existent.txt")
		assert.False(t, found)
		assert.False(t, mockServer.SetObjectMetadata("non-existent.txt", newMeta))
	})

	t.Run("head requests", func(t *testing.T) {
		mockServer := New("test-bucket", "us-east-1")
		defer mockServer.Close()

		// Create test object
		testContent := []byte("test content for HEAD request")
		etag := mockServer.PutObject("head-test.txt", testContent)

		// Test HEAD request directly using HTTP client to ensure handleHeadObject is tested
		// Make a direct HEAD request to test the handler
		req, err := http.NewRequest("HEAD", mockServer.URL()+"/test-bucket/head-test.txt", nil)
		assert.NoError(t, err)

		client := &http.Client{}
		resp, err := client.Do(req)
		assert.NoError(t, err)
		defer resp.Body.Close()

		// Verify HEAD response
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, etag, resp.Header.Get("ETag"))
		assert.Equal(t, strconv.Itoa(len(testContent)), resp.Header.Get("Content-Length"))

		// Test HEAD request for non-existent object
		req2, err := http.NewRequest("HEAD", mockServer.URL()+"/test-bucket/non-existent.txt", nil)
		assert.NoError(t, err)

		resp2, err := client.Do(req2)
		assert.NoError(t, err)
		defer resp2.Body.Close()
		assert.Equal(t, http.StatusNotFound, resp2.StatusCode)

		// Verify HEAD requests were logged
		assert.True(t, mockServer.HasRequestWithMethod("HEAD"))
		headRequests := mockServer.GetRequestsWithMethod("HEAD")
		assert.Len(t, headRequests, 2)
	})

	t.Run("content type detection", func(t *testing.T) {
		mockServer := New("test-bucket", "us-east-1")
		defer mockServer.Close()

		// Test various file types
		testCases := []struct {
			filename   string
			content    []byte
			expectedCT string
		}{
			{"test.txt", []byte("plain text"), "text/plain"},
			{"test.html", []byte("<html></html>"), "text/html"},
			{"test.htm", []byte("<html></html>"), "text/html"},
			{"test.json", []byte(`{"key": "value"}`), "application/json"},
			{"test.xml", []byte("<?xml version='1.0'?>"), "application/xml"},
			{"test.pdf", []byte("PDF content"), "application/pdf"},
			{"test.jpg", []byte("JPEG content"), "image/jpeg"},
			{"test.jpeg", []byte("JPEG content"), "image/jpeg"},
			{"test.png", []byte("PNG content"), "image/png"},
			{"test.gif", []byte("GIF content"), "image/gif"},
			{"test.bin", []byte("\x00\x01\x02\x03"), "application/octet-stream"}, // Use actual binary content
			{"no-extension", []byte("content"), "text/plain; charset=utf-8"},     // http.DetectContentType
		}

		for _, tc := range testCases {
			mockServer.PutObject(tc.filename, tc.content)
			obj, found := mockServer.GetObject(tc.filename)
			assert.True(t, found, "Object %s should exist", tc.filename)
			assert.Equal(t, tc.expectedCT, obj.ContentType, "Content type for %s", tc.filename)
		}
	})

	t.Run("range requests", func(t *testing.T) {
		mockServer := New("test-bucket", "us-east-1")
		defer mockServer.Close()

		// Create test content
		testContent := make([]byte, 1000)
		for i := range testContent {
			testContent[i] = byte(i % 256)
		}
		etag := mockServer.PutObject("range-test.bin", testContent)

		// Create S3 client
		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()
		bucket := s3.NewBucket(key, "test-bucket")

		// Test key range scenarios
		testCases := []struct {
			start    int64
			width    int64
			expected []byte
		}{
			{0, 100, testContent[0:100]},      // Beginning
			{200, 100, testContent[200:300]},  // Middle
			{900, 100, testContent[900:1000]}, // End
			{950, 50, testContent[950:1000]},  // Last 50 bytes
		}

		for _, tc := range testCases {
			reader, err := bucket.OpenRange("range-test.bin", etag, tc.start, tc.width)
			assert.NoError(t, err)
			defer reader.Close()

			content, err := io.ReadAll(reader)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, content)
		}

		_, err := bucket.OpenRange("range-test.bin", "stale-etag", 0, 100)
		assert.ErrorIs(t, err, s3.ErrETagChanged)
	})

	t.Run("multipart upload", func(t *testing.T) {
		mockServer := New("test-bucket", "us-east-1")
		defer mockServer.Close()

		// Create S3 client
		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		// Test multipart upload through WriteFrom (which uses internal uploader)
		bucket := s3.NewBucket(key, "test-bucket")

		// Create test data larger than MinPartSize to trigger multipart upload
		testData := make([]byte, s3.MinPartSize*2+1000) // ~10MB + 1000 bytes
		for i := range testData {
			testData[i] = byte(i % 256)
		}

		// Use WriteFrom which internally handles multipart upload
		err := bucket.WriteFrom(context.Background(), "multipart-test.bin", bytes.NewReader(testData), int64(len(testData)))
		assert.NoError(t, err)

		// Verify object was created
		assert.True(t, mockServer.ObjectExists("multipart-test.bin"))

		// Verify final content
		finalContent, found := mockServer.ObjectContent("multipart-test.bin")
		assert.True(t, found)
		assert.Equal(t, testData, finalContent)
		var partHashes []byte
		for start := 0; start < len(testData); start += s3.MinPartSize {
			sum := md5.Sum(testData[start:min(start+s3.MinPartSize, len(testData))])
			partHashes = append(partHashes, sum[:]...)
		}
		object, found := mockServer.GetObject("multipart-test.bin")
		require.True(t, found)
		assert.Equal(t, fmt.Sprintf(`"%x-%d"`, md5.Sum(partHashes), len(partHashes)/md5.Size), object.ETag)

		// Verify multipart upload requests were made
		assert.True(t, mockServer.HasRequestWithMethod("POST")) // Initiate multipart
		assert.True(t, mockServer.HasRequestWithMethod("PUT"))  // Upload parts
	})

	t.Run("error handling", func(t *testing.T) {
		mockServer := New("test-bucket", "us-east-1")
		defer mockServer.Close()

		// Create S3 client
		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		// Test invalid bucket name
		wrongBucket := s3.NewBucket(key, "wrong-bucket")
		_, err := wrongBucket.Write(context.Background(), "test.txt", []byte("content"))
		assert.Error(t, err)

		// Test with correct bucket for various error conditions
		bucket := s3.NewBucket(key, "test-bucket")

		// Test accessing non-existent object
		_, err = bucket.Open("non-existent.txt")
		assert.Error(t, err)

		// Test range request on non-existent object
		_, err = bucket.OpenRange("non-existent.bin", "", 0, 100)
		assert.Error(t, err)

		// Test listing non-existent directory
		_, err = bucket.ReadDir("non-existent-dir")
		assert.Error(t, err)

		// Test DELETE on non-existent object
		err = bucket.Delete(context.Background(), "non-existent.txt")
		if err != nil {
			assert.Contains(t, err.Error(), "404")
		}

		// Verify requests were logged
		assert.True(t, mockServer.RequestCount() > 0)
	})

	t.Run("advanced operations", func(t *testing.T) {
		mockServer := New("test-bucket", "us-east-1")
		defer mockServer.Close()

		// Test multipart upload utilities
		uploads := mockServer.ListMultipartUploads()
		assert.Len(t, uploads, 0)
		_, exists := mockServer.GetMultipartUpload("non-existent")
		assert.False(t, exists)

		// Test S3 Select and multipart abort via direct HTTP
		client := &http.Client{}

		// Test S3 Select
		mockServer.PutObject("test.json", []byte(`{"id": 1}`))
		req, _ := http.NewRequest("POST", mockServer.URL()+"/test-bucket/test.json?select=", strings.NewReader("SELECT * FROM S3Object"))
		resp, err := client.Do(req)
		assert.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		// Test multipart initiate and abort
		req2, _ := http.NewRequest("POST", mockServer.URL()+"/test-bucket/test.bin?uploads=", nil)
		resp2, err := client.Do(req2)
		assert.NoError(t, err)
		defer resp2.Body.Close()
		assert.Equal(t, http.StatusOK, resp2.StatusCode)

		// Get upload ID and abort
		uploads = mockServer.ListMultipartUploads()
		assert.Len(t, uploads, 1)
		var uploadID string
		for id := range uploads {
			uploadID = id
			break
		}

		req3, _ := http.NewRequest("DELETE", mockServer.URL()+"/test-bucket/test.bin?uploadId="+uploadID, nil)
		resp3, err := client.Do(req3)
		assert.NoError(t, err)
		defer resp3.Body.Close()
		assert.Equal(t, http.StatusNoContent, resp3.StatusCode)

		// Verify cleanup
		uploads = mockServer.ListMultipartUploads()
		assert.Len(t, uploads, 0)

		// Test request logging
		assert.True(t, mockServer.RequestCount() > 0)
		assert.True(t, mockServer.HasRequestWithMethod("POST"))
		assert.True(t, mockServer.HasRequestWithMethod("DELETE"))

		postRequests := mockServer.GetRequestsWithMethod("POST")
		assert.True(t, len(postRequests) >= 2) // S3 Select + multipart initiate
	})
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
