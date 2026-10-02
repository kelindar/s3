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
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/kelindar/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

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
