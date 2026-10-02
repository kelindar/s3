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
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

var (
	mockRand       = rand.New(rand.NewSource(time.Now().UnixNano()))
	mockRandMu     sync.Mutex
	uploadSequence atomic.Uint64
)

// Server provides a comprehensive mock implementation of the AWS S3 API
// for testing purposes. It implements all S3 operations used by the client library
// including object operations, multipart uploads, list operations, and S3 Select.
type Server struct {
	server        *fasthttp.Server
	objects       map[string]*Object
	listedObjects []ObjectInfo
	uploads       map[string]*Multipart
	mutex         sync.RWMutex
	bucket        string
	region        string
	requests      []RequestLog
	errors        *ErrorSimulation
	baseURL       string
	noLogging     atomic.Bool
	conns         map[net.Conn]struct{}
	connsMu       sync.Mutex
	closed        bool
}

// Object represents an S3 object stored in the mock server
type Object struct {
	Content      []byte
	ETag         string
	LastModified time.Time
	ContentType  string
	Metadata     map[string]string
}

// Multipart tracks the state of a multipart upload
type Multipart struct {
	ID       string
	Bucket   string
	Key      string
	Parts    map[int]*PartInfo
	Created  time.Time
	Metadata map[string]string
}

// PartInfo represents a single part in a multipart upload
type PartInfo struct {
	PartNumber int
	ETag       string
	Size       int64
	Content    []byte
}

// RequestLog captures details about requests made to the mock server
type RequestLog struct {
	Method    string
	Path      string
	Query     string
	Headers   map[string]string
	Body      []byte
	Timestamp time.Time
}

// ErrorSimulation controls error injection for testing
type ErrorSimulation struct {
	NetworkErrors    bool
	NotFoundErrors   bool
	PermissionErrors bool
	InternalErrors   bool
	ErrorRate        float64 // 0.0 to 1.0
}

// New creates a new mock S3 server for the specified bucket and region
func New(bucket, region string) *Server {
	mock := &Server{
		objects: make(map[string]*Object),
		uploads: make(map[string]*Multipart),
		bucket:  bucket,
		region:  region,
		errors:  &ErrorSimulation{},
		conns:   make(map[net.Conn]struct{}),
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Errorf("mock: listen: %w", err))
	}
	mock.server = &fasthttp.Server{
		Handler:            mock.fastHandler(),
		MaxRequestBodySize: int(^uint(0) >> 1),
		StreamRequestBody:  true,
		ConnState:          mock.trackConn,
	}
	mock.baseURL = "http://" + listener.Addr().String()
	go mock.server.Serve(listener)

	return mock
}

// URL returns the base URL of the mock server
func (m *Server) URL() string {
	return m.baseURL
}

// SetRequestLogging enables or disables recording future requests. Logging is enabled by default.
func (m *Server) SetRequestLogging(enabled bool) {
	m.noLogging.Store(!enabled)
}

// Close shuts down the mock server and cleans up resources
func (m *Server) Close() {
	if m.server == nil {
		return
	}
	m.connsMu.Lock()
	m.closed = true
	for conn := range m.conns {
		_ = conn.Close()
	}
	m.connsMu.Unlock()
	_ = m.server.Shutdown()
}

func (m *Server) trackConn(conn net.Conn, state fasthttp.ConnState) {
	switch state {
	case fasthttp.StateNew:
		m.connsMu.Lock()
		if m.closed {
			_ = conn.Close()
		} else {
			m.conns[conn] = struct{}{}
		}
		m.connsMu.Unlock()
	case fasthttp.StateClosed, fasthttp.StateHijacked:
		m.connsMu.Lock()
		delete(m.conns, conn)
		m.connsMu.Unlock()
	}
}

// Clear removes all objects and uploads from the mock server
func (m *Server) Clear() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.objects = make(map[string]*Object)
	m.listedObjects = nil
	m.uploads = make(map[string]*Multipart)
	m.requests = nil
}

// PutObject adds an object to the mock server
func (m *Server) PutObject(key string, content []byte) string {
	return m.PutObjectWithMetadata(key, content, nil)
}

// PutObjectWithMetadata adds an object with metadata to the mock server
func (m *Server) PutObjectWithMetadata(key string, content []byte, metadata map[string]string) string {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	etag := generateETag(content)
	contentType := detectContentType(key, content)

	m.listedObjects = nil
	m.objects[key] = &Object{
		Content:      content,
		ETag:         etag,
		LastModified: time.Now().UTC(),
		ContentType:  contentType,
		Metadata:     metadata,
	}

	return etag
}

// GetObject retrieves an object from the mock server
func (m *Server) GetObject(key string) (*Object, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	obj, exists := m.objects[key]
	return obj, exists
}

// DeleteObject removes an object from the mock server
func (m *Server) DeleteObject(key string) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	_, exists := m.objects[key]
	if exists {
		delete(m.objects, key)
		m.listedObjects = nil
	}
	return exists
}

// ListObjects returns a list of object keys matching the given prefix
func (m *Server) ListObjects(prefix string) []string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var keys []string
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)
	return keys
}

// PopulateTestData adds multiple test objects to the mock server
func (m *Server) PopulateTestData(data map[string][]byte) {
	for key, content := range data {
		m.PutObject(key, content)
	}
}

// GetRequestLog returns all logged requests
func (m *Server) GetRequestLog() []RequestLog {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// Return a copy to avoid race conditions
	logs := make([]RequestLog, len(m.requests))
	copy(logs, m.requests)
	return logs
}

// EnableErrorSimulation enables error simulation with the given configuration
func (m *Server) EnableErrorSimulation(config ErrorSimulation) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.errors = &config
}

// DisableErrorSimulation disables all error simulation
func (m *Server) DisableErrorSimulation() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.errors = &ErrorSimulation{}
}

// ServeHTTP handles HTTP requests to the mock S3 server
func (m *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Log the request
	m.logRequest(r)

	// Check for error simulation
	if m.shouldSimulateError() {
		m.writeErrorResponse(w, "InternalError", "Simulated error", http.StatusInternalServerError)
		return
	}

	// Parse the request path
	bucketPath, rawKey, hasKey := strings.Cut(strings.Trim(r.URL.Path, "/"), "/")
	switch {
	case bucketPath == "":
		m.writeErrorResponse(w, "InvalidRequest", "Invalid request path", http.StatusBadRequest)
		return
	case bucketPath != m.bucket:
		m.writeErrorResponse(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	// Extract key
	var key string
	if hasKey {
		key = strings.Clone(rawKey)
	}

	// Route based on method and query parameters
	var query url.Values
	if r.URL.RawQuery != "" {
		query = r.URL.Query()
	}

	switch {
	case r.Method == http.MethodGet && key == "":
		// List objects
		m.handleListObjects(w, r, query)
	case r.Method == http.MethodGet:
		// Get object
		m.handleGetObject(w, r, key)
	case r.Method == http.MethodHead:
		m.handleHeadObject(w, r, key)
	case r.Method == http.MethodPut && query.Has("partNumber") && query.Has("uploadId"):
		// Upload part
		m.handleUploadPart(w, r, key, query)
	case r.Method == http.MethodPut:
		// Put object
		m.handlePutObject(w, r, key)
	case r.Method == http.MethodPost && query.Has("uploads"):
		// Initiate multipart upload
		m.handleInitiateMultipartUpload(w, r, key)
	case r.Method == http.MethodPost && query.Has("uploadId"):
		// Complete multipart upload
		m.handleCompleteMultipartUpload(w, r, key, query)
	case r.Method == http.MethodPost && query.Has("select"):
		// S3 Select
		m.handleS3Select(w, r, key)
	case r.Method == http.MethodPost:
		m.writeErrorResponse(w, "InvalidRequest", "Invalid POST request", http.StatusBadRequest)
	case r.Method == http.MethodDelete && query.Has("uploadId"):
		// Abort multipart upload
		m.handleAbortMultipartUpload(w, r, key, query)
	case r.Method == http.MethodDelete:
		// Delete object
		m.handleDeleteObject(w, r, key)
	default:
		m.writeErrorResponse(w, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// logRequest logs details about an HTTP request
func (m *Server) logRequest(r *http.Request) {
	if m.noLogging.Load() {
		return
	}

	headers := make(map[string]string)
	for name, values := range r.Header {
		if len(values) > 0 {
			headers[strings.Clone(name)] = strings.Clone(values[0])
		}
	}

	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
	}

	log := RequestLog{
		Method:    strings.Clone(r.Method),
		Path:      strings.Clone(r.URL.Path),
		Query:     strings.Clone(r.URL.RawQuery),
		Headers:   headers,
		Body:      body,
		Timestamp: time.Now(),
	}
	m.mutex.Lock()
	m.requests = append(m.requests, log)
	m.mutex.Unlock()
}

// shouldSimulateError determines if an error should be simulated
func (m *Server) shouldSimulateError() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if m.errors.ErrorRate > 0 {
		mockRandMu.Lock()
		simulate := mockRand.Float64() < m.errors.ErrorRate
		mockRandMu.Unlock()
		if simulate {
			return true
		}
	}

	return m.errors.NetworkErrors || m.errors.InternalErrors
}

// writeErrorResponse writes an AWS-compatible error response
func (m *Server) writeErrorResponse(w http.ResponseWriter, code, message string, statusCode int) {
	errorResponse := struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}{
		Code:    code,
		Message: message,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)

	xml.NewEncoder(w).Encode(errorResponse)
}

// generateETag generates an ETag for the given content
func generateETag(content []byte) string {
	hash := md5.Sum(content)
	var etag [2 + 2*md5.Size]byte
	etag[0], etag[len(etag)-1] = '"', '"'
	hex.Encode(etag[1:len(etag)-1], hash[:])
	return string(etag[:])
}

// detectContentType detects the content type based on file extension and content
func detectContentType(key string, content []byte) string {
	ext := path.Ext(key)
	switch ext {
	case ".txt":
		return "text/plain"
	case ".html", ".htm":
		return "text/html"
	case ".json":
		return "application/json"
	case ".xml":
		return "application/xml"
	case ".pdf":
		return "application/pdf"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".gif":
		return "image/gif"
	default:
		if len(content) > 0 {
			return http.DetectContentType(content)
		}
		return "application/octet-stream"
	}
}

var xmlWriters = sync.Pool{New: func() any {
	return bufio.NewWriterSize(io.Discard, 32<<10)
}}

func writeXMLString(writer *bufio.Writer, value string) {
	start := 0
	for i := 0; i < len(value); i++ {
		var escaped string
		switch value[i] {
		case '"':
			escaped = "&#34;"
		case '\'':
			escaped = "&#39;"
		case '&':
			escaped = "&amp;"
		case '<':
			escaped = "&lt;"
		case '>':
			escaped = "&gt;"
		case '\t':
			escaped = "&#x9;"
		case '\n':
			escaped = "&#xA;"
		case '\r':
			escaped = "&#xD;"
		default:
			if value[i] < 0x20 || value[i] >= 0x80 {
				writer.WriteString(value[start:i])
				xml.EscapeText(writer, []byte(value[i:]))
				return
			}
			continue
		}
		writer.WriteString(value[start:i])
		writer.WriteString(escaped)
		start = i + 1
	}
	writer.WriteString(value[start:])
}

func writeXMLFields(w io.Writer, root string, fields ...string) error {
	writer := xmlWriters.Get().(*bufio.Writer)
	writer.Reset(w)
	writer.WriteByte('<')
	writer.WriteString(root)
	writer.WriteByte('>')
	for i := 0; i < len(fields); i += 2 {
		writer.WriteByte('<')
		writer.WriteString(fields[i])
		writer.WriteByte('>')
		writeXMLString(writer, fields[i+1])
		writer.WriteString("</")
		writer.WriteString(fields[i])
		writer.WriteByte('>')
	}
	writer.WriteString("</")
	writer.WriteString(root)
	writer.WriteByte('>')
	err := writer.Flush()
	writer.Reset(io.Discard)
	xmlWriters.Put(writer)
	return err
}

func (m *Server) fastHandler() fasthttp.RequestHandler {
	fallback := fasthttpadaptor.NewFastHTTPHandler(m)
	return func(ctx *fasthttp.RequestCtx) {
		if !m.noLogging.Load() {
			fallback(ctx)
			return
		}
		bucket, key, hasKey := bytes.Cut(bytes.Trim(ctx.URI().PathOriginal(), "/"), []byte{'/'})
		if string(bucket) != m.bucket {
			fallback(ctx)
			return
		}
		decodedKey, err := url.PathUnescape(string(key))
		if err != nil {
			fallback(ctx)
			return
		}
		query := ctx.QueryArgs()
		switch {
		case ctx.IsGet() && !hasKey:
			if m.shouldSimulateError() {
				m.fastError(ctx, "InternalError", "Simulated error", http.StatusInternalServerError)
				return
			}
			response := m.listResponse(
				string(query.Peek("prefix")),
				string(query.Peek("delimiter")),
				string(query.Peek("max-keys")),
				string(query.Peek("continuation-token")),
				string(query.Peek("start-after")),
			)
			ctx.SetContentType("application/xml")
			response.writeTo(ctx)
		case ctx.IsGet() && hasKey:
			if m.shouldSimulateError() {
				m.fastError(ctx, "InternalError", "Simulated error", http.StatusInternalServerError)
				return
			}
			m.fastObject(ctx, decodedKey, false)
		case ctx.IsHead() && hasKey:
			if m.shouldSimulateError() {
				m.fastError(ctx, "InternalError", "Simulated error", http.StatusInternalServerError)
				return
			}
			m.fastObject(ctx, decodedKey, true)
		case ctx.IsPut() && hasKey && query.Has("partNumber") && query.Has("uploadId") &&
			len(ctx.Request.Header.Peek("x-amz-copy-source")) == 0:
			if m.shouldSimulateError() {
				m.fastError(ctx, "InternalError", "Simulated error", http.StatusInternalServerError)
				return
			}
			m.fastPart(ctx)
		case ctx.IsPut() && hasKey && len(query.QueryString()) == 0 &&
			len(ctx.Request.Header.Peek("x-amz-copy-source")) == 0:
			if m.shouldSimulateError() {
				m.fastError(ctx, "InternalError", "Simulated error", http.StatusInternalServerError)
				return
			}
			m.fastPut(ctx, decodedKey)
		case ctx.IsPost() && query.Has("uploads"):
			if m.shouldSimulateError() {
				m.fastError(ctx, "InternalError", "Simulated error", http.StatusInternalServerError)
				return
			}
			uploadID := m.initiateMultipart(decodedKey)
			ctx.SetStatusCode(http.StatusOK)
			ctx.SetContentType("application/xml")
			writeXMLFields(ctx, "InitiateMultipartUploadResult", "Bucket", m.bucket, "Key", decodedKey, "UploadId", uploadID)
		case ctx.IsPost() && query.Has("uploadId"):
			if m.shouldSimulateError() {
				m.fastError(ctx, "InternalError", "Simulated error", http.StatusInternalServerError)
				return
			}
			m.fastCompleteMultipart(ctx, decodedKey, string(query.Peek("uploadId")))
		case ctx.IsDelete() && query.Has("uploadId"):
			if m.shouldSimulateError() {
				m.fastError(ctx, "InternalError", "Simulated error", http.StatusInternalServerError)
				return
			}
			if !m.abortMultipart(string(query.Peek("uploadId"))) {
				m.fastError(ctx, "NoSuchUpload", "The specified upload does not exist", http.StatusNotFound)
				return
			}
			ctx.SetStatusCode(http.StatusNoContent)
		case ctx.IsDelete() && hasKey && len(query.QueryString()) == 0:
			if m.shouldSimulateError() {
				m.fastError(ctx, "InternalError", "Simulated error", http.StatusInternalServerError)
				return
			}
			status := http.StatusNotFound
			if m.DeleteObject(decodedKey) {
				status = http.StatusNoContent
			}
			ctx.SetStatusCode(status)
		default:
			fallback(ctx)
		}
	}
}

func (m *Server) fastError(ctx *fasthttp.RequestCtx, code, message string, status int) {
	ctx.SetStatusCode(status)
	ctx.SetContentType("application/xml")
	if !ctx.IsHead() {
		writeXMLFields(ctx, "Error", "Code", code, "Message", message)
	}
}

// Testing utility functions

// ObjectExists checks if an object exists in the mock server
func (m *Server) ObjectExists(key string) bool {
	_, exists := m.GetObject(key)
	return exists
}

// ObjectContent returns the content of an object if it exists
func (m *Server) ObjectContent(key string) ([]byte, bool) {
	obj, exists := m.GetObject(key)
	if !exists {
		return nil, false
	}
	return obj.Content, true
}

// RequestCount returns the number of requests made to the server
func (m *Server) RequestCount() int {
	logs := m.GetRequestLog()
	return len(logs)
}

// HasRequestWithMethod checks if a request with the specified method was made
func (m *Server) HasRequestWithMethod(method string) bool {
	logs := m.GetRequestLog()
	for _, log := range logs {
		if log.Method == method {
			return true
		}
	}
	return false
}

// GetRequestsWithMethod returns all requests with the specified method
func (m *Server) GetRequestsWithMethod(method string) []RequestLog {
	logs := m.GetRequestLog()
	var filtered []RequestLog
	for _, log := range logs {
		if log.Method == method {
			filtered = append(filtered, log)
		}
	}
	return filtered
}

// GetMultipartUpload returns a multipart upload by ID
func (m *Server) GetMultipartUpload(uploadID string) (*Multipart, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	upload, exists := m.uploads[uploadID]
	return upload, exists
}

// ListMultipartUploads returns all active multipart uploads
func (m *Server) ListMultipartUploads() map[string]*Multipart {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// Return a copy to avoid race conditions
	uploads := make(map[string]*Multipart)
	for id, upload := range m.uploads {
		uploads[id] = upload
	}
	return uploads
}

// SetObjectMetadata sets metadata for an existing object
func (m *Server) SetObjectMetadata(key string, metadata map[string]string) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	obj, exists := m.objects[key]
	if !exists {
		return false
	}

	obj.Metadata = metadata
	return true
}

// GetObjectMetadata returns metadata for an object
func (m *Server) GetObjectMetadata(key string) (map[string]string, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	obj, exists := m.objects[key]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid race conditions
	metadata := make(map[string]string)
	for k, v := range obj.Metadata {
		metadata[k] = v
	}
	return metadata, true
}
