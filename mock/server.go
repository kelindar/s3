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
	"cmp"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"path"
	"slices"
	"sort"
	"strconv"
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

// parseRange parses an HTTP Range header
func parseRange(rangeHeader string, contentLength int64) (start, end int64, err error) {
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return 0, 0, fmt.Errorf("invalid range header")
	}

	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeSpec, "-")
	switch {
	case len(parts) != 2:
		return 0, 0, fmt.Errorf("invalid range format")
	case parts[0] == "" && parts[1] == "":
		return 0, 0, fmt.Errorf("invalid range format")
	case parts[0] == "":
		// Suffix range: -500
		suffixLength, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		start = max(contentLength-suffixLength, 0)
		end = contentLength - 1
	default:
		// Regular range: 0-499 or 0-
		start, err = strconv.ParseInt(parts[0], 10, 64)
		switch {
		case err != nil:
			return 0, 0, err
		case parts[1] == "":
			// Open-ended range: 0-
			end = contentLength - 1
		default:
			end, err = strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return 0, 0, err
			}
		}
	}

	if start < 0 || start >= contentLength || start > end {
		return 0, 0, fmt.Errorf("range not satisfiable")
	}
	end = min(end, contentLength-1)
	return start, end, nil
}

// generateUploadID generates a unique upload ID for multipart uploads
func generateUploadID() string {
	return fmt.Sprintf("upload-%d-%d", time.Now().UnixNano(), uploadSequence.Add(1))
}

// handleGetObject handles GET requests for objects
func (m *Server) handleGetObject(w http.ResponseWriter, r *http.Request, key string) {
	m.mutex.RLock()
	obj, exists := m.objects[key]
	m.mutex.RUnlock()

	switch match := r.Header.Get("If-Match"); {
	case !exists:
		m.writeErrorResponse(w, "NoSuchKey", "The specified key does not exist", http.StatusNotFound)
		return
	case match != "" && match != obj.ETag:
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}

	// Handle range requests
	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		// Full object
		w.Header().Set("Content-Length", strconv.Itoa(len(obj.Content)))
		w.Header().Set("Content-Type", obj.ContentType)
		w.Header().Set("ETag", obj.ETag)
		w.Header().Set("Last-Modified", obj.LastModified.Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		w.Write(obj.Content)
		return
	}

	start, end, err := parseRange(rangeHeader, int64(len(obj.Content)))
	if err != nil {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj.Content)))
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("ETag", obj.ETag)
	w.Header().Set("Last-Modified", obj.LastModified.Format(http.TimeFormat))
	w.WriteHeader(http.StatusPartialContent)
	w.Write(obj.Content[start : end+1])
}

// handleHeadObject handles HEAD requests for objects
func (m *Server) handleHeadObject(w http.ResponseWriter, r *http.Request, key string) {
	m.mutex.RLock()
	obj, exists := m.objects[key]
	m.mutex.RUnlock()

	if !exists {
		m.writeErrorResponse(w, "NoSuchKey", "The specified key does not exist", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(obj.Content)))
	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("ETag", obj.ETag)
	w.Header().Set("Last-Modified", obj.LastModified.Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

func readRequestContent(r *http.Request) ([]byte, error) {
	if r.ContentLength < 0 || r.ContentLength > int64(int(^uint(0)>>1)) {
		return io.ReadAll(r.Body)
	}
	content := make([]byte, int(r.ContentLength))
	_, err := io.ReadFull(r.Body, content)
	return content, err
}

// handlePutObject handles PUT requests for objects
func (m *Server) handlePutObject(w http.ResponseWriter, r *http.Request, key string) {
	content, err := readRequestContent(r)
	if err != nil {
		m.writeErrorResponse(w, "InvalidRequest", "Failed to read request body", http.StatusBadRequest)
		return
	}

	m.mutex.Lock()
	current, exists := m.objects[key]
	switch noneMatch, match := r.Header.Get("If-None-Match"), r.Header.Get("If-Match"); {
	case noneMatch != "" && noneMatch != "*":
		m.mutex.Unlock()
		m.writeErrorResponse(w, "InvalidRequest", "If-None-Match must be *", http.StatusBadRequest)
		return
	case noneMatch == "*" && exists:
		m.mutex.Unlock()
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	case match != "" && !exists:
		m.mutex.Unlock()
		m.writeErrorResponse(w, "NoSuchKey", "The specified key does not exist", http.StatusNotFound)
		return
	case match != "" && current.ETag != match:
		m.mutex.Unlock()
		m.writeErrorResponse(w, "PreconditionFailed", "If-Match condition failed", http.StatusPreconditionFailed)
		return
	}
	etag := generateETag(content)
	m.listedObjects = nil
	m.objects[key] = &Object{
		Content:      content,
		ETag:         etag,
		LastModified: time.Now().UTC(),
		ContentType:  detectContentType(key, content),
	}
	m.mutex.Unlock()

	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

// handleDeleteObject handles DELETE requests for objects
func (m *Server) handleDeleteObject(w http.ResponseWriter, r *http.Request, key string) {
	status := http.StatusNotFound
	if m.DeleteObject(key) {
		status = http.StatusNoContent
	}
	w.WriteHeader(status)
}

// ListObjectsV2Response represents the XML response for ListObjectsV2
type ListObjectsV2Response struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter"`
	MaxKeys               int            `xml:"MaxKeys"`
	IsTruncated           bool           `xml:"IsTruncated"`
	Contents              []ObjectInfo   `xml:"Contents"`
	CommonPrefixes        []CommonPrefix `xml:"CommonPrefixes"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
}

// ObjectInfo represents an object in the list response
type ObjectInfo struct {
	Key          string    `xml:"Key"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
}

// CommonPrefix represents a common prefix in the list response
type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
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

func (response *ListObjectsV2Response) writeTo(w io.Writer) error {
	writer := xmlWriters.Get().(*bufio.Writer)
	writer.Reset(w)
	var number [32]byte
	var timestamp [64]byte
	writer.WriteString("<ListBucketResult><Name>")
	writeXMLString(writer, response.Name)
	writer.WriteString("</Name><Prefix>")
	writeXMLString(writer, response.Prefix)
	writer.WriteString("</Prefix><Delimiter>")
	writeXMLString(writer, response.Delimiter)
	writer.WriteString("</Delimiter><MaxKeys>")
	writer.Write(strconv.AppendInt(number[:0], int64(response.MaxKeys), 10))
	writer.WriteString("</MaxKeys><IsTruncated>")
	writer.Write(strconv.AppendBool(number[:0], response.IsTruncated))
	writer.WriteString("</IsTruncated>")
	for _, object := range response.Contents {
		writer.WriteString("<Contents><Key>")
		writeXMLString(writer, object.Key)
		writer.WriteString("</Key><LastModified>")
		writer.Write(object.LastModified.AppendFormat(timestamp[:0], time.RFC3339Nano))
		writer.WriteString("</LastModified><ETag>")
		writeXMLString(writer, object.ETag)
		writer.WriteString("</ETag><Size>")
		writer.Write(strconv.AppendInt(number[:0], object.Size, 10))
		writer.WriteString("</Size></Contents>")
	}
	for _, prefix := range response.CommonPrefixes {
		writer.WriteString("<CommonPrefixes><Prefix>")
		writeXMLString(writer, prefix.Prefix)
		writer.WriteString("</Prefix></CommonPrefixes>")
	}
	if response.NextContinuationToken != "" {
		writer.WriteString("<NextContinuationToken>")
		writeXMLString(writer, response.NextContinuationToken)
		writer.WriteString("</NextContinuationToken>")
	}
	writer.WriteString("</ListBucketResult>")
	err := writer.Flush()
	writer.Reset(io.Discard)
	xmlWriters.Put(writer)
	return err
}

func (m *Server) listingSnapshot() []ObjectInfo {
	m.mutex.RLock()
	objects := m.listedObjects
	m.mutex.RUnlock()
	if objects != nil {
		return objects
	}

	m.mutex.Lock()
	if m.listedObjects == nil {
		objects = make([]ObjectInfo, 0, len(m.objects))
		for key, object := range m.objects {
			objects = append(objects, ObjectInfo{
				Key:          key,
				LastModified: object.LastModified,
				ETag:         object.ETag,
				Size:         int64(len(object.Content)),
			})
		}
		slices.SortFunc(objects, func(a, b ObjectInfo) int { return cmp.Compare(a.Key, b.Key) })
		m.listedObjects = objects
	}
	objects = m.listedObjects
	m.mutex.Unlock()
	return objects
}

func (m *Server) listResponse(prefix, delimiter, maxKeysStr, continuationToken, startAfter string) ListObjectsV2Response {
	maxKeys := 1000 // Default
	if parsed, err := strconv.Atoi(maxKeysStr); err == nil && parsed > 0 {
		maxKeys = parsed
	}

	allObjects := m.listingSnapshot()
	startIndex := 0
	endIndex := len(allObjects)
	if prefix != "" {
		startIndex = sort.Search(len(allObjects), func(i int) bool { return allObjects[i].Key >= prefix })
		endIndex = startIndex
		for endIndex < len(allObjects) && strings.HasPrefix(allObjects[endIndex].Key, prefix) {
			endIndex++
		}
	}

	// Handle continuation token and start-after
	if startKey := cmp.Or(continuationToken, startAfter); startKey != "" {
		startIndex += sort.Search(endIndex-startIndex, func(i int) bool { return allObjects[startIndex+i].Key > startKey })
	}

	capacity := min(maxKeys, endIndex-startIndex)
	var contents []ObjectInfo
	var commonPrefixes []CommonPrefix
	grouped := false

	// Process keys and build response
	count := 0
	lastKey := ""

	i := startIndex
	for ; i < endIndex && count < maxKeys; i++ {
		key := allObjects[i].Key
		lastKey = key

		if delimiter != "" {
			// Check if this key should be grouped under a common prefix
			relativePath := key
			if prefix != "" {
				relativePath = strings.TrimPrefix(key, prefix)
			}

			if delimiterIndex := strings.Index(relativePath, delimiter); delimiterIndex != -1 {
				if !grouped {
					contents = append(make([]ObjectInfo, 0, capacity), allObjects[startIndex:i]...)
					grouped = true
				}
				commonPrefix := prefix + relativePath[:delimiterIndex+1]
				if commonPrefixes == nil {
					commonPrefixes = make([]CommonPrefix, 0, capacity)
				}
				commonPrefixes = append(commonPrefixes, CommonPrefix{Prefix: commonPrefix})
				count++
				for i+1 < endIndex && strings.HasPrefix(allObjects[i+1].Key, commonPrefix) {
					i++
				}
				lastKey = allObjects[i].Key
				continue
			}
		}

		if grouped {
			contents = append(contents, allObjects[i])
		}
		count++
	}
	if !grouped {
		contents = allObjects[startIndex:i]
	}

	// Determine if truncated and next token
	isTruncated := false
	var nextToken string

	if i < endIndex {
		isTruncated = true
		nextToken = lastKey
	}

	return ListObjectsV2Response{
		Name:                  m.bucket,
		Prefix:                prefix,
		Delimiter:             delimiter,
		MaxKeys:               maxKeys,
		IsTruncated:           isTruncated,
		Contents:              contents,
		CommonPrefixes:        commonPrefixes,
		NextContinuationToken: nextToken,
	}
}

// handleListObjects handles GET requests for listing objects
func (m *Server) handleListObjects(w http.ResponseWriter, r *http.Request, query url.Values) {
	response := m.listResponse(query.Get("prefix"), query.Get("delimiter"), query.Get("max-keys"), query.Get("continuation-token"), query.Get("start-after"))
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	response.writeTo(w)
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

func (m *Server) fastPut(ctx *fasthttp.RequestCtx, key string) {
	content, err := readFastContent(ctx)
	if err != nil {
		m.fastError(ctx, "InvalidRequest", "Failed to read request body", http.StatusBadRequest)
		return
	}

	etag := generateETag(content)
	m.mutex.Lock()
	current, exists := m.objects[key]
	switch noneMatch, match := ctx.Request.Header.Peek("If-None-Match"), ctx.Request.Header.Peek("If-Match"); {
	case len(noneMatch) != 0 && string(noneMatch) != "*":
		m.mutex.Unlock()
		m.fastError(ctx, "InvalidRequest", "If-None-Match must be *", http.StatusBadRequest)
		return
	case string(noneMatch) == "*" && exists:
		m.mutex.Unlock()
		ctx.SetStatusCode(http.StatusPreconditionFailed)
		return
	case len(match) != 0 && !exists:
		m.mutex.Unlock()
		m.fastError(ctx, "NoSuchKey", "The specified key does not exist", http.StatusNotFound)
		return
	case len(match) != 0 && string(match) != current.ETag:
		m.mutex.Unlock()
		m.fastError(ctx, "PreconditionFailed", "If-Match condition failed", http.StatusPreconditionFailed)
		return
	}
	m.listedObjects = nil
	m.objects[strings.Clone(key)] = &Object{
		Content:      content,
		ETag:         etag,
		LastModified: time.Now().UTC(),
		ContentType:  detectContentType(key, content),
	}
	m.mutex.Unlock()
	ctx.Response.Header.Set("ETag", etag)
}

func readFastContent(ctx *fasthttp.RequestCtx) ([]byte, error) {
	stream := ctx.Request.BodyStream()
	if stream == nil {
		return bytes.Clone(ctx.Request.Body()), nil
	}
	var content []byte
	var err error
	switch size := ctx.Request.Header.ContentLength(); {
	case size >= 0:
		content = make([]byte, size)
		_, err = io.ReadFull(stream, content)
	default:
		content, err = io.ReadAll(stream)
	}
	_ = ctx.Request.CloseBodyStream()
	if err != nil {
		return nil, err
	}
	return content, nil
}

func (m *Server) fastPart(ctx *fasthttp.RequestCtx) {
	query := ctx.QueryArgs()
	partNumber, err := strconv.Atoi(string(query.Peek("partNumber")))
	if err != nil || partNumber < 1 {
		m.fastError(ctx, "InvalidPartNumber", "Invalid part number", http.StatusBadRequest)
		return
	}
	m.mutex.RLock()
	upload, exists := m.uploads[string(query.Peek("uploadId"))]
	m.mutex.RUnlock()
	if !exists {
		m.fastError(ctx, "NoSuchUpload", "The specified upload does not exist", http.StatusNotFound)
		return
	}
	content, err := readFastContent(ctx)
	if err != nil {
		m.fastError(ctx, "InvalidRequest", "Failed to read request body", http.StatusBadRequest)
		return
	}
	etag := generateETag(content)
	m.mutex.Lock()
	upload.Parts[partNumber] = &PartInfo{
		PartNumber: partNumber,
		ETag:       etag,
		Size:       int64(len(content)),
		Content:    content,
	}
	m.mutex.Unlock()
	ctx.Response.Header.Set("ETag", etag)
}

func (m *Server) fastError(ctx *fasthttp.RequestCtx, code, message string, status int) {
	ctx.SetStatusCode(status)
	ctx.SetContentType("application/xml")
	if !ctx.IsHead() {
		writeXMLFields(ctx, "Error", "Code", code, "Message", message)
	}
}

func (m *Server) fastObject(ctx *fasthttp.RequestCtx, key string, head bool) {
	m.mutex.RLock()
	object, exists := m.objects[key]
	m.mutex.RUnlock()
	if !exists {
		ctx.SetStatusCode(http.StatusNotFound)
		if !head {
			ctx.SetContentType("application/xml")
			writeXMLFields(ctx, "Error", "Code", "NoSuchKey", "Message", "The specified key does not exist")
		}
		return
	}

	if match := ctx.Request.Header.Peek("If-Match"); !head && len(match) > 0 && string(match) != object.ETag {
		ctx.SetStatusCode(http.StatusPreconditionFailed)
		return
	}

	body := object.Content
	if rangeHeader := ctx.Request.Header.Peek("Range"); !head && len(rangeHeader) > 0 {
		start, end, err := parseRange(string(rangeHeader), int64(len(body)))
		if err != nil {
			ctx.SetStatusCode(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		var contentRange [64]byte
		value := append(contentRange[:0], "bytes "...)
		value = strconv.AppendInt(value, start, 10)
		value = append(value, '-')
		value = strconv.AppendInt(value, end, 10)
		value = append(value, '/')
		value = strconv.AppendInt(value, int64(len(body)), 10)
		ctx.Response.Header.SetBytesV("Content-Range", value)
		ctx.SetStatusCode(http.StatusPartialContent)
		body = body[start : end+1]
	}

	var modified [64]byte
	ctx.Response.Header.SetBytesV("Last-Modified", object.LastModified.AppendFormat(modified[:0], http.TimeFormat))
	ctx.Response.Header.Set("ETag", object.ETag)
	ctx.SetContentType(object.ContentType)
	if head {
		ctx.Response.Header.SetContentLength(len(object.Content))
		return
	}
	ctx.Response.SetBodyRaw(body)
	ctx.Response.Header.SetContentLength(len(body))
}

// InitiateMultipartUploadResponse represents the XML response for initiating multipart upload
type InitiateMultipartUploadResponse struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

func (m *Server) initiateMultipart(key string) string {
	uploadID := generateUploadID()

	m.mutex.Lock()
	m.uploads[uploadID] = &Multipart{
		ID:       uploadID,
		Bucket:   m.bucket,
		Key:      key,
		Parts:    make(map[int]*PartInfo),
		Created:  time.Now().UTC(),
		Metadata: make(map[string]string),
	}
	m.mutex.Unlock()
	return uploadID
}

func (m *Server) multipartExists(uploadID string) bool {
	m.mutex.RLock()
	_, exists := m.uploads[uploadID]
	m.mutex.RUnlock()
	return exists
}

func (m *Server) abortMultipart(uploadID string) bool {
	m.mutex.Lock()
	_, exists := m.uploads[uploadID]
	if exists {
		delete(m.uploads, uploadID)
	}
	m.mutex.Unlock()
	return exists
}

// handleInitiateMultipartUpload handles POST requests to initiate multipart uploads
func (m *Server) handleInitiateMultipartUpload(w http.ResponseWriter, r *http.Request, key string) {
	uploadID := m.initiateMultipart(key)

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	writeXMLFields(w, "InitiateMultipartUploadResult", "Bucket", m.bucket, "Key", key, "UploadId", uploadID)
}

// handleUploadPart handles PUT requests for uploading parts
func (m *Server) handleUploadPart(w http.ResponseWriter, r *http.Request, key string, query url.Values) {
	uploadID := query.Get("uploadId")
	partNumberStr := query.Get("partNumber")

	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 {
		m.writeErrorResponse(w, "InvalidPartNumber", "Invalid part number", http.StatusBadRequest)
		return
	}

	m.mutex.RLock()
	upload, exists := m.uploads[uploadID]
	m.mutex.RUnlock()

	if !exists {
		m.writeErrorResponse(w, "NoSuchUpload", "The specified upload does not exist", http.StatusNotFound)
		return
	}

	// Check if this is a copy part operation
	copySource := r.Header.Get("x-amz-copy-source")
	if copySource != "" {
		m.handleCopyPart(w, r, upload, partNumber, copySource)
		return
	}

	// Regular upload part
	content, err := readRequestContent(r)
	if err != nil {
		m.writeErrorResponse(w, "InvalidRequest", "Failed to read request body", http.StatusBadRequest)
		return
	}

	etag := generateETag(content)

	m.mutex.Lock()
	upload.Parts[partNumber] = &PartInfo{
		PartNumber: partNumber,
		ETag:       etag,
		Size:       int64(len(content)),
		Content:    content,
	}
	m.mutex.Unlock()

	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

// handleCopyPart handles copy part operations for multipart uploads
func (m *Server) handleCopyPart(w http.ResponseWriter, r *http.Request, upload *Multipart, partNumber int, copySource string) {
	// Parse copy source: /bucket/key
	if !strings.HasPrefix(copySource, "/") {
		m.writeErrorResponse(w, "InvalidRequest", "Invalid copy source format", http.StatusBadRequest)
		return
	}

	sourceBucket, sourceKey, ok := strings.Cut(copySource[1:], "/")
	switch {
	case !ok:
		m.writeErrorResponse(w, "InvalidRequest", "Invalid copy source format", http.StatusBadRequest)
		return
	case sourceBucket != m.bucket:
		// For simplicity, we only support copying from the same bucket in the mock
		m.writeErrorResponse(w, "NoSuchBucket", "Source bucket not found", http.StatusNotFound)
		return
	}
	sourceKey, err := url.PathUnescape(sourceKey)
	if err != nil {
		m.writeErrorResponse(w, "InvalidRequest", "Invalid copy source escape", http.StatusBadRequest)
		return
	}

	// Get the source object
	m.mutex.RLock()
	sourceObj, exists := m.objects[sourceKey]
	m.mutex.RUnlock()

	ifMatch := r.Header.Get("x-amz-copy-source-if-match")
	switch {
	case !exists:
		m.writeErrorResponse(w, "NoSuchKey", "Source object not found", http.StatusNotFound)
		return
	case ifMatch != "" && ifMatch != sourceObj.ETag:
		m.writeErrorResponse(w, "PreconditionFailed", "Copy source if-match condition failed", http.StatusPreconditionFailed)
		return
	}

	// Handle range if specified
	content := sourceObj.Content
	rangeHeader := r.Header.Get("x-amz-copy-source-range")
	if rangeHeader != "" {
		// Parse range: bytes=start-end
		if !strings.HasPrefix(rangeHeader, "bytes=") {
			m.writeErrorResponse(w, "InvalidRequest", "Invalid range format", http.StatusBadRequest)
			return
		}
		startText, endText, ok := strings.Cut(rangeHeader[6:], "-")
		if !ok || strings.Contains(endText, "-") {
			m.writeErrorResponse(w, "InvalidRequest", "Invalid range format", http.StatusBadRequest)
			return
		}

		start, err := strconv.ParseInt(startText, 10, 64)
		if err != nil {
			m.writeErrorResponse(w, "InvalidRequest", "Invalid range start", http.StatusBadRequest)
			return
		}

		end, err := strconv.ParseInt(endText, 10, 64)
		if err != nil {
			m.writeErrorResponse(w, "InvalidRequest", "Invalid range end", http.StatusBadRequest)
			return
		}

		if start < 0 || end >= int64(len(sourceObj.Content)) || start > end {
			m.writeErrorResponse(w, "InvalidRange", "Invalid range", http.StatusRequestedRangeNotSatisfiable)
			return
		}

		content = sourceObj.Content[start : end+1]
	}

	// Ordinary object ETags already contain the MD5 of the full content.
	// Multipart ETags and partial ranges need a fresh part checksum.
	etag := sourceObj.ETag
	if len(content) != len(sourceObj.Content) || len(etag) != 2+md5.Size*2 {
		etag = generateETag(content)
	}

	m.mutex.Lock()
	upload.Parts[partNumber] = &PartInfo{
		PartNumber: partNumber,
		ETag:       etag,
		Size:       int64(len(content)),
		Content:    content,
	}
	m.mutex.Unlock()

	// Return copy part result XML
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
	writeXMLFields(w, "CopyPartResult", "ETag", etag)
}

// CompleteMultipartUploadRequest represents the XML request for completing multipart upload
type CompleteMultipartUploadRequest struct {
	XMLName xml.Name                `xml:"CompleteMultipartUpload"`
	Parts   []CompleteMultipartPart `xml:"Part"`
}

// CompleteMultipartPart represents a part in the complete request
type CompleteMultipartPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completePart struct {
	number    int
	etag      [md5.Size]byte
	etagValid bool
}

// CompleteMultipartUploadResponse represents the XML response for completing multipart upload
type CompleteMultipartUploadResponse struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type multipartFailure struct {
	code    string
	message string
	status  int
}

// scanCompleteMultipart recognizes the exact XML emitted by the library's uploader.
func scanCompleteMultipart(body []byte) ([]completePart, bool) {
	data, ok := bytes.CutPrefix(body, []byte(`<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`))
	if !ok {
		return nil, false
	}
	parts := make([]completePart, 0, 4)
	for !bytes.Equal(data, []byte(`</CompleteMultipartUpload>`)) {
		data, ok = bytes.CutPrefix(data, []byte(`<Part><PartNumber>`))
		if !ok {
			return nil, false
		}
		var number []byte
		number, data, ok = bytes.Cut(data, []byte(`</PartNumber><ETag>`))
		if !ok {
			return nil, false
		}
		part, err := strconv.Atoi(string(number))
		if err != nil || part < 1 {
			return nil, false
		}
		var etag []byte
		etag, data, ok = bytes.Cut(data, []byte(`</ETag></Part>`))
		if !ok || len(etag) != 42 || !bytes.HasPrefix(etag, []byte("&#34;")) || !bytes.HasSuffix(etag, []byte("&#34;")) {
			return nil, false
		}
		for _, c := range etag[5:37] {
			if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
				return nil, false
			}
		}
		var hash [md5.Size]byte
		if _, err := hex.Decode(hash[:], etag[5:37]); err != nil {
			return nil, false
		}
		parts = append(parts, completePart{number: part, etag: hash, etagValid: true})
	}
	return parts, true
}

func decodeCompleteETag(etag string) ([md5.Size]byte, bool) {
	var hash [md5.Size]byte
	if len(etag) != md5.Size*2+2 || etag[0] != '"' || etag[len(etag)-1] != '"' {
		return hash, false
	}
	for _, c := range etag[1 : len(etag)-1] {
		if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
			return hash, false
		}
	}
	if _, err := hex.Decode(hash[:], []byte(etag[1:len(etag)-1])); err != nil {
		return hash, false
	}
	return hash, true
}

func (m *Server) fastCompleteMultipart(ctx *fasthttp.RequestCtx, key, uploadID string) {
	if !m.multipartExists(uploadID) {
		m.fastError(ctx, "NoSuchUpload", "The specified upload does not exist", http.StatusNotFound)
		return
	}
	body, err := readFastContent(ctx)
	if err != nil {
		m.fastError(ctx, "MalformedXML", "Invalid XML in request body", http.StatusBadRequest)
		return
	}
	etag, failure := m.completeMultipart(uploadID, key, body)
	if failure != nil {
		m.fastError(ctx, failure.code, failure.message, failure.status)
		return
	}
	ctx.SetStatusCode(http.StatusOK)
	ctx.SetContentType("application/xml")
	writeXMLFields(ctx, "CompleteMultipartUploadResult",
		"Location", fmt.Sprintf("https://%s.s3.amazonaws.com/%s", m.bucket, key),
		"Bucket", m.bucket, "Key", key, "ETag", etag)
}

func (m *Server) completeMultipart(uploadID, key string, body []byte) (string, *multipartFailure) {
	parts, ok := scanCompleteMultipart(body)
	if !ok {
		var request CompleteMultipartUploadRequest
		if err := xml.NewDecoder(bytes.NewReader(body)).Decode(&request); err != nil {
			return "", &multipartFailure{code: "MalformedXML", message: "Invalid XML in request body", status: http.StatusBadRequest}
		}
		parts = make([]completePart, len(request.Parts))
		for i, part := range request.Parts {
			etag, valid := decodeCompleteETag(part.ETag)
			parts[i] = completePart{number: part.PartNumber, etag: etag, etagValid: valid}
		}
	}
	// Validate and assemble parts
	slices.SortFunc(parts, func(a, b completePart) int { return cmp.Compare(a.number, b.number) })

	var contents [][]byte
	partHashes := make([]byte, len(parts)*md5.Size)
	total := 0
	missing := false
	missingPart := 0
	tooLarge := false
	badETag := false
	m.mutex.RLock()
	upload, exists := m.uploads[uploadID]
	if exists {
		contents = make([][]byte, 0, len(parts))
		for i, requested := range parts {
			partNum := requested.number
			partInfo, ok := upload.Parts[partNum]
			if !ok {
				missing = true
				missingPart = partNum
				break
			}
			if len(partInfo.Content) > int(^uint(0)>>1)-total {
				tooLarge = true
				break
			}
			etag := partInfo.ETag
			if len(etag) != 34 || etag[0] != '"' || etag[33] != '"' {
				badETag = true
				break
			}
			hash := partHashes[i*md5.Size : (i+1)*md5.Size]
			if _, err := hex.Decode(hash, []byte(etag[1:33])); err != nil || !requested.etagValid || !bytes.Equal(hash, requested.etag[:]) {
				badETag = true
				break
			}
			total += len(partInfo.Content)
			contents = append(contents, partInfo.Content)
		}
	}
	m.mutex.RUnlock()
	switch {
	case !exists:
		return "", &multipartFailure{code: "NoSuchUpload", message: "The specified upload does not exist", status: http.StatusNotFound}
	case missing:
		return "", &multipartFailure{code: "InvalidPart", message: fmt.Sprintf("Part %d not found", missingPart), status: http.StatusBadRequest}
	case tooLarge:
		return "", &multipartFailure{code: "EntityTooLarge", message: "Completed object is too large", status: http.StatusBadRequest}
	case badETag:
		return "", &multipartFailure{code: "InvalidPart", message: "Invalid part ETag", status: http.StatusBadRequest}
	}
	finalContent := bytes.Join(contents, nil)

	finalObject := &Object{
		Content:      finalContent,
		ETag:         fmt.Sprintf(`"%x-%d"`, md5.Sum(partHashes), len(parts)),
		LastModified: time.Now().UTC(),
		ContentType:  detectContentType(key, finalContent),
	}
	// Publish only if the upload still exists after the response body was decoded.
	m.mutex.Lock()
	if m.uploads[uploadID] != upload {
		m.mutex.Unlock()
		return "", &multipartFailure{code: "NoSuchUpload", message: "The specified upload does not exist", status: http.StatusNotFound}
	}
	m.listedObjects = nil
	m.objects[key] = finalObject
	delete(m.uploads, uploadID)
	m.mutex.Unlock()
	return finalObject.ETag, nil
}

// handleCompleteMultipartUpload handles POST requests to complete multipart uploads
func (m *Server) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, key string, query url.Values) {
	uploadID := query.Get("uploadId")

	if !m.multipartExists(uploadID) {
		m.writeErrorResponse(w, "NoSuchUpload", "The specified upload does not exist", http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		m.writeErrorResponse(w, "MalformedXML", "Invalid XML in request body", http.StatusBadRequest)
		return
	}

	etag, failure := m.completeMultipart(uploadID, key, body)
	if failure != nil {
		m.writeErrorResponse(w, failure.code, failure.message, failure.status)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	writeXMLFields(w, "CompleteMultipartUploadResult",
		"Location", fmt.Sprintf("https://%s.s3.amazonaws.com/%s", m.bucket, key),
		"Bucket", m.bucket, "Key", key, "ETag", etag)
}

// handleAbortMultipartUpload handles DELETE requests to abort multipart uploads
func (m *Server) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, key string, query url.Values) {
	uploadID := query.Get("uploadId")

	if !m.abortMultipart(uploadID) {
		m.writeErrorResponse(w, "NoSuchUpload", "The specified upload does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleS3Select handles POST requests for S3 Select operations
func (m *Server) handleS3Select(w http.ResponseWriter, r *http.Request, key string) {
	m.mutex.RLock()
	_, exists := m.objects[key]
	m.mutex.RUnlock()

	if !exists {
		m.writeErrorResponse(w, "NoSuchKey", "The specified key does not exist", http.StatusNotFound)
		return
	}

	// For simplicity, we'll simulate S3 Select by returning a basic JSON response
	// In a real implementation, this would parse the SQL query and process the Parquet file
	simulatedResults := `{"id": 1, "name": "test", "value": 123}
{"id": 2, "name": "example", "value": 456}
`

	// S3 Select uses a special streaming format with binary frames
	// For testing purposes, we'll simulate this with a simplified response
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)

	// Write a simplified S3 Select response frame
	m.writeS3SelectFrame(w, simulatedResults)
	m.writeS3SelectEndFrame(w)
}

// writeS3SelectFrame writes a simulated S3 Select data frame
func (m *Server) writeS3SelectFrame(w http.ResponseWriter, data string) {
	// Simplified S3 Select frame format
	// In reality, this would be much more complex with proper binary encoding
	payload := []byte(data)

	// Write frame header (simplified)
	frameHeader := make([]byte, 12)
	binary.BigEndian.PutUint32(frameHeader[0:4], uint32(len(payload)+16)) // total length
	binary.BigEndian.PutUint32(frameHeader[4:8], 0)                       // header length
	binary.BigEndian.PutUint32(frameHeader[8:12], 0)                      // CRC

	w.Write(frameHeader)
	w.Write(payload)

	// Write frame footer (CRC)
	footer := make([]byte, 4)
	w.Write(footer)
}

// writeS3SelectEndFrame writes the end frame for S3 Select
func (m *Server) writeS3SelectEndFrame(w http.ResponseWriter) {
	// Write end frame
	endFrame := make([]byte, 16)
	binary.BigEndian.PutUint32(endFrame[0:4], 16) // total length
	binary.BigEndian.PutUint32(endFrame[4:8], 0)  // header length
	w.Write(endFrame)
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
