package mock

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Use a separate random source to avoid affecting global rand state
var mockRand = rand.New(rand.NewSource(time.Now().UnixNano()))

// Server provides a comprehensive mock implementation of the AWS S3 API
// for testing purposes. It implements all S3 operations used by the client library
// including object operations, multipart uploads, list operations, and S3 Select.
type Server struct {
	server   *httptest.Server
	objects  map[string]*MockObject
	uploads  map[string]*MockMultipartUpload
	mutex    sync.RWMutex
	bucket   string
	region   string
	requests []RequestLog
	errors   *ErrorSimulation
	baseURL  string
	debug    bool
}

// MockObject represents an S3 object stored in the mock server
type MockObject struct {
	Content      []byte
	ETag         string
	LastModified time.Time
	ContentType  string
	Metadata     map[string]string
}

// MockMultipartUpload tracks the state of a multipart upload
type MockMultipartUpload struct {
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
		objects: make(map[string]*MockObject),
		uploads: make(map[string]*MockMultipartUpload),
		bucket:  bucket,
		region:  region,
		errors:  &ErrorSimulation{},
	}

	mock.server = httptest.NewServer(http.HandlerFunc(mock.ServeHTTP))
	mock.baseURL = mock.server.URL

	return mock
}

// URL returns the base URL of the mock server
func (m *Server) URL() string {
	return m.baseURL
}

// Close shuts down the mock server and cleans up resources
func (m *Server) Close() {
	if m.server != nil {
		m.server.Close()
	}
}

// Clear removes all objects and uploads from the mock server
func (m *Server) Clear() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.objects = make(map[string]*MockObject)
	m.uploads = make(map[string]*MockMultipartUpload)
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

	m.objects[key] = &MockObject{
		Content:      content,
		ETag:         etag,
		LastModified: time.Now().UTC(),
		ContentType:  contentType,
		Metadata:     metadata,
	}

	return etag
}

// GetObject retrieves an object from the mock server
func (m *Server) GetObject(key string) (*MockObject, bool) {
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

// EnableDebug enables debug logging
func (m *Server) EnableDebug() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.debug = true
}

// DisableDebug disables debug logging
func (m *Server) DisableDebug() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.debug = false
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
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) < 1 {
		m.writeErrorResponse(w, "InvalidRequest", "Invalid request path", http.StatusBadRequest)
		return
	}

	// Extract bucket and key
	bucket := pathParts[0]
	var key string
	if len(pathParts) > 1 {
		key = strings.Join(pathParts[1:], "/")
	}

	// Validate bucket
	if bucket != m.bucket {
		m.writeErrorResponse(w, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		return
	}

	// Route based on method and query parameters
	query := r.URL.Query()

	switch r.Method {
	case http.MethodGet:
		if key == "" {
			// List objects
			m.handleListObjects(w, r, query)
		} else {
			// Get object
			m.handleGetObject(w, r, key)
		}
	case http.MethodHead:
		m.handleHeadObject(w, r, key)
	case http.MethodPut:
		if query.Has("partNumber") && query.Has("uploadId") {
			// Upload part
			m.handleUploadPart(w, r, key, query)
		} else {
			// Put object
			m.handlePutObject(w, r, key)
		}
	case http.MethodPost:
		if query.Has("uploads") {
			// Initiate multipart upload
			m.handleInitiateMultipartUpload(w, r, key)
		} else if query.Has("uploadId") {
			// Complete multipart upload
			m.handleCompleteMultipartUpload(w, r, key, query)
		} else if query.Has("select") {
			// S3 Select
			m.handleS3Select(w, r, key)
		} else {
			m.writeErrorResponse(w, "InvalidRequest", "Invalid POST request", http.StatusBadRequest)
		}
	case http.MethodDelete:
		if query.Has("uploadId") {
			// Abort multipart upload
			m.handleAbortMultipartUpload(w, r, key, query)
		} else {
			// Delete object
			m.handleDeleteObject(w, r, key)
		}
	default:
		m.writeErrorResponse(w, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// logRequest logs details about an HTTP request
func (m *Server) logRequest(r *http.Request) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	headers := make(map[string]string)
	for name, values := range r.Header {
		if len(values) > 0 {
			headers[name] = values[0]
		}
	}

	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
	}

	m.requests = append(m.requests, RequestLog{
		Method:    r.Method,
		Path:      r.URL.Path,
		Query:     r.URL.RawQuery,
		Headers:   headers,
		Body:      body,
		Timestamp: time.Now(),
	})
}

// shouldSimulateError determines if an error should be simulated
func (m *Server) shouldSimulateError() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if m.errors.ErrorRate > 0 && mockRand.Float64() < m.errors.ErrorRate {
		return true
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
	return fmt.Sprintf(`"%s"`, hex.EncodeToString(hash[:]))
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
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range format")
	}

	if parts[0] == "" {
		// Suffix range: -500
		if parts[1] == "" {
			return 0, 0, fmt.Errorf("invalid range format")
		}
		suffixLength, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		start = contentLength - suffixLength
		if start < 0 {
			start = 0
		}
		end = contentLength - 1
	} else {
		// Regular range: 0-499 or 0-
		start, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, 0, err
		}

		if parts[1] == "" {
			// Open-ended range: 0-
			end = contentLength - 1
		} else {
			end, err = strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return 0, 0, err
			}
		}
	}

	if start < 0 || end >= contentLength || start > end {
		return 0, 0, fmt.Errorf("range not satisfiable")
	}

	return start, end, nil
}

// generateUploadID generates a unique upload ID for multipart uploads
func generateUploadID() string {
	return fmt.Sprintf("upload-%d-%d", time.Now().UnixNano(), mockRand.Int63())
}

// handleGetObject handles GET requests for objects
func (m *Server) handleGetObject(w http.ResponseWriter, r *http.Request, key string) {
	m.mutex.RLock()
	obj, exists := m.objects[key]
	m.mutex.RUnlock()

	if !exists {
		m.writeErrorResponse(w, "NoSuchKey", "The specified key does not exist", http.StatusNotFound)
		return
	}

	// Handle range requests
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
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
	} else {
		// Full object
		w.Header().Set("Content-Length", strconv.Itoa(len(obj.Content)))
		w.Header().Set("Content-Type", obj.ContentType)
		w.Header().Set("ETag", obj.ETag)
		w.Header().Set("Last-Modified", obj.LastModified.Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		w.Write(obj.Content)
	}
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

// handlePutObject handles PUT requests for objects
func (m *Server) handlePutObject(w http.ResponseWriter, r *http.Request, key string) {
	content, err := io.ReadAll(r.Body)
	if err != nil {
		m.writeErrorResponse(w, "InvalidRequest", "Failed to read request body", http.StatusBadRequest)
		return
	}

	etag := m.PutObject(key, content)

	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

// handleDeleteObject handles DELETE requests for objects
func (m *Server) handleDeleteObject(w http.ResponseWriter, r *http.Request, key string) {
	deleted := m.DeleteObject(key)
	if deleted {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
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

// handleListObjects handles GET requests for listing objects
func (m *Server) handleListObjects(w http.ResponseWriter, r *http.Request, query url.Values) {
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	maxKeysStr := query.Get("max-keys")
	continuationToken := query.Get("continuation-token")
	startAfter := query.Get("start-after")

	maxKeys := 1000 // Default
	if maxKeysStr != "" {
		if parsed, err := strconv.Atoi(maxKeysStr); err == nil && parsed > 0 {
			maxKeys = parsed
		}
	}

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var allKeys []string
	for key := range m.objects {
		if prefix == "" || strings.HasPrefix(key, prefix) {
			allKeys = append(allKeys, key)
		}
	}
	sort.Strings(allKeys)

	// Handle continuation token and start-after
	startIndex := 0
	startKey := ""
	if continuationToken != "" {
		startKey = continuationToken
	} else if startAfter != "" {
		startKey = startAfter
	}

	if startKey != "" {
		for i, key := range allKeys {
			if key > startKey {
				startIndex = i
				break
			}
		}
	}

	var contents []ObjectInfo
	var commonPrefixes []CommonPrefix
	prefixSet := make(map[string]bool)

	// Process keys and build response
	count := 0
	lastKey := ""

	for i := startIndex; i < len(allKeys) && count < maxKeys; i++ {
		key := allKeys[i]
		lastKey = key

		if delimiter != "" {
			// Check if this key should be grouped under a common prefix
			relativePath := key
			if prefix != "" {
				relativePath = strings.TrimPrefix(key, prefix)
			}

			if delimiterIndex := strings.Index(relativePath, delimiter); delimiterIndex != -1 {
				commonPrefix := prefix + relativePath[:delimiterIndex+1]
				if !prefixSet[commonPrefix] {
					commonPrefixes = append(commonPrefixes, CommonPrefix{Prefix: commonPrefix})
					prefixSet[commonPrefix] = true
					count++
				}
				continue
			}
		}

		obj := m.objects[key]
		contents = append(contents, ObjectInfo{
			Key:          key,
			LastModified: obj.LastModified,
			ETag:         obj.ETag,
			Size:         int64(len(obj.Content)),
		})
		count++
	}

	// Determine if truncated and next token
	isTruncated := false
	var nextToken string

	if count >= maxKeys && startIndex+count < len(allKeys) {
		isTruncated = true
		nextToken = lastKey
	}

	response := ListObjectsV2Response{
		Name:                  m.bucket,
		Prefix:                prefix,
		Delimiter:             delimiter,
		MaxKeys:               maxKeys,
		IsTruncated:           isTruncated,
		Contents:              contents,
		CommonPrefixes:        commonPrefixes,
		NextContinuationToken: nextToken,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

// InitiateMultipartUploadResponse represents the XML response for initiating multipart upload
type InitiateMultipartUploadResponse struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

// handleInitiateMultipartUpload handles POST requests to initiate multipart uploads
func (m *Server) handleInitiateMultipartUpload(w http.ResponseWriter, r *http.Request, key string) {
	uploadID := generateUploadID()

	m.mutex.Lock()
	m.uploads[uploadID] = &MockMultipartUpload{
		ID:       uploadID,
		Bucket:   m.bucket,
		Key:      key,
		Parts:    make(map[int]*PartInfo),
		Created:  time.Now().UTC(),
		Metadata: make(map[string]string),
	}
	m.mutex.Unlock()

	response := InitiateMultipartUploadResponse{
		Bucket:   m.bucket,
		Key:      key,
		UploadId: uploadID,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
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

	content, err := io.ReadAll(r.Body)
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

// CompleteMultipartUploadResponse represents the XML response for completing multipart upload
type CompleteMultipartUploadResponse struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// handleCompleteMultipartUpload handles POST requests to complete multipart uploads
func (m *Server) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, key string, query url.Values) {
	uploadID := query.Get("uploadId")

	m.mutex.RLock()
	upload, exists := m.uploads[uploadID]
	m.mutex.RUnlock()

	if !exists {
		m.writeErrorResponse(w, "NoSuchUpload", "The specified upload does not exist", http.StatusNotFound)
		return
	}

	var request CompleteMultipartUploadRequest
	if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
		m.writeErrorResponse(w, "MalformedXML", "Invalid XML in request body", http.StatusBadRequest)
		return
	}

	// Validate and assemble parts
	var finalContent []byte
	var partNumbers []int
	for _, part := range request.Parts {
		partNumbers = append(partNumbers, part.PartNumber)
	}
	sort.Ints(partNumbers)

	for _, partNum := range partNumbers {
		partInfo, exists := upload.Parts[partNum]
		if !exists {
			m.writeErrorResponse(w, "InvalidPart", fmt.Sprintf("Part %d not found", partNum), http.StatusBadRequest)
			return
		}
		finalContent = append(finalContent, partInfo.Content...)
	}

	// Create the final object
	finalETag := m.PutObject(key, finalContent)

	// Clean up the upload
	m.mutex.Lock()
	delete(m.uploads, uploadID)
	m.mutex.Unlock()

	response := CompleteMultipartUploadResponse{
		Location: fmt.Sprintf("https://%s.s3.amazonaws.com/%s", m.bucket, key),
		Bucket:   m.bucket,
		Key:      key,
		ETag:     finalETag,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

// handleAbortMultipartUpload handles DELETE requests to abort multipart uploads
func (m *Server) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, key string, query url.Values) {
	uploadID := query.Get("uploadId")

	m.mutex.Lock()
	_, exists := m.uploads[uploadID]
	if exists {
		delete(m.uploads, uploadID)
	}
	m.mutex.Unlock()

	if exists {
		w.WriteHeader(http.StatusNoContent)
	} else {
		m.writeErrorResponse(w, "NoSuchUpload", "The specified upload does not exist", http.StatusNotFound)
	}
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
func (m *Server) GetMultipartUpload(uploadID string) (*MockMultipartUpload, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	upload, exists := m.uploads[uploadID]
	return upload, exists
}

// ListMultipartUploads returns all active multipart uploads
func (m *Server) ListMultipartUploads() map[string]*MockMultipartUpload {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// Return a copy to avoid race conditions
	uploads := make(map[string]*MockMultipartUpload)
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
