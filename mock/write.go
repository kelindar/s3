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
	"cmp"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/valyala/fasthttp"
)

// generateUploadID generates a unique upload ID for multipart uploads
func generateUploadID() string {
	return fmt.Sprintf("upload-%d-%d", time.Now().UnixNano(), uploadSequence.Add(1))
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
