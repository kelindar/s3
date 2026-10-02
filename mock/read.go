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
	"cmp"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/valyala/fasthttp"
)

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
