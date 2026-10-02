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
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kelindar/s3/aws"
	"github.com/valyala/fasthttp"
	"golang.org/x/sync/errgroup"
)

// uploader wraps the state of a multi-part upload.
// Bucket.WriteFrom and Bucket.Compose own its lifecycle.
type uploader struct {
	// Key is the key used to sign requests.
	// It cannot be nil.
	Key *aws.SigningKey
	// ContentType, if not an empty string,
	// will be the Content-Type of the new object.
	ContentType string

	Bucket, Object string

	// upload ID
	id string

	// ETag of the final result;
	// just the empty string until Close is called
	finalETag string

	// updated by Start and Close, respectively,
	// which require synchronization with concurrent
	// UploadPart calls
	started, finished bool

	// list of ETags collected as
	// parts are uploaded; these are
	// sent as part of the CompleteMultipartUpload call
	lock    sync.Mutex
	parts   []tagpart
	maxpart int64
}

type tagpart struct {
	Num  int64  `xml:"PartNumber"`
	ETag string `xml:"ETag"`
}

func encodeCompleteMultipart(parts []tagpart) ([]byte, error) {
	var body bytes.Buffer
	body.Grow(110 + len(parts)*96)
	body.WriteString(`<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	var digits [20]byte
	for _, part := range parts {
		body.WriteString("<Part><PartNumber>")
		body.Write(strconv.AppendInt(digits[:0], part.Num, 10))
		body.WriteString("</PartNumber><ETag>")
		if err := xml.EscapeText(&body, []byte(part.ETag)); err != nil {
			return nil, err
		}
		body.WriteString("</ETag></Part>")
	}
	body.WriteString("</CompleteMultipartUpload>")
	return body.Bytes(), nil
}

type multipartResponse struct {
	XMLName xml.Name
	Bucket  string `xml:"Bucket"`
	Key     string `xml:"Key"`
	ID      string `xml:"UploadId"`
	ETag    string `xml:"ETag"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

func decodeMultipartResponse(r io.Reader) (multipartResponse, error) {
	body := xmlBodies.Get().(*bytes.Buffer)
	body.Reset()
	defer func() {
		if body.Cap() <= 256<<10 {
			body.Reset()
			xmlBodies.Put(body)
		}
	}()
	if _, err := body.ReadFrom(r); err != nil {
		return multipartResponse{}, err
	}
	if response, ok := scanMultipartResponse(body.Bytes()); ok {
		return response, nil
	}
	var response multipartResponse
	err := xml.NewDecoder(bytes.NewReader(body.Bytes())).Decode(&response)
	return response, err
}

func scanMultipartResponse(data []byte) (multipartResponse, bool) {
	p := listingXML{data: data}
	if bytes.HasPrefix(data, []byte{0xef, 0xbb, 0xbf}) {
		p.pos = 3
	}
	root, end, ok := p.next()
	if !ok || end || root.name == nil {
		return multipartResponse{}, false
	}
	response := multipartResponse{XMLName: xml.Name{Local: string(root.name)}}
	if !root.self {
		for {
			tag, end, ok := p.next()
			if !ok || tag.name == nil {
				return multipartResponse{}, false
			}
			if end {
				if !bytes.Equal(tag.name, root.name) {
					return multipartResponse{}, false
				}
				break
			}
			var field *string
			switch {
			case listingXMLIs(tag.name, "Bucket"):
				field = &response.Bucket
			case listingXMLIs(tag.name, "Key"):
				field = &response.Key
			case listingXMLIs(tag.name, "UploadId"):
				field = &response.ID
			case listingXMLIs(tag.name, "ETag"):
				field = &response.ETag
			case listingXMLIs(tag.name, "Code"):
				field = &response.Code
			case listingXMLIs(tag.name, "Message"):
				field = &response.Message
			}
			switch {
			case field == nil:
				ok = p.skip(tag)
			default:
				*field, ok = p.text(tag)
			}
			if !ok {
				return multipartResponse{}, false
			}
		}
	}
	trailing, end, ok := p.next()
	return response, ok && !end && trailing.name == nil
}

func (u *uploader) signedRequest(method, query string, body []byte, headers ...[2]string) *fasthttp.Request {
	req := fasthttp.AcquireRequest()
	setURI(req, u.Key, u.Bucket, u.Object, query)
	req.Header.SetMethod(method)
	signRequest(u.Key, req, body, headers...)
	return req
}

// Start begins a multipart upload.
// Start must be called exactly once,
// before any calls to WritePart are made.
func (u *uploader) Start(ctx context.Context) error {
	if u.started {
		panic("multiple calls to uploader.Start()")
	}
	if u.Bucket == "" || u.Object == "" {
		return fmt.Errorf("s3.Uploader.Bucket and s3.Uploader.Object must be present")
	}
	req := u.signedRequest(fasthttp.MethodPost, "uploads=", nil)
	defer fasthttp.ReleaseRequest(req)
	if u.ContentType != "" {
		req.Header.SetContentType(u.ContentType)
	}
	res, err := doFastRequest(ctx, req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("s3.Uploader.Start: %s %q", res.status(), extractMessage(res.Body))
	}
	rt, err := decodeMultipartResponse(res.Body)
	if err != nil {
		return err
	}
	switch {
	case rt.Bucket != u.Bucket:
		return fmt.Errorf("returned bucket %q not input bucket %q?", rt.Bucket, u.Bucket)
	case rt.Key != u.Object:
		return fmt.Errorf("returned key %q not input key %q?", rt.Key, u.Object)
	}
	u.started = true
	u.id = rt.ID
	return nil
}

// MinPartSize is the minimum size for
// all of the parts of a multi-part upload
// except for the final part.

// Default upload configuration values
const (
	MinPartSize = 5 * 1024 * 1024
	MaxParts    = 10000 // AWS limit
)

// Retain at most 10 MiB of idle part buffers, independent of upload concurrency.
var uploadBuffers = make(chan *[MinPartSize]byte, 2)

// calculatePartSize determines the optimal part size for a given total size
func calculatePartSize(totalSize int64) int64 {
	partSize := int64(MinPartSize)
	if totalSize > 0 { // Keep doubling until we have ≤10,000 parts
		for (totalSize-1)/partSize >= MaxParts {
			partSize *= 2
		}
	}

	return partSize
}

func extractS3Error(r io.Reader) (code, message string) {
	rt := struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}{}
	if xml.NewDecoder(r).Decode(&rt) == nil {
		return rt.Code, rt.Message
	}
	return "", "(no message)"
}

// extractMessage tries to extract the <Message/> field of an XML response.
func extractMessage(r io.Reader) string {
	_, message := extractS3Error(r)
	return message
}

func (u *uploader) upload(ctx context.Context, num int64, contents []byte) error {
	query := fmt.Sprintf("partNumber=%d&uploadId=%s", num, queryEscape(u.id))
	req := u.signedRequest(fasthttp.MethodPut, query, contents)
	defer fasthttp.ReleaseRequest(req)
	res, err := flakyFast(ctx, req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("UploadPart: %s %q", res.status(), extractMessage(res.Body))
	}
	etag := res.Header.Get("ETag")
	if etag == "" {
		return fmt.Errorf("s3.Uploader.UploadPart: response missing ETag?")
	}
	u.lock.Lock()
	u.maxpart = max(u.maxpart, num)
	u.parts = append(u.parts, tagpart{
		Num:  num,
		ETag: etag,
	})
	u.lock.Unlock()
	return nil
}

func (u *uploader) uploadWithContext(ctx context.Context, num int64, contents []byte) error {
	switch {
	case !u.started:
		panic("s3.uploader.UploadPart before Start()")
	case len(contents) < MinPartSize:
		return fmt.Errorf("UploadPart size %d below min part size %d", len(contents), MinPartSize)
	}
	return u.upload(ctx, num, contents)
}

// CopyFrom performs a server side copy for the part number `num`.
//
// Set `start` and `end` to `0` to copy the entire source object.
//
// It is safe to call CopyFrom from multiple goroutines
// simultaneously. However, calls to CopyFrom must be
// synchronized to occur strictly after a call to Start
// and strictly before a call to Close.
func (u *uploader) CopyFrom(ctx context.Context, num int64, source *Reader, start int64, end int64) error {
	if !u.started {
		panic("s3.uploader.CopyFrom before Start()")
	}
	size := source.Size
	if start != 0 || end != 0 {
		switch {
		case start < 0 || end < 0:
			return errors.New("start and end values must be positive numbers")
		case end > size:
			return fmt.Errorf("end value %d greater than source size %d", end, size)
		}
		size = end - start
	}
	if size < MinPartSize {
		return fmt.Errorf("CopyFrom size %d below min part size %d", size, MinPartSize)
	}

	headers := [3][2]string{
		{"x-amz-copy-source", "/" + source.Bucket + "/" + almostPathEscape(source.Path)},
		{"x-amz-copy-source-if-match", source.ETag},
	}
	count := 2
	if start != 0 || end != 0 {
		headers[2] = [2]string{"x-amz-copy-source-range", fmt.Sprintf("bytes=%d-%d", start, end-1)}
		count++
	}
	req := u.signedRequest(fasthttp.MethodPut, fmt.Sprintf("partNumber=%d&uploadId=%s", num, queryEscape(u.id)), nil, headers[:count]...)
	defer fasthttp.ReleaseRequest(req)
	res, err := flakyFast(ctx, req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("CopyFrom: %s %q", res.status(), extractMessage(res.Body))
	}
	rt, err := decodeMultipartResponse(res.Body)
	if err != nil || rt.ETag == "" {
		return fmt.Errorf("s3.Uploader.CopyFrom: response missing ETag?")
	}
	u.lock.Lock()
	u.maxpart = max(u.maxpart, num)
	u.parts = append(u.parts, tagpart{
		Num:  num,
		ETag: rt.ETag,
	})
	u.lock.Unlock()
	return nil
}

// Close uploads the final part of the multi-part upload
// and asks S3 to finalize the object from its constituent parts.
// (If size is zero, then r may be nil, in which case no final
// part is uploaded before the multi-part object is finalized.)
//
// Close will panic if Start has never been called
// or if Close has already been called and returned successfully.
func (u *uploader) Close(ctx context.Context, final []byte) error {
	switch {
	case !u.started:
		panic("s3.uploader.Close before Start()")
	case u.finished:
		panic("multiple calls to s3.uploader.Close")
	}
	if len(final) > 0 {
		// it is safe to read maxpart here because
		// maxpart is updated in calls to CopyFrom and Upload,
		// and we've specified that it is not safe for the caller
		// to let those race with Close
		err := u.upload(ctx, u.maxpart+1, final)
		if err != nil {
			return err
		}
	}
	// the S3 API barfs if parts are not in ascending order
	sort.Slice(u.parts, func(i, j int) bool {
		return u.parts[i].Num < u.parts[j].Num
	})

	buf, err := encodeCompleteMultipart(u.parts)
	if err != nil {
		return err
	}
	req := u.signedRequest(fasthttp.MethodPost, "uploadId="+queryEscape(u.id), buf)
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetContentType("application/xml")
	res, err := flakyFast(ctx, req)
	if err != nil {
		return fmt.Errorf("s3.Uploader.Close: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("s3.Uploader.Close: %s %q", res.status(), extractMessage(res.Body))
	}

	// This is a bit nasty:
	// the upload can fail after a 200 if we
	// get a response with <Error/>, so we have
	// to examine the xml name of the returned value
	// in order to determine if we got the write thing
	rt, err := decodeMultipartResponse(res.Body)
	if err != nil {
		return fmt.Errorf("s3.Uploader.Close: decoding response: %w", err)
	}
	switch rt.XMLName.Local {
	default:
		return fmt.Errorf("s3.Uploader.Close: unexpected object %s", rt.XMLName.Local)
	case "Error":
		return fmt.Errorf("s3.Uploader.Close: %s %s", rt.Code, rt.Message)
	case "CompleteMultipartUploadResult":
		// ok; this is what we want
	}
	u.finalETag = rt.ETag
	u.finished = true
	return nil
}

// ETag returns the ETag of the final upload.
// The return value of ETag is only valid after
// Close has been called.
func (u *uploader) ETag() string {
	return u.finalETag
}

// Abort aborts a multi-part upload.
// It waits at most five seconds, or less if ctx expires sooner.
//
// Abort is *not* safe to call concurrently
// with Start, Close, or UploadPart.
//
// If Start has not been called on the Uploader,
// or if the uploader has successfully finished
// uploading, Abort does nothing.
//
// If Abort is called on a partially-finished Upload
// and returns without an error, then the state of
// the Uploader is reset so that Start may be called
// again to re-try the upload.
func (u *uploader) Abort(ctx context.Context) error {
	if !u.started || u.finished {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req := u.signedRequest(fasthttp.MethodDelete, "uploadId="+queryEscape(u.id), nil)
	defer fasthttp.ReleaseRequest(req)
	res, err := doFastRequest(ctx, req)
	if err != nil {
		return fmt.Errorf("s3.Uploader.Abort: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 204 {
		return fmt.Errorf("s3.Uploader.Abort: %s %s", res.status(), extractMessage(res.Body))
	}

	// reset internal state
	u.started = false
	u.finished = false
	u.id = ""
	u.parts = nil
	return nil
}

// UploadFrom is a utility method that performs
// a parallel upload of an io.ReaderAt of a given size.
//
// UploadFrom closes the Uploader after uploading
// the entirety of the contents of r.
//
// UploadFrom is not safe to call concurrently with
// UploadPart or Close.
func (u *uploader) UploadFrom(ctx context.Context, r io.ReaderAt, size int64) error {
	partSize := calculatePartSize(size)
	nonfinal := size / partSize
	endparts := nonfinal * partSize
	offset := int64(0)
	partCount := int(nonfinal)
	if size > endparts {
		partCount++
	}
	if len(u.parts) == 0 && cap(u.parts) < partCount {
		u.parts = make([]tagpart, 0, partCount)
	}
	parallel := int(min(max(nonfinal, 0), 40))

	g, uploadCtx := errgroup.WithContext(ctx)
	g.SetLimit(parallel)

	for i := 0; i < parallel; i++ {
		g.Go(func() error {
			var buf []byte
			if partSize == MinPartSize {
				var storage *[MinPartSize]byte
				select {
				case storage = <-uploadBuffers:
				default:
					storage = new([MinPartSize]byte)
				}
				defer func() {
					select {
					case uploadBuffers <- storage:
					default:
					}
				}()
				buf = storage[:]
			} else {
				buf = make([]byte, partSize)
			}
			for {
				loff := atomic.AddInt64(&offset, partSize) - partSize
				if loff >= endparts {
					break
				}

				// Check if context was cancelled
				select {
				case <-uploadCtx.Done():
					return uploadCtx.Err()
				default:
				}

				// 1-based part numbers
				part := (loff / partSize) + 1
				n, err := r.ReadAt(buf, loff)
				if int64(n) < partSize {
					if err == nil || errors.Is(err, io.EOF) {
						err = io.ErrUnexpectedEOF
					}
					return err
				}
				err = u.uploadWithContext(uploadCtx, part, buf)
				if err != nil {
					return fmt.Errorf("s3.UploadReaderAt part %d: %w", part, err)
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	var tail []byte
	tailsize := int(size - endparts)
	if tailsize > 0 {
		tail = make([]byte, tailsize)
		n, err := r.ReadAt(tail, endparts)
		if n < tailsize {
			if err == nil || errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
	}
	return u.Close(ctx, tail)
}
