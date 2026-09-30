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
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/kelindar/s3/aws"
	"github.com/valyala/fasthttp"
	"golang.org/x/sync/errgroup"
)

// uploader wraps the state of a multi-part upload.
// This is now an internal implementation detail.
// Use Bucket.Upload() for the public API.
type uploader struct {
	// Key is the key used to sign requests.
	// It cannot be nil.
	Key *aws.SigningKey
	// ContentType, if not an empty string,
	// will be the Content-Type of the new object.
	ContentType string

	Bucket, Object string

	Scheme string

	// Host, if not the empty string,
	// is the host of the bucket.
	// If Host is unset, then "s3.amazonaws.com" is used.
	//
	// Requests are always made to <bucket>.host
	Host string

	// Mbbs, if non-zero, is the expected
	// link speed in Mbps. This number is
	// used to determine the optimal parallelism
	// for uploads.
	// (For example, use Mbps = 25000 on a 25Gbps link, etc.)
	Mbps int

	// upload ID
	id string

	// next part
	part int64

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

	// background uploads
	bg       sync.WaitGroup
	asyncerr error
}

// MinPartSize returns the minimum part size
// for the uploader.
//
// (The return value of MinPartSize is always s3.MinPartSize.)
func (u *uploader) MinPartSize() int {
	return MinPartSize
}

type tagpart struct {
	Num  int64  `xml:"PartNumber"`
	ETag string `xml:"ETag"`
	size int64  `xml:"-"`
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

func (u *uploader) req(ctx context.Context, method, uri, query string) *http.Request {
	obj := url.URL{
		Scheme:   u.Scheme,
		RawQuery: query,
	}
	if u.Key.BaseURI == "" {
		obj.Path = "/" + uri                      // fully decoded path
		obj.RawPath = "/" + almostPathEscape(uri) // escaped path
		obj.Host = u.Bucket + "." + u.Host
	} else {
		obj.Path = "/" + u.Bucket + "/" + uri                      // fully decoded path
		obj.RawPath = "/" + u.Bucket + "/" + almostPathEscape(uri) // escaped path
		obj.Host = u.Host

	}
	req, _ := http.NewRequestWithContext(ctx, method, obj.String(), nil)
	return req
}

func (u *uploader) signedRequest(method, query string, body []byte) *fasthttp.Request {
	path := "/" + almostPathEscape(u.Object)
	host := u.Bucket + "." + u.Host
	if u.Key.BaseURI != "" {
		path = "/" + u.Bucket + path
		host = u.Host
	}
	var uri strings.Builder
	uri.Grow(len(u.Scheme) + len(host) + len(path) + len(query) + 4)
	uri.WriteString(u.Scheme)
	uri.WriteString("://")
	uri.WriteString(host)
	uri.WriteString(path)
	uri.WriteByte('?')
	uri.WriteString(query)

	req := fasthttp.AcquireRequest()
	req.SetRequestURI(uri.String())
	req.URI().DisablePathNormalizing = true
	req.Header.SetMethod(method)
	req.Header.SetHost(host)
	signRequest(u.Key, req, body)
	return req
}

// Start begins a multipart upload.
// Start must be called exactly once,
// before any calls to WritePart are made.
func (u *uploader) Start(ctx context.Context) error {
	if u.started {
		panic("multiple calls to uploader.Start()")
	}
	if u.Key.BaseURI == "" {
		u.Scheme = "https"
		u.Host = "s3." + u.Key.Region + ".amazonaws.com"
	} else {
		uu, _ := url.Parse(u.Key.BaseURI)
		u.Scheme = uu.Scheme
		u.Host = uu.Host
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

// NextPart atomically increments the internal
// part counter inside the uploader and returns
// the next available part number.
// Note that AWS multipart uploads have 1-based
// part numbers (i.e. the first part is part 1).
//
// If the data to be uploaded is intrinsically un-ordered,
// then NextPart() can be used to greedily assign part numbers.
//
// Note that currently the maximum part number
// allowed by AWS is 10000.
func (u *uploader) NextPart() int64 {
	return atomic.AddInt64(&u.part, 1)
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
		for totalSize/partSize > MaxParts {
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

// Upload uploads the part number num from
// the ReadCloser r, which must return exactly size bytes of data.
// S3 prohibits multi-part upload parts smaller than 5MB (except
// for the final bytes), so size must be at least 5MB.
//
// It is safe to call Upload from multiple goroutines
// simultaneously. However, calls to Upload must be
// synchronized to occur strictly after a call to Start
// and strictly before a call to Close.
func (u *uploader) Upload(num int64, contents []byte) error {
	switch {
	case !u.started:
		panic("s3.uploader.UploadPart before Start()")
	case len(contents) < MinPartSize:
		return fmt.Errorf("UploadPart size %d below min part size %d", len(contents), MinPartSize)
	}
	return u.upload(context.Background(), num, contents)
}

func (u *uploader) upload(ctx context.Context, num int64, contents []byte) error {
	query := fmt.Sprintf("partNumber=%d&uploadId=%s", num, u.id)
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
		size: int64(len(contents)),
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
//
// As an optimization, most of the work for CopyFrom is
// performed asynchronously. Callers must call Close and
// check its return value in order to correctly handle
// errors from CopyFrom.
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

	// update the max part before launching anything
	// so that Close can perform an upload at the same time
	// as the copy-part operation is still happening
	u.lock.Lock()
	u.maxpart = max(u.maxpart, num)
	u.lock.Unlock()

	u.bg.Add(1)
	go u.copy(ctx, num, source, start, end)
	return nil
}

func (u *uploader) noteErr(err error) {
	u.lock.Lock()
	defer u.lock.Unlock()
	if u.asyncerr == nil {
		u.asyncerr = err
	}
}

func (u *uploader) copy(ctx context.Context, num int64, source *Reader, start int64, end int64) {
	defer u.bg.Done()
	req := u.req(ctx, "PUT", u.Object, fmt.Sprintf("partNumber=%d&uploadId=%s", num, u.id))
	req.Header.Add("x-amz-copy-source", fmt.Sprintf("/%s/%s", source.Bucket, source.Path))
	req.Header.Add("x-amz-copy-source-if-match", source.ETag)
	size := source.Size
	if start != 0 || end != 0 {
		size = end - start
		req.Header.Add("x-amz-copy-source-range", fmt.Sprintf("bytes=%d-%d", start, end-1))
	}
	u.Key.SignV4(req, nil)
	res, err := flakyDo(req, nil)
	if err != nil {
		u.noteErr(err)
		return
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		u.noteErr(fmt.Errorf("CopyFrom: %s %q", res.status(), extractMessage(res.Body)))
		return
	}
	rt, err := decodeMultipartResponse(res.Body)
	if err != nil || rt.ETag == "" {
		u.noteErr(fmt.Errorf("s3.Uploader.CopyFrom: response missing ETag?"))
		return
	}
	u.lock.Lock()
	u.parts = append(u.parts, tagpart{
		Num:  num,
		ETag: rt.ETag,
		size: size,
	})
	u.lock.Unlock()
}

// CompletedParts returns the number of parts
// that have been successfully uploaded.
//
// It is safe to call CompletedParts from multiple
// goroutines that may also be calling UploadPart,
// but be wary of logical races involving the number
// of uploaded parts.
func (u *uploader) CompletedParts() int {
	u.lock.Lock()
	defer u.lock.Unlock()
	return len(u.parts)
}

// Closed returns whether or not Close
// has been called on u.
func (u *uploader) Closed() bool { return u.finished }

// ID returns the "Upload ID" of this upload.
// The return value of ID is only valid after
// Start has been called.
func (u *uploader) ID() string { return u.id }

func (u *uploader) Size() int64 {
	u.lock.Lock()
	defer u.lock.Unlock()
	if !u.finished {
		return 0
	}
	out := int64(0)
	for i := range u.parts {
		out += u.parts[i].size
	}
	return out
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
	// wait for any/all CopyFrom operations to finish;
	// after this we know u.parts will be fully up-to-date
	u.bg.Wait()
	if u.asyncerr != nil {
		return u.asyncerr
	}

	// the S3 API barfs if parts are not in ascending order
	sort.Slice(u.parts, func(i, j int) bool {
		return u.parts[i].Num < u.parts[j].Num
	})

	buf, err := encodeCompleteMultipart(u.parts)
	if err != nil {
		return err
	}
	req := u.signedRequest(fasthttp.MethodPost, "uploadId="+u.id, buf)
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

func (u *uploader) idealParallel(parts int64) int {
	const max = 40
	res := max
	if u.Mbps != 0 {
		// guess 640Mbps = 80MB/s per connection
		// (S3 guidelines say 85-90MB/s)
		res = u.Mbps / 800
	}
	switch {
	case parts < int64(res) && parts > 0:
		return int(parts)
	case res <= 0:
		return 1
	}
	return res
}

// Abort aborts a multi-part upload.
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
	u.bg.Wait()
	req := u.req(ctx, "DELETE", u.Object, fmt.Sprintf("uploadId=%s", u.id))
	u.Key.SignV4(req, nil)

	res, err := doFast(req, nil)
	if err != nil {
		return fmt.Errorf("s3.Uploader.Abort: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 204 {
		return fmt.Errorf("s3.Uploader.Abort: %s %s", res.status(), extractMessage(res.Body))
	}

	// reset internal state
	u.part = 0
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
	parallel := 0
	if nonfinal > 0 {
		parallel = u.idealParallel(nonfinal)
	}

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
