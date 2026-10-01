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

// Package s3 implements a lightweight
// client of the AWS S3 API.
//
// The Reader type can be used to view
// S3 objects as an io.Reader or io.ReaderAt.
package s3

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/kelindar/s3/aws"
	"github.com/valyala/fasthttp"
)

var (
	// ErrInvalidBucket is returned from calls that attempt
	// to use a bucket name that isn't valid according to
	// the S3 specification.
	ErrInvalidBucket = errors.New("invalid bucket name")
	// ErrETagChanged is returned from read operations where
	// the ETag of the underlying file has changed since
	// the file handle was constructed. (This package guarantees
	// that file read operations are always consistent with respect
	// to the ETag originally associated with the file handle.)
	ErrETagChanged = errors.New("file ETag changed")
)

func badBucket(name string) error {
	return fmt.Errorf("%w: %s", ErrInvalidBucket, name)
}

// ValidBucket returns whether or not
// bucket is a valid bucket name.
//
// See https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
//
// Note: ValidBucket does not allow '.' characters,
// since bucket names containing dots are not accessible
// over HTTPS. (AWS docs say "not recommended for uses other than static website hosting.")
func ValidBucket(bucket string) bool {
	switch {
	case len(bucket) < 3 || len(bucket) > 63:
		return false
	case strings.HasPrefix(bucket, "xn--"):
		return false
	case strings.HasSuffix(bucket, "-s3alias"):
		return false
	}
	for i := 0; i < len(bucket); i++ {
		switch {
		case bucket[i] >= 'a' && bucket[i] <= 'z':
			continue
		case bucket[i] >= '0' && bucket[i] <= '9':
			continue
		case i > 0 && i < len(bucket)-1 && bucket[i] == '-':
			continue
		case i > 0 && i < len(bucket)-1 && bucket[i] == '.' && bucket[i-1] != '.':
			continue
		}
		return false
	}
	return true
}

// Reader presents a read-only view of an S3 object
type Reader struct {
	// Key is the sigining key that
	// Reader uses to make HTTP requests.
	// The key may have to be refreshed
	// every so often (see aws.SigningKey.Expired)
	Key *aws.SigningKey `xml:"-"`

	ctx context.Context

	// ETag is the ETag of the object in S3
	// as returned by listing or a HEAD operation.
	ETag string `xml:"ETag"`
	// LastModified is the object's LastModified time
	// as returned by listing or a HEAD operation.
	LastModified time.Time `xml:"LastModified"`
	// Size is the object size in bytes.
	// It is populated on Open.
	Size int64 `xml:"Size"`
	// Bucket is the S3 bucket holding the object.
	Bucket string `xml:"-"`
	// Path is the S3 object key.
	Path string `xml:"Key"`
}

// rawURI produces a URI with a pre-escaped path+query string
func rawURI(k *aws.SigningKey, bucket string, query string) string {
	switch {
	case k.BaseURI != "":
		return k.BaseURI + "/" + bucket + "/" + query
	// use virtual-host style if the bucket is compatible
	// (fallback to path-style if not)
	case strings.IndexByte(bucket, '.') < 0:
		return "https://" + bucket + ".s3." + k.Region + ".amazonaws.com" + "/" + query
	default:
		return "https://s3." + k.Region + ".amazonaws.com" + "/" + bucket + "/" + query
	}
}

// setURI builds directly in the request's reusable header buffer. object is
// unescaped; query must already be escaped and in canonical order.
func setURI(req *fasthttp.Request, k *aws.SigningKey, bucket, object, query string) {
	req.SetRequestURI(cmp.Or(k.BaseURI, "https://"))
	target := req.Header.RequestURI()
	switch {
	case k.BaseURI != "":
		target = append(target, '/')
		target = append(target, bucket...)
	case !strings.Contains(bucket, "."):
		target = append(target, bucket...)
		target = append(target, ".s3."...)
		target = append(target, k.Region...)
		target = append(target, ".amazonaws.com"...)
	default:
		target = append(target, "s3."...)
		target = append(target, k.Region...)
		target = append(target, ".amazonaws.com/"...)
		target = append(target, bucket...)
	}
	target = appendPathEscape(append(target, '/'), object)
	if query != "" {
		target = append(target, '?')
		target = append(target, query...)
	}
	req.SetRequestURIBytes(target)
	req.URI().DisablePathNormalizing = true
}

func appendPathEscape(dst []byte, path string) []byte {
	const hex = "0123456789ABCDEF"
	for i := range len(path) {
		switch c := path[i]; {
		case c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9', c == '-' || c == '_' || c == '.' || c == '~' || c == '/':
			dst = append(dst, c)
		default:
			dst = append(dst, '%', hex[c>>4], hex[c&15])
		}
	}
	return dst
}

// perform S3-specific path escaping;
// all the special characters are turned
// into their quoted bits, but we turn %2F
// back into / because AWS accepts those
// as part of the URI
func almostPathEscape(s string) string {
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9':
		case c == '-' || c == '_' || c == '.' || c == '~' || c == '/':
		default:
			return string(appendPathEscape(nil, s))
		}
	}
	return s
}

func queryEscape(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

func appendQueryEscape(dst []byte, value string) []byte {
	for {
		before, after, found := strings.Cut(value, " ")
		dst = fasthttp.AppendQuotedArg(dst, []byte(before))
		if !found {
			return dst
		}
		dst = append(dst, "%20"...)
		value = after
	}
}

// uri produces a URI by path-escaping the object string
// and passing it to rawURI (see also almostPathEscape)
func uri(k *aws.SigningKey, bucket, object string) string {
	return rawURI(k, bucket, almostPathEscape(object))
}

// URL returns a signed URL for a bucket and object
// that can be used directly with http.Get.
func URL(k *aws.SigningKey, bucket, object string) (string, error) {
	if !ValidBucket(bucket) {
		return "", badBucket(bucket)
	}
	return k.SignURL(uri(k, bucket, object), 1*time.Hour)
}

// Stat performs a HEAD on an S3 object
// and returns an associated Reader.
func Stat(k *aws.SigningKey, bucket, object string) (*Reader, error) {
	r := new(Reader)
	body, err := r.open(k, bucket, object, false)
	if body != nil {
		body.Close()
	}
	return r, err
}

// NewFile constructs a File that points to the given
// bucket, object, etag, and file size. The caller is
// assumed to have correctly determined these attributes
// in advance; this call does not perform any I/O to verify
// that the provided object exists or has a matching ETag
// and size.
func NewFile(k *aws.SigningKey, bucket, object, etag string, size int64) *File {
	return &File{
		Reader: Reader{
			Key:    k,
			Bucket: bucket,
			Path:   object,
			ETag:   etag,
			Size:   size,
		},
	}
}

// Open performs a GET on an S3 object
// and returns the associated File.
func Open(k *aws.SigningKey, bucket, object string, contents bool) (*File, error) {
	return openContext(context.Background(), k, bucket, object, contents)
}

// openContext opens an object using ctx for S3 requests.
func openContext(ctx context.Context, k *aws.SigningKey, bucket, object string, contents bool) (*File, error) {
	f := new(File)
	err := f.openContext(ctx, k, bucket, object, contents)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (f *File) openContext(ctx context.Context, k *aws.SigningKey, bucket, object string, contents bool) error {
	body, err := f.Reader.openContext(ctx, k, bucket, object, contents)
	if err != nil {
		if body != nil {
			body.Close()
		}
		return err
	}
	if !contents {
		body.Close()
		body = nil
	}
	f.body = body
	return nil
}

func (r *Reader) open(k *aws.SigningKey, bucket, object string, contents bool) (io.ReadCloser, error) {
	return r.openContext(context.Background(), k, bucket, object, contents)
}

func (r *Reader) openContext(ctx context.Context, k *aws.SigningKey, bucket, object string, contents bool) (io.ReadCloser, error) {
	if !ValidBucket(bucket) {
		return nil, badBucket(bucket)
	}
	method := http.MethodHead
	if contents {
		method = http.MethodGet
	}
	res, err := doObject(ctx, k, method, bucket, object, nil)
	if err != nil {
		return nil, err
	}
	switch {
	case res.StatusCode != 200:
		var inner error
		switch res.StatusCode {
		case 404:
			inner = fs.ErrNotExist
		case 403:
			inner = fs.ErrPermission
		default:
			// NOTE: we can't extractMessage() here, because HEAD
			// errors do not produce a response with an error message
			inner = fmt.Errorf("s3.Open: %s returned %s", method, res.status())
		}
		err := &fs.PathError{
			Op:   "open",
			Path: "s3://" + bucket + "/" + object,
			Err:  inner,
		}
		return res.Body, err
	case res.ContentLength < 0:
		return res.Body, fmt.Errorf("s3.Open: content length %d invalid", res.ContentLength)
	}
	lm, _ := time.Parse(time.RFC1123, res.Header.Get("Last-Modified"))
	*r = Reader{
		Key:          k,
		ctx:          ctx,
		ETag:         res.Header.Get("ETag"),
		LastModified: lm,
		Size:         res.ContentLength,
		Bucket:       bucket,
		Path:         object,
	}
	return res.Body, nil
}

// WriteTo implements io.WriterTo
func (r *Reader) WriteTo(w io.Writer) (int64, error) {
	res, err := doObject(r.requestContext(), r.Key, http.MethodGet, r.Bucket, r.Path, nil)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return 0, fmt.Errorf("s3.Reader.WriteTo: status %s %q", res.status(), extractMessage(res.Body))
	}
	return io.Copy(w, res.Body)
}

// RangeReader produces an io.ReadCloser that reads
// bytes in the range from [off, off+width)
//
// It is the caller's responsibility to call Close()
// on the returned io.ReadCloser.
func (r *Reader) RangeReader(off, width int64) (io.ReadCloser, error) {
	return r.rangeReaderContext(r.requestContext(), off, width)
}

func (r *Reader) requestContext() context.Context {
	return cmp.Or(r.ctx, context.Background())
}

func (r *Reader) rangeReaderContext(ctx context.Context, off, width int64) (io.ReadCloser, error) {
	switch {
	case off < 0 || width < 0 || off > (1<<63-1)-width:
		return nil, fmt.Errorf("s3.Reader.RangeReader: invalid range %d + %d", off, width)
	case width == 0:
		return http.NoBody, nil
	}
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	setURI(req, r.Key, r.Bucket, r.Path, "")
	req.Header.SetMethod(fasthttp.MethodGet)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+width-1))
	var headers [1][2]string
	count := 0
	if r.ETag != "" {
		headers[0] = [2]string{"if-match", r.ETag}
		count++
	}
	signRequest(r.Key, req, nil, headers[:count]...)

	res, err := flakyFast(ctx, req)
	if err != nil {
		return nil, err
	}
	switch res.StatusCode {
	default:
		defer res.Body.Close()
		return nil, fmt.Errorf("s3.Reader.RangeReader: status %s %q", res.status(), extractMessage(res.Body))
	case http.StatusPreconditionFailed:
		res.Body.Close()
		return nil, ErrETagChanged
	case http.StatusNotFound:
		res.Body.Close()
		return nil, &fs.PathError{Op: "read", Path: r.Path, Err: fs.ErrNotExist}
	case http.StatusPartialContent, http.StatusOK:
		// okay; fallthrough
	}
	return res.Body, nil
}

// ReadAt implements io.ReaderAt
func (r *Reader) ReadAt(dst []byte, off int64) (int, error) {
	switch {
	case off < 0:
		return 0, fmt.Errorf("s3.Reader.ReadAt: negative offset %d", off)
	case len(dst) == 0:
		return 0, nil
	case off >= r.Size:
		return 0, io.EOF
	}
	width := min(int64(len(dst)), r.Size-off)
	rd, err := r.RangeReader(off, width)
	if err != nil {
		return 0, err
	}
	defer rd.Close()
	n, err := io.ReadFull(rd, dst[:int(width)])
	if err == nil && n < len(dst) {
		err = io.EOF
	}
	return n, err
}

// BucketRegion returns the region associated
// with the given bucket.
func BucketRegion(k *aws.SigningKey, bucket string) (string, error) {
	switch {
	case !ValidBucket(bucket):
		return "", badBucket(bucket)
	case k.BaseURI != "":
		return k.Region, nil
	}
	res, err := doObject(context.Background(), k, http.MethodHead, bucket, "", nil)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case 403:
		return k.Region, nil
	case 200, 301:
		// ok
	default:
		return "", fmt.Errorf("s3.BucketRegion: %s %q", res.status(), extractMessage(res.Body))
	}
	return cmp.Or(res.Header.Get("x-amz-bucket-region"), k.Region), nil
}

// DeriveForBucket can be passed to aws.AmbientCreds
// as a DeriveFn that automatically re-derives keys
// so that they apply to the region in which the
// given bucket lives.
func DeriveForBucket(bucket string) aws.DeriveFn {
	return func(baseURI, id, secret, token, region, service string) (*aws.SigningKey, error) {
		switch {
		case !ValidBucket(bucket):
			return nil, badBucket(bucket)
		case service != "s3" && service != "b2":
			return nil, fmt.Errorf("s3.DeriveForBucket: expected service \"s3\"; got %q", service)
		}

		k := aws.DeriveKey(baseURI, id, secret, region, service)
		k.Token = token
		bregion, err := BucketRegion(k, bucket)
		switch {
		case err != nil:
			return nil, err
		case bregion == region:
			return k, nil
		}
		k = aws.DeriveKey(baseURI, id, secret, bregion, service)
		k.Token = token
		return k, nil
	}
}
