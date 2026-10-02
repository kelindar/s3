package fuzz_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/kelindar/s3"
	signing "github.com/kelindar/s3/aws"
	"github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	bucketName = "fuzz-bucket"
	region     = "us-east-1"
)

func reference(t testing.TB, endpoint string) *sdk.Client {
	transport := &http.Transport{}
	t.Cleanup(transport.CloseIdleConnections)
	return sdk.New(sdk.Options{
		Region:                     region,
		Credentials:                credentials.NewStaticCredentialsProvider("fuzz-access", "fuzz-secret", ""),
		BaseEndpoint:               aws.String(endpoint),
		UsePathStyle:               true,
		Retryer:                    aws.NopRetryer{},
		HTTPClient:                 &http.Client{Transport: transport, Timeout: 5 * time.Second},
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
}

func objectKey(key string) bool {
	return len(key) <= 1024 && fs.ValidPath(key) && key != "."
}

func readBody(t *testing.T, body io.ReadCloser) []byte {
	t.Helper()
	data, err := io.ReadAll(body)
	closeErr := body.Close()
	require.NoError(t, err)
	assert.NoError(t, closeErr)
	return data
}

func FuzzSigning(f *testing.F) {
	f.Add("object", "", "", false)
	f.Add("folder/a b+%&☃", "next+token /=%", `"etag"`, true)
	f.Add("a/../b//c", "\x00\r\n", " \t match  value \t ", false)
	f.Fuzz(func(t *testing.T, object, queryValue, header string, unsigned bool) {
		if len(object)+len(queryValue)+len(header) > 4096 || !utf8.ValidString(object) || !utf8.ValidString(queryValue) || !utf8.ValidString(header) {
			t.Skip()
		}
		for _, r := range header {
			if r < ' ' && r != '\t' || r == 127 {
				t.Skip()
			}
		}
		key := signing.DeriveKey("https://example.com", "fuzz-access", "fuzz-secret", region, "s3")
		method := http.MethodGet
		var body []byte
		if unsigned {
			method, body, key.Token = http.MethodPut, []byte("payload"), "session+/="
		}
		target, err := s3.URL(key, bucketName, object)
		require.NoError(t, err)
		req, err := http.NewRequest(method, target, nil)
		require.NoError(t, err)
		req.URL.RawQuery = strings.ReplaceAll(url.Values{"prefix": {queryValue}, "list-type": {"2"}}.Encode(), "+", "%20")
		var headers [][2]string
		if header != "" {
			headers = append(headers, [2]string{"if-match", header})
			req.Header.Set("If-Match", header)
		}
		stamp, hash, auth := key.Sign(nil, []byte(method), []byte(req.URL.EscapedPath()), []byte(req.URL.RawQuery), []byte(req.URL.Host), body, headers...)
		when, err := time.Parse("20060102T150405Z", string(stamp))
		require.NoError(t, err)
		req.Header.Set("X-Amz-Content-Sha256", hash)
		err = v4.NewSigner().SignHTTP(context.Background(), aws.Credentials{AccessKeyID: key.AccessKey, SecretAccessKey: key.Secret, SessionToken: key.Token}, req, hash, "s3", region, when, func(o *v4.SignerOptions) {
			o.DisableURIPathEscaping = true
		})
		require.NoError(t, err)
		assert.Equal(t, req.Header.Get("Authorization"), string(auth))
	})
}

func FuzzObject(f *testing.F) {
	f.Add("object", []byte("payload"), uint16(0), uint16(3))
	f.Add("folder/a b+%&☃", []byte{0, 1, 255, '\n'}, uint16(2), uint16(1))
	f.Add("empty\x01", []byte{}, uint16(0), uint16(0))
	server := mock.New(bucketName, region)
	f.Cleanup(server.Close)
	server.SetRequestLogging(false)
	key := signing.DeriveKey(server.URL(), "fuzz-access", "fuzz-secret", region, "s3")
	bucket, sdkClient := s3.NewBucket(key, bucketName), reference(f, server.URL())
	f.Fuzz(func(t *testing.T, object string, data []byte, offset, width uint16) {
		if !objectKey(object) || len(data) > 64<<10 {
			t.Skip()
		}
		server.Clear()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		etag, err := bucket.Write(ctx, object, data)
		require.NoError(t, err)
		stored, exists := server.ObjectContent(object)
		require.True(t, exists)
		assert.True(t, bytes.Equal(data, stored))
		head, err := sdkClient.HeadObject(ctx, &sdk.HeadObjectInput{Bucket: aws.String(bucketName), Key: aws.String(object)})
		require.NoError(t, err)
		assert.Equal(t, etag, aws.ToString(head.ETag))
		assert.EqualValues(t, len(data), aws.ToInt64(head.ContentLength))
		get, err := sdkClient.GetObject(ctx, &sdk.GetObjectInput{Bucket: aws.String(bucketName), Key: aws.String(object), IfMatch: head.ETag})
		require.NoError(t, err)
		assert.Equal(t, data, readBody(t, get.Body))

		put, err := sdkClient.PutObject(ctx, &sdk.PutObjectInput{Bucket: aws.String(bucketName), Key: aws.String(object), Body: bytes.NewReader(data)})
		require.NoError(t, err)
		stored, exists = server.ObjectContent(object)
		require.True(t, exists)
		assert.True(t, bytes.Equal(data, stored))
		reader, err := s3.Stat(key, bucketName, object)
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(put.ETag), reader.ETag)
		assert.EqualValues(t, len(data), reader.Size)
		var copied bytes.Buffer
		n, err := reader.WriteTo(&copied)
		require.NoError(t, err)
		assert.EqualValues(t, len(data), n)
		assert.Equal(t, data, copied.Bytes())

		if len(data) > 0 {
			start := int(offset) % len(data)
			length := 1 + int(width)%(len(data)-start)
			body, err := bucket.OpenRangeContext(ctx, object, reader.ETag, int64(start), int64(length))
			require.NoError(t, err)
			got := readBody(t, body)
			get, err := sdkClient.GetObject(ctx, &sdk.GetObjectInput{Bucket: aws.String(bucketName), Key: aws.String(object), IfMatch: put.ETag, Range: aws.String(fmt.Sprintf("bytes=%d-%d", start, start+length-1))})
			require.NoError(t, err)
			assert.Equal(t, readBody(t, get.Body), got)
			assert.Equal(t, data[start:start+length], got)
		}
		require.NoError(t, bucket.Delete(ctx, object))
		_, err = sdkClient.GetObject(ctx, &sdk.GetObjectInput{Bucket: aws.String(bucketName), Key: aws.String(object)})
		var api smithy.APIError
		require.ErrorAs(t, err, &api)
		assert.Equal(t, "NoSuchKey", api.ErrorCode())
	})
}

func FuzzConditions(f *testing.F) {
	for mode := uint8(0); mode < 6; mode++ {
		f.Add("object", []byte("original"), []byte("replacement"), mode)
	}
	f.Add("folder/a b+%&☃", []byte{}, []byte{255, 0, 1}, uint8(3))
	server := mock.New(bucketName, region)
	f.Cleanup(server.Close)
	server.SetRequestLogging(false)
	key := signing.DeriveKey(server.URL(), "fuzz-access", "fuzz-secret", region, "s3")
	bucket, sdkClient := s3.NewBucket(key, bucketName), reference(f, server.URL())
	f.Fuzz(func(t *testing.T, object string, before, after []byte, mode uint8) {
		if !objectKey(object) || len(before)+len(after) > 64<<10 {
			t.Skip()
		}
		server.Clear()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		initialETag := server.PutObject(object, before)
		exists := mode%6 >= 3
		if !exists {
			server.DeleteObject(object)
		}
		input := &sdk.PutObjectInput{Bucket: aws.String(bucketName), Key: aws.String(object), Body: bytes.NewReader(after)}
		var condition s3.Condition
		switch mode % 3 {
		case 0:
			condition, input.IfNoneMatch = s3.IfNoneMatch("*"), aws.String("*")
		case 1:
			condition, input.IfMatch = s3.IfMatch(initialETag), aws.String(initialETag)
		case 2:
			condition, input.IfMatch = s3.IfMatch("stale"), aws.String("stale")
		}
		gotETag, gotApplied, err := bucket.WriteIf(ctx, object, after, condition)
		require.NoError(t, err)
		gotObject, gotExists := server.ObjectContent(object)

		// Each client starts from the same state; neither consumes the other's condition.
		server.DeleteObject(object)
		if exists {
			server.PutObject(object, before)
		}
		want, err := sdkClient.PutObject(ctx, input)
		wantApplied := err == nil
		if err != nil {
			var api smithy.APIError
			require.ErrorAs(t, err, &api)
			require.Contains(t, []string{"PreconditionFailed", "NoSuchKey"}, api.ErrorCode())
		}
		expectedApplied := mode%6 == 0 || mode%6 == 4
		assert.Equal(t, expectedApplied, gotApplied)
		assert.Equal(t, expectedApplied, wantApplied)
		if wantApplied {
			assert.Equal(t, aws.ToString(want.ETag), gotETag)
		} else {
			assert.Empty(t, gotETag)
		}
		wantObject, wantExists := server.ObjectContent(object)
		assert.Equal(t, wantExists, gotExists)
		assert.Equal(t, wantObject, gotObject)
		assert.Equal(t, exists || expectedApplied, gotExists)
		switch {
		case expectedApplied:
			assert.True(t, bytes.Equal(after, gotObject))
		case exists:
			assert.True(t, bytes.Equal(before, gotObject))
		}
	})
}

type listPage struct {
	XMLName        xml.Name     `xml:"ListBucketResult"`
	EncodingType   string       `xml:"EncodingType"`
	IsTruncated    bool         `xml:"IsTruncated"`
	Contents       []listObject `xml:"Contents"`
	CommonPrefixes []listPrefix `xml:"CommonPrefixes"`
	NextToken      string       `xml:"NextContinuationToken,omitempty"`
}

type listObject struct {
	Key, ETag string
	Size      int64
}

type listPrefix struct {
	Prefix string
}

type entry struct {
	path, etag string
	size       int64
	dir        bool
}

type listing struct {
	pages     [2][]byte
	paginated bool
}

func FuzzListing(f *testing.F) {
	f.Add("file.txt", uint16(1), false)
	f.Add("a b+%&☃", uint16(65535), true)
	f.Add("control\x01\r\n", uint16(0), true)
	const token = "opaque%2B+/= &token"
	var current atomic.Pointer[listing]
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		if r.Method != http.MethodGet || (r.URL.Path != "/"+bucketName && r.URL.Path != "/"+bucketName+"/") ||
			query.Get("list-type") != "2" || query.Get("encoding-type") != "url" || query.Get("prefix") != "root/" || query.Get("delimiter") != "/" {
			http.Error(w, "unexpected listing query", http.StatusBadRequest)
			return
		}
		fixture, page := current.Load(), 0
		if query.Get("continuation-token") != "" {
			if query.Get("continuation-token") != token || !fixture.paginated {
				http.Error(w, "unexpected continuation token", http.StatusBadRequest)
				return
			}
			page = 1
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write(fixture.pages[page])
	}))
	f.Cleanup(server.Close)
	key := signing.DeriveKey(server.URL, "fuzz-access", "fuzz-secret", region, "s3")
	bucket, sdkClient := s3.NewBucket(key, bucketName), reference(f, server.URL)
	f.Fuzz(func(t *testing.T, name string, size uint16, paginated bool) {
		if !objectKey(name) || strings.Contains(name, "/") || len(name) > 512 {
			t.Skip()
		}
		fixture := &listing{paginated: paginated}
		var expected []entry
		for i, page := range []string{"a", "b"} {
			objectPath, directoryPath := "root/"+page+"-file-"+name, "root/"+page+"-dir-"+name+"/"
			if i == 0 || paginated {
				expected = append(expected, entry{path: directoryPath, dir: true}, entry{path: objectPath, etag: `"etag%2B"`, size: int64(size)})
			}
			truncated := paginated && i == 0
			result := listPage{
				EncodingType: "url", IsTruncated: truncated,
				Contents:       []listObject{{Key: url.PathEscape(objectPath), ETag: `"etag%2B"`, Size: int64(size)}},
				CommonPrefixes: []listPrefix{{Prefix: url.PathEscape(directoryPath)}},
			}
			if truncated {
				result.NextToken = token
			}
			var err error
			fixture.pages[i], err = xml.Marshal(result)
			require.NoError(t, err)
		}
		current.Store(fixture)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var got, want []entry
		for item, err := range bucket.List(ctx, "root") {
			require.NoError(t, err)
			switch item := item.(type) {
			case *s3.File:
				got = append(got, entry{path: item.Path(), etag: item.ETag, size: item.Size()})
			case *s3.Prefix:
				got = append(got, entry{path: item.Path, dir: true})
			default:
				require.FailNow(t, "unexpected listing entry", "%T", item)
			}
		}
		pages := sdk.NewListObjectsV2Paginator(sdkClient, &sdk.ListObjectsV2Input{Bucket: aws.String(bucketName), Prefix: aws.String("root/"), Delimiter: aws.String("/"), EncodingType: types.EncodingTypeUrl})
		for pages.HasMorePages() {
			result, err := pages.NextPage(ctx)
			require.NoError(t, err)
			for _, object := range result.Contents {
				path, err := url.PathUnescape(aws.ToString(object.Key))
				require.NoError(t, err)
				want = append(want, entry{path: path, etag: aws.ToString(object.ETag), size: aws.ToInt64(object.Size)})
			}
			for _, prefix := range result.CommonPrefixes {
				path, err := url.PathUnescape(aws.ToString(prefix.Prefix))
				require.NoError(t, err)
				want = append(want, entry{path: path, dir: true})
			}
		}
		slices.SortFunc(want, func(a, b entry) int { return strings.Compare(a.path, b.path) })
		assert.Equal(t, expected, got)
		assert.Equal(t, expected, want)
	})
}
