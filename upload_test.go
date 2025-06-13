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
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/kelindar/s3/aws"
	"github.com/stretchr/testify/assert"
)

type testRoundTripper struct {
	t      *testing.T
	expect struct {
		method   string
		uri      string
		body     string
		skipBody bool
		headers  []string
	}
	response struct {
		code    int
		body    string
		headers http.Header
	}
}

var errUnexpected = errors.New("unexpected round-trip request")

func (t *testRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		defer req.Body.Close()
	}
	if req.Method != t.expect.method {
		assert.Equal(t.t, t.expect.method, req.Method, "unexpected HTTP method")
		return nil, errUnexpected
	}
	if uri := req.URL.RequestURI(); uri != t.expect.uri {
		assert.Equal(t.t, t.expect.uri, uri, "unexpected URI")
		return nil, errUnexpected
	}
	for i := range t.expect.headers {
		if req.Header.Get(t.expect.headers[i]) == "" {
			assert.NotEmpty(t.t, req.Header.Get(t.expect.headers[i]), "header %q missing", t.expect.headers[i])
			return nil, errUnexpected
		}
	}
	if !t.expect.skipBody && t.expect.body != "" {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		assert.Equal(t.t, t.expect.body, string(body), "unexpected request body")
	}

	res := &http.Response{
		StatusCode:    t.response.code,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Body:          io.NopCloser(strings.NewReader(t.response.body)),
		ContentLength: int64(len(t.response.body)),
		Header:        t.response.headers,
	}
	return res, nil
}

// Test an upload session against request/response
// strings from the documentation
func TestUpload(t *testing.T) {
	trt := &testRoundTripper{t: t}
	up := Uploader{
		Key:    aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3"),
		Client: &http.Client{Transport: trt},
		Bucket: "the-bucket",
		Object: "the-object",
	}

	trt.expect.method = "POST"
	trt.expect.uri = "/the-object?uploads="
	trt.expect.headers = []string{"Authorization"}
	trt.expect.body = ""
	trt.response.code = 200
	trt.response.headers = make(http.Header)
	trt.response.headers.Set("Content-Type", "application/xml")
	trt.response.body = `<InitiateMultipartUploadResult>
<Bucket>the-bucket</Bucket>
<Key>the-object</Key>
<UploadId>the-upload-id</UploadId>
</InitiateMultipartUploadResult>`

	err := up.Start()
	assert.NoError(t, err)

	assert.Equal(t, "the-upload-id", up.ID())

	// upload two parts in reverse order,
	// so we can test that the final
	// POST merges the parts in-order

	trt.expect.method = "PUT"
	trt.expect.uri = "/the-object?partNumber=2&uploadId=the-upload-id"
	trt.expect.skipBody = true
	trt.response.body = ""
	trt.response.headers = make(http.Header)
	trt.response.headers.Set("ETag", "the-ETag-2")
	part := make([]byte, MinPartSize+1)
	err = up.Upload(2, part)
	assert.NoError(t, err)
	assert.Equal(t, 1, up.CompletedParts())
	trt.expect.uri = "/the-object?partNumber=1&uploadId=the-upload-id"
	trt.response.headers.Set("ETag", "the-ETag-1")
	err = up.Upload(1, part)
	assert.NoError(t, err)

	trt.expect.method = "POST"
	trt.expect.uri = "/the-object?uploadId=the-upload-id"
	trt.expect.headers = []string{"Authorization", "Content-Type"}
	trt.expect.skipBody = false
	trt.expect.body = `<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Part><PartNumber>1</PartNumber><ETag>the-ETag-1</ETag></Part><Part><PartNumber>2</PartNumber><ETag>the-ETag-2</ETag></Part></CompleteMultipartUpload>`
	trt.response.body = `<CompleteMultipartUploadResult>
<Location>the-bucket.s3.amazonaws.com/the-object</Location>
<Bucket>the-bucket</Bucket>
<Key>the-object</Key>
<ETag>the-final-ETag</ETag>
</CompleteMultipartUploadResult>`
	trt.response.headers = make(http.Header)
	trt.response.headers.Set("Content-Type", "application/xml")
	err = up.Close(nil)
	assert.NoError(t, err)
	assert.Equal(t, "the-final-ETag", up.ETag())

	// Abort shouldn't do anything here:
	assert.NoError(t, up.Abort(), "abort")

	// rewind the state and try the error case
	up.finished = false
	up.finalETag = ""
	trt.response.body = `<Error>
<Code>InternalError</Code>
<Message>injected error message</Message>
</Error>`
	trt.response.headers = make(http.Header)
	trt.response.headers.Set("Content-Type", "application/xml")

	err = up.Close(nil)
	assert.Error(t, err, "should get error when <Error/> body returned")
	assert.Contains(t, err.Error(), "injected error message")

	// now test Abort
	trt.expect.method = "DELETE"
	trt.expect.headers = []string{"Authorization"}
	trt.expect.uri = "/the-object?uploadId=the-upload-id"
	trt.expect.body = ""
	trt.response.body = ""
	trt.response.code = 204
	trt.response.headers = make(http.Header)
	err = up.Abort()
	assert.NoError(t, err, "abort")
}
