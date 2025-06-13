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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kelindar/s3/aws"
	"github.com/stretchr/testify/assert"
)

func TestURI_Functions(t *testing.T) {
	key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	bucket := "test-bucket"
	object := "test/object with spaces.txt"

	// Test uri function
	result := uri(key, bucket, object)
	assert.Contains(t, result, bucket)
	assert.Contains(t, result, "test/object%20with%20spaces.txt")

	// Test rawURI function
	query := "?list-type=2"
	rawResult := rawURI(key, bucket, query)
	assert.Contains(t, rawResult, bucket)
	assert.Contains(t, rawResult, query)

	// Test with custom base URI
	customKey := aws.DeriveKey("https://custom.endpoint.com", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
	customResult := rawURI(customKey, bucket, query)
	assert.Contains(t, customResult, "custom.endpoint.com")
	assert.Contains(t, customResult, bucket)

	// Test with bucket containing dots (should use path-style)
	dotBucket := "bucket.with.dots"
	dotResult := rawURI(key, dotBucket, query)
	assert.Contains(t, dotResult, "s3.us-east-1.amazonaws.com/"+dotBucket)

	// Test with bucket without dots (should use virtual-hosted style)
	noDotBucket := "bucketwithoutdots"
	noDotResult := rawURI(key, noDotBucket, query)
	assert.Contains(t, noDotResult, noDotBucket+".s3.us-east-1.amazonaws.com")
}

func TestQueryEscape(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "simple"},
		{"with spaces", "with%20spaces"},
		{"with+plus", "with%2Bplus"},
		{"with/slash", "with%2Fslash"},
		{"with=equals", "with%3Dequals"},
		{"with&ampersand", "with%26ampersand"},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			result := queryEscape(test.input)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestAlmostPathEscape(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple/path", "simple/path"},
		{"path with spaces", "path%20with%20spaces"},
		{"path/with/slashes", "path/with/slashes"},
		{"path with+plus", "path%20with%2Bplus"},
		{"path=with&special", "path%3Dwith%26special"},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			result := almostPathEscape(test.input)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestBadPath(t *testing.T) {
	err := badpath("test-op", "invalid-path")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test-op")
	assert.Contains(t, err.Error(), "invalid-path")
}

func TestBadBucket(t *testing.T) {
	err := badBucket("invalid-bucket")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidBucket)
	assert.Contains(t, err.Error(), "invalid-bucket")
}

func TestExtractMessage(t *testing.T) {
	// Test with valid XML message
	xmlWithMessage := `<Error><Message>Test error message</Message></Error>`
	message := extractMessage(strings.NewReader(xmlWithMessage))
	assert.Equal(t, "Test error message", message)

	// Test with invalid XML
	invalidXML := "not xml"
	message = extractMessage(strings.NewReader(invalidXML))
	assert.Equal(t, "(no message)", message) // extractMessage returns "(no message)" for invalid XML

	// Test with XML without Message field
	xmlWithoutMessage := `<Error><Code>TestError</Code></Error>`
	message = extractMessage(strings.NewReader(xmlWithoutMessage))
	assert.Equal(t, "", message) // extractMessage returns empty string when Message field is empty
}

func TestDefaultClient(t *testing.T) {
	// Test that DefaultClient is properly configured
	assert.NotNil(t, DefaultClient.Transport)

	transport, ok := DefaultClient.Transport.(*http.Transport)
	assert.True(t, ok)
	assert.Equal(t, 60*time.Second, transport.ResponseHeaderTimeout)
	assert.Equal(t, 5, transport.MaxIdleConnsPerHost)
	assert.True(t, transport.DisableCompression)
	assert.NotNil(t, transport.DialContext)
}

func TestFlakyDo(t *testing.T) {
	// Test successful request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	}))
	defer server.Close()

	req, err := http.NewRequest("GET", server.URL, nil)
	assert.NoError(t, err)

	resp, err := flakyDo(&http.Client{}, req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// Test with 500 error (should retry)
	retryCount := 0
	retryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		retryCount++
		if retryCount == 1 {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("success after retry"))
		}
	}))
	defer retryServer.Close()

	req, err = http.NewRequest("GET", retryServer.URL, nil)
	assert.NoError(t, err)

	resp, err = flakyDo(&http.Client{}, req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 2, retryCount) // Should have retried once
	resp.Body.Close()
}

func TestSplitMeta(t *testing.T) {
	tests := []struct {
		pattern      string
		expectedPre  string
		expectedPost string
	}{
		{"simple", "simple", ""},
		{"pre*post", "pre", "*post"},
		{"pre?post", "pre", "?post"},
		{"pre[abc]post", "pre", "[abc]post"},
		{"pre\\*post", "pre", "\\*post"},
		{"*start", "", "*start"},
		{"no/meta/chars", "no/meta/chars", ""},
	}

	for _, test := range tests {
		t.Run(test.pattern, func(t *testing.T) {
			pre, post := splitMeta(test.pattern)
			assert.Equal(t, test.expectedPre, pre)
			assert.Equal(t, test.expectedPost, post)
		})
	}
}

func TestIgnoreKey(t *testing.T) {
	tests := []struct {
		key    string
		dirOK  bool
		ignore bool
	}{
		{"", false, true},          // empty key
		{"", true, true},           // empty key
		{"file.txt", false, false}, // normal file
		{"file.txt", true, false},  // normal file
		{"dir/", false, true},      // directory when dirOK=false
		{"dir/", true, false},      // directory when dirOK=true
		{".", false, true},         // current dir
		{".", true, true},          // current dir
		{"..", false, true},        // parent dir
		{"..", true, true},         // parent dir
		{"path/.", false, true},    // path ending with .
		{"path/..", false, true},   // path ending with ..
	}

	for _, test := range tests {
		t.Run(test.key, func(t *testing.T) {
			result := ignoreKey(test.key, test.dirOK)
			assert.Equal(t, test.ignore, result)
		})
	}
}

func TestPatmatch(t *testing.T) {
	tests := []struct {
		pattern string
		name    string
		match   bool
		hasErr  bool
	}{
		{"", "anything", true, false},      // empty pattern matches everything
		{"*.txt", "file.txt", true, false}, // simple glob
		{"*.txt", "file.doc", false, false},
		{"test*", "test123", true, false},
		{"test*", "other", false, false},
		{"[abc]", "a", true, false},
		{"[abc]", "d", false, false},
		{"[", "a", false, true}, // invalid pattern
	}

	for _, test := range tests {
		t.Run(test.pattern+"_"+test.name, func(t *testing.T) {
			match, err := patmatch(test.pattern, test.name)
			if test.hasErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.match, match)
			}
		})
	}
}
