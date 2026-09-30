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

package aws

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	faketime = true

	fn, err := time.Parse(longFormat, "20150830T123600Z")
	if err != nil {
		panic(err)
	}
	fakenow = fn.Local() // set non-UTC time, just to check that we fix it
}

// setnow sets fakenow and resets it at cleanup
func setnow(t *testing.T, tm time.Time) {
	old := fakenow
	t.Cleanup(func() { fakenow = old })
	fakenow = tm
}

// test against the example in the documentation
func TestCanonical(t *testing.T) {
	previous := sigheaders
	// use these headers
	sigheaders = []string{"content-type", "host", "x-amz-date"}
	defer func() {
		sigheaders = previous
	}()

	req, err := http.NewRequest("GET", "https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08 HTTP/1.1", nil)
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	req.Header.Set("X-Amz-Date", "20150830T123600Z")
	req.Header.Set("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

	var out bytes.Buffer
	canonical(&out, req)
	outstr := out.String()
	const want = `GET
/
Action=ListUsers&Version=2010-05-08
content-type:application/x-www-form-urlencoded; charset=utf-8
host:iam.amazonaws.com
x-amz-date:20150830T123600Z

content-type;host;x-amz-date
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
	assert.Equal(t, want, outstr, "canonical request didn't match")
	h := sha256.Sum256(out.Bytes())
	hstr := hex.EncodeToString(h[:])
	assert.Equal(t, "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59", hstr)
}

func TestConditionalHeader(t *testing.T) {
	for _, value := range []string{
		"Tue, 15 Nov 1994 08:12:31 GMT",
		"  Tue,  15\tNov 1994 08:12:31 GMT  ",
	} {
		t.Run(value, func(t *testing.T) {
			req, err := http.NewRequest("PUT", "https://examplebucket.s3.amazonaws.com/test.txt", nil)
			require.NoError(t, err)
			req.Header.Set("If-Unmodified-Since", value)

			key := DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
			key.SignV4(req, nil)

			assert.Contains(t, req.Header.Get("Authorization"), "SignedHeaders=host;if-unmodified-since;x-amz-content-sha256;x-amz-date")
			var out bytes.Buffer
			canonical(&out, req)
			assert.Contains(t, out.String(), "if-unmodified-since:Tue, 15 Nov 1994 08:12:31 GMT\n")
			assert.Equal(t, value, req.Header.Get("If-Unmodified-Since"), "signing must not modify caller-owned header values")
		})
	}
}

func TestHostHeader(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://example.com/object", nil)
	require.NoError(t, err)
	req.Host = "bucket.example.com"
	key := DeriveKey("", "access", "secret", "us-east-1", "s3")
	key.SignV4(req, nil)

	var canonicalRequest bytes.Buffer
	canonical(&canonicalRequest, req)
	assert.Contains(t, canonicalRequest.String(), "host:bucket.example.com\n")
	assert.Empty(t, req.Header.Get("Host"))
}

func TestSignV4Headers(t *testing.T) {
	req, err := http.NewRequest(http.MethodPut, "https://example.com/object", nil)
	require.NoError(t, err)

	const old = "old-value"
	headers := []string{"X-Amz-Date", "X-Amz-Content-Sha256", "Authorization", "X-Amz-Security-Token"}
	retained := make(map[string][]string, len(headers))
	for _, header := range headers {
		req.Header[header] = []string{old, "stale-value"}
		retained[header] = req.Header[header]
	}
	clone := req.Clone(t.Context())

	key := DeriveKey("", "access", "secret", "us-east-1", "s3")
	key.Token = "session-token"
	key.SignV4(req, nil)

	for _, header := range headers {
		assert.Equal(t, []string{old, "stale-value"}, retained[header])
		assert.Equal(t, retained[header], clone.Header[header])
		assert.Len(t, req.Header[header], 1)
		assert.Equal(t, 1, cap(req.Header[header]))
	}
	assert.Equal(t, "session-token", req.Header.Get("X-Amz-Security-Token"))
}

func TestSignV4Readers(t *testing.T) {
	const payload = "independent body readers"
	req, err := http.NewRequest(http.MethodPut, "https://example.com/object", nil)
	require.NoError(t, err)
	key := DeriveKey("", "access", "secret", "us-east-1", "s3")
	key.SignV4(req, []byte(payload))
	clone := req.Clone(t.Context())

	_, ok := req.Body.(io.WriterTo)
	assert.True(t, ok)

	first, err := req.GetBody()
	require.NoError(t, err)
	defer first.Close()
	second, err := clone.GetBody()
	require.NoError(t, err)
	defer second.Close()
	_, ok = first.(io.WriterTo)
	assert.True(t, ok)
	_, ok = second.(io.WriterTo)
	assert.True(t, ok)

	partial := make([]byte, 3)
	n, err := first.Read(partial)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, payload[:3], string(partial))

	got, err := io.ReadAll(second)
	require.NoError(t, err)
	assert.Equal(t, payload, string(got))
	got, err = io.ReadAll(first)
	require.NoError(t, err)
	assert.Equal(t, payload[3:], string(got))
}

func TestSignV4Raw(t *testing.T) {
	many := make([][2]string, 20)
	for i := range many {
		many[i] = [2]string{"if-custom-" + strconv.Itoa(i), "value"}
	}
	for _, test := range []struct {
		name, token, uri string
		body             []byte
		headers          [][2]string
	}{
		{name: "root", uri: "https://bucket.example.com"},
		{name: "payload", uri: "https://bucket.example.com/a%20b?partNumber=1&uploadId=id", body: []byte("part contents")},
		{name: "token", uri: "https://bucket.example.com/a%20b?partNumber=1&uploadId=id", token: "session-token", body: []byte("part contents")},
		{name: "conditions", uri: "https://bucket.example.com/object", headers: [][2]string{{"if-unmodified-since", "Tue, 15 Nov 1994 08:12:31 GMT"}, {"if-match", `"etag"`}, {"if-none-match", "*"}}},
		{name: "header whitespace", uri: "https://bucket.example.com/object", headers: [][2]string{{"if-unmodified-since", "  Tue,  15\tNov 1994 08:12:31 GMT  "}}},
		{name: "copy", uri: "https://bucket.example.com/object?partNumber=1&uploadId=id", token: "session-token", headers: [][2]string{{"x-amz-copy-source-range", "bytes=0-5242879"}, {"x-amz-copy-source", "/bucket/a%20b%25"}, {"x-amz-copy-source-if-match", `"etag"`}}},
		{name: "many conditions", uri: "https://bucket.example.com/object", headers: many},
		{name: "empty condition", uri: "https://bucket.example.com/object", headers: [][2]string{{"if-match", ""}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			key := DeriveKey("", "access", "secret", "us-east-1", "s3")
			key.Token = test.token
			req, err := http.NewRequest(http.MethodPut, test.uri, nil)
			require.NoError(t, err)
			for _, header := range test.headers {
				req.Header.Set(header[0], header[1])
			}
			retained := append([][2]string(nil), test.headers...)
			key.SignV4(req, test.body)
			date, hash, auth := key.SignV4Raw(req.Method, req.URL.EscapedPath(), req.URL.RawQuery, req.Host, test.body, test.headers...)
			assert.Equal(t, req.Header.Get("X-Amz-Date"), date)
			assert.Equal(t, req.Header.Get("X-Amz-Content-Sha256"), hash)
			assert.Equal(t, req.Header.Get("Authorization"), auth)
			assert.Equal(t, retained, test.headers, "signing must not modify caller-owned header pairs")
		})
	}
}

// test from
// https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
func TestToSign(t *testing.T) {
	const want = `AWS4-HMAC-SHA256
20150830T123600Z
20150830/us-east-1/iam/aws4_request
f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59`

	var dst bytes.Buffer
	s := &SigningKey{
		Region:  "us-east-1",
		Service: "iam",
	}
	s.tosign(&dst, []byte("20150830T123600Z"), []byte("f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59"))

	assert.Equal(t, want, dst.String())
}

// test from
// https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
func TestSigningKey(t *testing.T) {
	when := time.Date(2015, time.August, 30, 12, 36, 0, 0, time.UTC)
	k := derive("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", when, "us-east-1", "iam")
	x := hex.EncodeToString(k)
	const want = "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
	assert.Equal(t, want, x)

	const testvec = `AWS4-HMAC-SHA256
20150830T123600Z
20150830/us-east-1/iam/aws4_request
f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59`

	sk := DeriveKey("", "", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "us-east-1", "iam")

	var dst [2 * sha256.Size]byte
	sk.sign([]byte(testvec), dst[:], when)
	const wantsig = "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
	assert.Equal(t, wantsig, string(dst[:]))
}

func TestSigningHMAC(t *testing.T) {
	when := time.Date(2015, time.August, 30, 12, 36, 0, 0, time.UTC)
	key := DeriveKey("", "access", "secret", "us-east-1", "s3")
	for _, size := range []int{0, 1, 64, 256, 4096} {
		message := bytes.Repeat([]byte("x"), size)
		standard := hmac.New(sha256.New, key.pickKey(when))
		standard.Write(message)
		var got [2 * sha256.Size]byte
		key.sign(message, got[:], when)
		assert.Equal(t, hex.EncodeToString(standard.Sum(nil)), string(got[:]))
	}
}

func TestSigningKeyRollover(t *testing.T) {
	const (
		accessKey = "AKIAIOSFODNN7EXAMPLE"
		secretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	)
	setnow(t, time.Date(2015, time.August, 30, 12, 36, 0, 0, time.UTC))
	longLived := DeriveKey("", accessKey, secretKey, "us-east-1", "s3")

	setnow(t, time.Date(2015, time.September, 1, 12, 36, 0, 0, time.UTC))
	fresh := DeriveKey("", accessKey, secretKey, "us-east-1", "s3")
	longLivedRequest, err := http.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	assert.NoError(t, err)
	freshRequest := longLivedRequest.Clone(t.Context())
	longLived.SignV4(longLivedRequest, nil)
	fresh.SignV4(freshRequest, nil)

	assert.Equal(t, freshRequest.Header.Get("Authorization"), longLivedRequest.Header.Get("Authorization"))
}

func TestSigningKeyCache(t *testing.T) {
	when := time.Date(2015, time.August, 30, 12, 0, 0, 0, time.UTC)
	key := DeriveKey("", "access", "secret", "us-east-1", "s3")
	first := key.pickKey(when)
	firstEntry := key.cache
	assert.Equal(t, derive(key.Secret, when, key.Region, key.Service), first)
	assert.Same(t, firstEntry, key.cache)
	assert.Equal(t, first, key.pickKey(when.Add(time.Hour)))

	changed := key.pickKey(when.Add(24 * time.Hour))
	secondEntry := key.cache
	assert.NotSame(t, firstEntry, secondEntry)
	assert.Equal(t, derive(key.Secret, when.Add(24*time.Hour), key.Region, key.Service), changed)
	assert.Equal(t, derive(key.Secret, when, key.Region, key.Service), key.pickKey(when))

	mutations := []struct {
		name   string
		mutate func(*SigningKey)
	}{
		{"access key", func(k *SigningKey) { k.AccessKey = "other-access" }},
		{"secret", func(k *SigningKey) { k.Secret = "other-secret" }},
		{"token", func(k *SigningKey) { k.Token = "session-token" }},
		{"region", func(k *SigningKey) { k.Region = "eu-west-1" }},
		{"service", func(k *SigningKey) { k.Service = "iam" }},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			before := key.cache
			test.mutate(key)
			got := key.pickKey(when)
			assert.NotSame(t, before, key.cache)
			assert.Equal(t, derive(key.Secret, when, key.Region, key.Service), got)
		})
	}

	regional := key.InRegion("ap-south-1")
	assert.Nil(t, regional.cache)
	assert.Equal(t, key.Secret, regional.Secret)
}

func TestSigningCacheRace(t *testing.T) {
	key := DeriveKey("", "access", "secret", "us-east-1", "s3")
	when := time.Date(2026, time.September, 28, 0, 0, 0, 0, time.UTC)
	var want [2 * sha256.Size]byte
	key.sign([]byte("request"), want[:], when)
	got := make(chan string, 16)
	var group sync.WaitGroup
	for range cap(got) {
		group.Add(1)
		go func() {
			defer group.Done()
			var sig [2 * sha256.Size]byte
			key.sign([]byte("request"), sig[:], when)
			got <- string(sig[:])
		}()
	}
	group.Wait()
	close(got)
	for sig := range got {
		assert.Equal(t, string(want[:]), sig)
	}
}

// See https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html#sigv4-query-string-auth-v4-signing-example
func TestSignURL(t *testing.T) {
	// derive the key in the preceding day
	fn, err := time.Parse(longFormat, "20130523T010203Z")
	assert.NoError(t, err)
	setnow(t, fn)
	input := "https://examplebucket.s3.amazonaws.com/test.txt"
	k := DeriveKey("", "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1", "s3")

	// change the day to "tomorrow" and confirm
	// that the signature is still produced correctly
	fn, err = time.Parse(longFormat, "20130524T000000Z")
	assert.NoError(t, err)
	setnow(t, fn)
	ret, err := k.SignURL(input, 86400*time.Second)
	assert.NoError(t, err)
	want := "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	assert.Equal(t, want, ret, "URL signing didn't match expected result")
}

func TestSignURLEncoding(t *testing.T) {
	when := time.Date(2026, time.September, 29, 12, 34, 56, 0, time.UTC)
	setnow(t, when)
	for _, tc := range []struct {
		uri      string
		validFor time.Duration
	}{
		{"https://example.com/a%2Fb", -time.Second},
		{"https://example.com/a%2Fb?z=2&a=hello+world&a=percent%25", time.Hour},
	} {
		key := DeriveKey("", "A/B +%", "secret", "r/1", "s+3")
		key.Token = "T/+ &"
		got, err := key.SignURL(tc.uri, tc.validFor)
		require.NoError(t, err)

		u, err := url.Parse(tc.uri)
		require.NoError(t, err)
		stamp := when.Format(longFormat)
		scope := stamp[:8] + "/" + key.Region + "/" + key.Service + "/aws4_request"
		q := u.Query()
		q.Add("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
		q.Add("X-Amz-Credential", key.AccessKey+"/"+scope)
		q.Add("X-Amz-Date", stamp)
		q.Add("X-Amz-Expires", strconv.FormatInt(int64(tc.validFor/time.Second), 10))
		q.Add("X-Amz-Security-Token", key.Token)
		q.Add("X-Amz-SignedHeaders", "host")
		query := q.Encode()
		canonical := "GET\n" + u.EscapedPath() + "\n" + query + "\nhost:" + u.Host + "\n\nhost\nUNSIGNED-PAYLOAD"
		hash := sha256.Sum256([]byte(canonical))
		toSign := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + hex.EncodeToString(hash[:])
		mac := hmac.New(sha256.New, derive(key.Secret, when, key.Region, key.Service))
		_, err = mac.Write([]byte(toSign))
		require.NoError(t, err)
		want := u.Scheme + "://" + u.Host + u.EscapedPath() + "?" + query + "&X-Amz-Signature=" + hex.EncodeToString(mac.Sum(nil))
		assert.Equal(t, want, got)
	}
}

func BenchmarkSigning(b *testing.B) {
	key := DeriveKey("", "bench-access", "bench-secret", "us-east-1", "s3")
	req, err := http.NewRequest(http.MethodPut, "https://bench-bucket.s3.us-east-1.amazonaws.com/object", nil)
	if err != nil {
		b.Fatal(err)
	}
	body := []byte("benchmark payload")
	b.Run("v4", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			key.SignV4(req, nil)
		}
	})
	b.Run("v4-parallel", func(b *testing.B) {
		template, err := http.NewRequest(http.MethodPut, "https://bench-bucket.s3.us-east-1.amazonaws.com/object", nil)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			local := template.Clone(template.Context())
			for pb.Next() {
				key.SignV4(local, nil)
			}
		})
	})
	b.Run("body", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			key.SignV4(req, body)
		}
	})
	b.Run("v4-raw", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			key.SignV4Raw(http.MethodPut, "/object", "", "bench-bucket.s3.us-east-1.amazonaws.com", nil)
		}
	})
	b.Run("url", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := key.SignURL("https://bench-bucket.s3.us-east-1.amazonaws.com/object", time.Hour); err != nil {
				b.Fatal(err)
			}
		}
	})
}
