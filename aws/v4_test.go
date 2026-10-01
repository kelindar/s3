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
	"cmp"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"slices"
	"strconv"
	"strings"
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

func TestQueryEncoding(t *testing.T) {
	for _, test := range []struct{ query, want string }{
		{"b=2&a=hello+world&a=%2f&acl", "a=%2F&a=hello%20world&acl=&b=2"},
		{"a0=c&a=d&.=b&%C3%A9=a", "%C3%A9=a&.=b&a=d&a0=c"},
	} {
		t.Run(test.query, func(t *testing.T) {
			query, err := url.ParseQuery(test.query)
			require.NoError(t, err)
			var encoded bytes.Buffer
			encodeQuery(&encoded, query)
			assert.Equal(t, test.want, encoded.String())
		})
	}
}

func TestSign(t *testing.T) {
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
		{name: "empty payload", uri: "https://bucket.example.com/object", body: []byte{}},
		{name: "token", uri: "https://bucket.example.com/a%20b?partNumber=1&uploadId=id", token: "session-token", body: []byte("part contents")},
		{name: "conditions", uri: "https://bucket.example.com/object", headers: [][2]string{{"if-unmodified-since", "Tue, 15 Nov 1994 08:12:31 GMT"}, {"if-match", `"etag"`}, {"if-none-match", "*"}}},
		{name: "header whitespace", uri: "https://bucket.example.com/object", headers: [][2]string{{"if-unmodified-since", "  Tue,  15\tNov 1994 08:12:31 GMT  "}}},
		{name: "metadata", uri: "https://bucket.example.com/object", headers: [][2]string{{"x-amz-meta-test", "value"}}},
		{name: "copy", uri: "https://bucket.example.com/object?partNumber=1&uploadId=id", token: "session-token", headers: [][2]string{{"x-amz-copy-source-range", "bytes=0-5242879"}, {"x-amz-copy-source", "/bucket/a%20b%25"}, {"x-amz-copy-source-if-match", `"etag"`}}},
		{name: "many conditions", uri: "https://bucket.example.com/object", headers: many},
		{name: "empty condition", uri: "https://bucket.example.com/object", headers: [][2]string{{"if-match", ""}}},
		{name: "long path", uri: "https://bucket.example.com/" + strings.Repeat("a", 2048)},
		{name: "long credential", uri: "https://bucket.example.com/object", token: strings.Repeat("token", 256)},
	} {
		t.Run(test.name, func(t *testing.T) {
			key := DeriveKey("", "access", "secret", "us-east-1", "s3")
			key.Token = test.token
			u, err := url.Parse(test.uri)
			require.NoError(t, err)
			retained := append([][2]string(nil), test.headers...)
			date := fakenow.UTC().Format(longFormat)
			hash := "UNSIGNED-PAYLOAD"
			if test.body == nil {
				hash = hex.EncodeToString(sha256.New().Sum(nil))
			}

			// Rebuild the canonical request and HMAC independently of Sign.
			headers := [][2]string{{"host", u.Host}, {"x-amz-content-sha256", hash}, {"x-amz-date", date}}
			if test.token != "" {
				headers = append(headers, [2]string{"x-amz-security-token", test.token})
			}
			for _, header := range test.headers {
				if header[1] != "" {
					headers = append(headers, header)
				}
			}
			slices.SortFunc(headers, func(a, b [2]string) int { return strings.Compare(a[0], b[0]) })
			var canonical strings.Builder
			canonical.WriteString("PUT\n" + cmp.Or(u.EscapedPath(), "/") + "\n" + u.RawQuery + "\n")
			var names []string
			for _, header := range headers {
				canonical.WriteString(header[0] + ":" + strings.Join(strings.Fields(header[1]), " ") + "\n")
				names = append(names, header[0])
			}
			signed := strings.Join(names, ";")
			canonical.WriteString("\n" + signed + "\n" + hash)
			requestHash := sha256.Sum256([]byte(canonical.String()))
			scope := date[:8] + "/" + key.Region + "/" + key.Service + "/aws4_request"
			toSign := "AWS4-HMAC-SHA256\n" + date + "\n" + scope + "\n" + hex.EncodeToString(requestHash[:])
			signingKey := []byte("AWS4" + key.Secret)
			for _, value := range []string{date[:8], key.Region, key.Service, "aws4_request"} {
				mac := hmac.New(sha256.New, signingKey)
				_, err := mac.Write([]byte(value))
				require.NoError(t, err)
				signingKey = mac.Sum(nil)
			}
			mac := hmac.New(sha256.New, signingKey)
			_, err = mac.Write([]byte(toSign))
			require.NoError(t, err)
			auth := "AWS4-HMAC-SHA256 Credential=" + key.AccessKey + "/" + scope + ", SignedHeaders=" + signed + ", Signature=" + hex.EncodeToString(mac.Sum(nil))

			for _, capacity := range []int{0, 16, 512, 4096} {
				storage := bytes.Repeat([]byte{'x'}, capacity)
				gotDate, gotHash, gotAuth := key.Sign(storage, []byte("PUT"), []byte(u.EscapedPath()), []byte(u.RawQuery), []byte(u.Host), test.body, test.headers...)
				assert.Equal(t, date, string(gotDate))
				assert.Equal(t, hash, gotHash)
				assert.Equal(t, auth, string(gotAuth))
				if capacity == 4096 {
					assert.Same(t, &storage[0], &gotDate[0], "date aliases caller storage")
					assert.Same(t, &storage[len(gotDate)], &gotAuth[0], "authorization aliases caller storage")
				}
				assert.Equal(t, retained, test.headers, "signing must not modify caller-owned header pairs")
			}
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
	var oldStorage, newStorage [512]byte
	_, _, oldAuth := longLived.Sign(oldStorage[:0], []byte("GET"), []byte("/test.txt"), nil, []byte("examplebucket.s3.amazonaws.com"), nil)
	_, _, newAuth := fresh.Sign(newStorage[:0], []byte("GET"), []byte("/test.txt"), nil, []byte("examplebucket.s3.amazonaws.com"), nil)
	assert.Equal(t, newAuth, oldAuth)
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
		{"https://example.com/a%2Fb", time.Hour},
		{"https://example.com/a%2Fb?z=2&a=hello+world&a=percent%25", time.Hour},
		{"https://example.com/a%2Fb?a=z&a=a&a0=next&%C3%A9=unicode&.=dot&a=%2F", time.Hour},
		{"https://example.com", time.Hour},
		{"https://example.com/a?X-Amz-Date=old&X-Amz-Signature=old&X-Amz-Security-Token=stale", time.Hour},
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
		q.Del("X-Amz-Signature")
		q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
		q.Set("X-Amz-Credential", key.AccessKey+"/"+scope)
		q.Set("X-Amz-Date", stamp)
		q.Set("X-Amz-Expires", strconv.FormatInt(int64(tc.validFor/time.Second), 10))
		q.Set("X-Amz-Security-Token", key.Token)
		q.Set("X-Amz-SignedHeaders", "host")
		pairs := strings.Split(strings.ReplaceAll(q.Encode(), "+", "%20"), "&")
		slices.SortFunc(pairs, func(a, b string) int {
			aName, _, _ := strings.Cut(a, "=")
			bName, _, _ := strings.Cut(b, "=")
			return cmp.Or(strings.Compare(aName, bName), strings.Compare(a, b))
		})
		query := strings.Join(pairs, "&")
		path := u.EscapedPath()
		if path == "" {
			path = "/"
		}
		canonical := "GET\n" + path + "\n" + query + "\nhost:" + u.Host + "\n\nhost\nUNSIGNED-PAYLOAD"
		hash := sha256.Sum256([]byte(canonical))
		toSign := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + hex.EncodeToString(hash[:])
		mac := hmac.New(sha256.New, derive(key.Secret, when, key.Region, key.Service))
		_, err = mac.Write([]byte(toSign))
		require.NoError(t, err)
		want := u.Scheme + "://" + u.Host + path + "?" + query + "&X-Amz-Signature=" + hex.EncodeToString(mac.Sum(nil))
		assert.Equal(t, want, got)
	}
}

func TestPresignBounds(t *testing.T) {
	key := DeriveKey("", "access", "secret", "us-east-1", "s3")
	for _, duration := range []time.Duration{-time.Second, 0, time.Millisecond, 7*24*time.Hour + time.Second} {
		_, err := key.SignURL("https://example.com/object", duration)
		assert.Error(t, err)
	}
	for _, uri := range []string{"object", "ftp://example.com/object", "https://example.com/object?bad=%ZZ"} {
		_, err := key.SignURL(uri, time.Hour)
		assert.Error(t, err)
	}
}

func BenchmarkSigning(b *testing.B) {
	key := DeriveKey("", "bench-access", "bench-secret", "us-east-1", "s3")
	method, path, host := []byte("PUT"), []byte("/object"), []byte("bench-bucket.s3.us-east-1.amazonaws.com")
	body := []byte("benchmark payload")
	b.Run("v4", func(b *testing.B) {
		var storage [512]byte
		b.ReportAllocs()
		for range b.N {
			key.Sign(storage[:0], method, path, nil, host, nil)
		}
	})
	b.Run("v4-parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			var storage [512]byte
			for pb.Next() {
				key.Sign(storage[:0], method, path, nil, host, nil)
			}
		})
	})
	b.Run("body", func(b *testing.B) {
		var storage [512]byte
		b.ReportAllocs()
		for range b.N {
			key.Sign(storage[:0], method, path, nil, host, body)
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
