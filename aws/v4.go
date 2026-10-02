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

// Package aws is a lightweight implementation
// of the AWS API signature algorithms.
// Currently only the Version 4 algorithm is supported.
package aws

import (
	"bytes"
	"cmp"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	faketime bool = false
	fakenow  time.Time
)

func signtime() time.Time {
	if faketime {
		return fakenow
	}
	return time.Now()
}

const (
	longFormat  = "20060102T150405Z"
	shortFormat = "20060102"
)

// canonicalValue preserves embedded tabs, matching the SDK's space-only folding.
func canonicalValue(dst *bytes.Buffer, value string) {
	value = strings.TrimSpace(value)
	for {
		at := strings.IndexByte(value, ' ')
		if at < 0 {
			dst.WriteString(value)
			return
		}
		dst.WriteString(value[:at])
		dst.WriteByte(' ')
		value = strings.TrimLeft(value[at:], " ")
	}
}

func (s *SigningKey) toscope(dst *bytes.Buffer, date []byte) {
	dst.Write(date)
	dst.WriteByte('/')
	dst.WriteString(s.Region)
	dst.WriteByte('/')
	dst.WriteString(s.Service)
	dst.WriteString("/aws4_request")
}

// string to sign
// see
// https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
func (s *SigningKey) tosign(dst *bytes.Buffer, stamp, reqhash []byte) {
	dst.WriteString("AWS4-HMAC-SHA256\n")
	// date value
	dst.Write(stamp)
	dst.WriteByte('\n')
	// request scope
	s.toscope(dst, stamp[:8])
	dst.WriteByte('\n')
	// request hash
	dst.Write(reqhash)
}

// Sign computes an AWS S3 Signature Version 4 authorization header.
// It signs host, the payload hash, date, optional security token, and extra
// header name/value pairs. Extra names must be lowercase and unique and must
// not repeat those mandatory headers. Pairs may be unordered; empty values
// are omitted. A nil body uses the empty SHA-256 hash; otherwise Sign uses
// UNSIGNED-PAYLOAD.
// path must be escaped and query must already be in canonical order.
// The caller sets the returned headers and X-Amz-Security-Token when set.
//
// Sign overwrites dst from its beginning and grows it if capacity is insufficient.
// The returned date and authorization alias that storage and must be copied
// before it is reused. Inputs and extra header pairs are not retained or changed.
// dst must not overlap input storage.
func (s *SigningKey) Sign(dst, method, path, query, host, body []byte, extra ...[2]string) (date []byte, payloadHash string, authorization []byte) {
	buf := bytes.NewBuffer(dst[:0])
	now := signtime().UTC()
	var stampStorage [len(longFormat)]byte
	stamp := now.AppendFormat(stampStorage[:0], longFormat)
	payloadHash = "UNSIGNED-PAYLOAD"
	if body == nil {
		payloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	}
	var headerStorage [16][2]string
	headers := headerStorage[:3]
	// Host and date are emitted directly; these pairs only carry their names.
	headers[0] = [2]string{"host", ""}
	headers[1] = [2]string{"x-amz-content-sha256", payloadHash}
	headers[2] = [2]string{"x-amz-date", ""}
	if s.Token != "" {
		headers = append(headers, [2]string{"x-amz-security-token", s.Token})
	}
	for _, header := range extra {
		if header[1] != "" {
			headers = append(headers, header)
		}
	}
	if len(extra) > 0 {
		slices.SortFunc(headers, func(a, b [2]string) int { return strings.Compare(a[0], b[0]) })
	}

	buf.Write(method)
	buf.WriteByte('\n')
	if len(path) == 0 {
		buf.WriteByte('/')
	} else {
		buf.Write(path)
	}
	buf.WriteByte('\n')
	buf.Write(query)
	buf.WriteByte('\n')
	for _, header := range headers {
		buf.WriteString(header[0])
		buf.WriteByte(':')
		switch header[0] {
		case "host":
			buf.Write(host)
		case "x-amz-date":
			buf.Write(stamp)
		default:
			canonicalValue(buf, header[1])
		}
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')
	for i, header := range headers {
		if i > 0 {
			buf.WriteByte(';')
		}
		buf.WriteString(header[0])
	}
	buf.WriteByte('\n')
	buf.WriteString(payloadHash)

	var hexbuf [2 * sha256.Size]byte
	h := sha256.Sum256(buf.Bytes())
	buf.Reset()
	var reqhash [2 * sha256.Size]byte
	hex.Encode(reqhash[:], h[:])
	s.tosign(buf, stamp, reqhash[:])
	s.sign(buf.Bytes(), hexbuf[:], now)

	buf.Reset()
	buf.Write(stamp)
	buf.WriteString("AWS4-HMAC-SHA256 Credential=")
	buf.WriteString(s.AccessKey)
	buf.WriteByte('/')
	s.toscope(buf, stamp[:8])
	buf.WriteString(", SignedHeaders=")
	for i, header := range headers {
		if i > 0 {
			buf.WriteByte(';')
		}
		buf.WriteString(header[0])
	}
	buf.WriteString(", Signature=")
	buf.Write(hexbuf[:])
	result := buf.Bytes()
	return result[:len(stamp)], payloadHash, result[len(stamp):]
}

func queryEscape(value string) string {
	return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
}

func encodeQuery(dst *bytes.Buffer, query url.Values) {
	fields := make([][2]string, 0, len(query))
	for name, values := range query {
		name = queryEscape(name)
		for _, value := range values {
			fields = append(fields, [2]string{name, queryEscape(value)})
		}
	}
	slices.SortFunc(fields, func(a, b [2]string) int {
		return cmp.Or(strings.Compare(a[0], b[0]), strings.Compare(a[1], b[1]))
	})
	for i, field := range fields {
		if i > 0 {
			dst.WriteByte('&')
		}
		dst.WriteString(field[0])
		dst.WriteByte('=')
		dst.WriteString(field[1])
	}
}

// SignURL signs an HTTP request by creating
// a presigned URL string. The returned string
// is valid for only the specified duration, between one second and seven days.
func (s *SigningKey) SignURL(uri string, validfor time.Duration) (string, error) {
	if validfor < time.Second || validfor > 7*24*time.Hour {
		return "", fmt.Errorf("SignURL: validity must be between one second and seven days")
	}
	now := signtime().UTC()
	var stampStorage [len(longFormat)]byte
	stamp := now.AppendFormat(stampStorage[:0], longFormat)
	u, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	if u.Host == "" || u.Scheme != "https" && u.Scheme != "http" {
		return "", fmt.Errorf("SignURL: expected an absolute HTTP URL")
	}
	host := u.Host
	path := cmp.Or(u.EscapedPath(), "/")
	var queryStorage [512]byte
	query := bytes.NewBuffer(queryStorage[:0])
	if u.RawQuery == "" {
		// These fixed fields are already in canonical query order.
		query.WriteString("X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=")
		query.WriteString(queryEscape(s.AccessKey))
		query.WriteString("%2F")
		query.Write(stamp[:8])
		query.WriteString("%2F")
		query.WriteString(queryEscape(s.Region))
		query.WriteString("%2F")
		query.WriteString(queryEscape(s.Service))
		query.WriteString("%2Faws4_request&X-Amz-Date=")
		query.Write(stamp)
		query.WriteString("&X-Amz-Expires=")
		var digits [20]byte
		query.Write(strconv.AppendInt(digits[:0], int64(validfor/time.Second), 10))
		if s.Token != "" {
			query.WriteString("&X-Amz-Security-Token=")
			query.WriteString(queryEscape(s.Token))
		}
		query.WriteString("&X-Amz-SignedHeaders=host")
	} else {
		var scopeStorage [128]byte
		scope := bytes.NewBuffer(scopeStorage[:0])
		scope.WriteString(s.AccessKey)
		scope.WriteByte('/')
		s.toscope(scope, stamp[:8])
		q, err := url.ParseQuery(u.RawQuery)
		if err != nil {
			return "", fmt.Errorf("SignURL: parsing query: %w", err)
		}
		q.Del("X-Amz-Signature")
		q.Del("X-Amz-Security-Token")
		q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
		q.Set("X-Amz-Credential", scope.String())
		q.Set("X-Amz-Date", string(stamp))
		q.Set("X-Amz-Expires", strconv.FormatInt(int64(validfor/time.Second), 10))
		q.Set("X-Amz-SignedHeaders", "host")
		if s.Token != "" {
			q.Set("X-Amz-Security-Token", s.Token)
		}
		encodeQuery(query, q)
	}

	// build 'canonical request'
	// method
	var dstStorage [512]byte
	dst := bytes.NewBuffer(dstStorage[:0])
	dst.WriteString("GET\n")
	// canonical URI
	dst.WriteString(path)
	dst.WriteByte('\n')
	// canonical query string
	dst.Write(query.Bytes())
	dst.WriteByte('\n')
	// canonical headers: just 'host:<host>'
	dst.WriteString("host:")
	dst.WriteString(host)
	dst.WriteByte('\n')
	// signed headers (just host) plus payload hash (UNSIGNED-PAYLOAD)
	dst.WriteString("\nhost\nUNSIGNED-PAYLOAD")

	var hexbuf [2 * sha256.Size]byte
	h := sha256.Sum256(dst.Bytes())
	dst.Reset()
	var reqhash [2 * sha256.Size]byte
	hex.Encode(reqhash[:], h[:])
	s.tosign(dst, stamp, reqhash[:])
	s.sign(dst.Bytes(), hexbuf[:], now)
	query.WriteString("&X-Amz-Signature=")
	query.Write(hexbuf[:])
	var result strings.Builder
	result.Grow(len(u.Scheme) + 3 + len(host) + len(path) + 1 + query.Len())
	result.WriteString(u.Scheme)
	result.WriteString("://")
	result.WriteString(host)
	result.WriteString(path)
	result.WriteByte('?')
	result.Write(query.Bytes())
	return result.String(), nil
}

// SigningKey is a key that can be used
// to sign AWS service requests.
//
// SigningKey must not be copied after its first use. Credential fields may be
// changed between calls, but not concurrently with signing.
type SigningKey struct {
	BaseURI   string    // S3 base URI (empty is default AWS S3)
	Region    string    // AWS Region
	Service   string    // AWS Service
	AccessKey string    // AWS Access Key ID
	Secret    string    // AWS Secret key
	Token     string    // Token, if key is from STS
	Derived   time.Time // time token was derived
	cacheMu   sync.Mutex
	cache     *signingCache
}

type signingCache struct {
	year      int
	month     time.Month
	day       int
	accessKey string
	secret    string
	token     string
	region    string
	service   string
	key       [sha256.Size]byte
}

func macinto(key, mem []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(mem)
	return h.Sum(key[:0])
}

func derive(secret string, when time.Time, region, service string) []byte {
	datestr := when.Format(shortFormat)
	k := []byte("AWS4" + secret)
	k = macinto(k, []byte(datestr))
	k = macinto(k, []byte(region))
	k = macinto(k, []byte(service))
	k = macinto(k, []byte("aws4_request"))
	return k
}

// DeriveKey derives a SigningKey that can be used
// to sign requests
func DeriveKey(baseURI, accessKey, secret, region, service string) *SigningKey {
	now := signtime().UTC()
	return &SigningKey{
		BaseURI:   baseURI,
		Region:    region,
		Service:   service,
		AccessKey: accessKey,
		Secret:    secret,
		Derived:   now,
	}
}

func (s *SigningKey) InRegion(region string) *SigningKey {
	return &SigningKey{
		BaseURI:   s.BaseURI,
		Region:    region,
		Service:   s.Service,
		AccessKey: s.AccessKey,
		Secret:    s.Secret,
		Token:     s.Token,
		Derived:   s.Derived,
	}
}

func (s *SigningKey) pickKey(when time.Time) []byte {
	when = when.UTC()
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if c := s.cache; c != nil && c.year == when.Year() && c.month == when.Month() && c.day == when.Day() &&
		c.accessKey == s.AccessKey && c.secret == s.Secret && c.token == s.Token && c.region == s.Region && c.service == s.Service {
		return c.key[:]
	}
	key := derive(s.Secret, when, s.Region, s.Service)
	c := &signingCache{
		year: when.Year(), month: when.Month(), day: when.Day(),
		accessKey: s.AccessKey, secret: s.Secret, token: s.Token, region: s.Region, service: s.Service,
	}
	copy(c.key[:], key)
	s.cache = c
	return c.key[:]
}

func (s *SigningKey) sign(src, dst []byte, when time.Time) {
	var pad [sha256.BlockSize]byte
	// The derived signing key is always one SHA-256 digest, shorter than a block.
	copy(pad[:], s.pickKey(when))
	for i := range pad {
		pad[i] ^= 0x36
	}
	h := sha256.New()
	h.Write(pad[:])
	h.Write(src)
	var sum [sha256.Size]byte
	inner := h.Sum(sum[:0])
	for i := range pad {
		pad[i] ^= 0x36 ^ 0x5c
	}
	h.Reset()
	h.Write(pad[:])
	h.Write(inner)
	hex.Encode(dst, h.Sum(sum[:0]))
}
