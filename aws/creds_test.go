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
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestScan(t *testing.T) {
	var foo, bar, baz, quux string
	basespec := []scanspec{
		{prefix: "foo", dst: &foo},
		{prefix: "bar", dst: &bar},
		{prefix: "baz", dst: &baz},
		{prefix: "quux", dst: &quux},
	}
	text := strings.Join([]string{
		"[default]",
		"foo=foo_result",
		"ignore this line",
		"bar = bar_result",
		"baz= baz_result",
		"quux  =quux_result",
		"ignoreme=",
		"=invalid line",
		"x=y=z",
		"[section2]",
		"foo=section2_result",
		"bar=section2_bar_result",
	}, "\n")
	spec := make([]scanspec, len(basespec))
	copy(spec, basespec)
	err := scan(strings.NewReader(text), "default", spec)
	assert.NoError(t, err)
	assert.Equal(t, "foo_result", foo)
	assert.Equal(t, "bar_result", bar)
	assert.Equal(t, "baz_result", baz)
	assert.Equal(t, "quux_result", quux)
	copy(spec, basespec)
	err = scan(strings.NewReader(text), "section2", spec)
	assert.NoError(t, err)
	assert.Equal(t, "section2_result", foo)
	assert.Equal(t, "section2_bar_result", bar)
}

// helper to run STS-based tests with a mocked STS service
func withSTSServer(t *testing.T, handler http.HandlerFunc, fn func(client *http.Client)) {
	srv := httptest.NewTLSServer(handler)
	t.Cleanup(srv.Close)

	tr := srv.Client().Transport.(*http.Transport).Clone()
	tr.Proxy = nil
	if tr.TLSClientConfig != nil {
		tr.TLSClientConfig.InsecureSkipVerify = true
	}
	u, _ := url.Parse(srv.URL)
	dialer := &net.Dialer{}
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if strings.HasPrefix(addr, "sts.amazonaws.com") {
			addr = u.Host
		}
		return dialer.DialContext(ctx, network, addr)
	}
	client := &http.Client{Transport: tr}
	fn(client)
}

func TestWebIdentityCreds(t *testing.T) {
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	err := os.WriteFile(tokenFile, []byte("tok"), 0600)
	assert.NoError(t, err)

	os.Setenv("AWS_REGION", "us-west-1")
	defer os.Unsetenv("AWS_REGION")
	os.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/test")
	defer os.Unsetenv("AWS_ROLE_ARN")
	os.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", tokenFile)
	defer os.Unsetenv("AWS_WEB_IDENTITY_TOKEN_FILE")
	os.Setenv("AWS_ROLE_SESSION_NAME", "mysession")
	defer os.Unsetenv("AWS_ROLE_SESSION_NAME")

	handler := func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/xml", r.Header.Get("Accept"))
		q := r.URL.Query()
		assert.Equal(t, "AssumeRoleWithWebIdentity", q.Get("Action"))
		assert.Equal(t, "2011-06-15", q.Get("Version"))
		assert.Equal(t, "arn:aws:iam::123456789012:role/test", q.Get("RoleArn"))
		assert.Equal(t, "mysession", q.Get("RoleSessionName"))
		assert.Equal(t, "tok", q.Get("WebIdentityToken"))

		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(`
<AssumeRoleWithWebIdentityResponse>
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>AKID</AccessKeyId>
      <SecretAccessKey>SECRET</SecretAccessKey>
      <SessionToken>SESSION</SessionToken>
      <Expiration>2025-01-02T03:04:05Z</Expiration>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>`))
	}

	withSTSServer(t, handler, func(client *http.Client) {
		id, secret, region, token, expiration, err := WebIdentityCreds(client)
		assert.NoError(t, err)
		assert.Equal(t, "AKID", id)
		assert.Equal(t, "SECRET", secret)
		assert.Equal(t, "us-west-1", region)
		assert.Equal(t, "SESSION", token)
		wantTime, _ := time.Parse(time.RFC3339, "2025-01-02T03:04:05Z")
		assert.True(t, expiration.Equal(wantTime))
	})
}

func TestAmbientCreds(t *testing.T) {
	os.Setenv("AWS_ACCESS_KEY_ID", "AKID")
	defer os.Unsetenv("AWS_ACCESS_KEY_ID")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "SECRET")
	defer os.Unsetenv("AWS_SECRET_ACCESS_KEY")
	os.Setenv("AWS_REGION", "us-east-2")
	defer os.Unsetenv("AWS_REGION")
	os.Setenv("AWS_SESSION_TOKEN", "TOKEN")
	defer os.Unsetenv("AWS_SESSION_TOKEN")
	os.Setenv("HOME", t.TempDir())
	defer os.Unsetenv("HOME")

	id, secret, region, token, err := AmbientCreds("")
	assert.NoError(t, err)
	assert.Equal(t, "AKID", id)
	assert.Equal(t, "SECRET", secret)
	assert.Equal(t, "us-east-2", region)
	assert.Equal(t, "TOKEN", token)
}

func TestLoadCredentials(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials")

	assert.NoError(t, os.WriteFile(path, []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"), 2644))

	id, secret, err := loadCredentials(path, "default")
	assert.NoError(t, err)
	assert.Equal(t, "AKID", id)
	assert.Equal(t, "SECRET", secret)
}

func TestAmbientCreds_Local(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, ".aws", "credentials")
	defer os.RemoveAll(filepath.Dir(path))

	assert.NoError(t, os.MkdirAll(filepath.Dir(path), os.ModePerm))
	assert.NoError(t, os.WriteFile(path, []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"), 2644))

	id, secret, region, token, err := AmbientCreds("eu-central-1")
	assert.NoError(t, err)
	assert.Equal(t, "AKID", id)
	assert.Equal(t, "SECRET", secret)
	assert.Equal(t, "eu-central-1", region)
	assert.Equal(t, "", token)
}
