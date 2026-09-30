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
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestCreds(t *testing.T) {
	t.Chdir(t.TempDir())
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	for _, key := range []string{
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_REGION",
		"AWS_DEFAULT_REGION",
		"AWS_SESSION_TOKEN",
		"AWS_PROFILE",
		"AWS_DEFAULT_PROFILE",
		"AWS_CONFIG_FILE",
		"AWS_SHARED_CREDENTIALS_FILE",
		"AWS_ROLE_ARN",
		"AWS_WEB_IDENTITY_TOKEN_FILE",
		"AWS_ROLE_SESSION_NAME",
	} {
		t.Setenv(key, "")
	}

	t.Run("scan", func(t *testing.T) {
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
		require.NoError(t, err)
		assert.Equal(t, "foo_result", foo)
		assert.Equal(t, "bar_result", bar)
		assert.Equal(t, "baz_result", baz)
		assert.Equal(t, "quux_result", quux)
		copy(spec, basespec)
		err = scan(strings.NewReader(text), "section2", spec)
		require.NoError(t, err)
		assert.Equal(t, "section2_result", foo)
		assert.Equal(t, "section2_bar_result", bar)
	})

	t.Run("web identity", func(t *testing.T) {
		dir := t.TempDir()
		tokenFile := filepath.Join(dir, "token")
		err := os.WriteFile(tokenFile, []byte("tok"), 0600)
		require.NoError(t, err)

		t.Setenv("AWS_REGION", "us-west-1")
		t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/test")
		t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", tokenFile)
		t.Setenv("AWS_ROLE_SESSION_NAME", "mysession")

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
			require.NoError(t, err)
			assert.Equal(t, "AKID", id)
			assert.Equal(t, "SECRET", secret)
			assert.Equal(t, "us-west-1", region)
			assert.Equal(t, "SESSION", token)
			wantTime, _ := time.Parse(time.RFC3339, "2025-01-02T03:04:05Z")
			assert.True(t, expiration.Equal(wantTime))
		})
	})

	t.Run("ambient", func(t *testing.T) {
		t.Setenv("AWS_ACCESS_KEY_ID", "AKID")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "SECRET")
		t.Setenv("AWS_REGION", "us-east-2")
		t.Setenv("AWS_SESSION_TOKEN", "TOKEN")
		t.Setenv("HOME", t.TempDir())

		id, secret, region, token, err := AmbientCreds("")
		require.NoError(t, err)
		assert.Equal(t, "AKID", id)
		assert.Equal(t, "SECRET", secret)
		assert.Equal(t, "us-east-2", region)
		assert.Equal(t, "TOKEN", token)
	})

	t.Run("load credentials", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "credentials")

		require.NoError(t, os.WriteFile(path, []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"), 0644))

		id, secret, err := loadCredentials(path, "default")
		require.NoError(t, err)
		assert.Equal(t, "AKID", id)
		assert.Equal(t, "SECRET", secret)
	})

	t.Run("check special file", func(t *testing.T) {
		reader, writer, err := os.Pipe()
		require.NoError(t, err)
		t.Cleanup(func() {
			assert.NoError(t, reader.Close())
			assert.NoError(t, writer.Close())
		})

		info, err := reader.Stat()
		require.NoError(t, err)
		assert.ErrorContains(t, check(info), "is a special file")
	})

	t.Run("check permissions", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "credentials")
		require.NoError(t, os.WriteFile(path, nil, 0600))
		if runtime.GOOS != "windows" {
			require.NoError(t, os.Chmod(path, 0666))
		}

		info, err := os.Stat(path)
		require.NoError(t, err)
		if runtime.GOOS == "windows" {
			assert.NoError(t, check(info))
		} else {
			assert.ErrorContains(t, check(info), "is world-writeable")
		}
	})

	t.Run("ambient local", func(t *testing.T) {
		wd := t.TempDir()
		t.Chdir(wd)
		path := filepath.Join(wd, ".aws", "credentials")

		require.NoError(t, os.MkdirAll(filepath.Dir(path), os.ModePerm))
		require.NoError(t, os.WriteFile(path, []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"), 0644))

		// Verify that the credentials are loaded from the local file
		id, secret, region, token, err := AmbientCreds("eu-central-1")
		require.NoError(t, err)
		assert.Equal(t, "AKID", id)
		assert.Equal(t, "SECRET", secret)
		assert.Equal(t, "eu-central-1", region)
		assert.Equal(t, "", token)

		// Verify that the credentials are loaded from the local file
		key, err := AmbientKey("s3", "eu-central-1", DefaultDerive)
		require.NoError(t, err)
		require.NotNil(t, key)
		assert.Equal(t, "AKID", key.AccessKey)
		assert.Equal(t, "SECRET", key.Secret)
		assert.Equal(t, "eu-central-1", key.Region)
		assert.Equal(t, "", key.Token)
	})
}
