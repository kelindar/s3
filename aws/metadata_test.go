package aws

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// helper to run metadata tests with a mocked EC2 metadata service
func withMetadataServer(t *testing.T, handler http.HandlerFunc, fn func()) {
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	// patch http.DefaultClient so that requests to 169.254.169.254 go to our server
	origClient := http.DefaultClient
	t.Cleanup(func() { http.DefaultClient = origClient })

	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	u, _ := url.Parse(srv.URL)
	dialer := &net.Dialer{}
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if strings.HasPrefix(addr, "169.254.169.254") {
			addr = u.Host
		}
		return dialer.DialContext(ctx, network, addr)
	}
	http.DefaultClient = &http.Client{Transport: tr}

	fn()
}

func TestMetadataString(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latest/api/token":
			w.Write([]byte("tok"))
		case "/latest/meta-data/test":
			if r.Header.Get("X-Aws-Ec2-Metadata-Token") != "tok" {
				http.Error(w, "bad token", http.StatusForbidden)
				return
			}
			w.Write([]byte("value"))
		default:
			http.NotFound(w, r)
		}
	}

	withMetadataServer(t, handler, func() {
		val, err := MetadataString("test")
		assert.NoError(t, err)
		assert.Equal(t, "value", val)
	})
}

func TestMetadataJSON(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latest/api/token":
			w.Write([]byte("tok"))
		case "/latest/meta-data/info":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"foo":"bar"}`))
		default:
			http.NotFound(w, r)
		}
	}

	withMetadataServer(t, handler, func() {
		var out struct{ Foo string }
		err := MetadataJSON("info", &out)
		assert.NoError(t, err)
		assert.Equal(t, "bar", out.Foo)
	})
}

func TestEC2Region(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latest/api/token":
			w.Write([]byte("tok"))
		case "/latest/meta-data/placement/availability-zone":
			w.Write([]byte("us-east-2a"))
		default:
			http.NotFound(w, r)
		}
	}

	withMetadataServer(t, handler, func() {
		region, err := ec2Region()
		assert.NoError(t, err)
		assert.Equal(t, "us-east-2", region)
	})
}

func TestS3EndPoint(t *testing.T) {
	os.Unsetenv("S3_ENDPOINT")
	assert.Equal(t, "https://s3.us-west-1.amazonaws.com", S3EndPoint("us-west-1"))

	os.Setenv("S3_ENDPOINT", "http://localhost:9000/")
	defer os.Unsetenv("S3_ENDPOINT")
	assert.Equal(t, "http://localhost:9000", S3EndPoint("ignored"))
}

func TestB2EndPoint(t *testing.T) {
	assert.Equal(t, "https://s3.eu-west-2.backblazeb2.com", B2EndPoint("eu-west-2"))
}
