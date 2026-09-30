package s3

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/kelindar/s3/aws"
	"github.com/valyala/fasthttp"
)

var defaultClient = newClient(func(addr string) (net.Conn, error) {
	return fasthttp.DialTimeout(addr, 2*time.Second)
})

func newClient(dial fasthttp.DialFunc) *fasthttp.Client {
	return &fasthttp.Client{
		ReadTimeout:               60 * time.Second,
		Dial:                      dial,
		DisablePathNormalizing:    true,
		StreamResponseBody:        true,
		MaxIdemponentCallAttempts: 1,
	}
}

type cancelConn struct {
	net.Conn
	stop func() bool
}

func (c *cancelConn) Close() error {
	c.stop()
	return c.Conn.Close()
}

// A cancellable request needs its own connection so its context can close it.
func cancellableClient(ctx context.Context) *fasthttp.Client {
	return newClient(func(addr string) (net.Conn, error) {
		dialCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		conn, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		tracked := &cancelConn{Conn: conn}
		tracked.stop = context.AfterFunc(ctx, func() { _ = conn.Close() })
		return tracked, nil
	})
}

type response struct {
	StatusCode    int
	ContentLength int64
	Header        responseHeader
	Body          *responseBody
	body          responseBody
}

func (r *response) status() string {
	return fmt.Sprintf("%d %s", r.StatusCode, http.StatusText(r.StatusCode))
}

type responseHeader struct{ response *fasthttp.Response }

func (h responseHeader) Get(name string) string {
	return string(h.response.Header.Peek(name))
}

type responseBody struct {
	response *fasthttp.Response
	client   *fasthttp.Client
	stream   io.Reader
	buffer   bytes.Reader
	ctx      context.Context
	once     sync.Once
	err      error
}

func (b *responseBody) Read(p []byte) (int, error) {
	switch {
	case b.response == nil:
		return 0, io.ErrClosedPipe
	case b.ctx != nil && b.ctx.Err() != nil:
		return 0, b.ctx.Err()
	case b.stream != nil:
		return b.stream.Read(p)
	}
	return b.buffer.Read(p)
}

func (b *responseBody) Close() error {
	b.once.Do(func() {
		b.err = b.response.CloseBodyStream()
		fasthttp.ReleaseResponse(b.response)
		b.response = nil
		if b.client != nil {
			b.client.CloseIdleConnections()
		}
	})
	return b.err
}

func signRequest(key *aws.SigningKey, req *fasthttp.Request, body []byte) {
	uri := req.URI()
	uri.DisablePathNormalizing = true
	host := req.Header.Host()
	if len(host) == 0 {
		host = uri.Host()
		req.Header.SetHostBytes(host)
	}
	date, hash, auth := key.SignV4Raw(string(req.Header.Method()), string(uri.PathOriginal()), string(uri.QueryString()), string(host), body)
	req.Header.Set("X-Amz-Date", date)
	req.Header.Set("X-Amz-Content-Sha256", hash)
	if key.Token != "" {
		req.Header.Set("X-Amz-Security-Token", key.Token)
	}
	req.Header.Set("Authorization", auth)
	if body != nil {
		req.SetBodyRaw(body)
	}
}

func doSigned(ctx context.Context, key *aws.SigningKey, method, uri string, body []byte) (*response, error) {
	switch {
	case ctx == nil:
		return nil, errors.New("s3 request: nil context")
	case ctx.Err() != nil:
		return nil, ctx.Err()
	}
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI(uri)
	req.Header.SetMethod(method)
	signRequest(key, req, body)
	return flakyFast(ctx, req)
}

func doFast(req *http.Request, body []byte) (*response, error) {
	ctx := req.Context()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.Body != nil {
		defer req.Body.Close()
	}
	fastReq := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(fastReq)
	fastReq.SetRequestURI(req.URL.String())
	fastReq.Header.SetMethod(req.Method)
	if req.Host != "" {
		fastReq.Header.SetHost(req.Host)
	}
	for name, values := range req.Header {
		for _, value := range values {
			fastReq.Header.Add(name, value)
		}
	}
	if body != nil {
		fastReq.SetBodyRaw(body)
	}
	return doFastRequest(ctx, fastReq)
}

func doFastRequest(ctx context.Context, fastReq *fasthttp.Request) (*response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		fastReq.SetTimeout(time.Until(deadline))
	}
	fastRes := fasthttp.AcquireResponse()
	client := defaultClient
	if ctx.Done() != nil {
		client = cancellableClient(ctx)
	}
	if err := client.Do(fastReq, fastRes); err != nil {
		fasthttp.ReleaseResponse(fastRes)
		if client != defaultClient {
			client.CloseIdleConnections()
		}
		return nil, cmp.Or(ctx.Err(), err)
	}
	if err := ctx.Err(); err != nil {
		fasthttp.ReleaseResponse(fastRes)
		if client != defaultClient {
			client.CloseIdleConnections()
		}
		return nil, err
	}
	res := &response{
		StatusCode:    fastRes.StatusCode(),
		ContentLength: int64(fastRes.Header.ContentLength()),
		Header:        responseHeader{fastRes},
		body:          responseBody{response: fastRes, stream: fastRes.BodyStream()},
	}
	res.Body = &res.body
	if res.Body.stream == nil {
		res.Body.buffer.Reset(fastRes.Body())
	}
	if ctx.Done() != nil {
		res.Body.ctx = ctx
		res.Body.client = client
	}
	return res, nil
}

func flakyDo(req *http.Request, body []byte) (*response, error) {
	hasBody := req.Body != nil
	res, err := doFast(req, body)
	switch {
	case err == nil && res.StatusCode != 500 && res.StatusCode != 503:
		return res, nil
	case hasBody && req.GetBody == nil:
		return res, err
	}
	if res != nil {
		_ = res.Body.Close()
	}
	if hasBody {
		req.Body, err = req.GetBody()
		if err != nil {
			return nil, fmt.Errorf("req.GetBody: %w", err)
		}
	}
	return doFast(req, body)
}

func flakyFast(ctx context.Context, req *fasthttp.Request) (*response, error) {
	res, err := doFastRequest(ctx, req)
	if err == nil && res.StatusCode != 500 && res.StatusCode != 503 {
		return res, nil
	}
	if res != nil {
		_ = res.Body.Close()
	}
	return doFastRequest(ctx, req)
}
