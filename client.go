package s3

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
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
		Transport:                 transport{},
	}
}

type connLease struct {
	net.Conn
	release func(bool)
}

func acquireConn(ctx context.Context, host *fasthttp.HostClient, connectionClose bool) (connLease, error) {
	var timeout time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}
	acquire := func() (connLease, error) {
		conn, err := host.AcquireConn(timeout, connectionClose)
		if err != nil {
			return connLease{}, err
		}
		return connLease{Conn: conn.Conn(), release: func(discard bool) {
			if discard {
				host.CloseConn(conn)
				return
			}
			host.ReleaseConn(conn)
		}}, nil
	}
	if ctx.Done() == nil {
		return acquire()
	}

	// Dialing may block before a connection exists to interrupt. A cancelled
	// caller leaves the acquisition to close its lease when dialing finishes.
	type result struct {
		conn connLease
		err  error
	}
	ready := make(chan result)
	go func() {
		conn, err := acquire()
		select {
		case ready <- result{conn, err}:
		case <-ctx.Done():
			if conn.release != nil {
				conn.release(true)
			}
		}
	}()
	select {
	case res := <-ready:
		return res.conn, res.err
	case <-ctx.Done():
		return connLease{}, ctx.Err()
	}
}

type transport struct{}

func (transport) RoundTrip(host *fasthttp.HostClient, req *fasthttp.Request, res *fasthttp.Response) (bool, error) {
	body := req.UserValue("s3.body").(*responseBody)
	conn, err := acquireConn(body.ctx, host, req.ConnectionClose())
	if err != nil {
		return false, err
	}
	body.conn = conn
	body.host = host
	if body.ctx.Done() != nil {
		body.cancelDone = make(chan struct{})
		body.stop = context.AfterFunc(body.ctx, func() {
			_ = conn.Close()
			close(body.cancelDone)
		})
	}
	if err := body.ctx.Err(); err != nil {
		return false, err
	}
	res.ParseNetConn(conn.Conn)
	deadline, _ := body.ctx.Deadline()
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return false, err
	}
	bw := host.AcquireWriter(conn.Conn)
	err = req.Write(bw)
	if err == nil {
		err = bw.Flush()
	}
	host.ReleaseWriter(bw)
	if err != nil {
		return false, err
	}
	readDeadline := deadline
	if host.ReadTimeout > 0 {
		limit := time.Now().Add(host.ReadTimeout)
		if readDeadline.IsZero() || limit.Before(readDeadline) {
			readDeadline = limit
		}
	}
	if err := conn.SetReadDeadline(readDeadline); err != nil {
		return false, err
	}
	res.SkipBody = res.SkipBody || req.Header.IsHead()
	body.reader = host.AcquireReader(conn.Conn)
	if err := res.ReadLimitBody(body.reader, host.MaxResponseBodySize); err != nil {
		return false, err
	}

	// The default limit covers headers; only the request context limits the body.
	if err := conn.SetReadDeadline(deadline); err != nil {
		return false, err
	}
	body.stream = res.BodyStream()
	body.complete = body.stream == nil || res.Header.ContentLength() == 0
	body.discard = req.ConnectionClose() || res.ConnectionClose()
	return false, nil
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
	response   *fasthttp.Response
	stream     io.Reader
	buffer     bytes.Reader
	ctx        context.Context
	conn       connLease
	host       *fasthttp.HostClient
	reader     *bufio.Reader
	stop       func() bool
	cancelDone chan struct{}
	complete   bool
	discard    bool
	closed     atomic.Bool
	readLock   sync.Mutex
	once       sync.Once
	err        error
}

func (b *responseBody) Read(p []byte) (int, error) {
	b.readLock.Lock()
	defer b.readLock.Unlock()
	var n int
	var err error
	switch {
	case b.closed.Load():
		return 0, io.ErrClosedPipe
	case b.ctx.Err() != nil:
		return 0, b.ctx.Err()
	case b.stream != nil:
		n, err = b.stream.Read(p)
	default:
		n, err = b.buffer.Read(p)
	}
	if err != nil {
		err = requestError(b.ctx, err)
		b.complete = err == io.EOF
	}
	return n, err
}

func requestError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	// A socket deadline can fire before the context's timer goroutine runs.
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return err
}

func (b *responseBody) Close() error {
	b.once.Do(func() {
		b.closed.Store(true)

		// Join a running callback before the connection can reenter the pool.
		if b.stop != nil && !b.stop() {
			<-b.cancelDone
		}
		locked := b.readLock.TryLock()
		if !locked {
			_ = b.conn.Close()
			b.readLock.Lock()
		}
		defer b.readLock.Unlock()
		discard := b.discard || !locked || !b.complete || b.ctx.Err() != nil
		b.err = b.response.CloseBodyStream()
		fasthttp.ReleaseResponse(b.response)
		b.response = nil
		if b.reader != nil {
			b.host.ReleaseReader(b.reader)
		}
		if b.conn.release != nil {
			b.conn.release(discard)
		}
	})
	return b.err
}

func signRequest(key *aws.SigningKey, req *fasthttp.Request, body []byte, headers ...[2]string) {
	uri := req.URI()
	uri.DisablePathNormalizing = true
	host := req.Header.Host()
	if len(host) == 0 {
		host = uri.Host()
		req.Header.SetHostBytes(host)
	}
	for _, header := range headers {
		req.Header.Set(header[0], header[1])
	}
	var storage [512]byte
	date, hash, auth := key.SignV4Into(storage[:0], req.Header.Method(), uri.PathOriginal(), uri.QueryString(), host, body, headers...)
	req.Header.SetBytesV("X-Amz-Date", date)
	req.Header.Set("X-Amz-Content-Sha256", hash)
	if key.Token != "" {
		req.Header.Set("X-Amz-Security-Token", key.Token)
	}
	req.Header.SetBytesV("Authorization", auth)
	if body != nil {
		req.SetBodyRaw(body)
	}
}

func doObject(ctx context.Context, key *aws.SigningKey, method, bucket, object string, body []byte) (*response, error) {
	switch {
	case ctx == nil:
		return nil, errors.New("s3 request: nil context")
	case ctx.Err() != nil:
		return nil, ctx.Err()
	}
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	setURI(req, key, bucket, object, "")
	req.Header.SetMethod(method)
	signRequest(key, req, body)
	return flakyFast(ctx, req)
}

func doFastRequest(ctx context.Context, fastReq *fasthttp.Request) (*response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		fastReq.SetTimeout(time.Until(deadline))
	}
	fastRes := fasthttp.AcquireResponse()
	res := &response{body: responseBody{response: fastRes, ctx: ctx}}
	res.Body = &res.body
	fastReq.SetUserValue("s3.body", res.Body)
	// Request cleanup must not close the response body owned by the caller.
	defer fastReq.RemoveUserValue("s3.body")
	switch err := defaultClient.Do(fastReq, fastRes); {
	case err != nil:
		_ = res.Body.Close()
		return nil, requestError(ctx, err)
	case ctx.Err() != nil:
		_ = res.Body.Close()
		return nil, ctx.Err()
	}
	res.StatusCode = fastRes.StatusCode()
	res.ContentLength = int64(fastRes.Header.ContentLength())
	res.Header = responseHeader{fastRes}
	if res.Body.stream == nil {
		res.Body.buffer.Reset(fastRes.Body())
	}
	return res, nil
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
