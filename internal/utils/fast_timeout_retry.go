package utils

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

type fastTimeoutRetryTransport struct {
	base    http.RoundTripper
	timeout time.Duration
}

func newFastTimeoutRetryTransport(base http.RoundTripper, timeout time.Duration) *fastTimeoutRetryTransport {
	if base == nil {
		base = http.DefaultTransport
	}
	return &fastTimeoutRetryTransport{base: base, timeout: timeout}
}

func (t *fastTimeoutRetryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancel(req.Context())

	type roundTripResult struct {
		resp *http.Response
		err  error
	}

	resultCh := make(chan roundTripResult, 1)

	go func() {
		resp, err := t.base.RoundTrip(req.WithContext(ctx))
		resultCh <- roundTripResult{resp: resp, err: err}
	}()

	select {
	case res := <-resultCh:
		if res.err != nil {
			cancel()
			return nil, res.err
		}

		if res.resp.Body != nil {
			res.resp.Body = &fastTimeoutBody{
				body:    res.resp.Body,
				timeout: t.timeout,
				cancel:  cancel,
			}
		} else {
			cancel()
		}

		return res.resp, nil
	case <-time.After(t.timeout):
		cancel()
		return nil, fmt.Errorf("request timed out after %s: %w", t.timeout, context.DeadlineExceeded)
	}
}

type fastTimeoutBody struct {
	body    io.ReadCloser
	cancel  context.CancelFunc
	timeout time.Duration
	readed  int64
}

func (b *fastTimeoutBody) Read(p []byte) (n int, err error) {
	type readResult struct {
		n   int
		err error
	}

	resultCh := make(chan readResult, 1)

	go func() {
		n, err := b.body.Read(p)
		resultCh <- readResult{n: n, err: err}
	}()

	select {
	case res := <-resultCh:
		if res.err != nil && res.err != io.EOF {
			return 0, res.err
		}
		b.readed += int64(res.n)
		return res.n, res.err
	case <-time.After(b.timeout):
		return 0, fmt.Errorf("read timed out after %s, readed %d: %w", b.timeout, b.readed, context.DeadlineExceeded)
	}
}

func (b *fastTimeoutBody) Close() error {
	err := b.body.Close()
	b.cancel()
	return err
}
