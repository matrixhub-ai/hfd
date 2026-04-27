package utils

import (
	"context"
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
	// Perform the request using the base RoundTripper
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	if resp.Body != nil {
		resp.Body = &fastTimeoutBody{
			body:    resp.Body,
			timeout: t.timeout,
		}
	}

	return resp, nil
}

type fastTimeoutBody struct {
	body    io.ReadCloser
	timeout time.Duration
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
		return res.n, res.err
	case <-time.After(b.timeout):
		b.body.Close() // Close the body to stop any further reads
		return 0, context.DeadlineExceeded
	}
}

func (b *fastTimeoutBody) Close() error {
	return b.body.Close()
}
