// Package stallguard provides an http.RoundTripper that aborts transfers
// making no byte progress within an idle window, so retry layers above can
// resume instead of hanging forever.
package stallguard

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// ErrStalled marks transfers aborted by the watchdog; it deliberately does
// not wrap context.Canceled so retry layers treat it as retryable.
var ErrStalled = errors.New("transfer stalled")

type transport struct {
	base http.RoundTripper
	idle time.Duration
}

// NewTransport wraps base so any request phase (dial, headers, request body
// send, response body read) that moves no bytes for idle is aborted with
// ErrStalled. A nil base uses http.DefaultTransport.
func NewTransport(base http.RoundTripper, idle time.Duration) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &transport{base: base, idle: idle}
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.idle <= 0 {
		return t.base.RoundTrip(req)
	}
	ctx, cancel := context.WithCancelCause(req.Context())
	w := &watchdog{idle: t.idle, cancel: cancel}
	req = req.WithContext(ctx)
	if req.Body != nil {
		req.Body = &requestBody{body: req.Body, w: w}
	}
	if getBody := req.GetBody; getBody != nil {
		req.GetBody = func() (io.ReadCloser, error) {
			rc, err := getBody()
			if err != nil {
				return nil, err
			}
			return &requestBody{body: rc, w: w}, nil
		}
	}
	w.reset()
	resp, err := t.base.RoundTrip(req)
	w.stop()
	if err != nil {
		err = w.rewrite(err)
		cancel(context.Canceled)
		return nil, err
	}
	resp.Body = &responseBody{body: resp.Body, w: w, cancel: cancel}
	return resp, nil
}

// watchdog cancels the per-attempt context when no progress happens within
// the idle window.
type watchdog struct {
	idle   time.Duration
	cancel context.CancelCauseFunc

	mu      sync.Mutex
	timer   *time.Timer
	stalled bool
}

func (w *watchdog) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.timer == nil {
		w.timer = time.AfterFunc(w.idle, w.fire)
		return
	}
	w.timer.Reset(w.idle)
}

func (w *watchdog) fire() {
	w.mu.Lock()
	w.stalled = true
	w.mu.Unlock()
	w.cancel(fmt.Errorf("%w: no progress within %s", ErrStalled, w.idle))
}

func (w *watchdog) stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.timer != nil {
		w.timer.Stop()
	}
}

// rewrite converts errors of a watchdog-aborted attempt into ErrStalled;
// io.EOF passes through so range-resume layers keep their end-of-body logic.
func (w *watchdog) rewrite(err error) error {
	w.mu.Lock()
	stalled := w.stalled
	w.mu.Unlock()
	if err == nil || err == io.EOF || !stalled || errors.Is(err, ErrStalled) {
		return err
	}
	return fmt.Errorf("%w: no progress within %s: %v", ErrStalled, w.idle, err)
}

// requestBody resets the watchdog as the transport consumes the upload;
// exhaustion grants one fresh window for the server to start responding.
type requestBody struct {
	body io.ReadCloser
	w    *watchdog
}

func (b *requestBody) Read(p []byte) (int, error) {
	n, err := b.body.Read(p)
	if n > 0 || err != nil {
		b.w.reset()
	}
	return n, err
}

func (b *requestBody) Close() error {
	return b.body.Close()
}

// responseBody arms the watchdog only while a Read is blocked, so slow
// consumers are never mistaken for stalled producers.
type responseBody struct {
	body   io.ReadCloser
	w      *watchdog
	cancel context.CancelCauseFunc
}

func (b *responseBody) Read(p []byte) (int, error) {
	b.w.reset()
	n, err := b.body.Read(p)
	b.w.stop()
	return n, b.w.rewrite(err)
}

// Close releases the per-attempt context; resumes are issued as new requests
// by the layers above, so this never cancels an in-flight retry.
func (b *responseBody) Close() error {
	b.w.stop()
	err := b.body.Close()
	b.cancel(context.Canceled)
	return err
}
