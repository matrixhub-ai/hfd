package stallguard

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/httpseek"
)

const testIdle = 200 * time.Millisecond

func TestHealthyTransfer(t *testing.T) {
	content := strings.Repeat("healthy", 1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, content)
	}))
	defer srv.Close()

	client := &http.Client{Transport: NewTransport(nil, testIdle)}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != content {
		t.Fatalf("content mismatch: got %d bytes, want %d", len(got), len(content))
	}
}

func TestBodyStallReturnsErrStalled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		_, _ = w.Write([]byte("partial"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()

	client := &http.Client{Transport: NewTransport(nil, testIdle)}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if !errors.Is(err, ErrStalled) {
		t.Fatalf("want ErrStalled, got %v", err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stall error must not look like a caller cancellation: %v", err)
	}
	if string(got) != "partial" {
		t.Fatalf("bytes before the stall must be preserved, got %q", got)
	}
}

func TestHeaderStallReturnsErrStalled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	client := &http.Client{Transport: NewTransport(nil, testIdle)}
	resp, err := client.Get(srv.URL)
	if err == nil {
		resp.Body.Close()
		t.Fatal("want error, got response")
	}
	if !errors.Is(err, ErrStalled) {
		t.Fatalf("want ErrStalled, got %v", err)
	}
}

func TestCallerCancellationPassesThrough(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	client := &http.Client{Transport: NewTransport(nil, time.Minute)}
	_, err := client.Do(req)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if errors.Is(err, ErrStalled) {
		t.Fatalf("caller cancellation must not be reported as a stall: %v", err)
	}
}

// TestStallResumesWithHTTPSeek proves the composition used by the mirror
// download client: a stalled response is aborted by the guard and resumed by
// httpseek with a ranged retry, yielding the complete content.
func TestStallResumesWithHTTPSeek(t *testing.T) {
	content := strings.Repeat("0123456789abcdef", 4096)
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		start := int64(0)
		if rng := r.Header.Get("Range"); rng != "" {
			fmt.Sscanf(rng, "bytes=%d-", &start)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(content)-1, len(content)))
			w.Header().Set("Content-Length", strconv.Itoa(len(content)-int(start)))
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		}
		if n == 1 {
			// First attempt: half the payload, then a silent stall.
			_, _ = io.WriteString(w, content[start:len(content)/2])
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		_, _ = io.WriteString(w, content[start:])
	}))
	defer srv.Close()

	var retries atomic.Int32
	client := &http.Client{
		Transport: httpseek.NewMustReaderTransport(NewTransport(nil, testIdle), func(r *http.Request, retry int, err error) error {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			retries.Add(1)
			return nil
		}),
	}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != content {
		t.Fatalf("content mismatch after resume: got %d bytes, want %d", len(got), len(content))
	}
	if retries.Load() == 0 {
		t.Fatal("expected at least one stall-triggered retry")
	}
	if requests.Load() < 2 {
		t.Fatal("expected a ranged resume request")
	}
}

// slowBody yields a byte per interval; Close unblocks any pending Read.
type slowBody struct {
	interval time.Duration
	remain   int
	closed   chan struct{}
}

func (b *slowBody) Read(p []byte) (int, error) {
	if b.remain <= 0 {
		return 0, io.EOF
	}
	select {
	case <-time.After(b.interval):
	case <-b.closed:
		return 0, errors.New("body closed")
	}
	b.remain--
	p[0] = 'x'
	return 1, nil
}

func (b *slowBody) Close() error {
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}
	return nil
}

func TestSlowUploadProgressIsNotKilled(t *testing.T) {
	received := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, _ := io.Copy(io.Discard, r.Body)
		received = int(n)
	}))
	defer srv.Close()

	body := &slowBody{interval: testIdle / 4, remain: 10, closed: make(chan struct{})}
	req, _ := http.NewRequest(http.MethodPut, srv.URL, body)
	client := &http.Client{Transport: NewTransport(nil, testIdle)}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	resp.Body.Close()
	if received != 10 {
		t.Fatalf("server received %d bytes, want 10", received)
	}
}

// infiniteBody never blocks and never ends, so upload progress halts only
// when the transport stops draining it (a blocked conn.Write).
type infiniteBody struct{}

func (infiniteBody) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

func (infiniteBody) Close() error { return nil }

func TestStalledUploadReturnsErrStalled(t *testing.T) {
	// The handler must block on a channel: with an unread request body the
	// server never watches for client disconnect, so ctx.Done never fires.
	unblock := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Never drain the body: the client's send buffers fill and its
		// writes stall, mimicking a dead upstream mid-upload.
		<-unblock
	}))
	defer srv.Close()
	defer close(unblock)

	req, _ := http.NewRequest(http.MethodPut, srv.URL, infiniteBody{})
	client := &http.Client{Transport: NewTransport(nil, testIdle)}
	resp, err := client.Do(req)
	if err == nil {
		resp.Body.Close()
		t.Fatal("want error, got response")
	}
	if !errors.Is(err, ErrStalled) {
		t.Fatalf("want ErrStalled, got %v", err)
	}
}
