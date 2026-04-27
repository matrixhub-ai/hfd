package utils

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/wzshiming/httpseek"
)

var defaultTransport = http.DefaultTransport.(*http.Transport).Clone()

func init() {
	defaultTransport.DisableKeepAlives = true
}

var HTTPClient = &http.Client{
	Transport: newFixHFMirrorRoundTripper(
		httpseek.NewMustReaderTransport(
			newFastTimeoutRetryTransport(
				defaultTransport,
				// hf client has a default timeout of 10s.
				// we set it to 8s to avoid hitting the timeout too frequently.
				8*time.Second,
			),
			func(r *http.Request, retry int, err error) error {
				headerRange := r.Header.Get("Range")
				if errors.Is(err, context.Canceled) {
					slog.WarnContext(r.Context(), "Request canceled by context", "url", r.URL.String(), "range", headerRange, "error", err)
					return err
				}

				if retry >= 8 {
					return fmt.Errorf("max retries reached for %s: %w", r.URL.String(), err)
				}

				if errors.Is(err, context.DeadlineExceeded) {
					slog.WarnContext(r.Context(), "Retrying request due to deadline exceeded", "retry", retry+1, "url", r.URL.String(), "range", headerRange, "error", err)
				} else {
					backoff := 100 * time.Millisecond << retry
					slog.WarnContext(r.Context(), "Retrying request due to error, backoff applied", "retry", retry+1, "url", r.URL.String(), "range", headerRange, "backoff", backoff, "error", err)
					time.Sleep(backoff)
				}
				return nil
			},
		),
	),
}
