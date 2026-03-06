package utils

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/wzshiming/httpseek"
)

var HTTPClient = &http.Client{
	Transport: newFixHFMirrorRoundTripper(httpseek.NewMustReaderTransport(http.DefaultTransport,
		func(r *http.Request, retry int, err error) error {
			slog.Default().Warn("Retrying request", "retry", retry+1, "url", r.URL.String(), "error", err)
			if retry >= 5 {
				return fmt.Errorf("max retries reached for %s: %w", r.URL.String(), err)
			}
			// Simple backoff strategy
			time.Sleep(time.Duration(retry+1) * time.Second)
			return nil
		})),
}
