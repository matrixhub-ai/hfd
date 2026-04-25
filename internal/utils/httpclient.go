package utils

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/wzshiming/httpseek"
)

var HTTPClient = &http.Client{
	Transport: newFixHFMirrorRoundTripper(httpseek.NewMustReaderTransport(http.DefaultTransport,
		func(r *http.Request, retry int, err error) error {
			if err == context.Canceled {
				return err
			}

			if retry >= 8 {
				return fmt.Errorf("max retries reached for %s: %w", r.URL.String(), err)
			}

			slog.WarnContext(r.Context(), "Retrying request", "retry", retry+1, "url", r.URL.String(), "error", err)

			// Simple backoff strategy
			time.Sleep(100 * time.Millisecond << retry)
			return nil
		})),
}
