package e2e_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const oneGiB = int64(1) << 30

// TestHFMirrorOverChaosProxyMatrix validates that a mirror hfd can keep serving
// stable hf client downloads while upstream traffic passes through a chaos proxy.
func TestHFMirrorOverChaosProxyMatrix(t *testing.T) {
	if _, err := exec.LookPath("hf"); err != nil {
		t.Skip("hf CLI not available, skipping chaos proxy matrix test")
	}

	profiles := []struct {
		name          string
		wrappers      []Wrapper
		payloadSizeKB int
		largeFileSize int64
		downloadRuns  int
	}{
		{
			name:          "baseline",
			wrappers:      nil,
			payloadSizeKB: 64,
			downloadRuns:  2,
		},
		{
			name: "latency",
			wrappers: []Wrapper{
				&Latency{Delay: 1, Jitter: 1},
			},
			payloadSizeKB: 64,
			downloadRuns:  2,
		},
		{
			name: "bandwidth",
			wrappers: []Wrapper{
				&Bandwidth{Rate: 64 * 1024}, // 64 KB/s
			},
			payloadSizeKB: 64,
			downloadRuns:  2,
		},
		{
			name: "latency+bandwidth",
			wrappers: []Wrapper{
				&Latency{Delay: 1, Jitter: 1},
				&Bandwidth{Rate: 64 * 1024}, // 64 KB/s
			},
			payloadSizeKB: 64,
			downloadRuns:  2,
		},
		{
			name: "limit",
			wrappers: []Wrapper{
				&Limit{Limit: 8 * 1024 * 1024}, // 8 MB cap per connection
			},
			payloadSizeKB: 64,
			downloadRuns:  2,
		},
		{
			name:          "largefile-1gb",
			wrappers:      nil,
			payloadSizeKB: 64,
			largeFileSize: oneGiB,
			downloadRuns:  1,
		},
	}

	for _, profile := range profiles {
		t.Run(profile.name, func(t *testing.T) {
			upstream := newE2EServer(t)
			repoID := fmt.Sprintf("matrix-user/chaos-%s", strings.ReplaceAll(profile.name, "+", "-"))
			expectedContent := buildPayload(profile.name, profile.payloadSizeKB)

			seedRepoViaHF(t, upstream.httpURL, repoID, expectedContent, profile.largeFileSize)

			chaosProxy := setupChaosReverseProxy(t, upstream.httpURL, profile.wrappers)
			mirror := newE2EServer(t, withMirrorSource(chaosProxy.URL))

			for i := 0; i < profile.downloadRuns; i++ {
				downloadDir := t.TempDir()
				runHFCmd(t, mirror.httpURL, "download", repoID, "--local-dir", downloadDir)

				readme, err := os.ReadFile(filepath.Join(downloadDir, "README.md"))
				if err != nil {
					t.Fatalf("failed to read downloaded README.md: %v", err)
				}
				if string(readme) != expectedContent {
					t.Fatalf("downloaded content mismatch on run %d: got %q want %q", i+1, string(readme), expectedContent)
				}

				if profile.largeFileSize > 0 {
					info, err := os.Stat(filepath.Join(downloadDir, "large.bin"))
					if err != nil {
						t.Fatalf("failed to stat downloaded large.bin: %v", err)
					}
					if info.Size() != profile.largeFileSize {
						t.Fatalf("downloaded large.bin size mismatch on run %d: got %d want %d", i+1, info.Size(), profile.largeFileSize)
					}
				}
			}
		})
	}
}

func seedRepoViaHF(t *testing.T, endpoint, repoID, readmeContent string, largeFileSize int64) {
	t.Helper()

	uploadDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(uploadDir, "README.md"), []byte(readmeContent), 0644); err != nil {
		t.Fatalf("failed to write README.md for upload: %v", err)
	}
	if largeFileSize > 0 {
		if err := writeSparseFile(filepath.Join(uploadDir, "large.bin"), largeFileSize); err != nil {
			t.Fatalf("failed to write large.bin for upload: %v", err)
		}
	}

	runHFCmd(t, endpoint, "upload", repoID, uploadDir, ".", "--commit-message", "seed chaos matrix repo")
}

func writeSparseFile(path string, size int64) error {
	if size <= 0 {
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Seek(size-1, 0); err != nil {
		return err
	}
	if _, err := f.Write([]byte{0}); err != nil {
		return err
	}
	return nil
}

func setupChaosReverseProxy(t *testing.T, upstreamURL string, wrappers []Wrapper) *httptest.Server {
	t.Helper()

	targetURL, err := url.Parse(upstreamURL)
	if err != nil {
		t.Fatalf("invalid upstream URL %q: %v", upstreamURL, err)
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		for _, wrapper := range wrappers {
			conn = wrapper.Wrap(conn)
		}
		return conn, nil
	}
	proxy.Transport = transport
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		http.Error(w, err.Error(), http.StatusBadGateway)
	}

	server := httptest.NewServer(proxy)
	t.Cleanup(func() {
		transport.CloseIdleConnections()
		server.Close()
	})

	return server
}

func buildPayload(caseName string, payloadSizeKB int) string {
	if payloadSizeKB < 1 {
		payloadSizeKB = 1
	}
	payloadBytes := payloadSizeKB * 1024
	prefix := "# Chaos Matrix\ncase=" + caseName + "\n"
	if len(prefix) >= payloadBytes {
		return prefix
	}
	return prefix + strings.Repeat("x", payloadBytes-len(prefix))
}
