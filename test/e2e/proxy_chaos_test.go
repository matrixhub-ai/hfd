package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
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
	"sync/atomic"
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
		name           string
		wrappers       []Wrapper
		payloadSizeKB  int
		largeFileSize  int64
		downloadRuns   int
		cutLimit       int64
		minConnections int64
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
			name:           "limit",
			cutLimit:       512 * 1024,
			minConnections: 2,
			largeFileSize:  1024 * 1024,
			payloadSizeKB:  64,
			downloadRuns:   2,
		},
		{
			name:           "cut-every-256k",
			cutLimit:       256 * 1024,
			minConnections: 4,
			largeFileSize:  1024 * 1024,
			payloadSizeKB:  64,
			downloadRuns:   1,
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

			var expectedLarge []byte
			if profile.cutLimit > 0 {
				expectedLarge = make([]byte, int(profile.largeFileSize))
				if _, err := rand.Read(expectedLarge); err != nil {
					t.Fatalf("generate large.bin: %v", err)
				}
			}
			seedRepoViaHF(t, upstream.httpURL, repoID, expectedContent, profile.largeFileSize, expectedLarge)
			if expectedLarge != nil {
				var entries []struct {
					Path string `json:"path"`
					LFS  *struct {
						OID string `json:"oid"`
					} `json:"lfs"`
				}
				if err := json.Unmarshal(mustGet(t, upstream.httpURL+"/api/models/"+repoID+"/tree/main/"), &entries); err != nil {
					t.Fatalf("decode upstream tree: %v", err)
				}
				found := false
				for _, entry := range entries {
					if entry.Path == "large.bin" {
						found = true
						if entry.LFS == nil || entry.LFS.OID != fmt.Sprintf("%x", sha256.Sum256(expectedLarge)) {
							t.Fatalf("large.bin must be LFS with seeded sha256: entry=%+v", entry)
						}
					}
				}
				if !found {
					t.Fatal("large.bin missing from upstream tree")
				}
			}

			limit := &Limit{Limit: profile.cutLimit}
			wrappers := profile.wrappers
			if profile.cutLimit > 0 {
				wrappers = append(wrappers, limit)
			}
			chaosProxy, connections := setupChaosReverseProxy(t, upstream.httpURL, wrappers)
			defer func() { t.Logf("upstream connections=%d cuts=%d", connections.Load(), limit.Cuts.Load()) }()
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
						t.Fatalf("downloaded large.bin size mismatch on run %d: got %d want %d; connections=%d cuts=%d", i+1, info.Size(), profile.largeFileSize, connections.Load(), limit.Cuts.Load())
					}
					if expectedLarge != nil {
						got, err := os.ReadFile(filepath.Join(downloadDir, "large.bin"))
						if err != nil || !bytes.Equal(got, expectedLarge) {
							t.Fatalf("downloaded large.bin bytes mismatch on run %d: got=%d want=%d err=%v connections=%d cuts=%d", i+1, len(got), len(expectedLarge), err, connections.Load(), limit.Cuts.Load())
						}
					}
				}
			}
			if profile.cutLimit > 0 && (connections.Load() < profile.minConnections || limit.Cuts.Load() < profile.largeFileSize/profile.cutLimit) {
				t.Fatalf("interruption not exercised: connections=%d want>=%d cuts=%d want>=%d", connections.Load(), profile.minConnections, limit.Cuts.Load(), profile.largeFileSize/profile.cutLimit)
			}
		})
	}
}

func seedRepoViaHF(t *testing.T, endpoint, repoID, readmeContent string, largeFileSize int64, largeContent []byte) {
	t.Helper()

	uploadDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(uploadDir, "README.md"), []byte(readmeContent), 0644); err != nil {
		t.Fatalf("failed to write README.md for upload: %v", err)
	}
	if largeContent != nil {
		if err := os.WriteFile(filepath.Join(uploadDir, "large.bin"), largeContent, 0644); err != nil {
			t.Fatalf("failed to write large.bin for upload: %v", err)
		}
	} else if largeFileSize > 0 {
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

func setupChaosReverseProxy(t *testing.T, upstreamURL string, wrappers []Wrapper) (*httptest.Server, *atomic.Int64) {
	t.Helper()

	targetURL, err := url.Parse(upstreamURL)
	if err != nil {
		t.Fatalf("invalid upstream URL %q: %v", upstreamURL, err)
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	for _, wrapper := range wrappers {
		if _, ok := wrapper.(*Limit); ok {
			// Cut profiles strip the xet capability headers so the mirror ingests over plain HTTP
			// through the limited connections; the xet CAS path (presigned on S3) is not cut here.
			proxy.ModifyResponse = func(resp *http.Response) error {
				resp.Header.Del("X-Xet-Hash")
				resp.Header.Del("Link")
				if byteRange := resp.Request.Header.Get("Range"); byteRange != "" {
					t.Logf("upstream Range=%q status=%d Content-Range=%q", byteRange, resp.StatusCode, resp.Header.Get("Content-Range"))
				}
				return nil
			}
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	connections := &atomic.Int64{}
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		connections.Add(1)
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

	return server, connections
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
