package e2e_test

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// requestRecorder captures every request hitting the server so the matrix
// can prove which transfer protocol a client actually used.
type requestRecorder struct {
	mu   sync.Mutex
	reqs []string
}

func (rr *requestRecorder) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rr.mu.Lock()
		rr.reqs = append(rr.reqs, r.Method+" "+r.URL.Path)
		rr.mu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func (rr *requestRecorder) reset() {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	rr.reqs = nil
}

// saw reports whether a request with the method and a path containing frag
// was recorded. Method "" matches any non-GET/HEAD (write) request.
func (rr *requestRecorder) saw(method, frag string) bool {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	for _, req := range rr.reqs {
		m, p, _ := strings.Cut(req, " ")
		if !strings.Contains(p, frag) {
			continue
		}
		if method == "" {
			if m != http.MethodGet && m != http.MethodHead {
				return true
			}
			continue
		}
		if m == method {
			return true
		}
	}
	return false
}

func (rr *requestRecorder) dump() string {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	return strings.Join(rr.reqs, "\n")
}

// noProxyEnv neutralizes ambient proxy settings for subprocesses; every
// endpoint in this matrix is 127.0.0.1 and Go's exec keeps the last
// duplicate env entry.
func noProxyEnv() []string {
	return []string{
		"http_proxy=", "https_proxy=", "all_proxy=",
		"HTTP_PROXY=", "HTTPS_PROXY=", "ALL_PROXY=",
		"NO_PROXY=*", "no_proxy=*",
	}
}

// requireUpDownMatrixTools skips locally when a required client is missing;
// on CI (CI env set) it hard-fails so the matrix stays enforced there.
func requireUpDownMatrixTools(t *testing.T) {
	t.Helper()
	missing := func(format string, args ...any) {
		t.Helper()
		if os.Getenv("CI") != "" {
			t.Fatalf(format, args...)
		}
		t.Skipf(format, args...)
	}
	if _, err := exec.LookPath("hf"); err != nil {
		missing("hf CLI not found; pip install -U 'huggingface_hub[cli]'")
	}
	if _, err := exec.LookPath("git-lfs"); err != nil {
		missing("git-lfs not found; install git-lfs")
	}
	out, err := exec.CommandContext(t.Context(), "hf", "env").CombinedOutput()
	if err != nil {
		missing("hf env failed: %v\n%s", err, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		key, val, ok := strings.Cut(line, ":")
		if !ok || !strings.Contains(key, "hf_xet") {
			continue
		}
		if v := strings.TrimSpace(val); v != "" && !strings.EqualFold(v, "N/A") {
			return
		}
	}
	missing("hf_xet (xet-core) not available to the hf CLI; pip install hf_xet\nhf env:\n%s", out)
}

// runHFCmdXet runs the hf CLI against endpoint with xet-core enabled or
// disabled. HF_HOME is isolated per call so nothing is served from cache. A
// watchdog kills hung invocations so a stuck client fails fast with output.
func runHFCmdXet(t *testing.T, endpoint string, xet bool, args ...string) string {
	t.Helper()
	env := make([]string, 0, len(os.Environ())+6)
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "HF_HUB_DISABLE_XET=") {
			continue
		}
		// Everything talks to 127.0.0.1; ambient proxies break httpx.
		name, _, _ := strings.Cut(kv, "=")
		switch strings.ToUpper(name) {
		case "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY":
			continue
		}
		env = append(env, kv)
	}
	env = append(env,
		"HF_ENDPOINT="+endpoint,
		"HF_HUB_DISABLE_TELEMETRY=1",
		"HF_TOKEN=dummy-token",
		"HF_HOME="+t.TempDir(),
		"HF_HUB_VERBOSITY=debug",
	)
	if !xet {
		env = append(env, "HF_HUB_DISABLE_XET=1")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "hf", args...)
	cmd.Env = env
	cmd.WaitDelay = 10 * time.Second
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("hf %s failed (%v): %v\nOutput: %s", strings.Join(args, " "), ctx.Err(), err, output)
	}
	return string(output)
}

func hfUploadFile(t *testing.T, s *transferMatrixServer, repoID string, xet bool, data []byte) {
	t.Helper()
	src := filepath.Join(t.TempDir(), transferMatrixFile)
	if err := os.WriteFile(src, data, 0644); err != nil {
		t.Fatalf("write upload file: %v", err)
	}
	runHFCmdXet(t, s.httpURL, xet, "upload", repoID, src, transferMatrixFile, "--commit-message", "upload via hf cli")
}

func hfDownloadFile(t *testing.T, s *transferMatrixServer, repoID string, xet bool) []byte {
	t.Helper()
	dir := t.TempDir()
	runHFCmdXet(t, s.httpURL, xet, "download", repoID, transferMatrixFile, "--local-dir", dir)
	got, err := os.ReadFile(filepath.Join(dir, transferMatrixFile))
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	return got
}

// TestUploadDownloadClientMatrix crosses every upload channel with every
// download channel: hf CLI over plain http and over xet-core (Rust hf_xet),
// git-lfs over HTTP and SSH, and the Go xet client. The request recorder
// proves the hf CLI channels used the protocol they claim instead of
// silently falling back: xet-core traffic must hit the CAS xorb/shard and
// reconstruction routes and plain http traffic must hit the basic transfer
// and bridge routes.
// TestMain repeats the matrix for local and S3 storage.
func TestUploadDownloadClientMatrix(t *testing.T) {
	requireUpDownMatrixTools(t)

	uploads := []struct {
		name   string
		repo   string
		upload func(t *testing.T, s *transferMatrixServer, rec *requestRecorder, repoID string, data []byte)
	}{
		{
			name: "UpHFCliHTTP",
			repo: "matrix-org/updown-hf-http",
			upload: func(t *testing.T, s *transferMatrixServer, rec *requestRecorder, repoID string, data []byte) {
				hfUploadFile(t, s, repoID, false, data)
				if rec.saw("", "/xorbs/") || rec.saw("", "/shards") {
					t.Fatal("plain hf upload wrote to the CAS; expected basic transfer only")
				}
				if !rec.saw(http.MethodPut, "/objects/") {
					t.Fatal("plain hf upload never PUT the basic transfer endpoint")
				}
			},
		},
		{
			name: "UpHFCliXetCore",
			repo: "matrix-org/updown-hf-xet",
			upload: func(t *testing.T, s *transferMatrixServer, rec *requestRecorder, repoID string, data []byte) {
				hfUploadFile(t, s, repoID, true, data)
				// Chunk dedup queries are reads; only xorb/shard uploads prove a write.
				if !rec.saw("", "/xorbs/") && !rec.saw("", "/shards") {
					t.Fatalf("xet-core upload never wrote to the CAS; it fell back to another transfer\nrequests:\n%s", rec.dump())
				}
				if rec.saw(http.MethodPut, "/objects/") {
					t.Fatal("xet-core upload used the basic transfer PUT; expected CAS xorbs")
				}
			},
		},
		{
			name: "UpGitHTTPLFS",
			repo: "matrix-org/updown-git-http",
			upload: func(t *testing.T, s *transferMatrixServer, _ *requestRecorder, repoID string, data []byte) {
				remote, env := s.httpRemote(repoID)
				pushViaGitLFS(t, s, remote, append(env, noProxyEnv()...), repoID, data)
			},
		},
		{
			name: "UpGitSSHLFS",
			repo: "matrix-org/updown-git-ssh",
			upload: func(t *testing.T, s *transferMatrixServer, _ *requestRecorder, repoID string, data []byte) {
				remote, env := s.sshRemote(repoID)
				pushViaGitLFS(t, s, remote, append(env, noProxyEnv()...), repoID, data)
			},
		},
		{
			name: "UpXetGoBatch",
			repo: "matrix-org/updown-xet-go",
			upload: func(t *testing.T, s *transferMatrixServer, _ *requestRecorder, repoID string, data []byte) {
				pushViaXetBatch(t, s, repoID, data)
			},
		},
	}

	for i, up := range uploads {
		t.Run(up.name, func(t *testing.T) {
			rec := &requestRecorder{}
			s := newTransferMatrixServer(t, rec.wrap)
			org, name, _ := strings.Cut(up.repo, "/")
			s.createRepo(t, org, name)

			data := makeBinaryData(128*1024, byte(40+i))

			rec.reset()
			up.upload(t, s, rec, up.repo, data)

			t.Run("DownHFCliHTTP", func(t *testing.T) {
				rec.reset()
				got := hfDownloadFile(t, s, up.repo, false)
				if !bytes.Equal(got, data) {
					t.Fatal("hf http download bytes mismatch")
				}
				if rec.saw(http.MethodGet, "reconstructions") {
					t.Fatal("plain hf download used the xet reconstruction API")
				}
				if !rec.saw(http.MethodGet, "/xet-bridge/") {
					t.Fatal("plain hf download never followed the resolve redirect to the bridge")
				}
			})
			t.Run("DownHFCliXetCore", func(t *testing.T) {
				rec.reset()
				got := hfDownloadFile(t, s, up.repo, true)
				if !bytes.Equal(got, data) {
					t.Fatal("hf xet-core download bytes mismatch")
				}
				// Fragment matches the v1, v2, and batch reconstruction routes.
				if !rec.saw(http.MethodGet, "reconstructions") {
					t.Fatalf("xet-core download never queried the reconstruction API; it fell back to http\nrequests:\n%s", rec.dump())
				}
				if rec.saw(http.MethodGet, "/xet-bridge/") {
					t.Fatal("xet-core download fetched bytes over the plain bridge")
				}
			})
			t.Run("DownGitHTTPLFSPull", func(t *testing.T) {
				remote, env := s.httpRemote(up.repo)
				verifyGitLFSPull(t, s, remote, append(env, noProxyEnv()...), up.repo, data)
			})
			t.Run("DownGitSSHLFSPull", func(t *testing.T) {
				remote, env := s.sshRemote(up.repo)
				verifyGitLFSPull(t, s, remote, append(env, noProxyEnv()...), up.repo, data)
			})
			t.Run("DownXetGoResolve", func(t *testing.T) {
				verifyHFResolveXet(t, s, up.repo, data)
			})
		})
	}
}
