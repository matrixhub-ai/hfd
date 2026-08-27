package e2e_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	backendssh "github.com/matrixhub-ai/hfd/pkg/backend/ssh"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// e2eServer bundles an HTTP server (hf + LFS + git + xet data plane) and an
// optional SSH git server sharing the same storage and mirror.
type e2eServer struct {
	httpURL string
	sshURL  string
	sshEnv  []string
	storage *storage.Storage
	mirror  *mirror.Mirror
}

type e2eConfig struct {
	upstreamURL string
	ssh         bool
	sshLFSURL   bool
	wraps       []func(http.Handler) http.Handler
}

type e2eOption func(*e2eConfig)

// withUpstream points the xet mirror handler at the given upstream instead
// of the default always-404 one.
func withUpstream(url string) e2eOption {
	return func(c *e2eConfig) { c.upstreamURL = url }
}

// withSSH starts an SSH git server sharing the HTTP server's storage,
// filling sshURL and sshEnv.
func withSSH() e2eOption {
	return func(c *e2eConfig) { c.ssh = true }
}

// withSSHLFSURL is withSSH with the SSH server additionally configured with
// the HTTP base URL, so git-lfs discovers the LFS (and lock) endpoint on a
// pure SSH remote through git-lfs-authenticate, with no lfs.url override on
// the client.
func withSSHLFSURL() e2eOption {
	return func(c *e2eConfig) { c.ssh = true; c.sshLFSURL = true }
}

// withWrap applies mw outermost, so it observes every request including CAS
// traffic.
func withWrap(mw func(http.Handler) http.Handler) e2eOption {
	return func(c *e2eConfig) { c.wraps = append(c.wraps, mw) }
}

// newE2EServer wires the full production handler chain the way cmd/hfd does.
// The mirror handler serves the xet token routes, so the data plane needs an
// upstream; tests that never leave local content default to an always-404
// one, which fully ingested objects never contact. During the S3 pass the
// xet storage lives in the fake S3 bucket, like production.
func newE2EServer(t *testing.T, opts ...e2eOption) *e2eServer {
	t.Helper()
	cfg := &e2eConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	dataDir := newDataDir(t, "e2e-server-data")
	st := newTestStorage(t, dataDir)

	if cfg.upstreamURL == "" {
		upstream := httptest.NewServer(http.NotFoundHandler())
		t.Cleanup(upstream.Close)
		cfg.upstreamURL = upstream.URL
	}

	sharedMirror, dataPlane := newTestMirror(t, dataDir, cfg.upstreamURL, testS3Client != nil,
		mirror.WithRepositoriesFS(st.RepositoriesFS()),
	)

	var handler http.Handler
	handler = backendhf.NewHandler(
		backendhf.WithStorage(st),
		backendhf.WithMirror(sharedMirror),
		backendhf.WithNext(dataPlane),
	)
	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(st),
		backendlfs.WithNext(handler),
		backendlfs.WithMirror(sharedMirror),
	)
	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(st),
		backendhttp.WithNext(handler),
	)
	for _, w := range cfg.wraps {
		handler = w(handler)
	}

	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)

	s := &e2eServer{
		httpURL: httpServer.URL,
		storage: st,
		mirror:  sharedMirror,
	}
	if !cfg.ssh {
		return s
	}

	keyFile := filepath.Join(t.TempDir(), "id_e2e")
	pubKey := generateTestKeyFile(t, keyFile)

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate host key: %v", err)
	}
	hostKey, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("create host key signer: %v", err)
	}
	sshOpts := []backendssh.Option{
		backendssh.WithHostKey(hostKey),
		backendssh.WithStorage(st),
		backendssh.WithPublicKeyCallback(backendssh.AuthorizedKeysCallback([]ssh.PublicKey{pubKey})),
	}
	if cfg.sshLFSURL {
		sshOpts = append(sshOpts, backendssh.WithLFSURL(httpServer.URL))
	}
	sshServer := backendssh.NewServer(sshOpts...)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for SSH: %v", err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		_ = sshServer.Serve(t.Context(), listener)
	}()

	addr := listener.Addr().(*net.TCPAddr)
	s.sshURL = "ssh://git@" + addr.String() + "/"
	s.sshEnv = sshGitEnv(keyFile, strconv.Itoa(addr.Port))
	return s
}

func (s *e2eServer) createRepo(t *testing.T, org, name string) {
	t.Helper()
	body := fmt.Sprintf(`{"type":"model","name":%q,"organization":%q}`, name, org)
	resp, err := http.Post(s.httpURL+"/api/repos/create", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create repo status = %d, want 200", resp.StatusCode)
	}
}

func (s *e2eServer) httpRemote(repoID string) (string, []string) {
	return s.httpURL + "/" + repoID + ".git", []string{"GIT_TERMINAL_PROMPT=0"}
}

func (s *e2eServer) sshRemote(repoID string) (string, []string) {
	return s.sshURL + repoID + ".git", s.sshEnv
}

// gitCmd runs a git command, keeping stdout separate from stderr and
// returning the error, for call sites that parse output or assert failure.
// A watchdog kills wedged subprocesses so hangs fail fast.
func gitCmd(t *testing.T, dir string, env []string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = append(testEnv(), env...)
	cmd.WaitDelay = 10 * time.Second
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	err = cmd.Run()
	return outBuf.String(), errBuf.String(), err
}

// runGit runs a git command, failing the test on error with the full output
// and returning the combined stdout+stderr. A watchdog kills wedged
// subprocesses so hangs fail fast with their output.
func runGit(t *testing.T, dir string, env []string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = append(testEnv(), env...)
	cmd.WaitDelay = 10 * time.Second
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s failed (%v): %v\noutput: %s", strings.Join(args, " "), ctx.Err(), err, output)
	}
	return string(output)
}

// generateTestKeyFile generates an ED25519 key pair and writes the private key to path.
// Returns the SSH public key.
func generateTestKeyFile(t *testing.T, path string) ssh.PublicKey {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("Failed to generate client key: %v", err)
	}
	privKeyPEM, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatalf("Failed to marshal private key: %v", err)
	}
	if err := os.WriteFile(path, pem.EncodeToMemory(privKeyPEM), 0600); err != nil {
		t.Fatalf("Failed to write private key: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("Failed to create signer: %v", err)
	}
	return signer.PublicKey()
}

// sshGitEnv returns environment variables for git to use a specific SSH key and port.
func sshGitEnv(keyFile string, port string) []string {
	sshCmd := fmt.Sprintf("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s -p %s", keyFile, port)
	return []string{
		"GIT_TERMINAL_PROMPT=0",
		"GIT_SSH_COMMAND=" + sshCmd,
	}
}

// runSSHGitCmd runs a git command with the given environment in the specified directory.
func runSSHGitCmd(t *testing.T, dir string, env []string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir = dir
	cmd.Env = append(testEnv(), env...)
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("Git command failed: git %s\nError: %v\nOutput: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

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
	base := testEnv()
	env := make([]string, 0, len(base)+6)
	for _, kv := range base {
		if strings.HasPrefix(kv, "HF_HUB_DISABLE_XET=") {
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
