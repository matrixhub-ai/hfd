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

	xetinternalapi "github.com/wzshiming/xet/server/internalapi"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendinternalapi "github.com/matrixhub-ai/hfd/pkg/backend/internalapi"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	backendssh "github.com/matrixhub-ai/hfd/pkg/backend/ssh"
	"github.com/matrixhub-ai/hfd/pkg/gc"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// e2eServer bundles an HTTP server (hf + LFS + git + xet data plane) and an
// optional SSH git server sharing the same storage and mirror.
type e2eServer struct {
	httpURL string
	sshURL  string
	sshEnv  []string
	storage *storage.Storage
}

type e2eConfig struct {
	ssh          bool
	sshLFSURL    bool
	internalAPI  bool
	wraps        []func(http.Handler) http.Handler
	authUser     string
	authPass     string
	preReceive   receive.PreReceiveHookFunc
	postReceive  receive.PostReceiveHookFunc
	permission   permission.PermissionHookFunc
	mirrorSource string
	refFilter    mirror.RefFilterFunc
}

type e2eOption func(*e2eConfig)

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

// withInternalAPI mounts the /internal/ management endpoints outside auth the way cmd/hfd's internalAPI does.
func withInternalAPI() e2eOption {
	return func(c *e2eConfig) { c.internalAPI = true }
}

// withWrap applies mw outermost, so it observes every request including CAS
// traffic.
func withWrap(mw func(http.Handler) http.Handler) e2eOption {
	return func(c *e2eConfig) { c.wraps = append(c.wraps, mw) }
}

// withAuth wires the authenticate layer over the HTTP chain the way cmd/hfd
// does: basic, static-token, and sign validators for the credentials. The
// layer only establishes identity — anonymous requests pass through and
// enforcement stays in permission hooks.
func withAuth(username, password string) e2eOption {
	return func(c *e2eConfig) { c.authUser = username; c.authPass = password }
}

// withHooks installs pre/post receive hooks on the git transports (HTTP and
// SSH); nil hooks stay unset. The hf API layer is left unhooked, matching
// the pre-harness per-protocol fixtures.
func withHooks(pre receive.PreReceiveHookFunc, post receive.PostReceiveHookFunc) e2eOption {
	return func(c *e2eConfig) { c.preReceive = pre; c.postReceive = post }
}

// withPermissionHook installs the permission hook on the git transports
// (HTTP and SSH), leaving the hf API layer open so fixtures can create repos.
func withPermissionHook(fn permission.PermissionHookFunc) e2eOption {
	return func(c *e2eConfig) { c.permission = fn }
}

// withMirrorSource turns the server into a pull-through mirror of the given
// upstream: opening an unknown repository fetches it on demand via the
// pre-open hook, on HTTP and SSH alike, and pushes to it are refused.
func withMirrorSource(upstreamURL string) e2eOption {
	return func(c *e2eConfig) { c.mirrorSource = upstreamURL }
}

// withRefFilter narrows which upstream refs withMirrorSource mirrors.
func withRefFilter(fn mirror.RefFilterFunc) e2eOption {
	return func(c *e2eConfig) { c.refFilter = fn }
}

// newMirrorSourceFunc maps every repository name onto the same upstream base
// URL, marking all of them as mirror sources.
func newMirrorSourceFunc(baseURL string) mirror.SourceFunc {
	baseURL = strings.TrimSuffix(baseURL, "/")
	return func(ctx context.Context, repoName string) (string, bool, error) {
		return baseURL + "/" + repoName, true, nil
	}
}

// newMirrorPreOpenHook pulls mirror-source repositories from their remote
// before they are opened, creating them locally on first access.
func newMirrorPreOpenHook(sharedMirror *mirror.Mirror) func(context.Context, string, bool) error {
	return func(ctx context.Context, repoName string, write bool) error {
		if sharedMirror == nil {
			return nil
		}

		isMirror, err := sharedMirror.IsMirrorSource(ctx, repoName)
		if err != nil {
			return err
		}
		if !isMirror {
			return nil
		}

		repoPath := repository.ResolvePath(repoName)
		if repoPath == "" {
			return fmt.Errorf("repository path not found for %s", repoName)
		}
		return sharedMirror.PullFromRemote(ctx, repoPath, repoName, nil)
	}
}

// newE2EServer wires the handler chain in cmd/hfd's order (http → lfs → hf →
// xet CAS data plane), with one known deviation from the production wiring:
// the mirror reaches the git transports only under withMirrorSource, because
// injecting it unconditionally — as wire.go does — would make
// checkMirrorAccess refuse every non-mirror repository. The xet engine
// ingests from the mirror source when one is set — like cmd/hfd does with
// --pull-mirror-url — so mirrored LFS resolves stream through the engine
// instead of racing the batch-API fallback; plain servers only serve local
// content, so their engine points at an always-404 server that fully
// ingested objects never contact. During the S3 pass the xet storage lives
// in the fake S3 bucket, like production.
func newE2EServer(t *testing.T, opts ...e2eOption) *e2eServer {
	t.Helper()
	cfg := &e2eConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	dataDir := newDataDir(t, "e2e-server-data")
	st := newTestStorage(t, dataDir)

	engineUpstream := cfg.mirrorSource
	if engineUpstream == "" {
		notFound := httptest.NewServer(http.NotFoundHandler())
		t.Cleanup(notFound.Close)
		engineUpstream = notFound.URL
	}

	mirrorOpts := []mirror.Option{mirror.WithRepositoriesFS(st.RepositoriesFS())}
	if cfg.mirrorSource != "" {
		mirrorOpts = append(mirrorOpts, mirror.WithMirrorSourceFunc(newMirrorSourceFunc(cfg.mirrorSource)))
	}
	if cfg.refFilter != nil {
		mirrorOpts = append(mirrorOpts, mirror.WithMirrorRefFilterFunc(cfg.refFilter))
	}
	sharedMirror, xet := newTestMirror(t, dataDir, engineUpstream, testS3Client != nil, mirrorOpts...)

	// The git transports get the mirror (and its access rules) only in
	// pull-through mode: with a mirror set they refuse to serve non-mirror
	// repositories.
	var preOpen func(context.Context, string, bool) error
	if cfg.mirrorSource != "" {
		preOpen = newMirrorPreOpenHook(sharedMirror)
	}

	hfOpts := []backendhf.Option{
		backendhf.WithStorage(st),
		backendhf.WithMirror(sharedMirror),
		backendhf.WithNext(xet.tail),
	}
	if preOpen != nil {
		hfOpts = append(hfOpts, backendhf.WithPreOpenHookFunc(preOpen))
	}
	var handler http.Handler
	handler = backendhf.NewHandler(hfOpts...)
	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(st),
		backendlfs.WithNext(handler),
		backendlfs.WithMirror(sharedMirror),
	)
	httpOpts := []backendhttp.Option{
		backendhttp.WithStorage(st),
		backendhttp.WithNext(handler),
	}
	if preOpen != nil {
		httpOpts = append(httpOpts,
			backendhttp.WithMirror(sharedMirror),
			backendhttp.WithPreOpenHookFunc(preOpen))
	}
	if cfg.preReceive != nil {
		httpOpts = append(httpOpts, backendhttp.WithPreReceiveHookFunc(cfg.preReceive))
	}
	if cfg.postReceive != nil {
		httpOpts = append(httpOpts, backendhttp.WithPostReceiveHookFunc(cfg.postReceive))
	}
	if cfg.permission != nil {
		httpOpts = append(httpOpts, backendhttp.WithPermissionHookFunc(cfg.permission))
	}
	handler = backendhttp.NewHandler(httpOpts...)
	if cfg.authUser != "" || cfg.authPass != "" {
		handler = authenticate.NewHandler(
			authenticate.WithNext(handler),
			authenticate.WithBasicAuthValidator(authenticate.NewSimpleBasicAuthValidator(cfg.authUser, cfg.authPass)),
			authenticate.WithTokenValidator(authenticate.NewSimpleTokenValidator(cfg.authUser, cfg.authPass)),
			authenticate.WithTokenSignValidator(authenticate.NewTokenSignValidator([]byte(cfg.authPass))),
		)
	}
	if cfg.internalAPI {
		gcs, ok := xet.xs.(xetstorage.GCStore)
		if !ok {
			t.Fatalf("xet storage %T does not implement GCStore", xet.xs)
		}
		handler = backendinternalapi.NewHandler(
			backendinternalapi.WithCollector(gc.NewCollector(st.RepositoriesFS(), gcs)),
			backendinternalapi.WithGCGrace(time.Hour),
			backendinternalapi.WithNext(xetinternalapi.NewHandler(
				xetinternalapi.WithStorage(xet.xs),
				xetinternalapi.WithNext(handler),
			)),
		)
	}
	for _, w := range cfg.wraps {
		handler = w(handler)
	}

	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)

	s := &e2eServer{
		httpURL: httpServer.URL,
		storage: st,
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
	if preOpen != nil {
		sshOpts = append(sshOpts,
			backendssh.WithMirror(sharedMirror),
			backendssh.WithPreOpenHookFunc(preOpen))
	}
	if cfg.preReceive != nil {
		sshOpts = append(sshOpts, backendssh.WithPreReceiveHookFunc(cfg.preReceive))
	}
	if cfg.postReceive != nil {
		sshOpts = append(sshOpts, backendssh.WithPostReceiveHookFunc(cfg.postReceive))
	}
	if cfg.permission != nil {
		sshOpts = append(sshOpts, backendssh.WithPermissionHookFunc(cfg.permission))
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

func (s *e2eServer) deleteRepo(t *testing.T, org, name string) {
	t.Helper()
	body := fmt.Sprintf(`{"type":"model","name":%q,"organization":%q}`, name, org)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, s.httpURL+"/api/repos/delete", strings.NewReader(body))
	if err != nil {
		t.Fatalf("build delete repo request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete repo status = %d, want 200", resp.StatusCode)
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
	if _, err := exec.LookPath("python3"); err != nil {
		missing("python3 not found; install python3 with huggingface_hub")
	}
	if pyOut, err := exec.CommandContext(t.Context(), "python3", "-c", "import huggingface_hub").CombinedOutput(); err != nil {
		missing("python3 cannot import huggingface_hub: %v\n%s", err, pyOut)
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

// runHFCmd runs the hf CLI against endpoint with xet disabled: runHFCmdXet's
// non-xet form under the pre-matrix helper name.
func runHFCmd(t *testing.T, endpoint string, args ...string) string {
	t.Helper()
	return runHFCmdXet(t, endpoint, false, args...)
}

// runPyScript runs a Python3 script with HF_ENDPOINT and HF_TOKEN set and
// xet disabled: runPyXet's non-xet form under the pre-matrix helper name.
func runPyScript(t *testing.T, endpoint, script string) {
	t.Helper()
	runPyXet(t, endpoint, false, script)
}

// checkPythonHFHub skips locally when python3 or huggingface_hub are
// missing; on CI (CI env set) it hard-fails so the python rows stay
// enforced there, like requireUpDownMatrixTools.
func checkPythonHFHub(t *testing.T) {
	t.Helper()
	missing := func(format string, args ...any) {
		t.Helper()
		if os.Getenv("CI") != "" {
			t.Fatalf(format, args...)
		}
		t.Skipf(format, args...)
	}
	if _, err := exec.LookPath("python3"); err != nil {
		missing("python3 not available; install python3 with huggingface_hub")
	}
	cmd := exec.CommandContext(t.Context(), "python3", "-c", "import huggingface_hub")
	if err := cmd.Run(); err != nil {
		missing("huggingface_hub not installed; pip install huggingface_hub")
	}
}

// runPyXet runs a Python3 snippet driving huggingface_hub against endpoint
// with xet-core (hf_xet) enabled or disabled, mirroring runHFCmdXet. HF_HOME
// is isolated per call so nothing is served from cache. A watchdog kills
// hung invocations so a stuck client fails fast with output.
func runPyXet(t *testing.T, endpoint string, xet bool, script string) string {
	t.Helper()
	base := testEnv()
	env := make([]string, 0, len(base)+5)
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
	)
	if !xet {
		env = append(env, "HF_HUB_DISABLE_XET=1")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", "-c", script)
	cmd.Env = env
	cmd.WaitDelay = 10 * time.Second
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("python script failed (%v): %v\nScript:\n%s\nOutput: %s", ctx.Err(), err, script, output)
	}
	return string(output)
}
