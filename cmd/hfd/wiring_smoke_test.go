package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// TestServerWiringSmoke exercises git and resolve through the real HTTP and SSH builders.
func TestServerWiringSmoke(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}
	ctx := t.Context()
	cfg := defaultConfig()
	cfg.DataDir = t.TempDir()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	authorizedKey, err := ssh.NewPublicKey(publicKey)
	if err != nil {
		t.Fatalf("marshal public key: %v", err)
	}
	cfg.SSHAuthorizedKey = filepath.Join(cfg.DataDir, "authorized_keys")
	if err := os.WriteFile(cfg.SSHAuthorizedKey, ssh.MarshalAuthorizedKey(authorizedKey), 0600); err != nil {
		t.Fatalf("write authorized keys: %v", err)
	}
	privateKeyPEM, err := ssh.MarshalPrivateKey(privateKey, "")
	if err != nil {
		t.Fatalf("marshal private key: %v", err)
	}
	keyFile := filepath.Join(cfg.DataDir, "id_smoke")
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(privateKeyPEM), 0600); err != nil {
		t.Fatalf("write private key: %v", err)
	}

	_, hooks, auth := newWiringServer(t, cfg)
	sshServer, err := buildSSHServer(ctx, cfg, hooks.storage, hooks, hooks.mirror, auth)
	if err != nil {
		t.Fatalf("build SSH server: %v", err)
	}
	sshListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for SSH: %v", err)
	}
	t.Cleanup(func() { sshListener.Close() })
	go func() {
		_ = sshServer.Serve(ctx, sshListener)
	}()

	for _, name := range []string{"smoke-http", "smoke-ssh"} {
		wiringRequest(t, http.MethodPost, cfg.HostURL+"/api/repos/create",
			fmt.Sprintf(`{"type":"model","name":%q,"organization":"wiring-org"}`, name))
	}
	sshAddr := sshListener.Addr().(*net.TCPAddr)
	cases := []struct {
		name   string
		remote string
		env    []string
	}{
		{"http", cfg.HostURL + "/wiring-org/smoke-http.git", nil},
		{"ssh", "ssh://git@" + sshAddr.String() + "/wiring-org/smoke-ssh.git", []string{
			fmt.Sprintf("GIT_SSH_COMMAND=ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %q -p %d", keyFile, sshAddr.Port),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "http" {
				// Capture the raw response even when clone fails first.
				defer wiringRequest(t, http.MethodGet, tc.remote+"/info/refs?service=git-upload-pack", "")
			}
			dir := filepath.Join(t.TempDir(), "clone")
			wiringGit(t, "", tc.env, "clone", tc.remote, dir)
			wiringGit(t, dir, tc.env, "config", "user.name", "Wiring Test")
			wiringGit(t, dir, tc.env, "config", "user.email", "wiring@example.com")
			wiringGit(t, dir, tc.env, "checkout", "-B", "main")
			content := "# Wiring smoke over " + tc.name + "\n"
			if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte(content), 0644); err != nil {
				t.Fatalf("write README: %v", err)
			}
			wiringGit(t, dir, tc.env, "add", "README.md")
			wiringGit(t, dir, tc.env, "commit", "-m", "Add README")
			wiringGit(t, dir, tc.env, "push", "origin", "main")
			url := cfg.HostURL + "/wiring-org/smoke-" + tc.name + "/resolve/main/README.md"
			if got := wiringRequest(t, http.MethodGet, url, ""); got != content {
				t.Fatalf("README = %q, want %q", got, content)
			}
		})
	}
}

func newWiringServer(t *testing.T, cfg *config) (*httptest.Server, *serverHooks, *authenticate.Authenticators) {
	t.Helper()
	ctx := t.Context()
	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for HTTP: %v", err)
	}
	t.Cleanup(func() { httpListener.Close() })
	cfg.HostURL = "http://" + httpListener.Addr().String()

	st, err := buildStorage(ctx, cfg)
	if err != nil {
		t.Fatalf("build storage: %v", err)
	}
	xs, err := buildXETStorage(ctx, cfg)
	if err != nil {
		t.Fatalf("build xet storage: %v", err)
	}
	hooks := &serverHooks{
		storage:    st,
		proxyToken: cfg.ProxyToken,
		permission: permission.Logged(permission.AllowAll()),
		pullTTL:    cfg.ProxyCacheTTL,
	}
	auth, err := buildAuthenticators(ctx, cfg)
	if err != nil {
		t.Fatalf("build authenticators: %v", err)
	}
	mint, authFn, err := authenticate.NewXETTokenScheme(auth.TokenSign)
	if err != nil {
		t.Fatalf("build xet token scheme: %v", err)
	}
	xetC, err := buildXETClient(cfg)
	if err != nil {
		t.Fatalf("build xet client: %v", err)
	}
	engine, err := buildXETMirror(cfg, xs, xetC)
	if err != nil {
		t.Fatalf("build xet mirror: %v", err)
	}
	sharedMirror, err := buildMirror(ctx, cfg, st, xs, hooks, xetC, engine, mint)
	if err != nil {
		t.Fatalf("build mirror: %v", err)
	}
	hooks.mirror = sharedMirror
	t.Cleanup(sharedMirror.Wait)
	handler := buildHTTPHandler(ctx, cfg, st, xs, hooks, sharedMirror, auth, authFn)

	httpServer := httptest.NewUnstartedServer(handler)
	httpServer.Listener.Close()
	httpServer.Listener = httpListener
	httpServer.Start()
	t.Cleanup(httpServer.Close)
	return httpServer, hooks, auth
}

func wiringRequest(t *testing.T, method, url, body string) string {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Minute}
	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build %s %s: %v", method, url, err)
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s %s: %v\nbody: %s", method, url, err, data)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s %s: status = %d, want 200\nbody: %s", method, url, resp.StatusCode, data)
	}
	return string(data)
}

func wiringGitCmd(t *testing.T, dir string, extraEnv []string, args ...string) (string, error) {
	t.Helper()
	var env []string
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		switch strings.ToUpper(name) {
		case "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "GIT_SSH_COMMAND", "GIT_TERMINAL_PROMPT":
			continue
		}
		env = append(env, entry)
	}
	env = append(env, "GIT_TERMINAL_PROMPT=0", "NO_PROXY=*")
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = append(append([]string(nil), env...), extraEnv...)
	cmd.WaitDelay = 10 * time.Second
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func wiringGit(t *testing.T, dir string, env []string, args ...string) string {
	t.Helper()
	output, err := wiringGitCmd(t, dir, env, args...)
	if err != nil {
		t.Fatalf("git %s failed: %v\noutput: %s", strings.Join(args, " "), err, output)
	}
	return output
}

// TestServerWiringPullMirror checks read-only access, TTL refresh, and offline
// cache serving through the real HTTP builders without sleeping.
func TestServerWiringPullMirror(t *testing.T) {
	const repoName = "wiring-org/pull-mirror"
	upstreamConfig := defaultConfig()
	upstreamConfig.DataDir = t.TempDir()
	upstream, upstreamHooks, _ := newWiringServer(t, upstreamConfig)
	wiringRequest(t, http.MethodPost, upstream.URL+"/api/repos/create",
		`{"type":"model","name":"pull-mirror","organization":"wiring-org"}`)
	upstreamDir := filepath.Join(t.TempDir(), "upstream")
	wiringGit(t, "", nil, "clone", upstream.URL+"/"+repoName+".git", upstreamDir)
	wiringGit(t, upstreamDir, nil, "config", "user.name", "Wiring Test")
	wiringGit(t, upstreamDir, nil, "config", "user.email", "wiring@example.com")
	const content = "# Pull mirror wiring\n"
	if err := os.WriteFile(filepath.Join(upstreamDir, "README.md"), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	wiringGit(t, upstreamDir, nil, "add", "README.md")
	wiringGit(t, upstreamDir, nil, "commit", "-m", "Seed upstream")
	wiringGit(t, upstreamDir, nil, "push", "origin", "HEAD:main")
	oldHash := strings.TrimSpace(wiringGit(t, upstreamDir, nil, "rev-parse", "HEAD"))
	cfg := defaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.PullMirrorURL = upstream.URL
	cfg.ProxyCacheTTL = time.Hour
	proxy, hooks, _ := newWiringServer(t, cfg)
	remote := proxy.URL + "/" + repoName + ".git"
	clone := func(wantHash string) string {
		t.Helper()
		dir := filepath.Join(t.TempDir(), "proxy")
		wiringGit(t, "", nil, "clone", remote, dir)
		if got := strings.TrimSpace(wiringGit(t, dir, nil, "rev-parse", "HEAD")); got != wantHash {
			t.Fatalf("proxy HEAD = %s, want %s", got, wantHash)
		}
		if got, err := os.ReadFile(filepath.Join(dir, "README.md")); err != nil || string(got) != content {
			t.Fatalf("proxy README = %q, want %q; error: %v", got, content, err)
		}
		return dir
	}
	dir := clone(oldHash)
	wiringGit(t, dir, nil, "config", "user.name", "Wiring Test")
	wiringGit(t, dir, nil, "config", "user.email", "wiring@example.com")
	wiringGit(t, dir, nil, "commit", "--allow-empty", "-m", "Attempt proxy write")
	if output, err := wiringGitCmd(t, dir, nil, "push", "origin", "HEAD:main"); err == nil {
		t.Fatalf("pull-only proxy accepted push; output: %s", output)
	}
	repoPath := repository.ResolvePath(repoName)
	for name, server := range map[string]*serverHooks{"upstream": upstreamHooks, "proxy": hooks} {
		if got := mainRef(t, server.storage.RepositoriesFS(), repoPath); got != oldHash {
			t.Fatalf("%s main changed after refused push: got %s, want %s", name, got, oldHash)
		}
	}
	wiringGit(t, upstreamDir, nil, "commit", "--allow-empty", "-m", "Advance upstream")
	wiringGit(t, upstreamDir, nil, "push", "origin", "HEAD:main")
	newHash := strings.TrimSpace(wiringGit(t, upstreamDir, nil, "rev-parse", "HEAD"))
	clone(oldHash)
	hooks.lastPull.Store(repoPath, time.Now().Add(-2*time.Hour))
	clone(newHash)
	upstream.Close()
	clone(newHash)
}
