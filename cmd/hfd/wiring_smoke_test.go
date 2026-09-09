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
)

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
	sshServer, err := buildSSHServer(ctx, cfg, st, hooks, sharedMirror, auth)
	if err != nil {
		t.Fatalf("build SSH server: %v", err)
	}

	httpServer := httptest.NewUnstartedServer(handler)
	httpServer.Listener.Close()
	httpServer.Listener = httpListener
	httpServer.Start()
	t.Cleanup(httpServer.Close)
	sshListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for SSH: %v", err)
	}
	t.Cleanup(func() { sshListener.Close() })
	go func() {
		_ = sshServer.Serve(ctx, sshListener)
	}()

	client := &http.Client{Timeout: 2 * time.Minute}
	request := func(t *testing.T, method, url, body string) string {
		t.Helper()
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
	for _, name := range []string{"smoke-http", "smoke-ssh"} {
		request(t, http.MethodPost, cfg.HostURL+"/api/repos/create",
			fmt.Sprintf(`{"type":"model","name":%q,"organization":"wiring-org"}`, name))
	}

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
	runGit := func(t *testing.T, dir string, extraEnv []string, args ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = dir
		cmd.Env = append(append([]string(nil), env...), extraEnv...)
		cmd.WaitDelay = 10 * time.Second
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %s failed (%v): %v\noutput: %s", strings.Join(args, " "), ctx.Err(), err, output)
		}
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
				defer request(t, http.MethodGet, tc.remote+"/info/refs?service=git-upload-pack", "")
			}
			dir := filepath.Join(t.TempDir(), "clone")
			runGit(t, "", tc.env, "clone", tc.remote, dir)
			runGit(t, dir, tc.env, "config", "user.name", "Wiring Test")
			runGit(t, dir, tc.env, "config", "user.email", "wiring@example.com")
			runGit(t, dir, tc.env, "checkout", "-B", "main")
			content := "# Wiring smoke over " + tc.name + "\n"
			if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte(content), 0644); err != nil {
				t.Fatalf("write README: %v", err)
			}
			runGit(t, dir, tc.env, "add", "README.md")
			runGit(t, dir, tc.env, "commit", "-m", "Add README")
			runGit(t, dir, tc.env, "push", "origin", "main")
			url := cfg.HostURL + "/wiring-org/smoke-" + tc.name + "/resolve/main/README.md"
			if got := request(t, http.MethodGet, url, ""); got != content {
				t.Fatalf("README = %q, want %q", got, content)
			}
		})
	}
}
