package e2e_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wzshiming/hfd/internal/utils"
	backendhttp "github.com/wzshiming/hfd/pkg/backend/http"
	backendhuggingface "github.com/wzshiming/hfd/pkg/backend/huggingface"
	backendlfs "github.com/wzshiming/hfd/pkg/backend/lfs"
	backendssh "github.com/wzshiming/hfd/pkg/backend/ssh"
	"github.com/wzshiming/hfd/pkg/lfs"
	"github.com/wzshiming/hfd/pkg/mirror"
	"github.com/wzshiming/hfd/pkg/repository"
	"github.com/wzshiming/hfd/pkg/storage"
	"golang.org/x/crypto/ssh"
)

func newMirrorSourceFunc(baseURL string) repository.MirrorSourceFunc {
	baseURL = strings.TrimSuffix(baseURL, "/")
	return func(ctx context.Context, repoName string) (string, bool, error) {
		return baseURL + "/" + repoName, true, nil
	}
}

// setupProxyServer creates a proxy HTTP server that mirrors repositories from
// upstreamURL on demand.
func setupProxyServer(t *testing.T, upstreamURL string) (*httptest.Server, string) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "proxy-e2e-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	store := storage.NewStorage(storage.WithRootDir(dataDir))
	lfsStore := lfs.NewLocal(store.LFSDir())

	sharedMirror := mirror.NewMirror(
		mirror.WithMirrorSourceFunc(newMirrorSourceFunc(upstreamURL)),
	)

	var handler http.Handler

	handler = backendhuggingface.NewHandler(
		backendhuggingface.WithStorage(store),
		backendhuggingface.WithLFSStore(lfsStore),
		backendhuggingface.WithMirror(sharedMirror),
	)

	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(store),
		backendlfs.WithNext(handler),
		backendlfs.WithLFSStore(lfsStore),
	)

	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(store),
		backendhttp.WithNext(handler),
		backendhttp.WithMirror(sharedMirror),
	)

	server := httptest.NewServer(handler)
	t.Cleanup(func() { server.Close() })

	return server, dataDir
}

// setupSSHProxyServer creates a proxy SSH server that mirrors repositories from
// upstreamURL on demand. Returns the server, its listener, and the data directory.
func setupSSHProxyServer(t *testing.T, upstreamURL string) (net.Listener, string) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "ssh-proxy-e2e-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	store := storage.NewStorage(storage.WithRootDir(dataDir))

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("Failed to generate host key: %v", err)
	}
	hostKey, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("Failed to create host key signer: %v", err)
	}

	sharedMirror := mirror.NewMirror(
		mirror.WithMirrorSourceFunc(newMirrorSourceFunc(upstreamURL)),
	)

	sshServer := backendssh.NewServer(
		store.RepositoriesDir(),
		hostKey,
		backendssh.WithMirror(sharedMirror),
	)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for SSH: %v", err)
	}
	t.Cleanup(func() { listener.Close() })

	go func() {
		_ = sshServer.Serve(t.Context(), listener)
	}()

	return listener, dataDir
}

// runGitCmdE2E runs a git command in the given directory, using the provided env.
func runGitCmdE2E(t *testing.T, dir string, env []string, args ...string) {
	t.Helper()
	cmd := utils.Command(t.Context(), "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = append(os.Environ(), env...)
	if output, err := cmd.Output(); err != nil {
		t.Fatalf("Git command failed: git %s\nError: %v\nOutput: %s", strings.Join(args, " "), err, output)
	}
}

// TestHTTPProxyMirror verifies that cloning from a proxy server transparently
// mirrors from the upstream and serves the content to the client.
func TestHTTPProxyMirror(t *testing.T) {
	// Set up upstream server and push content to it.
	upstream, _ := setupTestServer(t)

	const org = "proxy-org"
	const name = "proxy-repo"

	// Create the repository on the upstream via the HuggingFace API.
	resp, err := http.Post(upstream.URL+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"`+name+`","organization":"`+org+`"}`))
	if err != nil {
		t.Fatalf("Failed to create upstream repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 creating upstream repo, got %d", resp.StatusCode)
	}

	clientDir, err := os.MkdirTemp("", "proxy-e2e-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	env := []string{"GIT_TERMINAL_PROMPT=0"}

	// Clone empty repo from upstream and push a commit.
	upstreamGitURL := upstream.URL + "/" + org + "/" + name + ".git"
	upstreamCloneDir := filepath.Join(clientDir, "upstream-clone")
	runGitCmdE2E(t, "", env, "clone", upstreamGitURL, upstreamCloneDir)
	runGitCmdE2E(t, upstreamCloneDir, env, "config", "user.email", "test@test.com")
	runGitCmdE2E(t, upstreamCloneDir, env, "config", "user.name", "Test User")

	if err := os.WriteFile(filepath.Join(upstreamCloneDir, "README.md"), []byte("# Proxy Test\n"), 0644); err != nil {
		t.Fatalf("Failed to write README.md: %v", err)
	}
	runGitCmdE2E(t, upstreamCloneDir, env, "add", "README.md")
	runGitCmdE2E(t, upstreamCloneDir, env, "commit", "-m", "Initial commit")
	runGitCmdE2E(t, upstreamCloneDir, env, "push", "origin", "main")

	// Set up proxy server pointing at the upstream.
	proxy, _ := setupProxyServer(t, upstream.URL)
	proxyGitURL := proxy.URL + "/" + org + "/" + name + ".git"

	t.Run("CloneFromProxy", func(t *testing.T) {
		// The repo does not exist on the proxy; it should be auto-mirrored from upstream.
		cloneDir := filepath.Join(clientDir, "proxy-clone")
		runGitCmdE2E(t, "", env, "clone", proxyGitURL, cloneDir)

		content, err := os.ReadFile(filepath.Join(cloneDir, "README.md"))
		if err != nil {
			t.Fatalf("Failed to read README.md from proxy clone: %v", err)
		}
		if string(content) != "# Proxy Test\n" {
			t.Errorf("Unexpected content from proxy clone: %q", content)
		}
	})

	t.Run("CloneFromProxyCached", func(t *testing.T) {
		// The mirrored repo now exists on the proxy; second clone should use the cache.
		cloneDir := filepath.Join(clientDir, "proxy-clone-cached")
		runGitCmdE2E(t, "", env, "clone", proxyGitURL, cloneDir)

		content, err := os.ReadFile(filepath.Join(cloneDir, "README.md"))
		if err != nil {
			t.Fatalf("Failed to read README.md from cached proxy clone: %v", err)
		}
		if string(content) != "# Proxy Test\n" {
			t.Errorf("Unexpected content from cached proxy clone: %q", content)
		}
	})

	t.Run("NonExistentRepoReturns404", func(t *testing.T) {
		r, err := http.Get(proxy.URL + "/nobody/doesnotexist.git/info/refs?service=git-upload-pack")
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		r.Body.Close()
		if r.StatusCode != http.StatusNotFound {
			t.Errorf("Expected 404, got %d", r.StatusCode)
		}
	})

	t.Run("PushToMirrorForbidden", func(t *testing.T) {
		// Mirrors are read-only; pushing should be rejected.
		r, err := http.Get(proxy.URL + "/" + org + "/" + name + ".git/info/refs?service=git-receive-pack")
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		r.Body.Close()
		if r.StatusCode == http.StatusOK {
			t.Errorf("Expected push to mirror to be forbidden, got 200")
		}
	})
}

// TestSSHProxyMirror verifies that cloning a repository via the SSH proxy server
// transparently mirrors it from the upstream HTTP server.
func TestSSHProxyMirror(t *testing.T) {
	// Set up upstream server and push content to it.
	upstream, _ := setupTestServer(t)

	const org = "ssh-proxy-org"
	const name = "ssh-proxy-repo"

	// Create the repository on the upstream via the HuggingFace API.
	resp, err := http.Post(upstream.URL+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"`+name+`","organization":"`+org+`"}`))
	if err != nil {
		t.Fatalf("Failed to create upstream repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 creating upstream repo, got %d", resp.StatusCode)
	}

	clientDir, err := os.MkdirTemp("", "ssh-proxy-e2e-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	httpEnv := []string{"GIT_TERMINAL_PROMPT=0"}

	// Push content to the upstream via HTTP.
	upstreamGitURL := upstream.URL + "/" + org + "/" + name + ".git"
	upstreamCloneDir := filepath.Join(clientDir, "upstream-clone")
	runGitCmdE2E(t, "", httpEnv, "clone", upstreamGitURL, upstreamCloneDir)
	runGitCmdE2E(t, upstreamCloneDir, httpEnv, "config", "user.email", "test@test.com")
	runGitCmdE2E(t, upstreamCloneDir, httpEnv, "config", "user.name", "Test User")

	if err := os.WriteFile(filepath.Join(upstreamCloneDir, "README.md"), []byte("# SSH Proxy Test\n"), 0644); err != nil {
		t.Fatalf("Failed to write README.md: %v", err)
	}
	runGitCmdE2E(t, upstreamCloneDir, httpEnv, "add", "README.md")
	runGitCmdE2E(t, upstreamCloneDir, httpEnv, "commit", "-m", "Initial commit")
	runGitCmdE2E(t, upstreamCloneDir, httpEnv, "push", "origin", "main")

	// Set up SSH proxy server pointing at the upstream HTTP server.
	sshListener, _ := setupSSHProxyServer(t, upstream.URL)
	addr := sshListener.Addr().(*net.TCPAddr)
	port := fmt.Sprintf("%d", addr.Port)
	sshURL := "ssh://git@" + addr.String() + "/"

	// Generate a client key (no auth configured on server — allow all).
	keyFile := filepath.Join(clientDir, "id_ed25519")
	_ = generateTestKeyFile(t, keyFile)
	sshEnv := sshGitEnv(keyFile, port)

	t.Run("CloneFromSSHProxy", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "ssh-proxy-clone")
		runSSHGitCmd(t, "", sshEnv, "clone", sshURL+org+"/"+name+".git", cloneDir)

		content, err := os.ReadFile(filepath.Join(cloneDir, "README.md"))
		if err != nil {
			t.Fatalf("Failed to read README.md from SSH proxy clone: %v", err)
		}
		if string(content) != "# SSH Proxy Test\n" {
			t.Errorf("Unexpected content from SSH proxy clone: %q", content)
		}
	})

	t.Run("CloneFromSSHProxyCached", func(t *testing.T) {
		// Second clone uses the cached mirror.
		cloneDir := filepath.Join(clientDir, "ssh-proxy-clone-cached")
		runSSHGitCmd(t, "", sshEnv, "clone", sshURL+org+"/"+name+".git", cloneDir)

		content, err := os.ReadFile(filepath.Join(cloneDir, "README.md"))
		if err != nil {
			t.Fatalf("Failed to read README.md from cached SSH proxy clone: %v", err)
		}
		if string(content) != "# SSH Proxy Test\n" {
			t.Errorf("Unexpected content from cached SSH proxy clone: %q", content)
		}
	})
}

// setupProxyServerWithRefFilter creates a proxy HTTP server that mirrors repositories
// from upstreamURL on demand, applying the given ref filter.
func setupProxyServerWithRefFilter(t *testing.T, upstreamURL string, refFilter repository.MirrorRefFilterFunc) (*httptest.Server, string) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "proxy-ref-filter-e2e-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	store := storage.NewStorage(storage.WithRootDir(dataDir))
	lfsStore := lfs.NewLocal(store.LFSDir())

	sharedMirror := mirror.NewMirror(
		mirror.WithMirrorSourceFunc(newMirrorSourceFunc(upstreamURL)),
		mirror.WithMirrorRefFilterFunc(refFilter),
	)

	var handler http.Handler

	handler = backendhuggingface.NewHandler(
		backendhuggingface.WithStorage(store),
		backendhuggingface.WithLFSStore(lfsStore),
		backendhuggingface.WithMirror(sharedMirror),
	)

	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(store),
		backendlfs.WithNext(handler),
		backendlfs.WithLFSStore(lfsStore),
	)

	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(store),
		backendhttp.WithNext(handler),
		backendhttp.WithMirror(sharedMirror),
	)

	server := httptest.NewServer(handler)
	t.Cleanup(func() { server.Close() })

	return server, dataDir
}

// TestHTTPProxyMirrorRefFilter verifies that cloning from a proxy server with
// ref filtering only mirrors the allowed refs from the upstream.
func TestHTTPProxyMirrorRefFilter(t *testing.T) {
	upstream, _ := setupTestServer(t)

	const org = "ref-filter-org"
	const name = "ref-filter-repo"

	// Create the repository on the upstream.
	resp, err := http.Post(upstream.URL+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"`+name+`","organization":"`+org+`"}`))
	if err != nil {
		t.Fatalf("Failed to create upstream repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 creating upstream repo, got %d", resp.StatusCode)
	}

	clientDir, err := os.MkdirTemp("", "ref-filter-e2e-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	env := []string{"GIT_TERMINAL_PROMPT=0"}

	// Clone empty repo from upstream and push a commit with a branch and tag.
	upstreamGitURL := upstream.URL + "/" + org + "/" + name + ".git"
	upstreamCloneDir := filepath.Join(clientDir, "upstream-clone")
	runGitCmdE2E(t, "", env, "clone", upstreamGitURL, upstreamCloneDir)
	runGitCmdE2E(t, upstreamCloneDir, env, "config", "user.email", "test@test.com")
	runGitCmdE2E(t, upstreamCloneDir, env, "config", "user.name", "Test User")

	if err := os.WriteFile(filepath.Join(upstreamCloneDir, "README.md"), []byte("# Ref Filter Test\n"), 0644); err != nil {
		t.Fatalf("Failed to write README.md: %v", err)
	}
	runGitCmdE2E(t, upstreamCloneDir, env, "add", "README.md")
	runGitCmdE2E(t, upstreamCloneDir, env, "commit", "-m", "Initial commit")
	runGitCmdE2E(t, upstreamCloneDir, env, "push", "origin", "main")

	// Create a feature branch and push it.
	runGitCmdE2E(t, upstreamCloneDir, env, "checkout", "-b", "feature")
	if err := os.WriteFile(filepath.Join(upstreamCloneDir, "feature.txt"), []byte("feature\n"), 0644); err != nil {
		t.Fatalf("Failed to write feature.txt: %v", err)
	}
	runGitCmdE2E(t, upstreamCloneDir, env, "add", "feature.txt")
	runGitCmdE2E(t, upstreamCloneDir, env, "commit", "-m", "Feature commit")
	runGitCmdE2E(t, upstreamCloneDir, env, "push", "origin", "feature")

	// Create a tag on main and push it.
	runGitCmdE2E(t, upstreamCloneDir, env, "checkout", "main")
	runGitCmdE2E(t, upstreamCloneDir, env, "tag", "v1.0")
	runGitCmdE2E(t, upstreamCloneDir, env, "push", "origin", "v1.0")

	// Set up proxy with ref filter that only allows refs/heads/main.
	onlyMainFilter := func(_ context.Context, _ string, refs []string) ([]string, error) {
		var filtered []string
		for _, ref := range refs {
			if ref == "refs/heads/main" {
				filtered = append(filtered, ref)
			}
		}
		return filtered, nil
	}

	proxy, proxyDataDir := setupProxyServerWithRefFilter(t, upstream.URL, onlyMainFilter)
	proxyGitURL := proxy.URL + "/" + org + "/" + name + ".git"

	t.Run("CloneFromFilteredProxy", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "filtered-proxy-clone")
		runGitCmdE2E(t, "", env, "clone", proxyGitURL, cloneDir)

		content, err := os.ReadFile(filepath.Join(cloneDir, "README.md"))
		if err != nil {
			t.Fatalf("Failed to read README.md from proxy clone: %v", err)
		}
		if string(content) != "# Ref Filter Test\n" {
			t.Errorf("Unexpected content from proxy clone: %q", content)
		}
	})

	t.Run("FilteredBranchNotMirrored", func(t *testing.T) {
		// The "feature" branch should not exist in the mirror.
		repoPath := filepath.Join(proxyDataDir, "repositories", org, name+".git")
		repo, err := repository.Open(repoPath)
		if err != nil {
			t.Fatalf("Failed to open proxy mirror repo: %v", err)
		}
		branches, err := repo.Branches()
		if err != nil {
			t.Fatalf("Failed to list branches: %v", err)
		}
		for _, b := range branches {
			if b == "feature" {
				t.Error("feature branch should not be mirrored, but found it")
			}
		}
	})

	t.Run("FilteredTagNotMirrored", func(t *testing.T) {
		// The "v1.0" tag should not exist in the mirror.
		repoPath := filepath.Join(proxyDataDir, "repositories", org, name+".git")
		repo, err := repository.Open(repoPath)
		if err != nil {
			t.Fatalf("Failed to open proxy mirror repo: %v", err)
		}
		tags, err := repo.Tags()
		if err != nil {
			t.Fatalf("Failed to list tags: %v", err)
		}
		for _, tag := range tags {
			if tag == "v1.0" {
				t.Error("v1.0 tag should not be mirrored, but found it")
			}
		}
	})
}
