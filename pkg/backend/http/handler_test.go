package backend_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/wzshiming/hfd/internal/utils"
	backendhttp "github.com/wzshiming/hfd/pkg/backend/http"
	"github.com/wzshiming/hfd/pkg/permission"
	"github.com/wzshiming/hfd/pkg/receive"
	"github.com/wzshiming/hfd/pkg/repository"
	"github.com/wzshiming/hfd/pkg/storage"
)

// runGitCmd runs a git command in the specified directory.
func runGitCmd(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := utils.Command(t.Context(), "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("Git command failed: git %s\nError: %v\nOutput: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

func TestHTTPHandler(t *testing.T) {
	// Create a temporary directory for the upstream server
	upstreamDir, err := os.MkdirTemp("", "http-test-upstream")
	if err != nil {
		t.Fatalf("Failed to create temp upstream dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(upstreamDir)
	}()

	// Create a temporary directory for client operations
	clientDir, err := os.MkdirTemp("", "http-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(clientDir)
	}()

	upstreamStorage := storage.NewStorage(storage.WithRootDir(upstreamDir))

	// Create a bare repository on the upstream
	repoName := "test-repo"
	repoPath := filepath.Join(upstreamStorage.RepositoriesDir(), repoName+".git")
	if err := os.MkdirAll(filepath.Dir(repoPath), 0755); err != nil {
		t.Fatalf("Failed to create repos dir: %v", err)
	}
	runGitCmd(t, "", "init", "--bare", repoPath)

	// Set up upstream HTTP handler
	upstreamHandler := backendhttp.NewHandler(
		backendhttp.WithStorage(upstreamStorage),
	)
	upstreamServer := httptest.NewServer(upstreamHandler)
	defer upstreamServer.Close()

	upstreamURL := upstreamServer.URL + "/" + repoName + ".git"

	t.Run("CloneEmptyRepository", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-empty")
		runGitCmd(t, "", "clone", upstreamURL, cloneDir)

		hfdir := filepath.Join(cloneDir, ".git")
		if _, err := os.Stat(hfdir); os.IsNotExist(err) {
			t.Errorf(".git directory not found in cloned repository")
		}
	})

	t.Run("PushToRepository", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-empty")

		runGitCmd(t, workDir, "config", "user.email", "test@test.com")
		runGitCmd(t, workDir, "config", "user.name", "Test User")

		testFile := filepath.Join(workDir, "README.md")
		if err := os.WriteFile(testFile, []byte("# Test Repository\n"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		runGitCmd(t, workDir, "add", "README.md")
		runGitCmd(t, workDir, "commit", "-m", "Initial commit")
		runGitCmd(t, workDir, "push", "-u", "origin", "master")
	})

	t.Run("CloneWithContent", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-with-content")
		runGitCmd(t, "", "clone", upstreamURL, cloneDir)

		readmePath := filepath.Join(cloneDir, "README.md")
		content, err := os.ReadFile(readmePath)
		if err != nil {
			t.Fatalf("Failed to read README.md: %v", err)
		}
		if string(content) != "# Test Repository\n" {
			t.Errorf("Unexpected content: %s", content)
		}
	})
}

func TestHTTPProxyMode(t *testing.T) {
	// Create a temporary directory for the upstream server
	upstreamDir, err := os.MkdirTemp("", "proxy-test-upstream")
	if err != nil {
		t.Fatalf("Failed to create temp upstream dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(upstreamDir)
	}()

	// Create a temporary directory for the proxy server
	proxyDir, err := os.MkdirTemp("", "proxy-test-proxy")
	if err != nil {
		t.Fatalf("Failed to create temp proxy dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(proxyDir)
	}()

	// Create a temporary directory for client operations
	clientDir, err := os.MkdirTemp("", "proxy-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(clientDir)
	}()

	// Set up upstream server with a repository
	upstreamStorage := storage.NewStorage(storage.WithRootDir(upstreamDir))

	repoName := "test-repo"
	repoPath := filepath.Join(upstreamStorage.RepositoriesDir(), repoName+".git")
	if err := os.MkdirAll(filepath.Dir(repoPath), 0755); err != nil {
		t.Fatalf("Failed to create repos dir: %v", err)
	}
	runGitCmd(t, "", "init", "--bare", repoPath)

	upstreamHandler := backendhttp.NewHandler(
		backendhttp.WithStorage(upstreamStorage),
	)
	upstreamServer := httptest.NewServer(upstreamHandler)
	defer upstreamServer.Close()

	// Push content to the upstream
	upstreamCloneDir := filepath.Join(clientDir, "upstream-clone")
	upstreamURL := upstreamServer.URL + "/" + repoName + ".git"
	runGitCmd(t, "", "clone", upstreamURL, upstreamCloneDir)
	runGitCmd(t, upstreamCloneDir, "config", "user.email", "test@test.com")
	runGitCmd(t, upstreamCloneDir, "config", "user.name", "Test User")

	testFile := filepath.Join(upstreamCloneDir, "README.md")
	if err := os.WriteFile(testFile, []byte("# Proxied Repository\n"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	runGitCmd(t, upstreamCloneDir, "add", "README.md")
	runGitCmd(t, upstreamCloneDir, "commit", "-m", "Initial commit")
	runGitCmd(t, upstreamCloneDir, "push", "-u", "origin", "master")

	// Set up proxy server pointing to upstream
	proxyStorage := storage.NewStorage(
		storage.WithRootDir(proxyDir),
	)

	proxyHandler := backendhttp.NewHandler(
		backendhttp.WithStorage(proxyStorage),
		backendhttp.WithProxyManager(repository.NewProxyManager(upstreamServer.URL)),
	)
	proxyServer := httptest.NewServer(proxyHandler)
	defer proxyServer.Close()

	proxyURL := proxyServer.URL + "/" + repoName + ".git"

	t.Run("CloneFromProxy", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "proxy-clone")
		runGitCmd(t, "", "clone", proxyURL, cloneDir)

		// Verify the cloned content matches the upstream
		readmePath := filepath.Join(cloneDir, "README.md")
		content, err := os.ReadFile(readmePath)
		if err != nil {
			t.Fatalf("Failed to read README.md: %v", err)
		}
		if string(content) != "# Proxied Repository\n" {
			t.Errorf("Unexpected content: %s", content)
		}
	})

	t.Run("CloneFromProxyCached", func(t *testing.T) {
		// Second clone should use the cached mirror
		cloneDir := filepath.Join(clientDir, "proxy-clone-cached")
		runGitCmd(t, "", "clone", proxyURL, cloneDir)

		readmePath := filepath.Join(cloneDir, "README.md")
		content, err := os.ReadFile(readmePath)
		if err != nil {
			t.Fatalf("Failed to read README.md: %v", err)
		}
		if string(content) != "# Proxied Repository\n" {
			t.Errorf("Unexpected content: %s", content)
		}
	})

	t.Run("ProxyRepoIsMirror", func(t *testing.T) {
		// Verify the proxied repo is marked as a mirror (push should be forbidden)
		proxyRepoPath := filepath.Join(proxyStorage.RepositoriesDir(), repoName+".git")
		if _, err := os.Stat(proxyRepoPath); os.IsNotExist(err) {
			t.Fatalf("Proxied repository was not created on disk")
		}
	})

	t.Run("NonExistentRepoReturns404", func(t *testing.T) {
		nonExistentURL := proxyServer.URL + "/nonexistent.git/info/refs?service=git-upload-pack"
		resp, err := http.Get(nonExistentURL)
		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Expected 404, got %d", resp.StatusCode)
		}
	})
}

func TestHTTPHandlerAuthHook(t *testing.T) {
	// Create a temporary directory for the upstream server
	upstreamDir, err := os.MkdirTemp("", "http-test-authhook")
	if err != nil {
		t.Fatalf("Failed to create temp upstream dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(upstreamDir)
	}()

	// Create a temporary directory for client operations
	clientDir, err := os.MkdirTemp("", "http-test-authhook-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(clientDir)
	}()

	upstreamStorage := storage.NewStorage(storage.WithRootDir(upstreamDir))

	// Create a bare repository on the upstream
	repoName := "test-repo"
	repoPath := filepath.Join(upstreamStorage.RepositoriesDir(), repoName+".git")
	if err := os.MkdirAll(filepath.Dir(repoPath), 0755); err != nil {
		t.Fatalf("Failed to create repos dir: %v", err)
	}
	runGitCmd(t, "", "init", "--bare", repoPath)

	t.Run("AuthHookAllowsRead", func(t *testing.T) {
		// Auth hook that allows all operations
		handler := backendhttp.NewHandler(
			backendhttp.WithStorage(upstreamStorage),
			backendhttp.WithPermissionHookFunc(func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) error {
				return nil
			}),
		)
		server := httptest.NewServer(handler)
		defer server.Close()

		// Clone should succeed
		cloneDir := filepath.Join(clientDir, "clone-allowed")
		runGitCmd(t, "", "clone", server.URL+"/"+repoName+".git", cloneDir)

		hfdir := filepath.Join(cloneDir, ".git")
		if _, err := os.Stat(hfdir); os.IsNotExist(err) {
			t.Errorf(".git directory not found in cloned repository")
		}
	})

	t.Run("AuthHookDeniesRead", func(t *testing.T) {
		// Auth hook that denies all operations
		handler := backendhttp.NewHandler(
			backendhttp.WithStorage(upstreamStorage),
			backendhttp.WithPermissionHookFunc(func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) error {
				return errors.New("access denied")
			}),
		)
		server := httptest.NewServer(handler)
		defer server.Close()

		// info/refs request should return 403
		resp, err := http.Get(server.URL + "/" + repoName + ".git/info/refs?service=git-upload-pack")
		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("Expected 403, got %d", resp.StatusCode)
		}
	})

	t.Run("AuthHookDeniesWriteAllowsRead", func(t *testing.T) {
		// Auth hook that allows fetches but denies pushes
		handler := backendhttp.NewHandler(
			backendhttp.WithStorage(upstreamStorage),
			backendhttp.WithPermissionHookFunc(func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) error {
				if op == permission.OperationUpdateRepo {
					return errors.New("push access denied")
				}
				return nil
			}),
		)
		server := httptest.NewServer(handler)
		defer server.Close()

		// Read (git-upload-pack) should succeed
		resp, err := http.Get(server.URL + "/" + repoName + ".git/info/refs?service=git-upload-pack")
		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusForbidden {
			t.Errorf("Expected read to be allowed, got 403")
		}

		// Write (git-receive-pack) should be denied
		resp2, err := http.Get(server.URL + "/" + repoName + ".git/info/refs?service=git-receive-pack")
		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}
		defer resp2.Body.Close()
		if resp2.StatusCode != http.StatusForbidden {
			t.Errorf("Expected write to be denied (403), got %d", resp2.StatusCode)
		}
	})

	t.Run("AuthHookReceivesCorrectRepoName", func(t *testing.T) {
		var capturedRepo string
		var capturedOp permission.Operation
		handler := backendhttp.NewHandler(
			backendhttp.WithStorage(upstreamStorage),
			backendhttp.WithPermissionHookFunc(func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) error {
				capturedRepo = repo
				capturedOp = op
				return nil
			}),
		)
		server := httptest.NewServer(handler)
		defer server.Close()

		resp, err := http.Get(server.URL + "/" + repoName + ".git/info/refs?service=git-upload-pack")
		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}
		defer resp.Body.Close()

		if capturedRepo != repoName {
			t.Errorf("Expected repo name %q, got %q", repoName, capturedRepo)
		}
		if capturedOp != permission.OperationReadRepo {
			t.Errorf("Expected operation %v, got %v", permission.OperationReadRepo, capturedOp)
		}
	})
}

func TestHTTPHandlerReceiveHook(t *testing.T) {
	upstreamDir, err := os.MkdirTemp("", "http-test-receivehook")
	if err != nil {
		t.Fatalf("Failed to create temp upstream dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(upstreamDir)
	}()

	clientDir, err := os.MkdirTemp("", "http-test-receivehook-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(clientDir)
	}()

	upstreamStorage := storage.NewStorage(storage.WithRootDir(upstreamDir))

	repoName := "hook-test-repo"
	repoPath := filepath.Join(upstreamStorage.RepositoriesDir(), repoName+".git")
	if err := os.MkdirAll(filepath.Dir(repoPath), 0755); err != nil {
		t.Fatalf("Failed to create repos dir: %v", err)
	}
	runGitCmd(t, "", "init", "--bare", repoPath)

	var mu sync.Mutex
	var capturedRepo string
	var capturedUpdates []receive.RefUpdate

	handler := backendhttp.NewHandler(
		backendhttp.WithStorage(upstreamStorage),
		backendhttp.WithReceiveHookFunc(func(ctx context.Context, repo string, updates []receive.RefUpdate) error {
			mu.Lock()
			defer mu.Unlock()
			capturedRepo = repo
			capturedUpdates = updates
			return nil
		}),
	)
	server := httptest.NewServer(handler)
	defer server.Close()

	upstreamURL := server.URL + "/" + repoName + ".git"

	// Clone, commit, and push
	workDir := filepath.Join(clientDir, "work")
	runGitCmd(t, "", "clone", upstreamURL, workDir)
	runGitCmd(t, workDir, "config", "user.email", "test@test.com")
	runGitCmd(t, workDir, "config", "user.name", "Test User")

	testFile := filepath.Join(workDir, "README.md")
	if err := os.WriteFile(testFile, []byte("# Hook Test\n"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	runGitCmd(t, workDir, "add", "README.md")
	runGitCmd(t, workDir, "commit", "-m", "Initial commit")
	runGitCmd(t, workDir, "push", "-u", "origin", "master")

	mu.Lock()
	defer mu.Unlock()

	if capturedRepo != repoName {
		t.Errorf("Expected repo %q, got %q", repoName, capturedRepo)
	}
	if len(capturedUpdates) != 1 {
		t.Fatalf("Expected 1 update, got %d", len(capturedUpdates))
	}
	u := capturedUpdates[0]
	if !u.IsBranch() {
		t.Error("Expected branch update")
	}
	if u.Name() != "master" {
		t.Errorf("Expected branch name 'master', got %q", u.Name())
	}
	if !u.IsCreate() {
		t.Error("Expected create (new branch)")
	}
}
