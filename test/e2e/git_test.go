package e2e_test

import (
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wzshiming/hfd/internal/utils"
	backendgit "github.com/wzshiming/hfd/pkg/backend/git"
	backendhttp "github.com/wzshiming/hfd/pkg/backend/http"
	backendhuggingface "github.com/wzshiming/hfd/pkg/backend/huggingface"
	backendlfs "github.com/wzshiming/hfd/pkg/backend/lfs"
	"github.com/wzshiming/hfd/pkg/storage"
)

// setupGitTestServer creates an HTTP test server and a git protocol server
// sharing the same storage.
func setupGitTestServer(t *testing.T) (*httptest.Server, net.Listener) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "git-e2e-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	store := storage.NewStorage(storage.WithRootDir(dataDir))

	// Set up HTTP handler chain (same order as main.go)
	var handler http.Handler

	handler = backendhuggingface.NewHandler(
		backendhuggingface.WithStorage(store),
	)

	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(store),
		backendlfs.WithNext(handler),
	)

	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(store),
		backendhttp.WithNext(handler),
	)

	httpServer := httptest.NewServer(handler)
	t.Cleanup(func() { httpServer.Close() })

	// Set up git protocol server
	gitServer := backendgit.NewServer(store.RepositoriesDir())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for git protocol: %v", err)
	}
	t.Cleanup(func() { listener.Close() })

	go func() {
		_ = gitServer.Serve(listener)
	}()

	return httpServer, listener
}

// runGitProtocolCmd runs a git command in the specified directory.
func runGitProtocolCmd(t *testing.T, dir string, args ...string) string {
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

func TestGitProtocolCloneAndPush(t *testing.T) {
	clientDir, err := os.MkdirTemp("", "git-e2e-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	httpServer, gitListener := setupGitTestServer(t)
	gitAddr := gitListener.Addr().String()
	gitURL := "git://" + gitAddr + "/"
	endpoint := httpServer.URL

	// Create a repo via the HuggingFace HTTP API
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"git-proto-model","organization":"test-user"}`))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 creating repo, got %d", resp.StatusCode)
	}

	t.Run("CloneEmptyRepo", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-empty")
		runGitProtocolCmd(t, "", "clone", gitURL+"test-user/git-proto-model.git", cloneDir)

		gitDir := filepath.Join(cloneDir, ".git")
		if _, err := os.Stat(gitDir); os.IsNotExist(err) {
			t.Errorf(".git directory not found in cloned repository")
		}
	})

	t.Run("PushToRepo", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-empty")

		runGitProtocolCmd(t, workDir, "config", "user.email", "test@test.com")
		runGitProtocolCmd(t, workDir, "config", "user.name", "Test User")

		testFile := filepath.Join(workDir, "README.md")
		if err := os.WriteFile(testFile, []byte("# Git Protocol Test\n"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		runGitProtocolCmd(t, workDir, "add", "README.md")
		runGitProtocolCmd(t, workDir, "commit", "-m", "Initial commit via git protocol")
		runGitProtocolCmd(t, workDir, "push", "origin", "main")
	})

	t.Run("CloneWithContent", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-content")
		runGitProtocolCmd(t, "", "clone", gitURL+"test-user/git-proto-model.git", cloneDir)

		readmePath := filepath.Join(cloneDir, "README.md")
		content, err := os.ReadFile(readmePath)
		if err != nil {
			t.Fatalf("Failed to read README.md: %v", err)
		}
		if string(content) != "# Git Protocol Test\n" {
			t.Errorf("Unexpected content: %s", content)
		}
	})

	t.Run("FetchFromRepo", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-content")
		runGitProtocolCmd(t, workDir, "fetch", "origin")
	})

	t.Run("PushMoreCommits", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-content")

		runGitProtocolCmd(t, workDir, "config", "user.email", "test@test.com")
		runGitProtocolCmd(t, workDir, "config", "user.name", "Test User")

		testFile := filepath.Join(workDir, "file2.txt")
		if err := os.WriteFile(testFile, []byte("Second file via git protocol\n"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		runGitProtocolCmd(t, workDir, "add", "file2.txt")
		runGitProtocolCmd(t, workDir, "commit", "-m", "Add second file")
		runGitProtocolCmd(t, workDir, "push")
	})

	t.Run("PullChanges", func(t *testing.T) {
		firstCloneDir := filepath.Join(clientDir, "clone-empty")
		runGitProtocolCmd(t, firstCloneDir, "config", "pull.rebase", "false")
		runGitProtocolCmd(t, firstCloneDir, "pull")

		file2Path := filepath.Join(firstCloneDir, "file2.txt")
		if _, err := os.Stat(file2Path); os.IsNotExist(err) {
			t.Errorf("file2.txt not found after pull")
		}
	})
}

func TestGitProtocolCrossProtocol(t *testing.T) {
	clientDir, err := os.MkdirTemp("", "git-e2e-cross-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	httpServer, gitListener := setupGitTestServer(t)
	gitAddr := gitListener.Addr().String()
	gitURL := "git://" + gitAddr + "/"
	endpoint := httpServer.URL

	// Create a repo via the HuggingFace HTTP API
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"cross-git-model","organization":"cross-user"}`))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 creating repo, got %d", resp.StatusCode)
	}

	// Push content via HTTP
	httpCloneDir := filepath.Join(clientDir, "http-clone")
	httpGitURL := endpoint + "/cross-user/cross-git-model.git"
	httpCmd := utils.Command(t.Context(), "git", "clone", httpGitURL, httpCloneDir)
	httpCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if output, err := httpCmd.Output(); err != nil {
		t.Fatalf("HTTP clone failed: %v\n%s", err, output)
	}

	if err := os.WriteFile(filepath.Join(httpCloneDir, "README.md"), []byte("# Cross Protocol Git Test\n"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	for _, args := range [][]string{
		{"config", "user.email", "test@test.com"},
		{"config", "user.name", "Test User"},
		{"add", "README.md"},
		{"commit", "-m", "Upload via HTTP"},
		{"push", "origin", "main"},
	} {
		cmd := utils.Command(t.Context(), "git", args...)
		cmd.Dir = httpCloneDir
		cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		if output, err := cmd.Output(); err != nil {
			t.Fatalf("Git command failed: git %s\n%v\n%s", strings.Join(args, " "), err, output)
		}
	}

	// Clone via git protocol and verify content
	gitCloneDir := filepath.Join(clientDir, "git-clone")
	runGitProtocolCmd(t, "", "clone", gitURL+"cross-user/cross-git-model.git", gitCloneDir)

	readmePath := filepath.Join(gitCloneDir, "README.md")
	content, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("Failed to read README.md from git protocol clone: %v", err)
	}
	if string(content) != "# Cross Protocol Git Test\n" {
		t.Errorf("Unexpected content from git protocol clone: %q", content)
	}
}
