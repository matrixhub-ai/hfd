package git_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wzshiming/hfd/internal/utils"
	backendgit "github.com/wzshiming/hfd/pkg/backend/git"
	"github.com/wzshiming/hfd/pkg/repository"
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

func TestGitProtocolServer(t *testing.T) {
	// Create a temporary directory for repositories
	repoDir, err := os.MkdirTemp("", "gitprotocol-test-repos")
	if err != nil {
		t.Fatalf("Failed to create temp repo dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(repoDir)
	}()

	// Create a temporary directory for client operations
	clientDir, err := os.MkdirTemp("", "gitprotocol-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(clientDir)
	}()

	repositoriesDir := filepath.Join(repoDir, "repositories")
	if err := os.MkdirAll(repositoriesDir, 0755); err != nil {
		t.Fatalf("Failed to create repositories dir: %v", err)
	}

	// Create a bare repository
	repoName := "test-repo.git"
	repoPath := filepath.Join(repositoriesDir, repoName)
	runGitCmd(t, "", "init", "--bare", repoPath)

	// Start git protocol server on a random port
	server := backendgit.NewServer(repositoriesDir)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	go func() {
		_ = server.Serve(listener)
	}()

	gitURL := "git://" + listener.Addr().String() + "/" + repoName

	t.Run("CloneEmptyRepository", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-empty")

		runGitCmd(t, "", "clone", gitURL, cloneDir)

		// Verify .git directory exists
		hfdir := filepath.Join(cloneDir, ".git")
		if _, err := os.Stat(hfdir); os.IsNotExist(err) {
			t.Errorf(".git directory not found in cloned repository")
		}
	})

	t.Run("PushToRepository", func(t *testing.T) {
		// Push requires git-receive-pack which is enabled via git protocol
		// First use a local clone to push content
		workDir := filepath.Join(clientDir, "clone-empty")

		runGitCmd(t, workDir, "config", "user.email", "test@test.com")
		runGitCmd(t, workDir, "config", "user.name", "Test User")

		// Create a test file
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

		runGitCmd(t, "", "clone", gitURL, cloneDir)

		// Verify README.md exists
		readmePath := filepath.Join(cloneDir, "README.md")
		content, err := os.ReadFile(readmePath)
		if err != nil {
			t.Fatalf("Failed to read README.md: %v", err)
		}
		if string(content) != "# Test Repository\n" {
			t.Errorf("Unexpected content: %s", content)
		}
	})

	t.Run("FetchFromRepository", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-with-content")
		runGitCmd(t, workDir, "fetch", "origin")
	})

	t.Run("PushMoreCommits", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-with-content")

		runGitCmd(t, workDir, "config", "user.email", "test@test.com")
		runGitCmd(t, workDir, "config", "user.name", "Test User")

		testFile := filepath.Join(workDir, "file2.txt")
		if err := os.WriteFile(testFile, []byte("Second file\n"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		runGitCmd(t, workDir, "add", "file2.txt")
		runGitCmd(t, workDir, "commit", "-m", "Add second file")
		runGitCmd(t, workDir, "push")
	})

	t.Run("PullChanges", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-empty")

		runGitCmd(t, workDir, "config", "user.email", "test@test.com")
		runGitCmd(t, workDir, "config", "user.name", "Test User")

		runGitCmd(t, workDir, "pull")

		// Verify file2.txt exists
		file2Path := filepath.Join(workDir, "file2.txt")
		if _, err := os.Stat(file2Path); os.IsNotExist(err) {
			t.Errorf("file2.txt not found after pull")
		}
	})
}

func TestGitProtocolLFSConfig(t *testing.T) {
	// Create a temporary directory for repositories
	repoDir, err := os.MkdirTemp("", "gitprotocol-lfs-test-repos")
	if err != nil {
		t.Fatalf("Failed to create temp repo dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(repoDir)
	}()

	// Create a temporary directory for client operations
	clientDir, err := os.MkdirTemp("", "gitprotocol-lfs-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(clientDir)
	}()

	repositoriesDir := filepath.Join(repoDir, "repositories")
	if err := os.MkdirAll(repositoriesDir, 0755); err != nil {
		t.Fatalf("Failed to create repositories dir: %v", err)
	}

	// Create a bare repository and add initial content
	repoName := "lfs-repo"
	bareRepoPath := filepath.Join(repositoriesDir, repoName+".git")
	repo, err := repository.Init(bareRepoPath, "main")
	if err != nil {
		t.Fatalf("Failed to init repository: %v", err)
	}

	// Create an initial commit so the repo has a HEAD
	_, err = repo.CreateCommit(context.Background(), "main", "Initial commit", "Test", "test@test.com",
		[]repository.CommitOperation{
			{Type: repository.CommitOperationAdd, Path: "README.md", Content: []byte("# LFS Test\n")},
		}, "")
	if err != nil {
		t.Fatalf("Failed to create initial commit: %v", err)
	}

	lfsURL := "http://localhost:8080"

	// Start git protocol server with LFS URL configured
	server := backendgit.NewServer(repositoriesDir,
		backendgit.WithLFSURL(lfsURL),
	)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	go func() {
		_ = server.Serve(listener)
	}()

	gitURL := "git://" + listener.Addr().String() + "/" + repoName

	t.Run("CloneIncludesLFSConfig", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-lfs")
		runGitCmd(t, "", "clone", gitURL, cloneDir)

		// Verify .lfsconfig exists in the cloned repository
		lfsConfigPath := filepath.Join(cloneDir, ".lfsconfig")
		content, err := os.ReadFile(lfsConfigPath)
		if err != nil {
			t.Fatalf("Failed to read .lfsconfig: %v", err)
		}

		expectedURL := "http://localhost:8080/" + repoName + ".git/info/lfs"
		if !strings.Contains(string(content), expectedURL) {
			t.Errorf(".lfsconfig does not contain expected URL %q, got:\n%s", expectedURL, content)
		}
	})

	t.Run("CloneIdempotent", func(t *testing.T) {
		// Clone again to verify idempotency (no duplicate commits)
		cloneDir := filepath.Join(clientDir, "clone-lfs-2")
		runGitCmd(t, "", "clone", gitURL, cloneDir)

		// Verify .lfsconfig still exists with correct URL
		lfsConfigPath := filepath.Join(cloneDir, ".lfsconfig")
		content, err := os.ReadFile(lfsConfigPath)
		if err != nil {
			t.Fatalf("Failed to read .lfsconfig: %v", err)
		}

		expectedURL := "http://localhost:8080/" + repoName + ".git/info/lfs"
		if !strings.Contains(string(content), expectedURL) {
			t.Errorf(".lfsconfig does not contain expected URL %q, got:\n%s", expectedURL, content)
		}
	})
}

func TestGitProtocolNoLFSConfigWhenNotConfigured(t *testing.T) {
	// Create a temporary directory for repositories
	repoDir, err := os.MkdirTemp("", "gitprotocol-nolfs-test-repos")
	if err != nil {
		t.Fatalf("Failed to create temp repo dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(repoDir)
	}()

	// Create a temporary directory for client operations
	clientDir, err := os.MkdirTemp("", "gitprotocol-nolfs-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(clientDir)
	}()

	repositoriesDir := filepath.Join(repoDir, "repositories")
	if err := os.MkdirAll(repositoriesDir, 0755); err != nil {
		t.Fatalf("Failed to create repositories dir: %v", err)
	}

	// Create a bare repository with content
	repoName := "nolfs-repo"
	bareRepoPath := filepath.Join(repositoriesDir, repoName+".git")
	repo, err := repository.Init(bareRepoPath, "main")
	if err != nil {
		t.Fatalf("Failed to init repository: %v", err)
	}

	_, err = repo.CreateCommit(context.Background(), "main", "Initial commit", "Test", "test@test.com",
		[]repository.CommitOperation{
			{Type: repository.CommitOperationAdd, Path: "README.md", Content: []byte("# No LFS\n")},
		}, "")
	if err != nil {
		t.Fatalf("Failed to create initial commit: %v", err)
	}

	// Start git protocol server WITHOUT LFS URL configured
	server := backendgit.NewServer(repositoriesDir)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	go func() {
		_ = server.Serve(listener)
	}()

	gitURL := "git://" + listener.Addr().String() + "/" + repoName

	cloneDir := filepath.Join(clientDir, "clone-nolfs")
	runGitCmd(t, "", "clone", gitURL, cloneDir)

	// Verify .lfsconfig does NOT exist
	lfsConfigPath := filepath.Join(cloneDir, ".lfsconfig")
	if _, err := os.Stat(lfsConfigPath); !os.IsNotExist(err) {
		t.Errorf(".lfsconfig should not exist when LFS URL is not configured")
	}
}
