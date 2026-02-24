package ssh_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wzshiming/gitd/internal/utils"
	backendssh "github.com/wzshiming/gitd/pkg/backend/ssh"
	"golang.org/x/crypto/ssh"
)

// runGitCmd runs a git command in the specified directory.
func runGitCmd(t *testing.T, dir string, env []string, args ...string) string {
	t.Helper()
	cmd := utils.Command(t.Context(), "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("Git command failed: git %s\nError: %v\nOutput: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

func TestSSHProtocolServer(t *testing.T) {
	// Create a temporary directory for repositories
	repoDir, err := os.MkdirTemp("", "sshprotocol-test-repos")
	if err != nil {
		t.Fatalf("Failed to create temp repo dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(repoDir)
	}()

	// Create a temporary directory for client operations
	clientDir, err := os.MkdirTemp("", "sshprotocol-test-client")
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
	runGitCmd(t, "", nil, "init", "--bare", repoPath)

	// Generate a host key for the SSH server
	hostKey, err := generateHostKey()
	if err != nil {
		t.Fatalf("Failed to generate host key: %v", err)
	}

	// Start SSH server on a random port
	server := backendssh.NewServer(repositoriesDir, hostKey)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	go func() {
		_ = server.Serve(listener)
	}()

	addr := listener.Addr().(*net.TCPAddr)
	sshURL := "ssh://git@" + addr.String() + "/" + repoName

	// Configure SSH to skip host key verification
	sshCmd := "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p " + strings.Split(addr.String(), ":")[1]
	env := []string{
		"GIT_TERMINAL_PROMPT=0",
		"GIT_SSH_COMMAND=" + sshCmd,
	}

	t.Run("CloneEmptyRepository", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-empty")

		runGitCmd(t, "", env, "clone", sshURL, cloneDir)

		// Verify .git directory exists
		gitDir := filepath.Join(cloneDir, ".git")
		if _, err := os.Stat(gitDir); os.IsNotExist(err) {
			t.Errorf(".git directory not found in cloned repository")
		}
	})

	t.Run("PushToRepository", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-empty")

		runGitCmd(t, workDir, env, "config", "user.email", "test@test.com")
		runGitCmd(t, workDir, env, "config", "user.name", "Test User")

		// Create a test file
		testFile := filepath.Join(workDir, "README.md")
		if err := os.WriteFile(testFile, []byte("# Test Repository\n"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		runGitCmd(t, workDir, env, "add", "README.md")
		runGitCmd(t, workDir, env, "commit", "-m", "Initial commit")
		runGitCmd(t, workDir, env, "push", "-u", "origin", "master")
	})

	t.Run("CloneWithContent", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-with-content")

		runGitCmd(t, "", env, "clone", sshURL, cloneDir)

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
		runGitCmd(t, workDir, env, "fetch", "origin")
	})

	t.Run("PushMoreCommits", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-with-content")

		runGitCmd(t, workDir, env, "config", "user.email", "test@test.com")
		runGitCmd(t, workDir, env, "config", "user.name", "Test User")

		testFile := filepath.Join(workDir, "file2.txt")
		if err := os.WriteFile(testFile, []byte("Second file\n"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		runGitCmd(t, workDir, env, "add", "file2.txt")
		runGitCmd(t, workDir, env, "commit", "-m", "Add second file")
		runGitCmd(t, workDir, env, "push")
	})

	t.Run("PullChanges", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-empty")

		runGitCmd(t, workDir, env, "config", "user.email", "test@test.com")
		runGitCmd(t, workDir, env, "config", "user.name", "Test User")

		runGitCmd(t, workDir, env, "pull")

		// Verify file2.txt exists
		file2Path := filepath.Join(workDir, "file2.txt")
		if _, err := os.Stat(file2Path); os.IsNotExist(err) {
			t.Errorf("file2.txt not found after pull")
		}
	})
}

func generateHostKey() (ssh.Signer, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating host key: %w", err)
	}
	return ssh.NewSignerFromKey(priv)
}
