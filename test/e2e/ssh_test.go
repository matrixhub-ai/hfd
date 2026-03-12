package e2e_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wzshiming/hfd/internal/utils"
	backendhf "github.com/wzshiming/hfd/pkg/backend/hf"
	backendhttp "github.com/wzshiming/hfd/pkg/backend/http"
	backendlfs "github.com/wzshiming/hfd/pkg/backend/lfs"
	backendssh "github.com/wzshiming/hfd/pkg/backend/ssh"
	"github.com/wzshiming/hfd/pkg/storage"
	"golang.org/x/crypto/ssh"
)

// setupSSHTestServer creates both an HTTP test server and an SSH server
// sharing the same storage, with optional SSH public key authentication.
func setupSSHTestServer(t *testing.T, authorizedKeys []ssh.PublicKey) (*httptest.Server, net.Listener, string) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "ssh-e2e-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	store := storage.NewStorage(storage.WithRootDir(dataDir))

	// Set up HTTP handler chain (same order as main.go)
	var handler http.Handler

	handler = backendhf.NewHandler(
		backendhf.WithStorage(store),
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

	// Generate SSH host key
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("Failed to generate host key: %v", err)
	}
	hostKey, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("Failed to create host key signer: %v", err)
	}

	// Set up SSH server options
	sshOpts := []backendssh.Option{
		backendssh.WithHostKey(hostKey),
		backendssh.WithStorage(store),
	}
	if len(authorizedKeys) > 0 {
		callback := backendssh.AuthorizedKeysCallback(authorizedKeys)
		sshOpts = append(sshOpts, backendssh.WithPublicKeyCallback(callback))
	}

	sshServer := backendssh.NewServer(sshOpts...)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for SSH: %v", err)
	}
	t.Cleanup(func() { listener.Close() })

	go func() {
		_ = sshServer.Serve(t.Context(), listener)
	}()

	return httpServer, listener, dataDir
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
	cmd := utils.Command(t.Context(), "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("Git command failed: git %s\nError: %v\nOutput: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

func TestSSHCloneAndPushWithAuth(t *testing.T) {
	clientDir, err := os.MkdirTemp("", "ssh-e2e-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	// Generate authorized key
	goodKeyFile := filepath.Join(clientDir, "id_good")
	goodPubKey := generateTestKeyFile(t, goodKeyFile)

	httpServer, sshListener, _ := setupSSHTestServer(t, []ssh.PublicKey{goodPubKey})

	addr := sshListener.Addr().(*net.TCPAddr)
	port := strings.Split(addr.String(), ":")[1]
	sshURL := "ssh://git@" + addr.String() + "/"
	goodEnv := sshGitEnv(goodKeyFile, port)

	// Create a repo via the HuggingFace HTTP API
	endpoint := httpServer.URL
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"ssh-auth-model","organization":"test-user"}`))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 creating repo, got %d", resp.StatusCode)
	}

	t.Run("CloneEmptyRepoWithAuthorizedKey", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-auth-empty")
		runSSHGitCmd(t, "", goodEnv, "clone", sshURL+"test-user/ssh-auth-model.git", cloneDir)

		hfdir := filepath.Join(cloneDir, ".git")
		if _, err := os.Stat(hfdir); os.IsNotExist(err) {
			t.Errorf(".git directory not found in cloned repository")
		}
	})

	t.Run("PushWithAuthorizedKey", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-auth-empty")

		runSSHGitCmd(t, workDir, goodEnv, "config", "user.email", "test@test.com")
		runSSHGitCmd(t, workDir, goodEnv, "config", "user.name", "Test User")

		testFile := filepath.Join(workDir, "README.md")
		if err := os.WriteFile(testFile, []byte("# SSH Auth Test\n"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		runSSHGitCmd(t, workDir, goodEnv, "add", "README.md")
		runSSHGitCmd(t, workDir, goodEnv, "commit", "-m", "Initial commit via SSH")
		runSSHGitCmd(t, workDir, goodEnv, "push", "origin", "main")
	})

	t.Run("CloneWithContentUsingAuthorizedKey", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-auth-content")
		runSSHGitCmd(t, "", goodEnv, "clone", sshURL+"test-user/ssh-auth-model.git", cloneDir)

		readmePath := filepath.Join(cloneDir, "README.md")
		content, err := os.ReadFile(readmePath)
		if err != nil {
			t.Fatalf("Failed to read README.md: %v", err)
		}
		if string(content) != "# SSH Auth Test\n" {
			t.Errorf("Unexpected content: %s", content)
		}
	})

	t.Run("PullChangesWithAuthorizedKey", func(t *testing.T) {
		// Push another file from second clone
		workDir := filepath.Join(clientDir, "clone-auth-content")
		runSSHGitCmd(t, workDir, goodEnv, "config", "user.email", "test@test.com")
		runSSHGitCmd(t, workDir, goodEnv, "config", "user.name", "Test User")

		testFile := filepath.Join(workDir, "file2.txt")
		if err := os.WriteFile(testFile, []byte("Second file\n"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		runSSHGitCmd(t, workDir, goodEnv, "add", "file2.txt")
		runSSHGitCmd(t, workDir, goodEnv, "commit", "-m", "Add second file")
		runSSHGitCmd(t, workDir, goodEnv, "push")

		// Pull from first clone
		firstCloneDir := filepath.Join(clientDir, "clone-auth-empty")
		runSSHGitCmd(t, firstCloneDir, goodEnv, "config", "pull.rebase", "false")
		runSSHGitCmd(t, firstCloneDir, goodEnv, "pull")

		file2Path := filepath.Join(firstCloneDir, "file2.txt")
		if _, err := os.Stat(file2Path); os.IsNotExist(err) {
			t.Errorf("file2.txt not found after pull")
		}
	})
}

func TestSSHCloneWithUnauthorizedKeyFails(t *testing.T) {
	clientDir, err := os.MkdirTemp("", "ssh-e2e-unauth-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	// Generate authorized and unauthorized keys
	goodKeyFile := filepath.Join(clientDir, "id_good")
	goodPubKey := generateTestKeyFile(t, goodKeyFile)

	badKeyFile := filepath.Join(clientDir, "id_bad")
	_ = generateTestKeyFile(t, badKeyFile)

	httpServer, sshListener, _ := setupSSHTestServer(t, []ssh.PublicKey{goodPubKey})

	addr := sshListener.Addr().(*net.TCPAddr)
	port := strings.Split(addr.String(), ":")[1]
	sshURL := "ssh://git@" + addr.String() + "/"
	badEnv := sshGitEnv(badKeyFile, port)

	// Create a repo via HTTP
	endpoint := httpServer.URL
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"ssh-unauth-model","organization":"test-user"}`))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	// Attempt clone with unauthorized key
	cloneDir := filepath.Join(clientDir, "clone-bad")
	cmd := utils.Command(t.Context(), "git", "clone", sshURL+"test-user/ssh-unauth-model.git", cloneDir)
	cmd.Env = append(os.Environ(), badEnv...)
	output, err := cmd.Output()
	if err == nil {
		t.Fatalf("Expected clone to fail with unauthorized key, but it succeeded: %s", output)
	}
}

func TestSSHCrossProtocolUploadHTTPCloneSSH(t *testing.T) {
	clientDir, err := os.MkdirTemp("", "ssh-e2e-cross-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	// Generate authorized key
	goodKeyFile := filepath.Join(clientDir, "id_good")
	goodPubKey := generateTestKeyFile(t, goodKeyFile)

	httpServer, sshListener, _ := setupSSHTestServer(t, []ssh.PublicKey{goodPubKey})

	addr := sshListener.Addr().(*net.TCPAddr)
	port := strings.Split(addr.String(), ":")[1]
	sshURL := "ssh://git@" + addr.String() + "/"
	goodEnv := sshGitEnv(goodKeyFile, port)
	endpoint := httpServer.URL

	// Upload files via HuggingFace HTTP API
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"cross-model","organization":"cross-user"}`))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 creating repo, got %d", resp.StatusCode)
	}

	// Push content via HTTP git protocol
	httpCloneDir := filepath.Join(clientDir, "http-clone")
	httpGitURL := endpoint + "/cross-user/cross-model.git"
	httpCmd := utils.Command(t.Context(), "git", "clone", httpGitURL, httpCloneDir)
	httpCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if output, err := httpCmd.Output(); err != nil {
		t.Fatalf("HTTP clone failed: %v\n%s", err, output)
	}

	// Add a file and push via HTTP
	if err := os.WriteFile(filepath.Join(httpCloneDir, "README.md"), []byte("# Cross Protocol Test\n"), 0644); err != nil {
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

	// Clone via SSH with authorized key and verify content
	sshCloneDir := filepath.Join(clientDir, "ssh-clone")
	runSSHGitCmd(t, "", goodEnv, "clone", sshURL+"cross-user/cross-model.git", sshCloneDir)

	readmePath := filepath.Join(sshCloneDir, "README.md")
	content, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("Failed to read README.md from SSH clone: %v", err)
	}
	if string(content) != "# Cross Protocol Test\n" {
		t.Errorf("Unexpected content from SSH clone: %q", content)
	}
}
