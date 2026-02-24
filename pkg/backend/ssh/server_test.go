package ssh_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
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

func generateClientKeyFile(path string) (ssh.PublicKey, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating client key: %w", err)
	}
	privKeyPEM, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		return nil, fmt.Errorf("marshaling private key: %w", err)
	}
	if err := os.WriteFile(path, pem.EncodeToMemory(privKeyPEM), 0600); err != nil {
		return nil, fmt.Errorf("writing private key: %w", err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		return nil, err
	}
	return signer.PublicKey(), nil
}

func TestSSHPublicKeyAuth(t *testing.T) {
	// Create a temporary directory for repositories
	repoDir, err := os.MkdirTemp("", "sshauth-test-repos")
	if err != nil {
		t.Fatalf("Failed to create temp repo dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(repoDir)
	}()

	clientDir, err := os.MkdirTemp("", "sshauth-test-client")
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
	repoName := "auth-test-repo.git"
	repoPath := filepath.Join(repositoriesDir, repoName)
	runGitCmd(t, "", nil, "init", "--bare", repoPath)

	// Generate host key and authorized client key
	hostKey, err := generateHostKey()
	if err != nil {
		t.Fatalf("Failed to generate host key: %v", err)
	}

	goodKeyFile := filepath.Join(clientDir, "id_good")
	goodPubKey, err := generateClientKeyFile(goodKeyFile)
	if err != nil {
		t.Fatalf("Failed to generate good client key: %v", err)
	}

	// Start SSH server with public key auth
	callback := backendssh.AuthorizedKeysCallback([]ssh.PublicKey{goodPubKey})
	server := backendssh.NewServer(repositoriesDir, hostKey, backendssh.WithPublicKeyCallback(callback))
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
	port := strings.Split(addr.String(), ":")[1]

	goodSSHCmd := fmt.Sprintf("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s -p %s", goodKeyFile, port)
	goodEnv := []string{
		"GIT_TERMINAL_PROMPT=0",
		"GIT_SSH_COMMAND=" + goodSSHCmd,
	}

	t.Run("CloneWithAuthorizedKey", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-auth")
		runGitCmd(t, "", goodEnv, "clone", sshURL, cloneDir)

		gitDir := filepath.Join(cloneDir, ".git")
		if _, err := os.Stat(gitDir); os.IsNotExist(err) {
			t.Errorf(".git directory not found in cloned repository")
		}
	})

	t.Run("CloneWithUnauthorizedKeyFails", func(t *testing.T) {
		badKeyFile := filepath.Join(clientDir, "id_bad")
		_, err := generateClientKeyFile(badKeyFile)
		if err != nil {
			t.Fatalf("Failed to generate bad client key: %v", err)
		}

		badSSHCmd := fmt.Sprintf("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s -p %s", badKeyFile, port)
		badEnv := []string{
			"GIT_TERMINAL_PROMPT=0",
			"GIT_SSH_COMMAND=" + badSSHCmd,
		}

		cloneDir := filepath.Join(clientDir, "clone-bad-auth")
		cmd := utils.Command(t.Context(), "git", "clone", sshURL, cloneDir)
		cmd.Env = append(os.Environ(), badEnv...)
		output, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("Expected clone to fail with unauthorized key, but it succeeded: %s", output)
		}
	})
}

func TestParseAuthorizedKeys(t *testing.T) {
	// Generate a test key
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("Failed to create signer: %v", err)
	}

	// Create an authorized_keys entry
	pubKey := signer.PublicKey()
	authorizedKey := ssh.MarshalAuthorizedKey(pubKey)

	t.Run("SingleKey", func(t *testing.T) {
		keys, err := backendssh.ParseAuthorizedKeys(authorizedKey)
		if err != nil {
			t.Fatalf("Failed to parse authorized keys: %v", err)
		}
		if len(keys) != 1 {
			t.Fatalf("Expected 1 key, got %d", len(keys))
		}
		if string(keys[0].Marshal()) != string(pubKey.Marshal()) {
			t.Error("Parsed key does not match original")
		}
	})

	t.Run("MultipleKeys", func(t *testing.T) {
		_, priv2, _ := ed25519.GenerateKey(rand.Reader)
		signer2, _ := ssh.NewSignerFromKey(priv2)
		authorizedKey2 := ssh.MarshalAuthorizedKey(signer2.PublicKey())

		combined := append(authorizedKey, authorizedKey2...)
		keys, err := backendssh.ParseAuthorizedKeys(combined)
		if err != nil {
			t.Fatalf("Failed to parse authorized keys: %v", err)
		}
		if len(keys) != 2 {
			t.Fatalf("Expected 2 keys, got %d", len(keys))
		}
	})

	t.Run("InvalidData", func(t *testing.T) {
		_, err := backendssh.ParseAuthorizedKeys([]byte("invalid-key-data"))
		if err == nil {
			t.Error("Expected error for invalid key data")
		}
	})
}

func TestAuthorizedKeysCallback(t *testing.T) {
	// Generate two key pairs
	_, priv1, _ := ed25519.GenerateKey(rand.Reader)
	signer1, _ := ssh.NewSignerFromKey(priv1)
	pub1 := signer1.PublicKey()

	_, priv2, _ := ed25519.GenerateKey(rand.Reader)
	signer2, _ := ssh.NewSignerFromKey(priv2)
	pub2 := signer2.PublicKey()

	// Only authorize key1
	callback := backendssh.AuthorizedKeysCallback([]ssh.PublicKey{pub1})

	t.Run("AuthorizedKeyAccepted", func(t *testing.T) {
		_, err := callback(nil, pub1)
		if err != nil {
			t.Errorf("Expected authorized key to be accepted, got: %v", err)
		}
	})

	t.Run("UnauthorizedKeyRejected", func(t *testing.T) {
		_, err := callback(nil, pub2)
		if err == nil {
			t.Error("Expected unauthorized key to be rejected")
		}
	})
}
