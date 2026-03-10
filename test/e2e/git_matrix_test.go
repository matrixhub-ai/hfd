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
	backendhttp "github.com/wzshiming/hfd/pkg/backend/http"
	backendhuggingface "github.com/wzshiming/hfd/pkg/backend/huggingface"
	backendlfs "github.com/wzshiming/hfd/pkg/backend/lfs"
	backendssh "github.com/wzshiming/hfd/pkg/backend/ssh"
	"github.com/wzshiming/hfd/pkg/storage"
	"golang.org/x/crypto/ssh"
)

// testProtocol defines a git protocol configuration for testing
type testProtocol struct {
	name      string
	setupFunc func(t *testing.T) (cloneURL string, env []string, cleanup func())
}

// setupHTTPProtocol creates an HTTP test server and returns its clone URL
func setupHTTPProtocol(t *testing.T) (cloneURL string, env []string, cleanup func()) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "git-http-matrix-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	store := storage.NewStorage(storage.WithRootDir(dataDir))

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

	server := httptest.NewServer(handler)

	// Create a test repo
	resp, err := http.Post(server.URL+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"test-repo","organization":"test-org"}`))
	if err != nil {
		server.Close()
		os.RemoveAll(dataDir)
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	return server.URL + "/test-org/test-repo.git",
		[]string{"GIT_TERMINAL_PROMPT=0"},
		func() {
			server.Close()
			os.RemoveAll(dataDir)
		}
}

// setupSSHProtocol creates an SSH test server and returns its clone URL
func setupSSHProtocol(t *testing.T) (cloneURL string, env []string, cleanup func()) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "git-ssh-matrix-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	clientDir, err := os.MkdirTemp("", "git-ssh-matrix-client")
	if err != nil {
		os.RemoveAll(dataDir)
		t.Fatalf("Failed to create client dir: %v", err)
	}

	store := storage.NewStorage(storage.WithRootDir(dataDir))

	// Set up HTTP handler for repo creation
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

	// Create a test repo via HTTP
	resp, err := http.Post(httpServer.URL+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"test-repo","organization":"test-org"}`))
	if err != nil {
		httpServer.Close()
		os.RemoveAll(dataDir)
		os.RemoveAll(clientDir)
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	// Generate SSH host key
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		httpServer.Close()
		os.RemoveAll(dataDir)
		os.RemoveAll(clientDir)
		t.Fatalf("Failed to generate host key: %v", err)
	}
	hostKey, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		httpServer.Close()
		os.RemoveAll(dataDir)
		os.RemoveAll(clientDir)
		t.Fatalf("Failed to create host key signer: %v", err)
	}

	// Generate client key
	_, clientPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		httpServer.Close()
		os.RemoveAll(dataDir)
		os.RemoveAll(clientDir)
		t.Fatalf("Failed to generate client key: %v", err)
	}
	privKeyPEM, err := ssh.MarshalPrivateKey(clientPriv, "")
	if err != nil {
		httpServer.Close()
		os.RemoveAll(dataDir)
		os.RemoveAll(clientDir)
		t.Fatalf("Failed to marshal private key: %v", err)
	}
	keyFile := filepath.Join(clientDir, "id_ed25519")
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(privKeyPEM), 0600); err != nil {
		httpServer.Close()
		os.RemoveAll(dataDir)
		os.RemoveAll(clientDir)
		t.Fatalf("Failed to write private key: %v", err)
	}

	// Set up SSH server
	sshServer := backendssh.NewServer(store.RepositoriesDir(), hostKey)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		httpServer.Close()
		os.RemoveAll(dataDir)
		os.RemoveAll(clientDir)
		t.Fatalf("Failed to listen for SSH: %v", err)
	}

	go func() {
		_ = sshServer.Serve(t.Context(), listener)
	}()

	addr := listener.Addr().(*net.TCPAddr)
	port := strings.Split(addr.String(), ":")[1]
	sshCmd := fmt.Sprintf("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s -p %s", keyFile, port)

	return "ssh://git@" + addr.String() + "/test-org/test-repo.git",
		[]string{
			"GIT_TERMINAL_PROMPT=0",
			"GIT_SSH_COMMAND=" + sshCmd,
		},
		func() {
			listener.Close()
			httpServer.Close()
			os.RemoveAll(dataDir)
			os.RemoveAll(clientDir)
		}
}

// TestGitOperationsMatrix tests all git operations across HTTP and SSH protocols
func TestGitOperationsMatrix(t *testing.T) {
	protocols := []testProtocol{
		{
			name:      "HTTP",
			setupFunc: setupHTTPProtocol,
		},
		{
			name:      "SSH",
			setupFunc: setupSSHProtocol,
		},
	}

	operations := []struct {
		name string
		test func(t *testing.T, cloneURL string, env []string)
	}{
		{
			name: "CloneEmptyRepo",
			test: testCloneEmptyRepo,
		},
		{
			name: "PushCommit",
			test: testPushCommit,
		},
		{
			name: "CloneWithContent",
			test: testCloneWithContent,
		},
		{
			name: "FetchFromRepo",
			test: testFetchFromRepo,
		},
		{
			name: "PushMoreCommits",
			test: testPushMoreCommits,
		},
		{
			name: "PullChanges",
			test: testPullChanges,
		},
		{
			name: "PushMultipleFiles",
			test: testPushMultipleFiles,
		},
		{
			name: "CreateAndPushBranch",
			test: testCreateAndPushBranch,
		},
		{
			name: "CreateAndPushTag",
			test: testCreateAndPushTag,
		},
		{
			name: "DeleteBranch",
			test: testDeleteBranch,
		},
		{
			name: "DeleteTag",
			test: testDeleteTag,
		},
	}

	for _, protocol := range protocols {
		t.Run(protocol.name, func(t *testing.T) {
			for _, op := range operations {
				t.Run(op.name, func(t *testing.T) {
					cloneURL, env, cleanup := protocol.setupFunc(t)
					defer cleanup()
					op.test(t, cloneURL, env)
				})
			}
		})
	}
}

func testCloneEmptyRepo(t *testing.T, cloneURL string, env []string) {
	clientDir, err := os.MkdirTemp("", "git-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	cmd := utils.Command(t.Context(), "git", "clone", cloneURL, cloneDir)
	cmd.Env = append(os.Environ(), env...)
	if output, err := cmd.Output(); err != nil {
		t.Fatalf("Clone failed: %v\n%s", err, output)
	}

	gitDir := filepath.Join(cloneDir, ".git")
	if _, err := os.Stat(gitDir); os.IsNotExist(err) {
		t.Errorf(".git directory not found in cloned repository")
	}
}

func testPushCommit(t *testing.T, cloneURL string, env []string) {
	clientDir, err := os.MkdirTemp("", "git-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	runGitCmd(t, "", env, "clone", cloneURL, cloneDir)
	runGitCmd(t, cloneDir, env, "config", "user.email", "test@test.com")
	runGitCmd(t, cloneDir, env, "config", "user.name", "Test User")

	testFile := filepath.Join(cloneDir, "README.md")
	if err := os.WriteFile(testFile, []byte("# Test\n"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	runGitCmd(t, cloneDir, env, "add", "README.md")
	runGitCmd(t, cloneDir, env, "commit", "-m", "Initial commit")
	runGitCmd(t, cloneDir, env, "push", "origin", "main")
}

func testCloneWithContent(t *testing.T, cloneURL string, env []string) {
	// First push content
	testPushCommit(t, cloneURL, env)

	// Then clone and verify
	clientDir, err := os.MkdirTemp("", "git-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone-verify")
	runGitCmd(t, "", env, "clone", cloneURL, cloneDir)

	readmePath := filepath.Join(cloneDir, "README.md")
	content, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("Failed to read README.md: %v", err)
	}
	if string(content) != "# Test\n" {
		t.Errorf("Unexpected content: %s", content)
	}
}

func testFetchFromRepo(t *testing.T, cloneURL string, env []string) {
	testPushCommit(t, cloneURL, env)

	clientDir, err := os.MkdirTemp("", "git-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	runGitCmd(t, "", env, "clone", cloneURL, cloneDir)
	runGitCmd(t, cloneDir, env, "fetch", "origin")
}

func testPushMoreCommits(t *testing.T, cloneURL string, env []string) {
	testPushCommit(t, cloneURL, env)

	clientDir, err := os.MkdirTemp("", "git-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	runGitCmd(t, "", env, "clone", cloneURL, cloneDir)
	runGitCmd(t, cloneDir, env, "config", "user.email", "test@test.com")
	runGitCmd(t, cloneDir, env, "config", "user.name", "Test User")

	testFile := filepath.Join(cloneDir, "file2.txt")
	if err := os.WriteFile(testFile, []byte("Second file\n"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	runGitCmd(t, cloneDir, env, "add", "file2.txt")
	runGitCmd(t, cloneDir, env, "commit", "-m", "Add second file")
	runGitCmd(t, cloneDir, env, "push")
}

func testPullChanges(t *testing.T, cloneURL string, env []string) {
	testPushCommit(t, cloneURL, env)

	clientDir, err := os.MkdirTemp("", "git-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	// First clone
	clone1Dir := filepath.Join(clientDir, "clone1")
	runGitCmd(t, "", env, "clone", cloneURL, clone1Dir)

	// Second clone, push changes
	clone2Dir := filepath.Join(clientDir, "clone2")
	runGitCmd(t, "", env, "clone", cloneURL, clone2Dir)
	runGitCmd(t, clone2Dir, env, "config", "user.email", "test@test.com")
	runGitCmd(t, clone2Dir, env, "config", "user.name", "Test User")

	testFile := filepath.Join(clone2Dir, "changes.txt")
	if err := os.WriteFile(testFile, []byte("Changes\n"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	runGitCmd(t, clone2Dir, env, "add", "changes.txt")
	runGitCmd(t, clone2Dir, env, "commit", "-m", "Add changes")
	runGitCmd(t, clone2Dir, env, "push")

	// Pull changes in first clone
	runGitCmd(t, clone1Dir, env, "config", "pull.rebase", "false")
	runGitCmd(t, clone1Dir, env, "pull")

	changesPath := filepath.Join(clone1Dir, "changes.txt")
	if _, err := os.Stat(changesPath); os.IsNotExist(err) {
		t.Errorf("changes.txt not found after pull")
	}
}

func testPushMultipleFiles(t *testing.T, cloneURL string, env []string) {
	clientDir, err := os.MkdirTemp("", "git-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	runGitCmd(t, "", env, "clone", cloneURL, cloneDir)
	runGitCmd(t, cloneDir, env, "config", "user.email", "test@test.com")
	runGitCmd(t, cloneDir, env, "config", "user.name", "Test User")

	files := map[string]string{
		"README.md":  "# Multi-File Test\n",
		"config.yml": "key: value\n",
		"data.json":  `{"name": "test"}` + "\n",
	}

	for name, content := range files {
		if err := os.WriteFile(filepath.Join(cloneDir, name), []byte(content), 0644); err != nil {
			t.Fatalf("Failed to create file %s: %v", name, err)
		}
	}

	runGitCmd(t, cloneDir, env, "add", ".")
	runGitCmd(t, cloneDir, env, "commit", "-m", "Add multiple files")
	runGitCmd(t, cloneDir, env, "push", "origin", "main")
}

func testCreateAndPushBranch(t *testing.T, cloneURL string, env []string) {
	testPushCommit(t, cloneURL, env)

	clientDir, err := os.MkdirTemp("", "git-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	runGitCmd(t, "", env, "clone", cloneURL, cloneDir)
	runGitCmd(t, cloneDir, env, "config", "user.email", "test@test.com")
	runGitCmd(t, cloneDir, env, "config", "user.name", "Test User")

	runGitCmd(t, cloneDir, env, "checkout", "-b", "feature")
	testFile := filepath.Join(cloneDir, "feature.txt")
	if err := os.WriteFile(testFile, []byte("feature\n"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	runGitCmd(t, cloneDir, env, "add", "feature.txt")
	runGitCmd(t, cloneDir, env, "commit", "-m", "Feature commit")
	runGitCmd(t, cloneDir, env, "push", "origin", "feature")
}

func testCreateAndPushTag(t *testing.T, cloneURL string, env []string) {
	testPushCommit(t, cloneURL, env)

	clientDir, err := os.MkdirTemp("", "git-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	runGitCmd(t, "", env, "clone", cloneURL, cloneDir)

	runGitCmd(t, cloneDir, env, "tag", "v1.0")
	runGitCmd(t, cloneDir, env, "push", "origin", "v1.0")
}

func testDeleteBranch(t *testing.T, cloneURL string, env []string) {
	testCreateAndPushBranch(t, cloneURL, env)

	clientDir, err := os.MkdirTemp("", "git-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	runGitCmd(t, "", env, "clone", cloneURL, cloneDir)

	runGitCmd(t, cloneDir, env, "push", "origin", "--delete", "feature")
}

func testDeleteTag(t *testing.T, cloneURL string, env []string) {
	testCreateAndPushTag(t, cloneURL, env)

	clientDir, err := os.MkdirTemp("", "git-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	runGitCmd(t, "", env, "clone", cloneURL, cloneDir)

	runGitCmd(t, cloneDir, env, "push", "origin", "--delete", "v1.0")
}

func runGitCmd(t *testing.T, dir string, env []string, args ...string) {
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
