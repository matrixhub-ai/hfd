package e2e_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/wzshiming/hfd/internal/utils"
	backendhf "github.com/wzshiming/hfd/pkg/backend/hf"
	backendhttp "github.com/wzshiming/hfd/pkg/backend/http"
	backendlfs "github.com/wzshiming/hfd/pkg/backend/lfs"
	backendssh "github.com/wzshiming/hfd/pkg/backend/ssh"
	"github.com/wzshiming/hfd/pkg/permission"
	"github.com/wzshiming/hfd/pkg/receive"
	"github.com/wzshiming/hfd/pkg/storage"
	"golang.org/x/crypto/ssh"
)

// TestReceiveHooksMatrix tests receive hooks across HTTP and SSH protocols
func TestReceiveHooksMatrix(t *testing.T) {
	type hookProtocol struct {
		name      string
		setupFunc func(t *testing.T, preHook receive.PreReceiveHookFunc, postHook receive.PostReceiveHookFunc) (repoURL string, env []string, createRepo func(), cleanup func())
	}

	protocols := []hookProtocol{
		{
			name:      "HTTP",
			setupFunc: setupHTTPWithHooks,
		},
		{
			name:      "SSH",
			setupFunc: setupSSHWithHooks,
		},
	}

	type hookTest struct {
		name string
		test func(t *testing.T, repoURL string, env []string, recorder *matrixHookRecorder)
	}

	tests := []hookTest{
		{name: "BranchPush", test: testHookBranchPush},
		{name: "TagCreate", test: testHookTagCreate},
		{name: "TagDelete", test: testHookTagDelete},
		{name: "BranchCreateAndDelete", test: testHookBranchCreateDelete},
	}

	for _, protocol := range protocols {
		t.Run(protocol.name, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					recorder := &matrixHookRecorder{}
					repoURL, env, createRepo, cleanup := protocol.setupFunc(t, nil, recorder.hook)
					defer cleanup()
					createRepo()
					test.test(t, repoURL, env, recorder)
				})
			}
		})
	}
}

// TestPreReceiveHookDenyMatrix tests pre-receive hook denial across protocols
func TestPreReceiveHookDenyMatrix(t *testing.T) {
	type hookProtocol struct {
		name      string
		setupFunc func(t *testing.T, preHook receive.PreReceiveHookFunc, postHook receive.PostReceiveHookFunc) (repoURL string, env []string, createRepo func(), cleanup func())
	}

	protocols := []hookProtocol{
		{
			name:      "HTTP",
			setupFunc: setupHTTPWithHooks,
		},
		{
			name:      "SSH",
			setupFunc: setupSSHWithHooks,
		},
	}

	for _, protocol := range protocols {
		t.Run(protocol.name, func(t *testing.T) {
			preHook := func(ctx context.Context, repoName string, updates []receive.RefUpdate) error {
				for _, e := range updates {
					if e.IsTag() {
						return errors.New("tag operations not allowed")
					}
				}
				return nil
			}

			postRecorder := &matrixHookRecorder{}
			repoURL, env, createRepo, cleanup := protocol.setupFunc(t, preHook, postRecorder.hook)
			defer cleanup()
			createRepo()

			clientDir, err := os.MkdirTemp("", "hook-deny-client")
			if err != nil {
				t.Fatalf("Failed to create temp client dir: %v", err)
			}
			defer os.RemoveAll(clientDir)

			// Clone and push commit (should succeed)
			cloneDir := filepath.Join(clientDir, "clone")
			runHookGitCmd(t, "", env, "clone", repoURL, cloneDir)
			runHookGitCmd(t, cloneDir, env, "config", "user.email", "test@test.com")
			runHookGitCmd(t, cloneDir, env, "config", "user.name", "Test User")

			if err := os.WriteFile(filepath.Join(cloneDir, "README.md"), []byte("# Test\n"), 0644); err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}

			runHookGitCmd(t, cloneDir, env, "add", "README.md")
			runHookGitCmd(t, cloneDir, env, "commit", "-m", "Initial commit")
			runHookGitCmd(t, cloneDir, env, "push", "origin", "main")

			// Tag push should be denied
			runHookGitCmd(t, cloneDir, env, "tag", "v1.0")
			cmd := utils.Command(t.Context(), "git", "push", "origin", "v1.0")
			cmd.Dir = cloneDir
			cmd.Env = append(os.Environ(), env...)
			output, err := cmd.Output()
			if err == nil {
				t.Fatalf("Expected tag push to fail, but it succeeded: %s", output)
			}
		})
	}
}

func setupHTTPWithHooks(t *testing.T, preHook receive.PreReceiveHookFunc, postHook receive.PostReceiveHookFunc) (repoURL string, env []string, createRepo func(), cleanup func()) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "hook-http-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	store := storage.NewStorage(storage.WithRootDir(dataDir))

	var httpOpts []backendhttp.Option
	httpOpts = append(httpOpts, backendhttp.WithStorage(store))
	if preHook != nil {
		httpOpts = append(httpOpts, backendhttp.WithPreReceiveHookFunc(preHook))
	}
	if postHook != nil {
		httpOpts = append(httpOpts, backendhttp.WithPostReceiveHookFunc(postHook))
	}

	var handler http.Handler
	handler = backendhf.NewHandler(
		backendhf.WithStorage(store),
	)
	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(store),
		backendlfs.WithNext(handler),
	)
	httpOpts = append(httpOpts, backendhttp.WithNext(handler))
	handler = backendhttp.NewHandler(httpOpts...)

	server := httptest.NewServer(handler)

	return server.URL + "/hook-org/hook-repo.git",
		[]string{"GIT_TERMINAL_PROMPT=0"},
		func() {
			resp, err := http.Post(server.URL+"/api/repos/create", "application/json",
				strings.NewReader(`{"type":"model","name":"hook-repo","organization":"hook-org"}`))
			if err != nil {
				t.Fatalf("Failed to create repo: %v", err)
			}
			resp.Body.Close()
		},
		func() {
			server.Close()
			os.RemoveAll(dataDir)
		}
}

func setupSSHWithHooks(t *testing.T, preHook receive.PreReceiveHookFunc, postHook receive.PostReceiveHookFunc) (repoURL string, env []string, createRepo func(), cleanup func()) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "hook-ssh-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	clientDir, err := os.MkdirTemp("", "hook-ssh-client")
	if err != nil {
		os.RemoveAll(dataDir)
		t.Fatalf("Failed to create client dir: %v", err)
	}

	store := storage.NewStorage(storage.WithRootDir(dataDir))

	// HTTP handler for repo creation
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
	keyFile := filepath.Join(clientDir, "id_ed25519")
	generateTestKeyFile(t, keyFile)

	// SSH server with hooks
	var sshOpts []backendssh.Option
	if preHook != nil {
		sshOpts = append(sshOpts, backendssh.WithPreReceiveHookFunc(preHook))
	}
	if postHook != nil {
		sshOpts = append(sshOpts, backendssh.WithPostReceiveHookFunc(postHook))
	}

	sshServer := backendssh.NewServer(store.RepositoriesDir(), hostKey, sshOpts...)
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

	return "ssh://git@" + addr.String() + "/hook-org/hook-repo.git",
		[]string{
			"GIT_TERMINAL_PROMPT=0",
			"GIT_SSH_COMMAND=" + sshCmd,
		},
		func() {
			resp, err := http.Post(httpServer.URL+"/api/repos/create", "application/json",
				strings.NewReader(`{"type":"model","name":"hook-repo","organization":"hook-org"}`))
			if err != nil {
				t.Fatalf("Failed to create repo: %v", err)
			}
			resp.Body.Close()
		},
		func() {
			listener.Close()
			httpServer.Close()
			os.RemoveAll(dataDir)
			os.RemoveAll(clientDir)
		}
}

func testHookBranchPush(t *testing.T, repoURL string, env []string, recorder *matrixHookRecorder) {
	clientDir, err := os.MkdirTemp("", "hook-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	runHookGitCmd(t, "", env, "clone", repoURL, cloneDir)
	runHookGitCmd(t, cloneDir, env, "config", "user.email", "test@test.com")
	runHookGitCmd(t, cloneDir, env, "config", "user.name", "Test User")

	if err := os.WriteFile(filepath.Join(cloneDir, "README.md"), []byte("# Test\n"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	runHookGitCmd(t, cloneDir, env, "add", "README.md")
	runHookGitCmd(t, cloneDir, env, "commit", "-m", "Initial commit")
	runHookGitCmd(t, cloneDir, env, "push", "origin", "main")

	calls := recorder.getCalls()
	if len(calls) == 0 {
		t.Fatal("Expected receive hook to be called")
	}
	call := calls[len(calls)-1]
	if len(call.updates) == 0 {
		t.Fatal("Expected at least one ref update")
	}
	update := call.updates[0]
	if !update.IsBranch() {
		t.Errorf("Expected branch update, got ref %q", update.RefName())
	}
}

func testHookTagCreate(t *testing.T, repoURL string, env []string, recorder *matrixHookRecorder) {
	testHookBranchPush(t, repoURL, env, recorder)

	recorder.reset()

	clientDir, err := os.MkdirTemp("", "hook-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	runHookGitCmd(t, "", env, "clone", repoURL, cloneDir)

	runHookGitCmd(t, cloneDir, env, "tag", "v1.0")
	runHookGitCmd(t, cloneDir, env, "push", "origin", "v1.0")

	calls := recorder.getCalls()
	if len(calls) == 0 {
		t.Fatal("Expected receive hook to be called for tag push")
	}
	call := calls[len(calls)-1]
	if len(call.updates) == 0 {
		t.Fatal("Expected at least one ref update for tag")
	}
	update := call.updates[0]
	if !update.IsTag() {
		t.Errorf("Expected tag update, got ref %q", update.RefName())
	}
	if !update.IsCreate() {
		t.Errorf("Expected tag create")
	}
}

func testHookTagDelete(t *testing.T, repoURL string, env []string, recorder *matrixHookRecorder) {
	testHookTagCreate(t, repoURL, env, recorder)

	recorder.reset()

	clientDir, err := os.MkdirTemp("", "hook-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	runHookGitCmd(t, "", env, "clone", repoURL, cloneDir)

	runHookGitCmd(t, cloneDir, env, "push", "origin", "--delete", "v1.0")

	calls := recorder.getCalls()
	if len(calls) == 0 {
		t.Fatal("Expected receive hook to be called for tag delete")
	}
	call := calls[len(calls)-1]
	if len(call.updates) == 0 {
		t.Fatal("Expected at least one ref update for tag delete")
	}
	update := call.updates[0]
	if !update.IsTag() {
		t.Errorf("Expected tag update, got ref %q", update.RefName())
	}
	if !update.IsDelete() {
		t.Errorf("Expected tag delete")
	}
}

func testHookBranchCreateDelete(t *testing.T, repoURL string, env []string, recorder *matrixHookRecorder) {
	testHookBranchPush(t, repoURL, env, recorder)

	recorder.reset()

	clientDir, err := os.MkdirTemp("", "hook-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	runHookGitCmd(t, "", env, "clone", repoURL, cloneDir)
	runHookGitCmd(t, cloneDir, env, "config", "user.email", "test@test.com")
	runHookGitCmd(t, cloneDir, env, "config", "user.name", "Test User")

	runHookGitCmd(t, cloneDir, env, "checkout", "-b", "feature")
	if err := os.WriteFile(filepath.Join(cloneDir, "feature.txt"), []byte("feature\n"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	runHookGitCmd(t, cloneDir, env, "add", "feature.txt")
	runHookGitCmd(t, cloneDir, env, "commit", "-m", "Feature commit")
	runHookGitCmd(t, cloneDir, env, "push", "origin", "feature")

	calls := recorder.getCalls()
	if len(calls) == 0 {
		t.Fatal("Expected receive hook for branch create")
	}
	call := calls[len(calls)-1]
	found := false
	for _, u := range call.updates {
		if u.IsBranch() && u.IsCreate() && u.Name() == "feature" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected branch create for 'feature' in updates: %+v", call.updates)
	}

	// Delete the branch
	recorder.reset()
	runHookGitCmd(t, cloneDir, env, "checkout", "main")
	runHookGitCmd(t, cloneDir, env, "push", "origin", "--delete", "feature")

	calls = recorder.getCalls()
	if len(calls) == 0 {
		t.Fatal("Expected receive hook for branch delete")
	}
	call = calls[len(calls)-1]
	found = false
	for _, u := range call.updates {
		if u.IsBranch() && u.IsDelete() && u.Name() == "feature" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected branch delete for 'feature' in updates: %+v", call.updates)
	}
}

func runHookGitCmd(t *testing.T, dir string, env []string, args ...string) {
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

// matrixHookRecorder records receive hook calls in a thread-safe manner (for matrix tests)
type matrixHookRecorder struct {
	mu      sync.Mutex
	calls   []matrixHookCall
	hookErr error
}

type matrixHookCall struct {
	repoName string
	updates  []receive.RefUpdate
}

func (r *matrixHookRecorder) hook(ctx context.Context, repoName string, updates []receive.RefUpdate) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, matrixHookCall{repoName: repoName, updates: updates})
	return r.hookErr
}

func (r *matrixHookRecorder) getCalls() []matrixHookCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]matrixHookCall, len(r.calls))
	copy(result, r.calls)
	return result
}

func (r *matrixHookRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = nil
}

// TestPermissionHookMatrix tests permission hooks across protocols
func TestPermissionHookMatrix(t *testing.T) {
	type hookProtocol struct {
		name      string
		setupFunc func(t *testing.T, permHook permission.PermissionHookFunc) (repoURL string, env []string, createRepo func(), cleanup func())
	}

	protocols := []hookProtocol{
		{
			name:      "HTTP",
			setupFunc: setupHTTPWithPermission,
		},
		{
			name:      "SSH",
			setupFunc: setupSSHWithPermission,
		},
	}

	for _, protocol := range protocols {
		t.Run(protocol.name, func(t *testing.T) {
			permHook := func(ctx context.Context, op permission.Operation, repoName string, opCtx permission.Context) (bool, error) {
				// Deny write operations (only allow read)
				if !op.IsRead() {
					return false, nil
				}
				return true, nil
			}

			repoURL, env, createRepo, cleanup := protocol.setupFunc(t, permHook)
			defer cleanup()
			createRepo()

			clientDir, err := os.MkdirTemp("", "perm-test-client")
			if err != nil {
				t.Fatalf("Failed to create temp client dir: %v", err)
			}
			defer os.RemoveAll(clientDir)

			// Clone should succeed (read permission)
			cloneDir := filepath.Join(clientDir, "clone")
			runHookGitCmd(t, "", env, "clone", repoURL, cloneDir)

			// Push should fail (write denied)
			runHookGitCmd(t, cloneDir, env, "config", "user.email", "test@test.com")
			runHookGitCmd(t, cloneDir, env, "config", "user.name", "Test User")

			if err := os.WriteFile(filepath.Join(cloneDir, "README.md"), []byte("# Test\n"), 0644); err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}

			runHookGitCmd(t, cloneDir, env, "add", "README.md")
			runHookGitCmd(t, cloneDir, env, "commit", "-m", "Initial commit")

			cmd := utils.Command(t.Context(), "git", "push", "origin", "main")
			cmd.Dir = cloneDir
			cmd.Env = append(os.Environ(), env...)
			output, err := cmd.Output()
			if err == nil {
				t.Fatalf("Expected push to fail due to permission hook, but it succeeded: %s", output)
			}
		})
	}
}

func setupHTTPWithPermission(t *testing.T, permHook permission.PermissionHookFunc) (repoURL string, env []string, createRepo func(), cleanup func()) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "perm-http-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	store := storage.NewStorage(storage.WithRootDir(dataDir))

	var httpOpts []backendhttp.Option
	httpOpts = append(httpOpts, backendhttp.WithStorage(store))
	if permHook != nil {
		httpOpts = append(httpOpts, backendhttp.WithPermissionHookFunc(permHook))
	}

	var handler http.Handler
	handler = backendhf.NewHandler(
		backendhf.WithStorage(store),
	)
	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(store),
		backendlfs.WithNext(handler),
	)
	httpOpts = append(httpOpts, backendhttp.WithNext(handler))
	handler = backendhttp.NewHandler(httpOpts...)

	server := httptest.NewServer(handler)

	return server.URL + "/perm-org/perm-repo.git",
		[]string{"GIT_TERMINAL_PROMPT=0"},
		func() {
			resp, err := http.Post(server.URL+"/api/repos/create", "application/json",
				strings.NewReader(`{"type":"model","name":"perm-repo","organization":"perm-org"}`))
			if err != nil {
				t.Fatalf("Failed to create repo: %v", err)
			}
			resp.Body.Close()
		},
		func() {
			server.Close()
			os.RemoveAll(dataDir)
		}
}

func setupSSHWithPermission(t *testing.T, permHook permission.PermissionHookFunc) (repoURL string, env []string, createRepo func(), cleanup func()) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "perm-ssh-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	clientDir, err := os.MkdirTemp("", "perm-ssh-client")
	if err != nil {
		os.RemoveAll(dataDir)
		t.Fatalf("Failed to create client dir: %v", err)
	}

	store := storage.NewStorage(storage.WithRootDir(dataDir))

	// HTTP handler for repo creation
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
	keyFile := filepath.Join(clientDir, "id_ed25519")
	generateTestKeyFile(t, keyFile)

	// SSH server with permission hook
	var sshOpts []backendssh.Option
	if permHook != nil {
		sshOpts = append(sshOpts, backendssh.WithPermissionHookFunc(permHook))
	}

	sshServer := backendssh.NewServer(store.RepositoriesDir(), hostKey, sshOpts...)
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

	return "ssh://git@" + addr.String() + "/perm-org/perm-repo.git",
		[]string{
			"GIT_TERMINAL_PROMPT=0",
			"GIT_SSH_COMMAND=" + sshCmd,
		},
		func() {
			resp, err := http.Post(httpServer.URL+"/api/repos/create", "application/json",
				strings.NewReader(`{"type":"model","name":"perm-repo","organization":"perm-org"}`))
			if err != nil {
				t.Fatalf("Failed to create repo: %v", err)
			}
			resp.Body.Close()
		},
		func() {
			listener.Close()
			httpServer.Close()
			os.RemoveAll(dataDir)
			os.RemoveAll(clientDir)
		}
}
