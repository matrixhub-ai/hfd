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
	backendhttp "github.com/wzshiming/hfd/pkg/backend/http"
	backendhuggingface "github.com/wzshiming/hfd/pkg/backend/huggingface"
	backendlfs "github.com/wzshiming/hfd/pkg/backend/lfs"
	backendssh "github.com/wzshiming/hfd/pkg/backend/ssh"
	"github.com/wzshiming/hfd/pkg/permission"
	"github.com/wzshiming/hfd/pkg/receive"
	"github.com/wzshiming/hfd/pkg/storage"
	"golang.org/x/crypto/ssh"
)

// hookRecorder records receive hook calls in a thread-safe manner.
type hookRecorder struct {
	mu      sync.Mutex
	calls   []hookCall
	hookErr error // optional error to return from the hook
}

type hookCall struct {
	repoName string
	updates  []receive.RefUpdate
}

func (r *hookRecorder) hook(ctx context.Context, repoName string, updates []receive.RefUpdate) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, hookCall{repoName: repoName, updates: updates})
	return r.hookErr
}

func (r *hookRecorder) getCalls() []hookCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]hookCall, len(r.calls))
	copy(result, r.calls)
	return result
}

func (r *hookRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = nil
}

// setupHTTPServerWithHooks creates an HTTP test server with pre/post receive and/or permission hooks.
func setupHTTPServerWithHooks(t *testing.T, preReceiveHook receive.PreReceiveHook, postReceiveHook receive.PostReceiveHook, permissionHook permission.PermissionHook) (*httptest.Server, string) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "hook-e2e-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	store := storage.NewStorage(storage.WithRootDir(dataDir))

	var httpOpts []backendhttp.Option
	httpOpts = append(httpOpts, backendhttp.WithStorage(store))
	if preReceiveHook != nil {
		httpOpts = append(httpOpts, backendhttp.WithPreReceiveHookFunc(preReceiveHook))
	}
	if postReceiveHook != nil {
		httpOpts = append(httpOpts, backendhttp.WithPostReceiveHookFunc(postReceiveHook))
	}
	if permissionHook != nil {
		httpOpts = append(httpOpts, backendhttp.WithPermissionHookFunc(permissionHook))
	}

	var handler http.Handler
	handler = backendhuggingface.NewHandler(
		backendhuggingface.WithStorage(store),
	)
	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(store),
		backendlfs.WithNext(handler),
	)
	httpOpts = append(httpOpts, backendhttp.WithNext(handler))
	handler = backendhttp.NewHandler(httpOpts...)

	server := httptest.NewServer(handler)
	t.Cleanup(func() { server.Close() })

	return server, dataDir
}

// setupSSHServerWithHooks creates both HTTP and SSH servers with hooks.
func setupSSHServerWithHooks(t *testing.T, preReceiveHook receive.PreReceiveHook, postReceiveHook receive.PostReceiveHook, permissionHook permission.PermissionHook) (*httptest.Server, net.Listener, string) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "ssh-hook-e2e-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	store := storage.NewStorage(storage.WithRootDir(dataDir))

	// HTTP handler chain
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

	// SSH server with hooks
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("Failed to generate host key: %v", err)
	}
	hostKey, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("Failed to create host key signer: %v", err)
	}

	var sshOpts []backendssh.Option
	if preReceiveHook != nil {
		sshOpts = append(sshOpts, backendssh.WithPreReceiveHookFunc(preReceiveHook))
	}
	if postReceiveHook != nil {
		sshOpts = append(sshOpts, backendssh.WithPostReceiveHookFunc(postReceiveHook))
	}
	if permissionHook != nil {
		sshOpts = append(sshOpts, backendssh.WithPermissionHookFunc(permissionHook))
	}

	sshServer := backendssh.NewServer(store.RepositoriesDir(), hostKey, sshOpts...)
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

// createRepoViaAPI creates a repository via the HuggingFace API.
func createRepoViaAPI(t *testing.T, endpoint, org, name string) {
	t.Helper()
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
		strings.NewReader(fmt.Sprintf(`{"type":"model","name":"%s","organization":"%s"}`, name, org)))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 creating repo, got %d", resp.StatusCode)
	}
}

// runHTTPGitCmd runs a git command with HTTP environment.
func runHTTPGitCmd(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := utils.Command(t.Context(), "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if output, err := cmd.Output(); err != nil {
		t.Fatalf("Git command failed: git %s\nError: %v\nOutput: %s", strings.Join(args, " "), err, output)
	}
}

// initWorkDir clones a repo and configures git user.
func initWorkDir(t *testing.T, cloneURL, dir string) {
	t.Helper()
	runHTTPGitCmd(t, "", "clone", cloneURL, dir)
	runHTTPGitCmd(t, dir, "config", "user.email", "test@test.com")
	runHTTPGitCmd(t, dir, "config", "user.name", "Test User")
}

// TestHTTPReceiveHook tests that the receive hook fires with correct ref updates on HTTP push.
func TestHTTPReceiveHook(t *testing.T) {
	recorder := &hookRecorder{}
	server, _ := setupHTTPServerWithHooks(t, nil, recorder.hook, nil)
	endpoint := server.URL

	clientDir, err := os.MkdirTemp("", "hook-http-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	createRepoViaAPI(t, endpoint, "hook-user", "hook-model")
	gitURL := endpoint + "/hook-user/hook-model.git"

	workDir := filepath.Join(clientDir, "clone")
	initWorkDir(t, gitURL, workDir)

	t.Run("PushBranchUpdate", func(t *testing.T) {
		// The repo already has a 'main' branch from the API-created initial commit,
		// so pushing to main is an update, not a create.
		recorder.reset()

		if err := os.WriteFile(filepath.Join(workDir, "README.md"), []byte("# Hook Test\n"), 0644); err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		runHTTPGitCmd(t, workDir, "add", "README.md")
		runHTTPGitCmd(t, workDir, "commit", "-m", "Initial commit")
		runHTTPGitCmd(t, workDir, "push", "origin", "main")

		calls := recorder.getCalls()
		if len(calls) == 0 {
			t.Fatal("Expected receive hook to be called, but it wasn't")
		}
		call := calls[len(calls)-1]
		if call.repoName != "hook-user/hook-model" {
			t.Errorf("Expected repo name 'hook-user/hook-model', got %q", call.repoName)
		}
		if len(call.updates) == 0 {
			t.Fatal("Expected at least one ref update")
		}
		update := call.updates[0]
		if !update.IsBranch() {
			t.Errorf("Expected branch update, got ref %q", update.RefName())
		}
		if update.IsCreate() {
			t.Errorf("Expected non-create (branch already exists from API)")
		}
		if update.IsDelete() {
			t.Errorf("Expected non-delete")
		}
		if update.Name() != "main" {
			t.Errorf("Expected branch name 'main', got %q", update.Name())
		}
	})

	t.Run("CreateTag", func(t *testing.T) {
		recorder.reset()

		runHTTPGitCmd(t, workDir, "tag", "v1.0")
		runHTTPGitCmd(t, workDir, "push", "origin", "v1.0")

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
		if update.Name() != "v1.0" {
			t.Errorf("Expected tag name 'v1.0', got %q", update.Name())
		}
	})

	t.Run("DeleteTag", func(t *testing.T) {
		recorder.reset()

		runHTTPGitCmd(t, workDir, "push", "origin", "--delete", "v1.0")

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
		if update.Name() != "v1.0" {
			t.Errorf("Expected tag name 'v1.0', got %q", update.Name())
		}
	})

	t.Run("CreateAndDeleteBranch", func(t *testing.T) {
		recorder.reset()

		runHTTPGitCmd(t, workDir, "checkout", "-b", "feature-branch")
		if err := os.WriteFile(filepath.Join(workDir, "feature.txt"), []byte("feature\n"), 0644); err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		runHTTPGitCmd(t, workDir, "add", "feature.txt")
		runHTTPGitCmd(t, workDir, "commit", "-m", "Feature commit")
		runHTTPGitCmd(t, workDir, "push", "origin", "feature-branch")

		calls := recorder.getCalls()
		if len(calls) == 0 {
			t.Fatal("Expected receive hook for branch create")
		}
		call := calls[len(calls)-1]
		found := false
		for _, u := range call.updates {
			if u.IsBranch() && u.IsCreate() && u.Name() == "feature-branch" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected branch create for 'feature-branch' in updates: %+v", call.updates)
		}

		// Delete the branch
		recorder.reset()
		runHTTPGitCmd(t, workDir, "checkout", "main")
		runHTTPGitCmd(t, workDir, "push", "origin", "--delete", "feature-branch")

		calls = recorder.getCalls()
		if len(calls) == 0 {
			t.Fatal("Expected receive hook for branch delete")
		}
		call = calls[len(calls)-1]
		found = false
		for _, u := range call.updates {
			if u.IsBranch() && u.IsDelete() && u.Name() == "feature-branch" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected branch delete for 'feature-branch' in updates: %+v", call.updates)
		}
	})
}

// TestHTTPPreReceiveHookReceivesUpdates tests that the pre-receive hook receives ref updates.
func TestHTTPPreReceiveHookReceivesUpdates(t *testing.T) {
	preRecorder := &hookRecorder{}
	postRecorder := &hookRecorder{}

	server, _ := setupHTTPServerWithHooks(t, preRecorder.hook, postRecorder.hook, nil)
	endpoint := server.URL

	clientDir, err := os.MkdirTemp("", "hook-pre-updates-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	createRepoViaAPI(t, endpoint, "pre-upd-user", "pre-upd-model")
	gitURL := endpoint + "/pre-upd-user/pre-upd-model.git"

	workDir := filepath.Join(clientDir, "clone")
	initWorkDir(t, gitURL, workDir)

	if err := os.WriteFile(filepath.Join(workDir, "README.md"), []byte("# Pre-hook Updates Test\n"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	runHTTPGitCmd(t, workDir, "add", "README.md")
	runHTTPGitCmd(t, workDir, "commit", "-m", "Test commit")
	runHTTPGitCmd(t, workDir, "push", "origin", "main")

	// Both pre and post hooks should fire with the same updates
	preCalls := preRecorder.getCalls()
	postCalls := postRecorder.getCalls()

	if len(preCalls) == 0 {
		t.Fatal("Expected pre-receive hook to be called")
	}
	if len(postCalls) == 0 {
		t.Fatal("Expected post-receive hook to be called")
	}

	preUpdate := preCalls[len(preCalls)-1].updates
	postUpdate := postCalls[len(postCalls)-1].updates

	if len(preUpdate) != len(postUpdate) {
		t.Fatalf("Pre and post hooks received different number of updates: pre=%d, post=%d", len(preUpdate), len(postUpdate))
	}

	for i := range preUpdate {
		if preUpdate[i].RefName() != postUpdate[i].RefName() {
			t.Errorf("Update[%d] ref mismatch: pre=%q, post=%q", i, preUpdate[i].RefName(), postUpdate[i].RefName())
		}
		if preUpdate[i].OldRev() != postUpdate[i].OldRev() {
			t.Errorf("Update[%d] old rev mismatch: pre=%q, post=%q", i, preUpdate[i].OldRev(), postUpdate[i].OldRev())
		}
		if preUpdate[i].NewRev() != postUpdate[i].NewRev() {
			t.Errorf("Update[%d] new rev mismatch: pre=%q, post=%q", i, preUpdate[i].NewRev(), postUpdate[i].NewRev())
		}
	}

	if !preUpdate[0].IsBranch() || preUpdate[0].Name() != "main" {
		t.Errorf("Expected branch 'main' in pre-receive updates, got %+v", preUpdate[0])
	}
}

// TestHTTPPreReceiveHookDeniesPush tests that a pre-receive hook can block a push.
func TestHTTPPreReceiveHookDeniesPush(t *testing.T) {
	preHook := func(ctx context.Context, repoName string, updates []receive.RefUpdate) error {
		for _, e := range updates {
			if e.IsTag() {
				return errors.New("tag operations not allowed")
			}
		}
		return nil
	}

	postRecorder := &hookRecorder{}

	server, _ := setupHTTPServerWithHooks(t, preHook, postRecorder.hook, nil)
	endpoint := server.URL

	clientDir, err := os.MkdirTemp("", "hook-deny-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	createRepoViaAPI(t, endpoint, "deny-user", "deny-model")
	gitURL := endpoint + "/deny-user/deny-model.git"

	workDir := filepath.Join(clientDir, "clone")
	initWorkDir(t, gitURL, workDir)

	// Branch push should succeed
	if err := os.WriteFile(filepath.Join(workDir, "README.md"), []byte("# Deny Test\n"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	runHTTPGitCmd(t, workDir, "add", "README.md")
	runHTTPGitCmd(t, workDir, "commit", "-m", "Initial commit")
	runHTTPGitCmd(t, workDir, "push", "origin", "main")

	// Tag push should be denied
	runHTTPGitCmd(t, workDir, "tag", "v1.0")
	cmd := utils.Command(t.Context(), "git", "push", "origin", "v1.0")
	cmd.Dir = workDir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err := cmd.Output()
	if err == nil {
		t.Fatalf("Expected tag push to fail, but it succeeded: %s", output)
	}
}

// TestSSHReceiveHook tests that the receive hook fires with correct ref updates on SSH push.
func TestSSHReceiveHook(t *testing.T) {
	recorder := &hookRecorder{}

	clientDir, err := os.MkdirTemp("", "ssh-hook-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	// Generate client key
	keyFile := filepath.Join(clientDir, "id_ed25519")
	generateTestKeyFile(t, keyFile)

	httpServer, sshListener, _ := setupSSHServerWithHooks(t, nil, recorder.hook, nil)

	addr := sshListener.Addr().(*net.TCPAddr)
	port := strings.Split(addr.String(), ":")[1]
	sshURL := "ssh://git@" + addr.String() + "/"
	env := sshGitEnv(keyFile, port)

	// Create a repo via HTTP API
	createRepoViaAPI(t, httpServer.URL, "ssh-hook-user", "ssh-hook-model")

	workDir := filepath.Join(clientDir, "clone")
	runSSHGitCmd(t, "", env, "clone", sshURL+"ssh-hook-user/ssh-hook-model.git", workDir)
	runSSHGitCmd(t, workDir, env, "config", "user.email", "test@test.com")
	runSSHGitCmd(t, workDir, env, "config", "user.name", "Test User")

	t.Run("PushBranchUpdate", func(t *testing.T) {
		// The repo already has a 'main' branch from the API-created initial commit,
		// so pushing to main is an update, not a create.
		recorder.reset()

		if err := os.WriteFile(filepath.Join(workDir, "README.md"), []byte("# SSH Hook Test\n"), 0644); err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		runSSHGitCmd(t, workDir, env, "add", "README.md")
		runSSHGitCmd(t, workDir, env, "commit", "-m", "Initial SSH commit")
		runSSHGitCmd(t, workDir, env, "push", "origin", "main")

		calls := recorder.getCalls()
		if len(calls) == 0 {
			t.Fatal("Expected receive hook to be called for SSH push")
		}
		call := calls[len(calls)-1]
		if call.repoName != "/ssh-hook-user/ssh-hook-model.git" {
			t.Errorf("Expected repo name '/ssh-hook-user/ssh-hook-model.git', got %q", call.repoName)
		}
		if len(call.updates) == 0 {
			t.Fatal("Expected at least one ref update")
		}
		update := call.updates[0]
		if !update.IsBranch() {
			t.Errorf("Expected branch update, got ref %q", update.RefName())
		}
		if update.IsCreate() {
			t.Errorf("Expected non-create (branch already exists from API)")
		}
		if update.Name() != "main" {
			t.Errorf("Expected branch name 'main', got %q", update.Name())
		}
	})

	t.Run("PushMoreCommits", func(t *testing.T) {
		recorder.reset()

		if err := os.WriteFile(filepath.Join(workDir, "file2.txt"), []byte("second\n"), 0644); err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		runSSHGitCmd(t, workDir, env, "add", "file2.txt")
		runSSHGitCmd(t, workDir, env, "commit", "-m", "Second commit")
		runSSHGitCmd(t, workDir, env, "push", "origin", "main")

		calls := recorder.getCalls()
		if len(calls) == 0 {
			t.Fatal("Expected receive hook to be called")
		}
		call := calls[len(calls)-1]
		if len(call.updates) == 0 {
			t.Fatal("Expected ref update")
		}
		update := call.updates[0]
		if update.IsCreate() || update.IsDelete() {
			t.Errorf("Expected regular push (not create/delete)")
		}
	})

	t.Run("CreateAndDeleteTag", func(t *testing.T) {
		recorder.reset()

		runSSHGitCmd(t, workDir, env, "tag", "v2.0")
		runSSHGitCmd(t, workDir, env, "push", "origin", "v2.0")

		calls := recorder.getCalls()
		if len(calls) == 0 {
			t.Fatal("Expected receive hook for tag push")
		}
		call := calls[len(calls)-1]
		found := false
		for _, u := range call.updates {
			if u.IsTag() && u.IsCreate() && u.Name() == "v2.0" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected tag create for 'v2.0' in updates: %+v", call.updates)
		}

		// Delete tag
		recorder.reset()
		runSSHGitCmd(t, workDir, env, "push", "origin", "--delete", "v2.0")

		calls = recorder.getCalls()
		if len(calls) == 0 {
			t.Fatal("Expected receive hook for tag delete")
		}
		call = calls[len(calls)-1]
		found = false
		for _, u := range call.updates {
			if u.IsTag() && u.IsDelete() && u.Name() == "v2.0" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected tag delete for 'v2.0' in updates: %+v", call.updates)
		}
	})
}

// TestSSHPreReceiveHookDeniesReceivePack tests that the SSH pre-receive hook can deny a push.
func TestSSHPreReceiveHookDeniesReceivePack(t *testing.T) {
	postRecorder := &hookRecorder{}

	preHook := func(ctx context.Context, repoName string, updates []receive.RefUpdate) error {
		// Deny pushes that contain tag updates
		for _, e := range updates {
			if e.IsTag() {
				return errors.New("tag pushes are denied via SSH")
			}
		}
		return nil
	}

	clientDir, err := os.MkdirTemp("", "ssh-deny-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	keyFile := filepath.Join(clientDir, "id_ed25519")
	generateTestKeyFile(t, keyFile)

	httpServer, sshListener, _ := setupSSHServerWithHooks(t, preHook, postRecorder.hook, nil)

	addr := sshListener.Addr().(*net.TCPAddr)
	port := strings.Split(addr.String(), ":")[1]
	sshURL := "ssh://git@" + addr.String() + "/"
	env := sshGitEnv(keyFile, port)

	createRepoViaAPI(t, httpServer.URL, "ssh-deny-user", "ssh-deny-model")

	workDir := filepath.Join(clientDir, "clone")
	runSSHGitCmd(t, "", env, "clone", sshURL+"ssh-deny-user/ssh-deny-model.git", workDir)
	runSSHGitCmd(t, workDir, env, "config", "user.email", "test@test.com")
	runSSHGitCmd(t, workDir, env, "config", "user.name", "Test User")

	// Branch push should succeed
	if err := os.WriteFile(filepath.Join(workDir, "README.md"), []byte("# SSH Deny Test\n"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	runSSHGitCmd(t, workDir, env, "add", "README.md")
	runSSHGitCmd(t, workDir, env, "commit", "-m", "Initial commit")
	runSSHGitCmd(t, workDir, env, "push", "origin", "main")

	// Tag push should be denied
	runSSHGitCmd(t, workDir, env, "tag", "v1.0-denied")
	cmd := utils.Command(t.Context(), "git", "push", "origin", "v1.0-denied")
	cmd.Dir = workDir
	cmd.Env = append(os.Environ(), env...)
	output, err := cmd.Output()
	if err == nil {
		t.Fatalf("Expected tag push to fail via SSH permission hook, but it succeeded: %s", output)
	}
}

// TestHTTPReceiveHookString tests that String produces correct event descriptions.
func TestHTTPReceiveHookString(t *testing.T) {
	var mu sync.Mutex
	var capturedUpdates []receive.RefUpdate

	receiveHook := func(ctx context.Context, repoName string, updates []receive.RefUpdate) error {
		mu.Lock()
		defer mu.Unlock()
		capturedUpdates = append(capturedUpdates, updates...)
		return nil
	}

	server, _ := setupHTTPServerWithHooks(t, nil, receiveHook, nil)
	endpoint := server.URL

	clientDir, err := os.MkdirTemp("", "hook-format-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	createRepoViaAPI(t, endpoint, "fmt-user", "fmt-model")
	gitURL := endpoint + "/fmt-user/fmt-model.git"

	workDir := filepath.Join(clientDir, "clone")
	initWorkDir(t, gitURL, workDir)

	// Create initial commit and push
	if err := os.WriteFile(filepath.Join(workDir, "README.md"), []byte("# Format Test\n"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	runHTTPGitCmd(t, workDir, "add", "README.md")
	runHTTPGitCmd(t, workDir, "commit", "-m", "Initial commit")
	runHTTPGitCmd(t, workDir, "push", "origin", "main")

	// Create and push a tag
	runHTTPGitCmd(t, workDir, "tag", "v1.0")
	runHTTPGitCmd(t, workDir, "push", "origin", "v1.0")

	mu.Lock()
	updates := capturedUpdates
	mu.Unlock()

	// Verify FormatEvent works correctly for captured updates
	for _, u := range updates {
		event := u.String()
		if event == "" {
			t.Errorf("FormatEvent returned empty string for %+v", u)
		}
		// Check that the format is one of the expected patterns
		validPrefixes := []string{"branch_create:", "branch_delete:", "branch_push:", "tag_create:", "tag_delete:", "ref_update:"}
		valid := false
		for _, prefix := range validPrefixes {
			if strings.HasPrefix(event, prefix) {
				valid = true
				break
			}
		}
		if !valid {
			t.Errorf("FormatEvent returned unexpected format %q for %+v", event, u)
		}
	}
}
