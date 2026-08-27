package e2e_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/receive"
)

// setupHookServer starts a harness server with the given option, creates the
// hook test repo, and returns the remote for the requested protocol.
func setupHookServer(t *testing.T, sshProto bool, opt e2eOption) (repoURL string, env []string) {
	t.Helper()
	opts := []e2eOption{opt}
	if sshProto {
		opts = append(opts, withSSH())
	}
	s := newE2EServer(t, opts...)
	s.createRepo(t, "hook-org", "hook-repo")
	if sshProto {
		return s.sshRemote("hook-org/hook-repo")
	}
	return s.httpRemote("hook-org/hook-repo")
}

// TestReceiveHooksMatrix tests receive hooks across HTTP and SSH protocols
func TestReceiveHooksMatrix(t *testing.T) {
	protocols := []struct {
		name string
		ssh  bool
	}{
		{name: "HTTP"},
		{name: "SSH", ssh: true},
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
					repoURL, env := setupHookServer(t, protocol.ssh, withHooks(nil, recorder.hook))
					test.test(t, repoURL, env, recorder)
				})
			}
		})
	}
}

// TestPreReceiveHookDenyMatrix tests pre-receive hook denial across protocols
func TestPreReceiveHookDenyMatrix(t *testing.T) {
	protocols := []struct {
		name string
		ssh  bool
	}{
		{name: "HTTP"},
		{name: "SSH", ssh: true},
	}

	for _, protocol := range protocols {
		t.Run(protocol.name, func(t *testing.T) {
			preHook := func(ctx context.Context, repoName string, updates []receive.RefUpdate) (bool, error) {
				for _, e := range updates {
					if e.IsTag() {
						return false, nil
					}
				}
				return true, nil
			}

			postRecorder := &matrixHookRecorder{}
			repoURL, env := setupHookServer(t, protocol.ssh, withHooks(preHook, postRecorder.hook))

			clientDir, err := os.MkdirTemp("", "hook-deny-client")
			if err != nil {
				t.Fatalf("Failed to create temp client dir: %v", err)
			}
			defer os.RemoveAll(clientDir)

			// Clone and push commit (should succeed)
			cloneDir := filepath.Join(clientDir, "clone")
			runGit(t, "", env, "clone", repoURL, cloneDir)
			runGit(t, cloneDir, env, "config", "user.email", "test@test.com")
			runGit(t, cloneDir, env, "config", "user.name", "Test User")

			if err := os.WriteFile(filepath.Join(cloneDir, "README.md"), []byte("# Test\n"), 0644); err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}

			runGit(t, cloneDir, env, "add", "README.md")
			runGit(t, cloneDir, env, "commit", "-m", "Initial commit")
			runGit(t, cloneDir, env, "push", "origin", "main")

			// Tag push should be denied
			runGit(t, cloneDir, env, "tag", "v1.0")
			cmd := exec.CommandContext(t.Context(), "git", "push", "origin", "v1.0")
			cmd.Dir = cloneDir
			cmd.Env = append(testEnv(), env...)
			output, err := cmd.Output()
			if err == nil {
				t.Fatalf("Expected tag push to fail, but it succeeded: %s", output)
			}
		})
	}
}

func testHookBranchPush(t *testing.T, repoURL string, env []string, recorder *matrixHookRecorder) {
	clientDir, err := os.MkdirTemp("", "hook-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	runGit(t, "", env, "clone", repoURL, cloneDir)
	runGit(t, cloneDir, env, "config", "user.email", "test@test.com")
	runGit(t, cloneDir, env, "config", "user.name", "Test User")

	if err := os.WriteFile(filepath.Join(cloneDir, "README.md"), []byte("# Test\n"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	runGit(t, cloneDir, env, "add", "README.md")
	runGit(t, cloneDir, env, "commit", "-m", "Initial commit")
	runGit(t, cloneDir, env, "push", "origin", "main")

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
	runGit(t, "", env, "clone", repoURL, cloneDir)

	runGit(t, cloneDir, env, "tag", "v1.0")
	runGit(t, cloneDir, env, "push", "origin", "v1.0")

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
	runGit(t, "", env, "clone", repoURL, cloneDir)

	runGit(t, cloneDir, env, "push", "origin", "--delete", "v1.0")

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
	runGit(t, "", env, "clone", repoURL, cloneDir)
	runGit(t, cloneDir, env, "config", "user.email", "test@test.com")
	runGit(t, cloneDir, env, "config", "user.name", "Test User")

	runGit(t, cloneDir, env, "checkout", "-b", "feature")
	if err := os.WriteFile(filepath.Join(cloneDir, "feature.txt"), []byte("feature\n"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	runGit(t, cloneDir, env, "add", "feature.txt")
	runGit(t, cloneDir, env, "commit", "-m", "Feature commit")
	runGit(t, cloneDir, env, "push", "origin", "feature")

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
	runGit(t, cloneDir, env, "checkout", "main")
	runGit(t, cloneDir, env, "push", "origin", "--delete", "feature")

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

// matrixHookRecorder records receive hook calls in a thread-safe manner (for matrix tests)
type matrixHookRecorder struct {
	mu    sync.Mutex
	calls []matrixHookCall
}

type matrixHookCall struct {
	repoName string
	updates  []receive.RefUpdate
}

func (r *matrixHookRecorder) hook(ctx context.Context, repoName string, updates []receive.RefUpdate) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, matrixHookCall{repoName: repoName, updates: updates})
	return nil
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
	protocols := []struct {
		name string
		ssh  bool
	}{
		{name: "HTTP"},
		{name: "SSH", ssh: true},
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

			repoURL, env := setupHookServer(t, protocol.ssh, withPermissionHook(permHook))

			clientDir, err := os.MkdirTemp("", "perm-test-client")
			if err != nil {
				t.Fatalf("Failed to create temp client dir: %v", err)
			}
			defer os.RemoveAll(clientDir)

			// Clone should succeed (read permission)
			cloneDir := filepath.Join(clientDir, "clone")
			runGit(t, "", env, "clone", repoURL, cloneDir)

			// Push should fail (write denied)
			runGit(t, cloneDir, env, "config", "user.email", "test@test.com")
			runGit(t, cloneDir, env, "config", "user.name", "Test User")

			if err := os.WriteFile(filepath.Join(cloneDir, "README.md"), []byte("# Test\n"), 0644); err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}

			runGit(t, cloneDir, env, "add", "README.md")
			runGit(t, cloneDir, env, "commit", "-m", "Initial commit")

			cmd := exec.CommandContext(t.Context(), "git", "push", "origin", "main")
			cmd.Dir = cloneDir
			cmd.Env = append(testEnv(), env...)
			output, err := cmd.Output()
			if err == nil {
				t.Fatalf("Expected push to fail due to permission hook, but it succeeded: %s", output)
			}
		})
	}
}
