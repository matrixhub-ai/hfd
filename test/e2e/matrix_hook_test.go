package e2e_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/wzshiming/hfd/internal/utils"
	"github.com/wzshiming/hfd/pkg/receive"
)

// TestMatrixReceiveHooks tests that receive hooks fire correctly across backends
func TestMatrixReceiveHooks(t *testing.T) {
	recorder := &hookRecorder{}
	opts := &BackendOptions{
		PostReceiveHook: recorder.hook,
	}

	RunMatrixTests(t, []BackendType{BackendHTTP, BackendSSH}, opts, func(t *testing.T, backend *TestBackend) {
		clientDir, err := os.MkdirTemp("", "matrix-hook")
		if err != nil {
			t.Fatalf("Failed to create client dir: %v", err)
		}
		defer os.RemoveAll(clientDir)

		backend.CreateRepo("hook-org", "hook-repo")
		repoPath := "hook-org/hook-repo"

		workDir := filepath.Join(clientDir, "work")
		backend.InitWorkDir(repoPath, workDir)

		// Expected repo name depends on backend type
		expectedRepoName := repoPath
		if backend.Type == BackendSSH {
			expectedRepoName = "/" + repoPath + ".git"
		}

		t.Run("BranchPush", func(t *testing.T) {
			recorder.reset()

			testFile := filepath.Join(workDir, "README.md")
			if err := os.WriteFile(testFile, []byte("# Hook Test\n"), 0644); err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}
			backend.RunGitCmd(workDir, "add", "README.md")
			backend.RunGitCmd(workDir, "commit", "-m", "Initial commit")
			backend.RunGitCmd(workDir, "push", "origin", "main")

			calls := recorder.getCalls()
			if len(calls) == 0 {
				t.Fatal("Expected receive hook to be called")
			}
			call := calls[len(calls)-1]
			if call.repoName != expectedRepoName {
				t.Errorf("Expected repo name %q, got %q", expectedRepoName, call.repoName)
			}
			if len(call.updates) == 0 {
				t.Fatal("Expected at least one ref update")
			}
			update := call.updates[0]
			if !update.IsBranch() {
				t.Errorf("Expected branch update, got ref %q", update.RefName())
			}
			if update.Name() != "main" {
				t.Errorf("Expected branch name 'main', got %q", update.Name())
			}
		})

		t.Run("TagCreate", func(t *testing.T) {
			recorder.reset()

			backend.RunGitCmd(workDir, "tag", "v1.0")
			backend.RunGitCmd(workDir, "push", "origin", "v1.0")

			calls := recorder.getCalls()
			if len(calls) == 0 {
				t.Fatal("Expected receive hook for tag push")
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

		t.Run("TagDelete", func(t *testing.T) {
			recorder.reset()

			backend.RunGitCmd(workDir, "push", "origin", "--delete", "v1.0")

			calls := recorder.getCalls()
			if len(calls) == 0 {
				t.Fatal("Expected receive hook for tag delete")
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

		t.Run("BranchCreateAndDelete", func(t *testing.T) {
			recorder.reset()

			backend.RunGitCmd(workDir, "checkout", "-b", "feature-branch")
			featureFile := filepath.Join(workDir, "feature.txt")
			if err := os.WriteFile(featureFile, []byte("feature\n"), 0644); err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}
			backend.RunGitCmd(workDir, "add", "feature.txt")
			backend.RunGitCmd(workDir, "commit", "-m", "Feature commit")
			backend.RunGitCmd(workDir, "push", "origin", "feature-branch")

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
				t.Errorf("Expected branch create for 'feature-branch'")
			}

			// Delete the branch
			recorder.reset()
			backend.RunGitCmd(workDir, "checkout", "main")
			backend.RunGitCmd(workDir, "push", "origin", "--delete", "feature-branch")

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
				t.Errorf("Expected branch delete for 'feature-branch'")
			}
		})
	})
}

// TestMatrixPreReceiveHook tests that pre-receive hooks can deny pushes
func TestMatrixPreReceiveHook(t *testing.T) {
	postRecorder := &hookRecorder{}

	preHook := func(ctx context.Context, repoName string, updates []receive.RefUpdate) error {
		for _, e := range updates {
			if e.IsTag() {
				return errors.New("tag operations not allowed")
			}
		}
		return nil
	}

	opts := &BackendOptions{
		PreReceiveHook:  preHook,
		PostReceiveHook: postRecorder.hook,
	}

	RunMatrixTests(t, []BackendType{BackendHTTP, BackendSSH}, opts, func(t *testing.T, backend *TestBackend) {
		clientDir, err := os.MkdirTemp("", "matrix-pre-hook")
		if err != nil {
			t.Fatalf("Failed to create client dir: %v", err)
		}
		defer os.RemoveAll(clientDir)

		backend.CreateRepo("deny-org", "deny-repo")
		repoPath := "deny-org/deny-repo"

		workDir := filepath.Join(clientDir, "work")
		backend.InitWorkDir(repoPath, workDir)

		t.Run("BranchPushSucceeds", func(t *testing.T) {
			testFile := filepath.Join(workDir, "README.md")
			if err := os.WriteFile(testFile, []byte("# Deny Test\n"), 0644); err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}
			backend.RunGitCmd(workDir, "add", "README.md")
			backend.RunGitCmd(workDir, "commit", "-m", "Initial commit")
			backend.RunGitCmd(workDir, "push", "origin", "main")
		})

		t.Run("TagPushDenied", func(t *testing.T) {
			backend.RunGitCmd(workDir, "tag", "v1.0")

			// This should fail
			cmd := utils.Command(t.Context(), "git", "push", "origin", "v1.0")
			cmd.Dir = workDir
			cmd.Env = append(os.Environ(), backend.GitEnv()...)
			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("Expected tag push to fail, but it succeeded: %s", output)
			}
		})
	})
}

// TestMatrixPrePostReceiveHooks tests that pre and post receive hooks both fire
func TestMatrixPrePostReceiveHooks(t *testing.T) {
	preRecorder := &hookRecorder{}
	postRecorder := &hookRecorder{}

	opts := &BackendOptions{
		PreReceiveHook:  preRecorder.hook,
		PostReceiveHook: postRecorder.hook,
	}

	RunMatrixTests(t, []BackendType{BackendHTTP, BackendSSH}, opts, func(t *testing.T, backend *TestBackend) {
		clientDir, err := os.MkdirTemp("", "matrix-pre-post")
		if err != nil {
			t.Fatalf("Failed to create client dir: %v", err)
		}
		defer os.RemoveAll(clientDir)

		backend.CreateRepo("pre-post-org", "pre-post-repo")
		repoPath := "pre-post-org/pre-post-repo"

		workDir := filepath.Join(clientDir, "work")
		backend.InitWorkDir(repoPath, workDir)

		testFile := filepath.Join(workDir, "README.md")
		if err := os.WriteFile(testFile, []byte("# Pre-Post Test\n"), 0644); err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		backend.RunGitCmd(workDir, "add", "README.md")
		backend.RunGitCmd(workDir, "commit", "-m", "Test commit")
		backend.RunGitCmd(workDir, "push", "origin", "main")

		// Both pre and post hooks should fire
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

		// Verify updates are the same
		for i := range preUpdate {
			if preUpdate[i].RefName() != postUpdate[i].RefName() {
				t.Errorf("Update[%d] ref mismatch: pre=%q, post=%q", i, preUpdate[i].RefName(), postUpdate[i].RefName())
			}
			if preUpdate[i].OldRev() != postUpdate[i].OldRev() {
				t.Errorf("Update[%d] old rev mismatch", i)
			}
			if preUpdate[i].NewRev() != postUpdate[i].NewRev() {
				t.Errorf("Update[%d] new rev mismatch", i)
			}
		}
	})
}
