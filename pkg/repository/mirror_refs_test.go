package repository

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestSyncMirrorRefs(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	upstream := setupMirrorSyncUpstream(t, root)

	mirrorPath := filepath.Join(root, "mirror.git")
	repo, err := InitMirror(ctx, mirrorPath, upstream)
	if err != nil {
		t.Fatalf("init mirror: %v", err)
	}

	remoteRefs, err := GetRemoteRefs(ctx, upstream)
	if err != nil {
		t.Fatalf("remote refs: %v", err)
	}

	refsToSync := []string{"refs/heads/main", "refs/heads/feature", "refs/tags/v1"}
	if err := repo.SyncMirrorRefs(ctx, upstream, refsToSync); err != nil {
		t.Fatalf("sync mirror refs: %v", err)
	}

	localRefs, err := repo.Refs()
	if err != nil {
		t.Fatalf("local refs: %v", err)
	}

	expected := make(map[string]string, len(refsToSync))
	for _, ref := range refsToSync {
		expected[ref] = remoteRefs[ref]
	}

	for ref, want := range expected {
		if got, ok := localRefs[ref]; !ok {
			t.Fatalf("expected %s to be fetched", ref)
		} else if got != want {
			t.Fatalf("ref %s mismatch: got %s, want %s", ref, got, want)
		}
	}
	if len(localRefs) != len(expected) {
		t.Fatalf("unexpected refs present after sync: got %d, want %d (%v)", len(localRefs), len(expected), localRefs)
	}

	mainHash := localRefs["refs/heads/main"]
	runGit(t, mirrorPath, "update-ref", "refs/heads/stale", mainHash)

	if err := repo.SyncMirrorRefs(ctx, upstream, []string{"refs/heads/main", "refs/tags/v1"}); err != nil {
		t.Fatalf("resync mirror refs: %v", err)
	}

	prunedRefs, err := repo.Refs()
	if err != nil {
		t.Fatalf("pruned refs: %v", err)
	}

	expectedAfterPrune := map[string]string{
		"refs/heads/main": remoteRefs["refs/heads/main"],
		"refs/tags/v1":    remoteRefs["refs/tags/v1"],
	}
	for ref, want := range expectedAfterPrune {
		if got, ok := prunedRefs[ref]; !ok {
			t.Fatalf("expected %s to remain after prune", ref)
		} else if got != want {
			t.Fatalf("ref %s mismatch after prune: got %s, want %s", ref, got, want)
		}
	}
	if len(prunedRefs) != len(expectedAfterPrune) {
		t.Fatalf("unexpected refs present after prune: got %d, want %d (%v)", len(prunedRefs), len(expectedAfterPrune), prunedRefs)
	}
}

func setupMirrorSyncUpstream(t *testing.T, root string) string {
	t.Helper()

	upstream := filepath.Join(root, "upstream.git")
	runGit(t, "", "init", "--bare", "--initial-branch=main", upstream)

	work := filepath.Join(root, "work")
	runGit(t, "", "init", "--initial-branch=main", work)
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test User")

	if err := os.WriteFile(filepath.Join(work, "file.txt"), []byte("main\n"), 0o644); err != nil {
		t.Fatalf("write main file: %v", err)
	}

	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", "initial")
	runGit(t, work, "remote", "add", "origin", upstream)
	runGit(t, work, "push", "-u", "origin", "main")
	runGit(t, work, "tag", "v1")
	runGit(t, work, "push", "origin", "v1")

	runGit(t, work, "checkout", "-b", "feature")
	if err := os.WriteFile(filepath.Join(work, "file.txt"), []byte("feature\n"), 0o644); err != nil {
		t.Fatalf("write feature file: %v", err)
	}
	runGit(t, work, "commit", "-am", "feature change")
	runGit(t, work, "push", "-u", "origin", "feature")

	runGit(t, work, "checkout", "main")
	runGit(t, work, "checkout", "-b", "other")
	if err := os.WriteFile(filepath.Join(work, "file.txt"), []byte("other\n"), 0o644); err != nil {
		t.Fatalf("write other file: %v", err)
	}
	runGit(t, work, "commit", "-am", "other change")
	runGit(t, work, "push", "-u", "origin", "other")

	runGit(t, work, "checkout", "main")
	runGit(t, work, "checkout", "-b", "noise/deep")
	if err := os.WriteFile(filepath.Join(work, "file.txt"), []byte("noise\n"), 0o644); err != nil {
		t.Fatalf("write noise file: %v", err)
	}
	runGit(t, work, "commit", "-am", "noise change")
	runGit(t, work, "push", "-u", "origin", "noise/deep")
	runGit(t, work, "tag", "v2")
	runGit(t, work, "push", "origin", "v2")

	return upstream
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
}
