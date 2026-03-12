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

	remoteRefs, err := repo.RemoteRefs(ctx, upstream)
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

	for _, ref := range refsToSync {
		if got, ok := localRefs[ref]; !ok {
			t.Fatalf("expected %s to be fetched", ref)
		} else if want := remoteRefs[ref]; got != want {
			t.Fatalf("ref %s mismatch: got %s, want %s", ref, got, want)
		}
	}

	if _, ok := localRefs["refs/heads/other"]; ok {
		t.Fatalf("unexpected ref synced: refs/heads/other")
	}

	if _, err := os.Stat(filepath.Join(mirrorPath, "refs", "heads", "other")); err == nil {
		t.Fatalf("unexpected ref file present for refs/heads/other")
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

	if _, ok := prunedRefs["refs/heads/feature"]; ok {
		t.Fatalf("expected refs/heads/feature to be pruned")
	}
	if _, ok := prunedRefs["refs/heads/stale"]; ok {
		t.Fatalf("expected refs/heads/stale to be pruned")
	}

	if got := prunedRefs["refs/heads/main"]; got != remoteRefs["refs/heads/main"] {
		t.Fatalf("main ref mismatch after prune: got %s, want %s", got, remoteRefs["refs/heads/main"])
	}
	if got := prunedRefs["refs/tags/v1"]; got != remoteRefs["refs/tags/v1"] {
		t.Fatalf("tag ref mismatch after prune: got %s, want %s", got, remoteRefs["refs/tags/v1"])
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
