package mirror

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOpenOrSyncRespectsTTL(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	upstream := setupUpstreamRepo(t, root)
	mirrorPath := filepath.Join(root, "mirror.git")

	m := NewMirror(
		WithMirrorSourceFunc(func(ctx context.Context, repoName string) (string, bool, error) {
			return upstream, true, nil
		}),
		WithTTL(50*time.Millisecond),
	)

	if _, err := m.OpenOrSync(ctx, mirrorPath, "sample"); err != nil {
		t.Fatalf("initial sync failed: %v", err)
	}

	if err := os.RemoveAll(upstream); err != nil {
		t.Fatalf("remove upstream: %v", err)
	}

	t.Run("skip sync within ttl", func(t *testing.T) {
		if _, err := m.OpenOrSync(ctx, mirrorPath, "sample"); err != nil {
			t.Fatalf("expected mirror access to succeed within TTL, got error: %v", err)
		}
	})

	t.Run("sync after ttl expires", func(t *testing.T) {
		time.Sleep(60 * time.Millisecond)
		if _, err := m.OpenOrSync(ctx, mirrorPath, "sample"); err == nil {
			t.Fatalf("expected mirror sync to fail after TTL expiry when upstream is missing")
		}
	})
}

func setupUpstreamRepo(t *testing.T, root string) string {
	t.Helper()

	upstream := filepath.Join(root, "upstream.git")
	git(t, "", "init", "--bare", "--initial-branch=main", upstream)

	work := filepath.Join(root, "work")
	git(t, "", "init", "--initial-branch=main", work)
	git(t, work, "config", "user.email", "test@example.com")
	git(t, work, "config", "user.name", "Test User")

	if err := os.WriteFile(filepath.Join(work, "file.txt"), []byte("content"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	git(t, work, "add", ".")
	git(t, work, "commit", "-m", "initial")
	git(t, work, "remote", "add", "origin", upstream)
	git(t, work, "push", "-u", "origin", "main")

	return upstream
}

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
}
