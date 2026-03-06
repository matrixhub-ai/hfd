package repository

import (
	"context"
	"io"
	"os"
	"testing"
)

func TestEnsureLFSConfig(t *testing.T) {
	dir, err := os.MkdirTemp("", "repo-lfsconfig-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	repo, err := Init(dir, "main")
	if err != nil {
		t.Fatalf("Failed to init repository: %v", err)
	}

	ctx := context.Background()
	lfsHref := "http://localhost:8080/test-repo.git/info/lfs"

	t.Run("NoOpWhenNoCommits", func(t *testing.T) {
		err := repo.EnsureLFSConfig(ctx, lfsHref)
		if err != nil {
			t.Fatalf("EnsureLFSConfig should not fail on empty repo: %v", err)
		}

		// Verify .lfsconfig does not exist (no commits to attach it to)
		_, err = repo.Blob("", ".lfsconfig")
		if err == nil {
			t.Error(".lfsconfig should not exist in an empty repo")
		}
	})

	// Create an initial commit
	_, err = repo.CreateCommit(ctx, "main", "Initial commit", "Test", "test@test.com",
		[]CommitOperation{{Type: CommitOperationAdd, Path: "README.md", Content: []byte("# Test\n")}}, "")
	if err != nil {
		t.Fatalf("Failed to create initial commit: %v", err)
	}

	t.Run("CreatesLFSConfig", func(t *testing.T) {
		err := repo.EnsureLFSConfig(ctx, lfsHref)
		if err != nil {
			t.Fatalf("EnsureLFSConfig failed: %v", err)
		}

		// Verify .lfsconfig exists with correct content
		blob, err := repo.Blob("", ".lfsconfig")
		if err != nil {
			t.Fatalf("Failed to read .lfsconfig: %v", err)
		}

		reader, err := blob.NewReader()
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		content, _ := io.ReadAll(reader)
		reader.Close()

		if got := string(content); got != "[lfs]\n\turl = "+lfsHref+"\n" {
			t.Errorf("Unexpected .lfsconfig content: %q", got)
		}
	})

	t.Run("IdempotentWithSameURL", func(t *testing.T) {
		// Get current HEAD before calling EnsureLFSConfig
		headBefore, err := repo.ResolveRevision("")
		if err != nil {
			t.Fatalf("Failed to resolve HEAD: %v", err)
		}

		err = repo.EnsureLFSConfig(ctx, lfsHref)
		if err != nil {
			t.Fatalf("EnsureLFSConfig failed: %v", err)
		}

		// HEAD should not change (no new commit)
		headAfter, err := repo.ResolveRevision("")
		if err != nil {
			t.Fatalf("Failed to resolve HEAD: %v", err)
		}

		if headBefore != headAfter {
			t.Errorf("HEAD changed from %s to %s; EnsureLFSConfig should be idempotent", headBefore, headAfter)
		}
	})

	t.Run("UpdatesWhenURLChanges", func(t *testing.T) {
		headBefore, err := repo.ResolveRevision("")
		if err != nil {
			t.Fatalf("Failed to resolve HEAD: %v", err)
		}

		newHref := "http://newserver:9090/test-repo.git/info/lfs"
		err = repo.EnsureLFSConfig(ctx, newHref)
		if err != nil {
			t.Fatalf("EnsureLFSConfig failed: %v", err)
		}

		// HEAD should change (new commit with updated URL)
		headAfter, err := repo.ResolveRevision("")
		if err != nil {
			t.Fatalf("Failed to resolve HEAD: %v", err)
		}

		if headBefore == headAfter {
			t.Error("HEAD should change when LFS URL is updated")
		}

		// Verify new URL
		blob, err := repo.Blob("", ".lfsconfig")
		if err != nil {
			t.Fatalf("Failed to read .lfsconfig: %v", err)
		}

		reader, err := blob.NewReader()
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		content, _ := io.ReadAll(reader)
		reader.Close()

		if got := string(content); got != "[lfs]\n\turl = "+newHref+"\n" {
			t.Errorf("Unexpected .lfsconfig content: %q", got)
		}
	})
}
