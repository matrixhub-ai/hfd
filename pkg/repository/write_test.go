package repository

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6/storage/filesystem/dotgit"
)

func initTestRepo(t *testing.T) *Repository {
	t.Helper()
	repo, err := Init(context.Background(), osfs.Default, t.TempDir(), "main")
	if err != nil {
		t.Fatalf("init repo: %v", err)
	}
	return repo
}

func mustCommit(t *testing.T, repo *Repository, rev, parent string, ops ...CommitOperation) string {
	t.Helper()
	hash, err := repo.CreateCommit(context.Background(), rev, "test commit", "Test", "test@test.com", ops, parent)
	if err != nil {
		t.Fatalf("create commit: %v", err)
	}
	return hash
}

func readBlob(t *testing.T, repo *Repository, rev, path string) string {
	t.Helper()
	blob, err := repo.Blob(rev, path)
	if err != nil {
		t.Fatalf("blob %s@%s: %v", path, rev, err)
	}
	r, err := blob.NewReader()
	if err != nil {
		t.Fatalf("blob reader %s@%s: %v", path, rev, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read blob %s@%s: %v", path, rev, err)
	}
	return string(data)
}

func TestCreateCommitAddAndRead(t *testing.T) {
	repo := initTestRepo(t)

	hash := mustCommit(t, repo, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "README.md", Content: []byte("# Test\n")},
		CommitOperation{Type: CommitOperationAdd, Path: "a/b/c.txt", Content: []byte("nested\n")},
	)

	if got, err := repo.ResolveRevision("main"); err != nil {
		t.Fatalf("resolve main: %v", err)
	} else if got != hash {
		t.Fatalf("branch tip = %s, want %s", got, hash)
	}

	if got := readBlob(t, repo, "main", "README.md"); got != "# Test\n" {
		t.Fatalf("README.md = %q, want %q", got, "# Test\n")
	}
	if got := readBlob(t, repo, "main", "a/b/c.txt"); got != "nested\n" {
		t.Fatalf("a/b/c.txt = %q, want %q", got, "nested\n")
	}
}

func TestCreateCommitUpdateFile(t *testing.T) {
	repo := initTestRepo(t)

	mustCommit(t, repo, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "file.txt", Content: []byte("v1\n")})
	hash2 := mustCommit(t, repo, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "file.txt", Content: []byte("v2\n")})

	if got, err := repo.ResolveRevision("main"); err != nil {
		t.Fatalf("resolve main: %v", err)
	} else if got != hash2 {
		t.Fatalf("branch tip = %s, want %s", got, hash2)
	}
	if got := readBlob(t, repo, "main", "file.txt"); got != "v2\n" {
		t.Fatalf("file.txt = %q, want %q", got, "v2\n")
	}
}

func TestCreateCommitDeleteFile(t *testing.T) {
	repo := initTestRepo(t)

	mustCommit(t, repo, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "keep.txt", Content: []byte("keep\n")},
		CommitOperation{Type: CommitOperationAdd, Path: "drop.txt", Content: []byte("drop\n")},
	)
	mustCommit(t, repo, "main", "",
		CommitOperation{Type: CommitOperationDelete, Path: "drop.txt"})

	if got := readBlob(t, repo, "main", "keep.txt"); got != "keep\n" {
		t.Fatalf("keep.txt = %q, want %q", got, "keep\n")
	}
	if _, err := repo.Blob("main", "drop.txt"); err == nil {
		t.Fatal("expected error reading deleted file, got nil")
	}
}

func TestCreateCommitParentCheck(t *testing.T) {
	repo := initTestRepo(t)

	hash1 := mustCommit(t, repo, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "file.txt", Content: []byte("v1\n")})
	hash2 := mustCommit(t, repo, "main", hash1,
		CommitOperation{Type: CommitOperationAdd, Path: "file.txt", Content: []byte("v2\n")})

	// Stale parent must be rejected (optimistic concurrency).
	_, err := repo.CreateCommit(context.Background(), "main", "stale", "Test", "test@test.com",
		[]CommitOperation{{Type: CommitOperationAdd, Path: "file.txt", Content: []byte("v3\n")}}, hash1)
	if err == nil {
		t.Fatal("expected error for stale parent commit, got nil")
	}
	if !errors.Is(err, ErrParentMismatch) {
		t.Fatalf("error = %v, want ErrParentMismatch", err)
	}
	if !strings.Contains(err.Error(), hash1) || !strings.Contains(err.Error(), hash2) {
		t.Fatalf("error %q must name expected %s and actual %s", err, hash1, hash2)
	}

	// Tip must be unchanged after the failed commit.
	if got, err := repo.ResolveRevision("main"); err != nil {
		t.Fatalf("resolve main: %v", err)
	} else if got != hash2 {
		t.Fatalf("branch tip = %s, want %s", got, hash2)
	}
}

func TestCreateCommitUnreadableRefIsNotParentMismatch(t *testing.T) {
	dir := t.TempDir()
	repo, err := Init(context.Background(), osfs.Default, dir, "main")
	if err != nil {
		t.Fatalf("init repo: %v", err)
	}
	hash := mustCommit(t, repo, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "file.txt", Content: []byte("v1\n")})

	// go-git only consults packed-refs once the loose ref is gone.
	loose := filepath.Join(dir, "refs", "heads", "main")
	if err := os.Remove(loose); err != nil {
		t.Fatalf("remove loose ref: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "packed-refs"), []byte("# pack-refs with: peeled fully-peeled sorted \n"+hash+"\n"), 0o644); err != nil {
		t.Fatalf("write packed-refs: %v", err)
	}

	_, err = repo.CreateCommit(context.Background(), "main", "update", "Test", "test@test.com",
		[]CommitOperation{{Type: CommitOperationAdd, Path: "file.txt", Content: []byte("v2\n")}}, hash)
	if !errors.Is(err, dotgit.ErrPackedRefsBadFormat) || errors.Is(err, ErrParentMismatch) {
		t.Fatalf("error = %v, want ErrPackedRefsBadFormat and not ErrParentMismatch", err)
	}
	if _, err := os.Stat(loose); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refs/heads/main after failed commit: %v, want absent", err)
	}
}

func TestCreateCommitUnsupportedOperation(t *testing.T) {
	repo := initTestRepo(t)

	_, err := repo.CreateCommit(context.Background(), "main", "bad", "Test", "test@test.com",
		[]CommitOperation{{Type: CommitOperationType("rename"), Path: "x"}}, "")
	if err == nil {
		t.Fatal("expected error for unsupported operation type, got nil")
	}
}

func TestCreateCommitEmptyRevUsesDefaultBranch(t *testing.T) {
	repo := initTestRepo(t)

	hash := mustCommit(t, repo, "", "",
		CommitOperation{Type: CommitOperationAdd, Path: "file.txt", Content: []byte("data\n")})

	refs, err := repo.Refs()
	if err != nil {
		t.Fatalf("refs: %v", err)
	}
	if refs["refs/heads/main"] != hash {
		t.Fatalf("refs/heads/main = %s, want %s", refs["refs/heads/main"], hash)
	}
}

func TestCreateCommitNewBranchIsOrphan(t *testing.T) {
	repo := initTestRepo(t)

	mustCommit(t, repo, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "main.txt", Content: []byte("main\n")})
	mustCommit(t, repo, "dev", "",
		CommitOperation{Type: CommitOperationAdd, Path: "dev.txt", Content: []byte("dev\n")})

	// Orphan branch must not inherit files from main.
	if _, err := repo.Blob("dev", "main.txt"); err == nil {
		t.Fatal("expected main.txt to be absent on orphan branch dev")
	}
	if got := readBlob(t, repo, "dev", "dev.txt"); got != "dev\n" {
		t.Fatalf("dev.txt = %q, want %q", got, "dev\n")
	}
}
