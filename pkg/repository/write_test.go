package repository

import (
	"context"
	"io"
	"strings"
	"testing"
)

func initTestRepo(t *testing.T) *Repository {
	t.Helper()
	repo, err := Init(context.Background(), t.TempDir(), "main")
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
	if !strings.Contains(err.Error(), "expected parent commit") {
		t.Fatalf("unexpected error: %v", err)
	}

	// Tip must be unchanged after the failed commit.
	if got, err := repo.ResolveRevision("main"); err != nil {
		t.Fatalf("resolve main: %v", err)
	} else if got != hash2 {
		t.Fatalf("branch tip = %s, want %s", got, hash2)
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
