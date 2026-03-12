package repository

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestDiskUsage(t *testing.T) {
	dir, err := os.MkdirTemp("", "repo-diskusage-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	repo, err := Init(context.Background(), dir, "main")
	if err != nil {
		t.Fatalf("Failed to init repo: %v", err)
	}

	usage, err := repo.DiskUsage()
	if err != nil {
		t.Fatalf("DiskUsage returned error: %v", err)
	}
	if usage <= 0 {
		t.Errorf("Expected DiskUsage > 0 for non-empty repo dir, got %d", usage)
	}

	// Add a file directly to the repo directory and verify usage increases.
	testFile := filepath.Join(dir, "testfile")
	content := make([]byte, 1024)
	if err := os.WriteFile(testFile, content, 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	usage2, err := repo.DiskUsage()
	if err != nil {
		t.Fatalf("DiskUsage returned error after adding file: %v", err)
	}
	if usage2 <= usage {
		t.Errorf("Expected DiskUsage to increase after adding a file: before=%d, after=%d", usage, usage2)
	}
}

func TestDiskUsageIncludesLFSSize(t *testing.T) {
	dir, err := os.MkdirTemp("", "repo-diskusage-lfs-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	repo, err := Init(context.Background(), dir, "main")
	if err != nil {
		t.Fatalf("Failed to init repo: %v", err)
	}

	// Commit a regular file first to get a baseline
	if _, err := repo.CreateCommit(context.Background(), "main", "init", "Test", "test@test.com",
		[]CommitOperation{{Type: CommitOperationAdd, Path: "README.md", Content: []byte("# Test\n")}}, ""); err != nil {
		t.Fatalf("Failed to create initial commit: %v", err)
	}

	usageBefore, err := repo.DiskUsage()
	if err != nil {
		t.Fatalf("DiskUsage returned error: %v", err)
	}

	// Commit an LFS pointer file with a declared size of 10 MB
	const lfsSize = 10 * 1024 * 1024
	lfsPointer := "version https://git-lfs.github.com/spec/v1\n" +
		"oid sha256:4d7a214614ab2935c943f9e0ff69d22eadbb8f32b1258daaa5e2ca24d17e2393\n" +
		"size 10485760\n"

	if _, err := repo.CreateCommit(context.Background(), "main", "add lfs file", "Test", "test@test.com",
		[]CommitOperation{{Type: CommitOperationAdd, Path: "model.bin", Content: []byte(lfsPointer)}}, ""); err != nil {
		t.Fatalf("Failed to commit LFS pointer: %v", err)
	}

	usageAfter, err := repo.DiskUsage()
	if err != nil {
		t.Fatalf("DiskUsage returned error after LFS commit: %v", err)
	}

	// The declared LFS size is 10 MB; usage must increase by at least that amount.
	if usageAfter-usageBefore < lfsSize {
		t.Errorf("Expected DiskUsage to include LFS size (%d): before=%d, after=%d, delta=%d",
			lfsSize, usageBefore, usageAfter, usageAfter-usageBefore)
	}
}
