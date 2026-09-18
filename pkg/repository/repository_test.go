package repository

import (
	"context"
	"errors"
	iofs "io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"

	"github.com/matrixhub-ai/hfd/internal/lru"
)

func TestDiskUsage(t *testing.T) {
	dir, err := os.MkdirTemp("", "repo-diskusage-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	repo, err := Init(context.Background(), osfs.Default, dir, "main")
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

	repo, err := Init(context.Background(), osfs.Default, dir, "main")
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

func TestWalk(t *testing.T) {
	ctx := context.Background()
	mustInit := func(t *testing.T, fs billy.Filesystem, paths ...string) {
		t.Helper()
		for _, p := range paths {
			if _, err := Init(ctx, fs, p, "main"); err != nil {
				t.Fatalf("Init(%s): %v", p, err)
			}
		}
	}

	t.Run("Layouts", func(t *testing.T) {
		fs := osfs.New(t.TempDir())
		mustInit(t, fs, "/repo.git", "/org/repo.git", "/datasets/org/ds.git", "/org/a/b/c.git", "/ns.git/repo.git")
		var got []string
		err := Walk(ctx, fs, "/", func(path string) error {
			got = append(got, path)
			return nil
		})
		if err != nil {
			t.Fatalf("Walk: %v", err)
		}
		want := []string{"/datasets/org/ds.git", "/ns.git/repo.git", "/org/a/b/c.git", "/org/repo.git", "/repo.git"}
		if !slices.Equal(got, want) {
			t.Fatalf("Walk yielded %v, want %v", got, want)
		}
	})

	t.Run("Noise", func(t *testing.T) {
		dir := t.TempDir()
		fs := osfs.New(dir)
		if err := os.WriteFile(filepath.Join(dir, "README"), []byte("hi"), 0o644); err != nil {
			t.Fatal(err)
		}
		for _, d := range []string{"/empty", "/empty.git"} {
			if err := fs.MkdirAll(d, 0o755); err != nil {
				t.Fatal(err)
			}
		}
		mustInit(t, fs, "/org/repo.git")
		var got []string
		err := Walk(ctx, fs, "/", func(path string) error {
			got = append(got, path)
			return nil
		})
		if err != nil {
			t.Fatalf("Walk: %v", err)
		}
		if want := []string{"/org/repo.git"}; !slices.Equal(got, want) {
			t.Fatalf("Walk yielded %v, want %v", got, want)
		}
	})

	t.Run("MissingRoot", func(t *testing.T) {
		fs := osfs.New(t.TempDir())
		err := Walk(ctx, fs, "/missing", func(path string) error {
			t.Errorf("fn called with %s", path)
			return nil
		})
		if !errors.Is(err, iofs.ErrNotExist) {
			t.Fatalf("Walk = %v, want ErrNotExist", err)
		}
	})

	t.Run("Abort", func(t *testing.T) {
		fs := osfs.New(t.TempDir())
		mustInit(t, fs, "/a/x.git", "/b/y.git")
		errStop := errors.New("stop")
		calls := 0
		err := Walk(ctx, fs, "/", func(string) error {
			calls++
			return errStop
		})
		if err != errStop {
			t.Fatalf("Walk = %v, want errStop unwrapped", err)
		}
		if calls != 1 {
			t.Fatalf("fn called %d times, want 1", calls)
		}
	})

	t.Run("Canceled", func(t *testing.T) {
		fs := osfs.New(t.TempDir())
		mustInit(t, fs, "/org/repo.git")
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		err := Walk(cctx, fs, "/", func(path string) error {
			t.Errorf("fn called with %s after cancel", path)
			return nil
		})
		if err != context.Canceled {
			t.Fatalf("Walk = %v, want context.Canceled", err)
		}
	})

	t.Run("RootIsRepo", func(t *testing.T) {
		fs := osfs.New(t.TempDir())
		mustInit(t, fs, "/org/repo.git")
		var got []string
		err := Walk(ctx, fs, "/org/repo.git", func(path string) error {
			got = append(got, path)
			return nil
		})
		if err != nil {
			t.Fatalf("Walk: %v", err)
		}
		if want := []string{"/org/repo.git"}; !slices.Equal(got, want) {
			t.Fatalf("Walk yielded %v, want %v", got, want)
		}
	})

	t.Run("StopsAtRepository", func(t *testing.T) {
		fs := osfs.New(t.TempDir())
		mustInit(t, fs, "/org/a.git", "/org/a.git/nested.git", "/org/b.git/nested.git", "/org/b.git")
		var got []string
		err := Walk(ctx, fs, "/", func(path string) error {
			got = append(got, path)
			return nil
		})
		if err != nil {
			t.Fatalf("Walk: %v", err)
		}
		want := []string{"/org/a.git", "/org/b.git"}
		if !slices.Equal(got, want) {
			t.Fatalf("Walk yielded %v, want %v", got, want)
		}
		got = nil
		err = Walk(ctx, fs, "/org/a.git", func(path string) error {
			got = append(got, path)
			return nil
		})
		if err != nil {
			t.Fatalf("Walk: %v", err)
		}
		if want := want[:1]; !slices.Equal(got, want) {
			t.Fatalf("Walk from a repository yielded %v, want %v", got, want)
		}
	})

	t.Run("HEADStatError", func(t *testing.T) {
		fs := &headFaultFS{Filesystem: osfs.New(t.TempDir()), err: errors.New("unreadable")}
		mustInit(t, fs, "/a/x.git", "/b/y.git")
		fs.head = "/b/y.git/HEAD"
		var got []string
		err := Walk(ctx, fs, "/", func(path string) error {
			got = append(got, path)
			return nil
		})
		if !errors.Is(err, fs.err) {
			t.Fatalf("Walk = %v, want the HEAD stat error", err)
		}
		if want := []string{"/a/x.git"}; !slices.Equal(got, want) {
			t.Fatalf("Walk yielded %v, want %v", got, want)
		}
	})

	t.Run("LeavesLRUOrder", func(t *testing.T) {
		fs := osfs.New(t.TempDir())
		saved := lruCache
		lruCache = lru.New[cacheKey, *Repository](128)
		t.Cleanup(func() { lruCache = saved })
		// Init opens x then y, so x is the eviction candidate unless the walk (y first) touches the cache.
		mustInit(t, fs, "/b/x.git", "/a/y.git")
		if err := Walk(ctx, fs, "/", func(string) error { return nil }); err != nil {
			t.Fatalf("Walk: %v", err)
		}
		lruCache.RemoveOldest()
		if _, ok := lruCache.Get(cacheKey{fs, "/b/x.git"}); ok {
			t.Fatal("Walk promoted /b/x.git in the LRU: /a/y.git was evicted instead")
		}
	})
}

// headFaultFS fails Stat on one HEAD with a non-NotExist error.
type headFaultFS struct {
	billy.Filesystem
	head string
	err  error
}

func (h *headFaultFS) Stat(name string) (os.FileInfo, error) {
	if name == h.head {
		return nil, &os.PathError{Op: "stat", Path: name, Err: h.err}
	}
	return h.Filesystem.Stat(name)
}
