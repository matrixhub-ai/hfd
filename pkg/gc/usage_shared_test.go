package gc

import (
	"context"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"

	"github.com/matrixhub-ai/hfd/pkg/repository"
)

type cancelOnReadDir struct {
	billy.Filesystem
	pattern string
	cancel  context.CancelFunc
}

func (storage cancelOnReadDir) ReadDir(name string) ([]fs.DirEntry, error) {
	if ok, _ := filepath.Match(storage.pattern, name); ok {
		storage.cancel()
	}
	return storage.Filesystem.ReadDir(name)
}

// sharedCommit creates name with one commit on main, messaged name, adding each file with its own name as content.
func sharedCommit(t *testing.T, repos billy.Filesystem, name string, files ...string) {
	t.Helper()
	ctx := context.Background()
	repo, err := repository.Init(ctx, repos, name, "main")
	if err != nil {
		t.Fatalf("init %s: %v", name, err)
	}
	var ops []repository.CommitOperation
	for _, file := range files {
		ops = append(ops, repository.CommitOperation{Type: repository.CommitOperationAdd, Path: file, Content: []byte(file + "\n")})
	}
	if _, err := repo.CreateCommit(ctx, "main", name, "Test", "test@test.com", ops, ""); err != nil {
		t.Fatalf("commit %s: %v", name, err)
	}
}

// sharedPoolFiles sums the loose objects of the shared store under root as the host lists them.
func sharedPoolFiles(t *testing.T, root string) repository.ObjectUsage {
	t.Helper()
	objects := filepath.Join(root, "git", "sha1", "objects")
	var usage repository.ObjectUsage
	err := filepath.WalkDir(objects, func(path string, e fs.DirEntry, err error) error {
		if err != nil || !e.Type().IsRegular() {
			return err
		}
		rel, err := filepath.Rel(objects, path)
		if err != nil {
			return err
		}
		fan, name, _ := strings.Cut(filepath.ToSlash(rel), "/")
		if _, err := hex.DecodeString(fan + name); err != nil || len(fan) != 2 || len(name) != 38 {
			return nil
		}
		info, err := e.Info()
		if err != nil {
			return err
		}
		usage.Count++
		usage.Bytes += info.Size()
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", objects, err)
	}
	return usage
}

// requireSharedObjects asserts g reports the shared store under root as the host lists it, holding count objects.
func requireSharedObjects(t *testing.T, g *GC, root string, count int64) {
	t.Helper()
	want := sharedPoolFiles(t, root)
	if want.Count != count {
		t.Fatalf("shared store holds %d objects, want the %d written", want.Count, count)
	}
	if got, err := g.SharedObjectsUsage(context.Background()); err != nil || got != want {
		t.Fatalf("SharedObjectsUsage = %+v, %v; want %+v, nil", got, err, want)
	}
}

func TestSharedObjectsUsage(t *testing.T) {
	ctx := context.Background()
	t.Run("Unbound", func(t *testing.T) {
		repos := osfs.New(t.TempDir())
		sharedCommit(t, repos, "/org/a.git", "a.txt")
		if got, err := NewGC(repos, nil).SharedObjectsUsage(ctx); err != nil || got != (repository.ObjectUsage{}) {
			t.Fatalf("SharedObjectsUsage = %+v, %v; want zero, nil for a filesystem sharing no store", got, err)
		}
	})
	t.Run("MissingPool", func(t *testing.T) {
		repos := repository.BindSharedObjects(osfs.New(t.TempDir()), "/repositories", "/git/sha1")
		if got, err := NewGC(repos, nil).SharedObjectsUsage(ctx); err != nil || got != (repository.ObjectUsage{}) {
			t.Fatalf("SharedObjectsUsage = %+v, %v; want zero, nil before any repository creates the store", got, err)
		}
	})
	t.Run("SumsPool", func(t *testing.T) {
		root := t.TempDir()
		repos := repository.BindSharedObjects(osfs.New(root), "/repositories", "/git/sha1")
		sharedCommit(t, repos, "/org/a.git", "shared.txt")
		sharedCommit(t, repos, "/org/b.git", "shared.txt", "b.txt")
		g := NewGC(repos, nil)
		// Both repositories hold the shared.txt blob, stored once: two commits, two trees, two blobs.
		requireSharedObjects(t, g, root, 6)
		if got, err := g.Usage(ctx); err != nil || got.Objects.Count != 0 {
			t.Fatalf("Usage = %+v, %v; want no repository counting a pooled object", got, err)
		}
	})
	t.Run("Canceled", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		repos := repository.BindSharedObjects(osfs.New(t.TempDir()), "/repositories", "/git/sha1")
		g := NewGC(repos, nil)
		if _, err := g.SharedObjectsUsage(canceled); !errors.Is(err, context.Canceled) {
			t.Errorf("missing store: SharedObjectsUsage error = %v; want context.Canceled", err)
		}
		sharedCommit(t, repos, "/org/a.git", "a.txt")
		if _, err := g.SharedObjectsUsage(canceled); !errors.Is(err, context.Canceled) {
			t.Errorf("populated store: SharedObjectsUsage error = %v; want context.Canceled", err)
		}
	})
	t.Run("CanceledMidWalk", func(t *testing.T) {
		root := t.TempDir()
		sharedCommit(t, repository.BindSharedObjects(osfs.New(root), "/repositories", "/git/sha1"), "/org/a.git", "a.txt", "b.txt")
		const fans = "/git/sha1/objects/[0-9a-f][0-9a-f]"
		if listed, err := filepath.Glob(filepath.Join(root, fans)); err != nil || len(listed) < 2 {
			t.Fatalf("shared store fan-out directories = %v, %v; want at least two so the walk has more to list after the cancel", listed, err)
		}
		canceled, cancel := context.WithCancel(ctx)
		defer cancel()
		data := cancelOnReadDir{Filesystem: osfs.New(root), pattern: fans, cancel: cancel}
		g := NewGC(repository.BindSharedObjects(data, "/repositories", "/git/sha1"), nil)
		if _, err := g.SharedObjectsUsage(canceled); !errors.Is(err, context.Canceled) {
			t.Fatalf("SharedObjectsUsage error = %v; want context.Canceled from the walk canceled as it lists its first fan-out directory", err)
		}
	})
	t.Run("WalkError", func(t *testing.T) {
		root := t.TempDir()
		if err := os.MkdirAll(filepath.Join(root, "git", "sha1", "objects", "ab"), 0o755); err != nil {
			t.Fatal(err)
		}
		injected := errors.New("injected readdir failure")
		data := readDirFailure{Filesystem: osfs.New(root), path: "/git/sha1/objects/ab", err: injected}
		g := NewGC(repository.BindSharedObjects(data, "/repositories", "/git/sha1"), nil)
		if _, err := g.SharedObjectsUsage(ctx); err == nil || !strings.Contains(err.Error(), injected.Error()) {
			t.Fatalf("SharedObjectsUsage error = %v; want the injected failure", err)
		}
	})
}
