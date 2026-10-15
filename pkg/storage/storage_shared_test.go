package storage

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/util"

	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// sharedTestAlternatesLine is the alternates line of /org/repo.git below the repositories directory.
const sharedTestAlternatesLine = "../../../../git/sha1/objects\n"

func TestRepositoriesFSSharesGitObjects(t *testing.T) {
	t.Run("LocalFS", func(t *testing.T) {
		root := t.TempDir()
		st, err := NewStorage(WithRootDir(root))
		if err != nil {
			t.Fatalf("NewStorage: %v", err)
		}
		if _, err := os.Stat(filepath.Join(root, "git")); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("stat <root>/git after NewStorage: %v, want not exist until a repository opens", err)
		}
		if first, second := st.RepositoriesFS(), st.RepositoriesFS(); first != second {
			t.Fatal("RepositoriesFS returns a different filesystem per call, so cached repository handles are never reused")
		}
		if _, err := repository.Init(t.Context(), st.RepositoriesFS(), "/org/repo.git", "main"); err != nil {
			t.Fatalf("Init: %v", err)
		}
		data, err := os.ReadFile(filepath.Join(root, "repositories", "org", "repo.git", "objects", "info", "alternates"))
		if err != nil || string(data) != sharedTestAlternatesLine {
			t.Fatalf("alternates = %q, %v; want %q", data, err, sharedTestAlternatesLine)
		}
		if fi, err := os.Stat(filepath.Join(root, "git", "sha1", "objects", "pack")); err != nil || !fi.IsDir() {
			t.Fatalf("stat <root>/git/sha1/objects/pack: %v, want a directory", err)
		}
	})

	t.Run("CustomFS", func(t *testing.T) {
		mem := memfs.New()
		st, err := NewStorage(WithRootDir(t.TempDir()), WithFilesystem(mem))
		if err != nil {
			t.Fatalf("NewStorage: %v", err)
		}
		if _, err := mem.Stat("/git"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("stat /git after NewStorage: %v, want not exist until a repository opens", err)
		}
		if _, err := repository.Init(t.Context(), st.RepositoriesFS(), "/org/repo.git", "main"); err != nil {
			t.Fatalf("Init: %v", err)
		}
		data, err := util.ReadFile(mem, "/repositories/org/repo.git/objects/info/alternates")
		if err != nil || string(data) != sharedTestAlternatesLine {
			t.Fatalf("alternates = %q, %v; want %q", data, err, sharedTestAlternatesLine)
		}
		if fi, err := mem.Stat("/git/sha1/objects/pack"); err != nil || !fi.IsDir() {
			t.Fatalf("stat /git/sha1/objects/pack: %v, want a directory", err)
		}
	})
}
