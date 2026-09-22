package server

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
	xetclient "github.com/wzshiming/xet/client"

	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

func addCommit(t *testing.T, repo *repository.Repository, file string) string {
	t.Helper()
	hash, err := repo.CreateCommit(context.Background(), "main", "commit "+file, "Test", "test@test.com",
		[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: file, Content: []byte("# a\n")}}, "")
	if err != nil {
		t.Fatalf("create commit: %v", err)
	}
	return hash
}

func mainRef(t *testing.T, fs billy.Filesystem, path string) string {
	t.Helper()
	repo, err := repository.Open(fs, path)
	if err != nil {
		t.Fatalf("open repo %s: %v", path, err)
	}
	refs, err := repo.Refs()
	if err != nil {
		t.Fatalf("refs of %s: %v", path, err)
	}
	return refs["refs/heads/main"]
}

func TestPreOpenPullTTL(t *testing.T) {
	ctx := context.Background()
	st := newStorage(t)
	srcRoot := t.TempDir()
	xetC, err := xetclient.NewClient(xetclient.WithCacheDir(filepath.Join(st.XETDir(), "chunks")))
	if err != nil {
		t.Fatalf("build xet client: %v", err)
	}
	hooks := &Hooks{PullTTL: time.Hour}
	m, err := mirror.NewMirror(
		mirror.WithRepositoriesFS(st.RepositoriesFS()),
		mirror.WithXETStorage(st.XETStorage()),
		mirror.WithXETClient(xetC),
		mirror.WithMirrorSourceFunc(func(ctx context.Context, repoName string) (string, bool, error) {
			return srcRoot + "/" + repoName, true, nil
		}),
	)
	if err != nil {
		t.Fatalf("build mirror: %v", err)
	}
	hooks.Mirror = m
	t.Cleanup(m.Wait)

	// The sync uses the source URL verbatim, so a local-path source needs the .git suffix.
	repoName := "org/repo.git"
	repoPath := repository.ResolvePath(repoName)
	localFS := st.RepositoriesFS()

	if err := hooks.PreOpen(ctx, repoName, false); err == nil {
		t.Fatal("PreOpen with missing source: want error, got nil")
	}
	if _, ok := hooks.lastPull.Load(repoPath); ok {
		t.Fatal("failed pull must not be recorded in lastPull")
	}

	src, err := repository.Init(ctx, osfs.Default, filepath.Join(srcRoot, "org", "repo.git"), "main")
	if err != nil {
		t.Fatalf("init source repo: %v", err)
	}
	first := addCommit(t, src, "README.md")
	if err := hooks.PreOpen(ctx, repoName, false); err != nil {
		t.Fatalf("first PreOpen: %v", err)
	}
	if got := mainRef(t, localFS, repoPath); got != first {
		t.Fatalf("after first pull main = %s, want %s", got, first)
	}

	second := addCommit(t, src, "new.txt")
	if err := hooks.PreOpen(ctx, repoName, false); err != nil {
		t.Fatalf("throttled PreOpen: %v", err)
	}
	if got := mainRef(t, localFS, repoPath); got != first {
		t.Fatalf("within TTL main = %s, want unchanged %s", got, first)
	}

	hooks.lastPull.Store(repoPath, time.Now().Add(-2*time.Hour))
	if err := hooks.PreOpen(ctx, repoName, false); err != nil {
		t.Fatalf("PreOpen after TTL expiry: %v", err)
	}
	if got := mainRef(t, localFS, repoPath); got != second {
		t.Fatalf("after TTL expiry main = %s, want %s", got, second)
	}
}
