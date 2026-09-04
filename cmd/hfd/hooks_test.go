package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"

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
	cfg := defaultConfig()
	cfg.DataDir = t.TempDir()
	srcRoot := t.TempDir()
	cfg.PullMirrorURL = srcRoot
	cfg.ProxyCacheTTL = time.Hour

	st, err := buildStorage(ctx, cfg)
	if err != nil {
		t.Fatalf("build storage: %v", err)
	}
	xs, err := buildXETStorage(ctx, cfg)
	if err != nil {
		t.Fatalf("build xet storage: %v", err)
	}
	xetC, err := buildXETClient(cfg)
	if err != nil {
		t.Fatalf("build xet client: %v", err)
	}
	hooks := &serverHooks{storage: st, pullTTL: cfg.ProxyCacheTTL}
	m, err := buildMirror(ctx, cfg, st, xs, hooks, xetC, nil, nil)
	if err != nil {
		t.Fatalf("build mirror: %v", err)
	}
	hooks.mirror = m
	t.Cleanup(m.Wait)

	// The sync uses the source URL verbatim, so a local-path source needs the .git suffix.
	repoName := "org/repo.git"
	repoPath := repository.ResolvePath(repoName)
	localFS := st.RepositoriesFS()

	// (a) missing source: the pull fails and is not recorded.
	if err := hooks.preOpen(ctx, repoName, false); err == nil {
		t.Fatal("preOpen with missing source: want error, got nil")
	}
	if _, ok := hooks.lastPull.Load(repoPath); ok {
		t.Fatal("failed pull must not be recorded in lastPull")
	}

	// (b) first pull syncs.
	src, err := repository.Init(ctx, osfs.Default, filepath.Join(srcRoot, "org", "repo.git"), "main")
	if err != nil {
		t.Fatalf("init source repo: %v", err)
	}
	h1 := addCommit(t, src, "README.md")
	if err := hooks.preOpen(ctx, repoName, false); err != nil {
		t.Fatalf("first preOpen: %v", err)
	}
	if got := mainRef(t, localFS, repoPath); got != h1 {
		t.Fatalf("after first pull main = %s, want %s", got, h1)
	}

	// (c) within TTL: skipped.
	h2 := addCommit(t, src, "new.txt")
	if err := hooks.preOpen(ctx, repoName, false); err != nil {
		t.Fatalf("throttled preOpen: %v", err)
	}
	if got := mainRef(t, localFS, repoPath); got != h1 {
		t.Fatalf("within TTL main = %s, want unchanged %s", got, h1)
	}

	// (d) expired TTL: re-syncs.
	hooks.lastPull.Store(repoPath, time.Now().Add(-2*time.Hour))
	if err := hooks.preOpen(ctx, repoName, false); err != nil {
		t.Fatalf("preOpen after TTL expiry: %v", err)
	}
	if got := mainRef(t, localFS, repoPath); got != h2 {
		t.Fatalf("after TTL expiry main = %s, want %s", got, h2)
	}
}
