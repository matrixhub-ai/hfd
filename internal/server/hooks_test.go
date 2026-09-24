package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
	xetclient "github.com/wzshiming/xet/client"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// TestHooksCatalogDefaults pins the filesystem catalog behavior carried over from the builtin hf handlers, and that each call logs once.
func TestHooksCatalogDefaults(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	logged := func(t *testing.T, want string) {
		t.Helper()
		if got := logs.String(); strings.Count(got, "\n") != 1 || !strings.Contains(got, want) {
			t.Errorf("log = %q, want one line containing %q", got, want)
		}
		logs.Reset()
	}

	alice := authenticate.NewIdentity("alice", "alice@example.com")
	ctx := authenticate.WithIdentity(context.Background(), alice)
	hooks := &Hooks{Storage: newStorage(t)}
	fs := hooks.Storage.RepositoriesFS()

	updates, err := hooks.CreateRepo(ctx, "datasets/org/repo", backendhf.CreateRepoRequest{Private: true})
	if err != nil || len(updates) != 1 || !updates[0].IsCreate() || updates[0].RefName() != "refs/heads/main" {
		t.Fatalf("CreateRepo = %v, %v; want one main branch creation", updates, err)
	}
	logged(t, `msg="Create repository" user=alice repo=datasets/org/repo private=true`)
	repo, err := repository.Open(fs, repository.ResolvePath("datasets/org/repo"))
	if err != nil {
		t.Fatalf("open created repo: %v", err)
	}
	commits, err := repo.Commits("main", &repository.CommitsOptions{Limit: 2})
	if err != nil || len(commits) != 1 || commits[0].Hash().String() != updates[0].NewRev() || commits[0].Author().Name() != "alice" || commits[0].Author().Email() != "alice@example.com" {
		t.Fatalf("commits = %v, %v; want the one initial commit by alice", commits, err)
	}
	if _, err := repo.Blob("main", repository.GitattributesFileName); err != nil {
		t.Errorf("initial commit lacks %s: %v", repository.GitattributesFileName, err)
	}
	if updates, err := hooks.CreateRepo(ctx, "datasets/org/repo", backendhf.CreateRepoRequest{}); err != nil || updates != nil {
		t.Errorf("second CreateRepo = %v, %v; want nil, nil", updates, err)
	}
	logs.Reset()
	if _, err := hooks.CreateRepo(context.Background(), "datasets/org/other", backendhf.CreateRepoRequest{}); err != nil {
		t.Fatalf("CreateRepo other: %v", err)
	}
	logged(t, `msg="Create repository" user=<anonymous> repo=datasets/org/other`)
	other, err := repository.Open(fs, repository.ResolvePath("datasets/org/other"))
	if err != nil {
		t.Fatalf("open other repo: %v", err)
	}
	if commits, err := other.Commits("main", nil); err != nil || len(commits) != 1 || commits[0].Author().Name() != "HuggingFace" {
		t.Errorf("anonymous initial commit = %v, %v; want the HuggingFace fallback author", commits, err)
	}

	if err := hooks.MoveRepo(ctx, "datasets/org/repo", "datasets/org/other"); !errors.Is(err, repository.ErrRepositoryAlreadyExists) {
		t.Errorf("MoveRepo onto existing = %v, want ErrRepositoryAlreadyExists", err)
	}
	logged(t, `msg="Move repository" user=alice from=datasets/org/repo to=datasets/org/other`)
	if err := hooks.MoveRepo(ctx, "datasets/org/missing", "datasets/org/new"); !errors.Is(err, repository.ErrRepositoryNotExists) {
		t.Errorf("MoveRepo missing = %v, want ErrRepositoryNotExists", err)
	}
	logs.Reset()
	if err := hooks.MoveRepo(ctx, "datasets/org/repo", "datasets/org/moved"); err != nil {
		t.Fatalf("MoveRepo: %v", err)
	}
	logs.Reset()
	if !repository.IsRepository(fs, repository.ResolvePath("datasets/org/moved")) || repository.IsRepository(fs, repository.ResolvePath("datasets/org/repo")) {
		t.Error("MoveRepo did not rename datasets/org/repo to datasets/org/moved")
	}

	if err := hooks.UpdateRepoSettings(ctx, "datasets/org/moved", backendhf.RepoSettings{}); err != nil {
		t.Errorf("UpdateRepoSettings = %v, want nil", err)
	}
	logged(t, `msg="Update repository settings" user=alice repo=datasets/org/moved`)
	if err := hooks.UpdateRepoSettings(ctx, "datasets/org/missing", backendhf.RepoSettings{}); !errors.Is(err, repository.ErrRepositoryNotExists) {
		t.Errorf("UpdateRepoSettings missing = %v, want ErrRepositoryNotExists", err)
	}
	logs.Reset()

	items, more, err := hooks.ListRepos(ctx, "datasets", backendhf.ListQuery{Author: "org", Limit: 1})
	if err != nil || !more || len(items) != 1 || items[0].RepoID != "org/moved" {
		t.Errorf("ListRepos page 1 = %+v, %v, %v; want [org/moved] with more", items, more, err)
	}
	logged(t, `msg="List repositories" user=alice repoType=datasets author=org search=""`)
	items, more, err = hooks.ListRepos(ctx, "datasets", backendhf.ListQuery{Limit: 1, Offset: 1})
	if err != nil || more || len(items) != 1 || items[0].RepoID != "org/other" {
		t.Errorf("ListRepos page 2 = %+v, %v, %v; want [org/other] without more", items, more, err)
	}
	logs.Reset()
	if items, _, err := hooks.ListRepos(ctx, "models", backendhf.ListQuery{}); err != nil || len(items) != 0 {
		t.Errorf("ListRepos models = %+v, %v; want none", items, err)
	}
	logs.Reset()

	if err := hooks.DeleteRepo(ctx, "datasets/org/missing"); !errors.Is(err, repository.ErrRepositoryNotExists) || !strings.Contains(err.Error(), `"datasets/org/missing"`) {
		t.Errorf("DeleteRepo missing = %v, want a named ErrRepositoryNotExists", err)
	}
	logged(t, `msg="Delete repository" user=alice repo=datasets/org/missing`)
	if err := hooks.DeleteRepo(ctx, "datasets/org/moved"); err != nil || repository.IsRepository(fs, repository.ResolvePath("datasets/org/moved")) {
		t.Errorf("DeleteRepo = %v; want nil and the repository gone", err)
	}
	logs.Reset()

	who, err := hooks.Whoami(ctx)
	want := &backendhf.WhoamiResponse{Type: "user", ID: "alice", Name: "alice", Fullname: "alice", Email: "alice@example.com", Orgs: []any{},
		Auth: backendhf.WhoamiAuth{AccessToken: backendhf.WhoamiAccessToken{DisplayName: "token", Role: "write"}}}
	if err != nil || fmt.Sprintf("%+v", who) != fmt.Sprintf("%+v", want) {
		t.Errorf("Whoami = %+v, %v; want %+v", who, err, want)
	}
	logged(t, `msg=Whoami user=alice`)
}

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
