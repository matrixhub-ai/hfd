package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
	xetclient "github.com/wzshiming/xet/client"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/receive"
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
	head := mainRef(t, fs, repository.ResolvePath("datasets/org/repo"))
	if err := hooks.MoveRepo(ctx, "datasets/org/repo", "datasets/org/moved"); err != nil {
		t.Fatalf("MoveRepo: %v", err)
	}
	logged(t, `msg="Move repository" user=alice from=datasets/org/repo to=datasets/org/moved`)
	if !repository.IsRepository(fs, repository.ResolvePath("datasets/org/moved")) || repository.IsRepository(fs, repository.ResolvePath("datasets/org/repo")) {
		t.Error("MoveRepo did not rename datasets/org/repo to datasets/org/moved")
	}
	if got := mainRef(t, fs, repository.ResolvePath("datasets/org/moved")); got != head {
		t.Errorf("moved main = %s, want the source's %s", got, head)
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
	items, _, err = hooks.ListRepos(ctx, "datasets", backendhf.ListQuery{Author: "org", Expand: []string{"lastModified", "usedStorage"}})
	if err != nil || len(items) != 2 {
		t.Fatalf("ListRepos expanded = %+v, %v; want both org datasets", items, err)
	}
	otherCommits, err := other.Commits("main", nil)
	if err != nil || len(otherCommits) != 1 {
		t.Fatalf("other commits = %v, %v; want one", otherCommits, err)
	}
	wantModified := map[string]string{
		"org/moved": commits[0].Committer().When().UTC().Format(repository.TimeFormat),
		"org/other": otherCommits[0].Committer().When().UTC().Format(repository.TimeFormat),
	}
	for _, item := range items {
		if item.LastModified == "" || item.LastModified != wantModified[item.RepoID] || item.UsedStorage <= 0 {
			t.Errorf("expanded %s = lastModified %q usedStorage %d; want %q and positive usage", item.RepoID, item.LastModified, item.UsedStorage, wantModified[item.RepoID])
		}
	}
	logged(t, `msg="List repositories" user=alice repoType=datasets author=org search=""`)
	items, _, err = hooks.ListRepos(ctx, "datasets", backendhf.ListQuery{Author: "org"})
	if err != nil || len(items) != 2 {
		t.Fatalf("ListRepos plain = %+v, %v; want both org datasets", items, err)
	}
	for _, item := range items {
		if item.LastModified != "" || item.UsedStorage != 0 {
			t.Errorf("plain %s = lastModified %q usedStorage %d; want neither without Expand", item.RepoID, item.LastModified, item.UsedStorage)
		}
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

// TestHooksDuplicateRepo pins that DuplicateRepo copies every ref, commit and the default branch into a new repository, leaves the source alone, and logs each call once.
func TestHooksDuplicateRepo(t *testing.T) {
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

	created, err := hooks.CreateRepo(ctx, "datasets/org/src", backendhf.CreateRepoRequest{})
	if err != nil || len(created) != 1 {
		t.Fatalf("CreateRepo = %v, %v; want the initial commit", created, err)
	}
	initial := created[0].NewRev()
	src, err := repository.Open(fs, repository.ResolvePath("datasets/org/src"))
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	second := addCommit(t, src, "README.md")
	// dev stays on the initial commit so each copied ref must keep its own hash.
	if err := src.CreateBranch("dev", initial); err != nil {
		t.Fatalf("create branch dev: %v", err)
	}
	if err := src.CreateTag("v1", "main"); err != nil {
		t.Fatalf("create tag v1: %v", err)
	}
	srcRefs, err := src.Refs()
	if err != nil {
		t.Fatalf("source refs: %v", err)
	}
	logs.Reset()

	if err := hooks.DuplicateRepo(ctx, "datasets/org/src", "datasets/org/copy", backendhf.DuplicateRepoRequest{Repository: "org/copy"}); err != nil {
		t.Fatalf("DuplicateRepo: %v", err)
	}
	logged(t, `msg="Duplicate repository" user=alice from=datasets/org/src to=datasets/org/copy private=false`)

	dst, err := repository.Open(fs, repository.ResolvePath("datasets/org/copy"))
	if err != nil {
		t.Fatalf("open copy: %v", err)
	}
	if refs, err := dst.Refs(); err != nil || !maps.Equal(refs, srcRefs) {
		t.Errorf("copy refs = %v, %v; want the source's %v", refs, err, srcRefs)
	}
	if branch := dst.DefaultBranch(); branch != "main" {
		t.Errorf("copy default branch = %q, want main", branch)
	}
	commits, err := dst.Commits("main", nil)
	var hashes []string
	for _, c := range commits {
		hashes = append(hashes, c.Hash().String())
	}
	if want := []string{second, initial}; err != nil || !slices.Equal(hashes, want) {
		t.Errorf("copy main commits = %v, %v; want the source's %v", hashes, err, want)
	}
	if _, err := dst.Blob("main", "README.md"); err != nil {
		t.Errorf("copy lacks README.md: %v", err)
	}
	if refs, err := src.Refs(); err != nil || !maps.Equal(refs, srcRefs) {
		t.Errorf("source refs after DuplicateRepo = %v, %v; want unchanged %v", refs, err, srcRefs)
	}

	if err := hooks.DuplicateRepo(ctx, "datasets/org/src", "datasets/org/copy", backendhf.DuplicateRepoRequest{Repository: "org/copy"}); !errors.Is(err, repository.ErrRepositoryAlreadyExists) || !strings.Contains(err.Error(), `destination repository "datasets/org/copy"`) {
		t.Errorf("DuplicateRepo onto existing = %v, want a named destination ErrRepositoryAlreadyExists", err)
	}
	logged(t, `msg="Duplicate repository" user=alice from=datasets/org/src to=datasets/org/copy private=false`)
	if err := hooks.DuplicateRepo(ctx, "datasets/org/missing", "datasets/org/new", backendhf.DuplicateRepoRequest{Repository: "org/new"}); !errors.Is(err, repository.ErrRepositoryNotExists) || !strings.Contains(err.Error(), `"datasets/org/missing"`) {
		t.Errorf("DuplicateRepo missing = %v, want a named ErrRepositoryNotExists", err)
	}
	logged(t, `msg="Duplicate repository" user=alice from=datasets/org/missing to=datasets/org/new private=false`)
	if repository.IsRepository(fs, repository.ResolvePath("datasets/org/new")) {
		t.Error("DuplicateRepo from a missing source created datasets/org/new")
	}

	if err := hooks.DuplicateRepo(context.Background(), "datasets/org/src", "datasets/org/anon", backendhf.DuplicateRepoRequest{Repository: "org/anon", Private: true}); err != nil {
		t.Errorf("anonymous DuplicateRepo: %v", err)
	}
	logged(t, `msg="Duplicate repository" user=<anonymous> from=datasets/org/src to=datasets/org/anon private=true`)
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
	hooks := &Hooks{PullTTL: time.Hour}
	m, err := mirror.NewMirror(
		mirror.WithRepositoriesFS(st.RepositoriesFS()),
		mirror.WithXETStorage(st.XETStorage()),
		mirror.WithXETCache(xetclient.NewCache(st.XETMirrorDir(), 0, 0)),
		mirror.WithDataDir(st.XETDir()),
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

// TestPreOpenPullOnlyMirrorReadOnly pins that PreOpen refuses a write to a pull-only mirror before any sync and lets every other open through.
func TestPreOpenPullOnlyMirrorReadOnly(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	ctx := context.Background()
	// The sync uses the source URL verbatim, so a local-path source needs the .git suffix.
	repoName := "org/repo.git"
	repoPath := repository.ResolvePath(repoName)

	t.Run("write before first sync is refused", func(t *testing.T) {
		// No source repository exists, so a sync attempt would fail without fs.ErrPermission.
		hooks := roMirrorHooks(t, roMirrorUnder(t.TempDir()))
		logs.Reset()
		roMirrorRefused(t, "PreOpen write", hooks.PreOpen(ctx, repoName, true), repoName)
		if repository.IsRepository(hooks.Storage.RepositoriesFS(), repoPath) {
			t.Error("refused PreOpen write created the local repository")
		}
		if _, ok := hooks.lastPull.Load(repoPath); ok {
			t.Error("refused PreOpen write recorded lastPull")
		}
		if logs.Len() != 0 {
			t.Errorf("refused PreOpen write logged %q, want nothing", logs.String())
		}
	})

	t.Run("write within TTL is refused", func(t *testing.T) {
		srcRoot := t.TempDir()
		hooks := roMirrorHooks(t, roMirrorUnder(srcRoot))
		fs := hooks.Storage.RepositoriesFS()
		src, head := roMirrorSource(t, srcRoot, repoName)
		if err := hooks.PreOpen(ctx, repoName, false); err != nil {
			t.Fatalf("PreOpen read: %v", err)
		}
		if got := mainRef(t, fs, repoPath); got != head {
			t.Fatalf("main after the read = %s, want the source's %s", got, head)
		}
		last, ok := hooks.lastPull.Load(repoPath)
		if !ok {
			t.Fatal("PreOpen read did not record lastPull")
		}
		// The TTL skips the sync even for a permitted write; only the error shows the refusal.
		addCommit(t, src, "new.txt")
		roMirrorRefused(t, "PreOpen write", hooks.PreOpen(ctx, repoName, true), repoName)
		if got, _ := hooks.lastPull.Load(repoPath); got != last {
			t.Errorf("lastPull after the refused write = %v, want unchanged %v", got, last)
		}
		if got := mainRef(t, fs, repoPath); got != head {
			t.Errorf("main after the refused write = %s, want unchanged %s", got, head)
		}
	})

	t.Run("write to push target syncs", func(t *testing.T) {
		srcRoot := t.TempDir()
		hooks := roMirrorHooks(t, roMirrorUnder(srcRoot), roMirrorPushed())
		_, head := roMirrorSource(t, srcRoot, repoName)
		if err := hooks.PreOpen(ctx, repoName, true); err != nil {
			t.Fatalf("PreOpen write: %v", err)
		}
		if got := mainRef(t, hooks.Storage.RepositoriesFS(), repoPath); got != head {
			t.Errorf("main = %s, want the source's %s", got, head)
		}
		if _, ok := hooks.lastPull.Load(repoPath); !ok {
			t.Error("PreOpen write did not record lastPull")
		}
	})

	t.Run("write to non-mirror passes", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			hooks *Hooks
		}{
			{"other source", roMirrorHooks(t, roMirrorOnly("datasets/org/mirrored"))},
			{"nil mirror", &Hooks{Storage: newStorage(t), PullTTL: time.Hour}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if err := tc.hooks.PreOpen(ctx, repoName, true); err != nil {
					t.Errorf("PreOpen write = %v, want nil", err)
				}
				if repository.IsRepository(tc.hooks.Storage.RepositoriesFS(), repoPath) {
					t.Error("PreOpen write created the local repository")
				}
				if _, ok := tc.hooks.lastPull.Load(repoPath); ok {
					t.Error("PreOpen write recorded lastPull")
				}
			})
		}
	})

	t.Run("lookup errors pass through", func(t *testing.T) {
		errBoom := errors.New("boom")
		failing := func(context.Context, string) (string, bool, error) { return "", false, errBoom }
		srcRoot := t.TempDir()
		roMirrorSource(t, srcRoot, repoName)
		for _, tc := range []struct {
			name  string
			hooks *Hooks
		}{
			{"source", roMirrorHooks(t, failing)},
			{"destination", roMirrorHooks(t, roMirrorUnder(srcRoot), mirror.WithMirrorDestinationFunc(failing))},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if err := tc.hooks.PreOpen(ctx, repoName, true); !errors.Is(err, errBoom) || errors.Is(err, iofs.ErrPermission) {
					t.Errorf("PreOpen write = %v, want the lookup's errBoom without fs.ErrPermission", err)
				}
				if repository.IsRepository(tc.hooks.Storage.RepositoriesFS(), repoPath) {
					t.Error("PreOpen write created the local repository")
				}
			})
		}
	})
}

// TestHooksCatalogPullOnlyMirrorReadOnly pins that MoveRepo, DuplicateRepo and UpdateRepoSettings refuse a pull-only mirror target before anything else, logging each call once.
func TestHooksCatalogPullOnlyMirrorReadOnly(t *testing.T) {
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

	ctx := authenticate.WithIdentity(context.Background(), authenticate.NewIdentity("alice", "alice@example.com"))
	const mirrored = "datasets/org/mirrored"
	create := func(t *testing.T, hooks *Hooks, repoName string) {
		t.Helper()
		if _, err := hooks.CreateRepo(ctx, repoName, backendhf.CreateRepoRequest{}); err != nil {
			t.Fatalf("CreateRepo %s: %v", repoName, err)
		}
		logs.Reset()
	}
	exists := func(hooks *Hooks, repoName string) bool {
		return repository.IsRepository(hooks.Storage.RepositoriesFS(), repository.ResolvePath(repoName))
	}
	private := true
	settings := backendhf.RepoSettings{Private: &private}

	t.Run("MoveRepo onto mirror is refused", func(t *testing.T) {
		hooks := roMirrorHooks(t, roMirrorOnly(mirrored))
		create(t, hooks, "datasets/org/repo")
		roMirrorRefused(t, "MoveRepo", hooks.MoveRepo(ctx, "datasets/org/repo", mirrored), mirrored)
		logged(t, `msg="Move repository" user=alice from=datasets/org/repo to=datasets/org/mirrored`)
		if !exists(hooks, "datasets/org/repo") || exists(hooks, mirrored) {
			t.Error("refused MoveRepo moved datasets/org/repo onto datasets/org/mirrored")
		}
	})

	t.Run("MoveRepo of missing repo is refused first", func(t *testing.T) {
		hooks := roMirrorHooks(t, roMirrorOnly(mirrored))
		logs.Reset()
		err := hooks.MoveRepo(ctx, "datasets/org/missing", mirrored)
		roMirrorRefused(t, "MoveRepo", err, mirrored)
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			t.Errorf("MoveRepo = %v, want the refusal ahead of ErrRepositoryNotExists", err)
		}
		logged(t, `msg="Move repository" user=alice from=datasets/org/missing to=datasets/org/mirrored`)
	})

	t.Run("MoveRepo onto push target moves", func(t *testing.T) {
		hooks := roMirrorHooks(t, roMirrorOnly(mirrored), roMirrorPushed())
		create(t, hooks, "datasets/org/repo")
		if err := hooks.MoveRepo(ctx, "datasets/org/repo", mirrored); err != nil {
			t.Errorf("MoveRepo = %v, want nil", err)
		}
		logged(t, `msg="Move repository" user=alice from=datasets/org/repo to=datasets/org/mirrored`)
		if exists(hooks, "datasets/org/repo") || !exists(hooks, mirrored) {
			t.Error("MoveRepo did not move datasets/org/repo onto datasets/org/mirrored")
		}
	})

	t.Run("MoveRepo onto non-mirror moves", func(t *testing.T) {
		hooks := roMirrorHooks(t, roMirrorOnly(mirrored))
		create(t, hooks, "datasets/org/repo")
		if err := hooks.MoveRepo(ctx, "datasets/org/repo", "datasets/org/other"); err != nil {
			t.Errorf("MoveRepo = %v, want nil", err)
		}
		logged(t, `msg="Move repository" user=alice from=datasets/org/repo to=datasets/org/other`)
		if exists(hooks, "datasets/org/repo") || !exists(hooks, "datasets/org/other") {
			t.Error("MoveRepo did not move datasets/org/repo onto datasets/org/other")
		}
	})

	t.Run("DuplicateRepo onto mirror is refused", func(t *testing.T) {
		hooks := roMirrorHooks(t, roMirrorOnly(mirrored))
		create(t, hooks, "datasets/org/src")
		src, err := repository.Open(hooks.Storage.RepositoriesFS(), repository.ResolvePath("datasets/org/src"))
		if err != nil {
			t.Fatalf("open source: %v", err)
		}
		srcRefs, err := src.Refs()
		if err != nil {
			t.Fatalf("source refs: %v", err)
		}
		roMirrorRefused(t, "DuplicateRepo", hooks.DuplicateRepo(ctx, "datasets/org/src", mirrored, backendhf.DuplicateRepoRequest{Repository: "org/mirrored"}), mirrored)
		logged(t, `msg="Duplicate repository" user=alice from=datasets/org/src to=datasets/org/mirrored private=false`)
		if exists(hooks, mirrored) {
			t.Error("refused DuplicateRepo created datasets/org/mirrored")
		}
		if refs, err := src.Refs(); err != nil || !maps.Equal(refs, srcRefs) {
			t.Errorf("source refs after the refused DuplicateRepo = %v, %v; want unchanged %v", refs, err, srcRefs)
		}
	})

	t.Run("DuplicateRepo onto push target copies", func(t *testing.T) {
		hooks := roMirrorHooks(t, roMirrorOnly(mirrored), roMirrorPushed())
		create(t, hooks, "datasets/org/src")
		fs := hooks.Storage.RepositoriesFS()
		src, err := repository.Open(fs, repository.ResolvePath("datasets/org/src"))
		if err != nil {
			t.Fatalf("open source: %v", err)
		}
		srcRefs, err := src.Refs()
		if err != nil {
			t.Fatalf("source refs: %v", err)
		}
		if err := hooks.DuplicateRepo(ctx, "datasets/org/src", mirrored, backendhf.DuplicateRepoRequest{Repository: "org/mirrored"}); err != nil {
			t.Errorf("DuplicateRepo = %v, want nil", err)
		}
		logged(t, `msg="Duplicate repository" user=alice from=datasets/org/src to=datasets/org/mirrored private=false`)
		dst, err := repository.Open(fs, repository.ResolvePath(mirrored))
		if err != nil {
			t.Fatalf("open copy: %v", err)
		}
		if refs, err := dst.Refs(); err != nil || !maps.Equal(refs, srcRefs) {
			t.Errorf("copy refs = %v, %v; want the source's %v", refs, err, srcRefs)
		}
	})

	t.Run("UpdateRepoSettings is refused", func(t *testing.T) {
		hooks := roMirrorHooks(t, roMirrorOnly(mirrored))
		create(t, hooks, mirrored)
		roMirrorRefused(t, "UpdateRepoSettings", hooks.UpdateRepoSettings(ctx, mirrored, settings), mirrored)
		logged(t, `msg="Update repository settings" user=alice repo=datasets/org/mirrored`)
	})

	t.Run("UpdateRepoSettings of missing repo is refused first", func(t *testing.T) {
		hooks := roMirrorHooks(t, roMirrorOnly(mirrored))
		logs.Reset()
		err := hooks.UpdateRepoSettings(ctx, mirrored, settings)
		roMirrorRefused(t, "UpdateRepoSettings", err, mirrored)
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			t.Errorf("UpdateRepoSettings = %v, want the refusal ahead of ErrRepositoryNotExists", err)
		}
		logged(t, `msg="Update repository settings" user=alice repo=datasets/org/mirrored`)
	})

	t.Run("UpdateRepoSettings of push target passes", func(t *testing.T) {
		hooks := roMirrorHooks(t, roMirrorOnly(mirrored), roMirrorPushed())
		create(t, hooks, mirrored)
		if err := hooks.UpdateRepoSettings(ctx, mirrored, settings); err != nil {
			t.Errorf("UpdateRepoSettings = %v, want nil", err)
		}
		logged(t, `msg="Update repository settings" user=alice repo=datasets/org/mirrored`)
	})

	t.Run("UpdateRepoSettings of non-mirror passes", func(t *testing.T) {
		hooks := roMirrorHooks(t, roMirrorOnly(mirrored))
		create(t, hooks, "datasets/org/repo")
		if err := hooks.UpdateRepoSettings(ctx, "datasets/org/repo", settings); err != nil {
			t.Errorf("UpdateRepoSettings = %v, want nil", err)
		}
		logged(t, `msg="Update repository settings" user=alice repo=datasets/org/repo`)
	})

	t.Run("UpdateRepoSettings lookup error passes through", func(t *testing.T) {
		errBoom := errors.New("boom")
		hooks := roMirrorHooks(t, func(context.Context, string) (string, bool, error) { return "", false, errBoom })
		create(t, hooks, mirrored)
		if err := hooks.UpdateRepoSettings(ctx, mirrored, settings); !errors.Is(err, errBoom) || errors.Is(err, iofs.ErrPermission) {
			t.Errorf("UpdateRepoSettings = %v, want the lookup's errBoom without fs.ErrPermission", err)
		}
		logged(t, `msg="Update repository settings" user=alice repo=datasets/org/mirrored`)
	})
}

func roMirrorHooks(t *testing.T, source mirror.SourceFunc, opts ...mirror.Option) *Hooks {
	t.Helper()
	st := newStorage(t)
	m, err := mirror.NewMirror(append([]mirror.Option{
		mirror.WithRepositoriesFS(st.RepositoriesFS()),
		mirror.WithXETStorage(st.XETStorage()),
		mirror.WithXETCache(xetclient.NewCache(st.XETMirrorDir(), 0, 0)),
		mirror.WithDataDir(st.XETDir()),
		mirror.WithMirrorSourceFunc(source),
	}, opts...)...)
	if err != nil {
		t.Fatalf("build mirror: %v", err)
	}
	t.Cleanup(m.Wait)
	return &Hooks{Storage: st, Mirror: m, PullTTL: time.Hour}
}

func roMirrorUnder(root string) mirror.SourceFunc {
	return func(_ context.Context, repoName string) (string, bool, error) {
		return root + "/" + repoName, true, nil
	}
}

// roMirrorOnly needs no source URL: only PreOpen pulls, and never for a name it rejects.
func roMirrorOnly(name string) mirror.SourceFunc {
	return func(_ context.Context, repoName string) (string, bool, error) {
		return "", repoName == name, nil
	}
}

func roMirrorPushed() mirror.Option {
	return mirror.WithMirrorDestinationFunc(func(context.Context, string) (string, bool, error) {
		return "", true, nil
	})
}

func roMirrorSource(t *testing.T, root, repoName string) (*repository.Repository, string) {
	t.Helper()
	src, err := repository.Init(context.Background(), osfs.Default, filepath.Join(root, repoName), "main")
	if err != nil {
		t.Fatalf("init source repo: %v", err)
	}
	return src, addCommit(t, src, "README.md")
}

func roMirrorRefused(t *testing.T, call string, err error, repoName string) {
	t.Helper()
	if !errors.Is(err, iofs.ErrPermission) || !errors.Is(err, errReadOnlyMirror) || !strings.Contains(err.Error(), fmt.Sprintf("repository %q", repoName)) {
		t.Errorf("%s = %v, want the errReadOnlyMirror refusal (fs.ErrPermission) naming repository %q", call, err, repoName)
	}
}

// mtRecorder is a Maintainer recording each Notify.
type mtRecorder struct {
	mu    sync.Mutex
	calls []mtNotify
}

// mtNotify is one Notify an mtRecorder saw.
type mtNotify struct {
	fs   billy.Filesystem
	path string
}

func (r *mtRecorder) Notify(fs billy.Filesystem, repoPath string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, mtNotify{fs, repoPath})
}

// take returns the calls since the last take.
func (r *mtRecorder) take() []mtNotify {
	r.mu.Lock()
	defer r.mu.Unlock()
	calls := r.calls
	r.calls = nil
	return calls
}

// mtRequireNotified requires calls to be exactly one Notify of path on fs.
func mtRequireNotified(t *testing.T, what string, calls []mtNotify, fs billy.Filesystem, path string) {
	t.Helper()
	if len(calls) == 1 && calls[0].fs == fs && calls[0].path == path {
		return
	}
	var got []string
	for _, c := range calls {
		if c.fs != fs {
			c.path += " on another filesystem"
		}
		got = append(got, c.path)
	}
	t.Errorf("%s notified %q, want one Notify of %q on the repositories filesystem", what, got, path)
}

// TestMaintenancePostReceive pins that PostReceive notifies Hooks.Maintainer once for any ref update, ahead of the mirror checks, and logs as before without one.
func TestMaintenancePostReceive(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	ctx := authenticate.WithIdentity(context.Background(), authenticate.NewIdentity("alice", "alice@example.com"))
	const repoName = "org/repo"
	branch := receive.NewRefUpdate(strings.Repeat("1", 40), strings.Repeat("2", 40), "refs/heads/main", nil)
	pr := receive.NewRefUpdate(receive.ZeroHash, strings.Repeat("3", 40), "refs/pr/1", nil)
	branchLog := `msg="Post-receive hook" user=alice repo=org/repo event=branch_push:main ref=refs/heads/main`
	prLog := `msg="Post-receive hook" user=alice repo=org/repo event=ref_update:refs/pr/1 ref=refs/pr/1`
	withoutMirror := func(t *testing.T) *Hooks { return &Hooks{Storage: newStorage(t)} }
	// No destination is configured, so the push mirror returns before pushing.
	withMirror := func(t *testing.T) *Hooks { return roMirrorHooks(t, roMirrorOnly("other/repo")) }
	for _, tc := range []struct {
		name   string
		hooks  func(t *testing.T) *Hooks
		update receive.RefUpdate
		logs   []string
	}{
		{"BranchWithoutMirror", withoutMirror, branch, []string{branchLog}},
		{"PRWithoutMirror", withoutMirror, pr, []string{prLog}},
		{"PRSkippedByPushFilter", withMirror, pr, []string{prLog, `msg="Skip push mirror for non-branch/tag refs" repo=org/repo`}},
		{"BranchToPushMirror", withMirror, branch, []string{branchLog}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("Maintainer", func(t *testing.T) {
				rec := &mtRecorder{}
				hooks := tc.hooks(t)
				hooks.Maintainer = rec
				if err := hooks.PostReceive(ctx, repoName, []receive.RefUpdate{tc.update}); err != nil {
					t.Fatalf("PostReceive = %v, want nil", err)
				}
				mtRequireNotified(t, "PostReceive of "+tc.update.RefName(), rec.take(), hooks.Storage.RepositoriesFS(), repository.ResolvePath(repoName))
			})
			t.Run("MaintainerNil", func(t *testing.T) {
				hooks := tc.hooks(t)
				logs.Reset()
				if err := hooks.PostReceive(ctx, repoName, []receive.RefUpdate{tc.update}); err != nil {
					t.Fatalf("PostReceive = %v, want nil", err)
				}
				got := strings.Split(strings.TrimSuffix(logs.String(), "\n"), "\n")
				if len(got) != len(tc.logs) {
					t.Fatalf("log = %q, want %d lines holding %q", logs.String(), len(tc.logs), tc.logs)
				}
				for i, want := range tc.logs {
					if !strings.Contains(got[i], want) {
						t.Errorf("log line %d = %q, want it to hold %q", i+1, got[i], want)
					}
				}
			})
		})
	}
}

// TestMaintenancePreOpenFirstSight pins that PreOpen notifies Hooks.Maintainer the first time it sees a repository, under any name form or access, with or without a mirror.
func TestMaintenancePreOpenFirstSight(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		hooks func(t *testing.T) *Hooks
	}{
		{"Mirror", func(t *testing.T) *Hooks { return roMirrorHooks(t, roMirrorOnly("datasets/org/mirrored")) }},
		{"MirrorNil", func(t *testing.T) *Hooks { return &Hooks{Storage: newStorage(t)} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &mtRecorder{}
			hooks := tc.hooks(t)
			hooks.Maintainer = rec
			fs := hooks.Storage.RepositoriesFS()
			open := func(repoName string, write bool) {
				t.Helper()
				if err := hooks.PreOpen(ctx, repoName, write); err != nil {
					t.Fatalf("PreOpen %s = %v, want nil", repoName, err)
				}
			}
			open("org/a", false)
			open("org/a", true)
			open("org/a.git", false)
			mtRequireNotified(t, "PreOpen of org/a by two names, read and write,", rec.take(), fs, "/org/a.git")
			open("org/b", true)
			mtRequireNotified(t, "the first PreOpen of org/b", rec.take(), fs, "/org/b.git")
			open("org/a", false)
			open("org/b", false)
			if calls := rec.take(); len(calls) != 0 {
				t.Errorf("PreOpen of repositories already seen notified %d times, want none", len(calls))
			}
		})
	}
}

func mtName(i int) string { return fmt.Sprintf("org/r%d", i) }

func mtOpen(t *testing.T, hooks *Hooks, repoName string) {
	t.Helper()
	if err := hooks.PreOpen(context.Background(), repoName, false); err != nil {
		t.Fatalf("PreOpen %s = %v, want nil", repoName, err)
	}
}

// mtOpenNew opens mtName(from) through mtName(to-1) and requires one Notify of each, in order.
func mtOpenNew(t *testing.T, hooks *Hooks, rec *mtRecorder, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		mtOpen(t, hooks, mtName(i))
	}
	calls := rec.take()
	if len(calls) != to-from {
		t.Fatalf("the first PreOpens of %s through %s notified %d times, want %d", mtName(from), mtName(to-1), len(calls), to-from)
	}
	fs := hooks.Storage.RepositoriesFS()
	for i, c := range calls {
		if want := repository.ResolvePath(mtName(from + i)); c.fs != fs || c.path != want {
			t.Fatalf("Notify %d of the first PreOpens of %s through %s = %q, want %q on the repositories filesystem", i, mtName(from), mtName(to-1), c.path, want)
		}
	}
}

// mtRequireQuiet requires calls to hold no Notify.
func mtRequireQuiet(t *testing.T, what string, calls []mtNotify) {
	t.Helper()
	if len(calls) != 0 {
		t.Errorf("%s notified %d times, want none", what, len(calls))
	}
}

// TestMaintenanceSeenBoundedByLimit pins that PreOpen forgets the least recently seen repository past maintenanceSeenLimit and notifies it again on its next sight.
func TestMaintenanceSeenBoundedByLimit(t *testing.T) {
	rec := &mtRecorder{}
	hooks := &Hooks{Storage: newStorage(t), Maintainer: rec}
	mtOpenNew(t, hooks, rec, 0, maintenanceSeenLimit+1)
	mtOpen(t, hooks, mtName(0))
	mtRequireNotified(t, fmt.Sprintf("PreOpen of %s after %d newer repositories", mtName(0), maintenanceSeenLimit), rec.take(), hooks.Storage.RepositoriesFS(), repository.ResolvePath(mtName(0)))
	mtOpen(t, hooks, mtName(maintenanceSeenLimit))
	mtRequireQuiet(t, fmt.Sprintf("PreOpen of %s, the last new repository seen,", mtName(maintenanceSeenLimit)), rec.take())
}

// TestMaintenanceSeenResightRefreshesRecency pins that seeing a repository again makes it the most recently seen, so an older one is forgotten first.
func TestMaintenanceSeenResightRefreshesRecency(t *testing.T) {
	rec := &mtRecorder{}
	hooks := &Hooks{Storage: newStorage(t), Maintainer: rec}
	mtOpenNew(t, hooks, rec, 0, maintenanceSeenLimit)
	mtOpen(t, hooks, mtName(0))
	mtRequireQuiet(t, fmt.Sprintf("PreOpen of %s among %d repositories seen", mtName(0), maintenanceSeenLimit), rec.take())
	mtOpenNew(t, hooks, rec, maintenanceSeenLimit, maintenanceSeenLimit+1)
	mtOpen(t, hooks, mtName(0))
	mtRequireQuiet(t, fmt.Sprintf("PreOpen of %s, seen again just before %s,", mtName(0), mtName(maintenanceSeenLimit)), rec.take())
	mtOpen(t, hooks, mtName(1))
	mtRequireNotified(t, fmt.Sprintf("PreOpen of %s, the least recently seen when %s came,", mtName(1), mtName(maintenanceSeenLimit)), rec.take(), hooks.Storage.RepositoriesFS(), repository.ResolvePath(mtName(1)))
}

// TestMaintenanceSeenConcurrentFirstSight pins that concurrent first PreOpens of one repository notify Maintainer exactly once.
func TestMaintenanceSeenConcurrentFirstSight(t *testing.T) {
	rec := &mtRecorder{}
	hooks := &Hooks{Storage: newStorage(t), Maintainer: rec}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			<-start
			if err := hooks.PreOpen(context.Background(), mtName(0), false); err != nil {
				t.Errorf("PreOpen %s = %v, want nil", mtName(0), err)
			}
		})
	}
	close(start)
	wg.Wait()
	mtRequireNotified(t, "64 concurrent first PreOpens of "+mtName(0), rec.take(), hooks.Storage.RepositoriesFS(), repository.ResolvePath(mtName(0)))
}
