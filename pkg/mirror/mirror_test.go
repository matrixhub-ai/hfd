package mirror_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/osfs"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// initSourceRepo creates a bare repo with one commit on main. The path ends
// with .git because InitMirror normalizes source URLs to a .git suffix.
func initSourceRepo(t *testing.T, root, name string) (*repository.Repository, string) {
	t.Helper()
	path := filepath.Join(root, name+".git")
	repo, err := repository.Init(context.Background(), osfs.Default, path, "main")
	if err != nil {
		t.Fatalf("init source repo: %v", err)
	}
	addCommit(t, repo, "main", "README.md", "# src\n")
	return repo, path
}

func addCommit(t *testing.T, repo *repository.Repository, rev, file, content string) string {
	t.Helper()
	hash, err := repo.CreateCommit(context.Background(), rev, "commit "+file, "Test", "test@test.com",
		[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: file, Content: []byte(content)}}, "")
	if err != nil {
		t.Fatalf("create commit: %v", err)
	}
	return hash
}

func refsAt(t *testing.T, path string) map[string]string {
	t.Helper()
	repo, err := repository.Open(osfs.Default, path)
	if err != nil {
		t.Fatalf("open repo %s: %v", path, err)
	}
	refs, err := repo.Refs()
	if err != nil {
		t.Fatalf("refs of %s: %v", path, err)
	}
	return refs
}

func staticSource(path string) mirror.SourceFunc {
	return func(ctx context.Context, repoName string) (string, bool, error) {
		return path, true, nil
	}
}

func staticDestination(path string) mirror.DestinationFunc {
	return func(ctx context.Context, repoName string) (string, bool, error) {
		return path, true, nil
	}
}

func TestIsMirrorSourceAndDestinationUnset(t *testing.T) {
	m := mirror.NewMirror()
	if ok, err := m.IsMirrorSource(context.Background(), "org/repo"); err != nil || ok {
		t.Fatalf("IsMirrorSource = (%v, %v), want (false, nil)", ok, err)
	}
	if ok, err := m.IsMirrorDestination(context.Background(), "org/repo"); err != nil || ok {
		t.Fatalf("IsMirrorDestination = (%v, %v), want (false, nil)", ok, err)
	}
}

func TestPullFromRemoteInitializesMirror(t *testing.T) {
	root := t.TempDir()
	src, srcPath := initSourceRepo(t, root, "src")
	addCommit(t, src, "feature", "feature.txt", "feature\n")

	m := mirror.NewMirror(mirror.WithMirrorSourceFunc(staticSource(srcPath)))

	destPath := filepath.Join(root, "dest.git")
	if err := m.PullFromRemote(context.Background(), destPath, "org/repo", nil); err != nil {
		t.Fatalf("pull from remote: %v", err)
	}

	srcRefs, err := src.Refs()
	if err != nil {
		t.Fatalf("source refs: %v", err)
	}
	destRefs := refsAt(t, destPath)
	for ref, want := range srcRefs {
		if destRefs[ref] != want {
			t.Fatalf("ref %s = %s, want %s (all: %v)", ref, destRefs[ref], want, destRefs)
		}
	}
	if len(destRefs) != len(srcRefs) {
		t.Fatalf("dest refs = %v, want same set as source %v", destRefs, srcRefs)
	}
}

func TestPullFromRemoteSyncsNewCommitsAndFiresHooks(t *testing.T) {
	root := t.TempDir()
	src, srcPath := initSourceRepo(t, root, "src")

	var postUpdates []receive.RefUpdate
	m := mirror.NewMirror(
		mirror.WithMirrorSourceFunc(staticSource(srcPath)),
		mirror.WithPostReceiveHookFunc(func(ctx context.Context, repoName string, updates []receive.RefUpdate) error {
			postUpdates = append(postUpdates, updates...)
			return nil
		}),
	)

	destPath := filepath.Join(root, "dest.git")
	if err := m.PullFromRemote(context.Background(), destPath, "org/repo", nil); err != nil {
		t.Fatalf("initial pull: %v", err)
	}
	postUpdates = nil

	newHash := addCommit(t, src, "main", "new.txt", "new\n")
	if err := m.PullFromRemote(context.Background(), destPath, "org/repo", nil); err != nil {
		t.Fatalf("second pull: %v", err)
	}

	if got := refsAt(t, destPath)["refs/heads/main"]; got != newHash {
		t.Fatalf("refs/heads/main = %s, want %s", got, newHash)
	}
	if len(postUpdates) != 1 {
		t.Fatalf("post-receive updates = %d, want 1 (%v)", len(postUpdates), postUpdates)
	}
	if postUpdates[0].RefName() != "refs/heads/main" || postUpdates[0].NewRev() != newHash {
		t.Fatalf("unexpected update: %s %s -> %s", postUpdates[0].RefName(), postUpdates[0].OldRev(), postUpdates[0].NewRev())
	}
}

func TestPullFromRemotePreReceiveReject(t *testing.T) {
	root := t.TempDir()
	_, srcPath := initSourceRepo(t, root, "src")

	m := mirror.NewMirror(
		mirror.WithMirrorSourceFunc(staticSource(srcPath)),
		mirror.WithPreReceiveHookFunc(func(ctx context.Context, repoName string, updates []receive.RefUpdate) (bool, error) {
			return false, nil
		}),
	)

	destPath := filepath.Join(root, "dest.git")
	if err := m.PullFromRemote(context.Background(), destPath, "org/repo", nil); err != nil {
		t.Fatalf("pull from remote: %v", err)
	}

	// Rejected sync leaves the initialized mirror without any refs.
	if refs := refsAt(t, destPath); len(refs) != 0 {
		t.Fatalf("expected no refs after rejected sync, got %v", refs)
	}
}

func TestPullFromRemoteNotAMirror(t *testing.T) {
	m := mirror.NewMirror(mirror.WithMirrorSourceFunc(
		func(ctx context.Context, repoName string) (string, bool, error) {
			return "", false, nil
		}))

	err := m.PullFromRemote(context.Background(), filepath.Join(t.TempDir(), "dest.git"), "org/repo", nil)
	if err == nil || !strings.Contains(err.Error(), "not configured as a mirror") {
		t.Fatalf("expected not-a-mirror error, got %v", err)
	}
}

func TestPullFromRemoteTTLSkipsFreshSync(t *testing.T) {
	root := t.TempDir()
	src, srcPath := initSourceRepo(t, root, "src")

	m := mirror.NewMirror(
		mirror.WithMirrorSourceFunc(staticSource(srcPath)),
		mirror.WithTTL(time.Hour),
	)

	destPath := filepath.Join(root, "dest.git")
	if err := m.PullFromRemote(context.Background(), destPath, "org/repo", nil); err != nil {
		t.Fatalf("initial pull: %v", err)
	}
	oldHash := refsAt(t, destPath)["refs/heads/main"]

	addCommit(t, src, "main", "new.txt", "new\n")
	if err := m.PullFromRemote(context.Background(), destPath, "org/repo", nil); err != nil {
		t.Fatalf("second pull: %v", err)
	}

	if got := refsAt(t, destPath)["refs/heads/main"]; got != oldHash {
		t.Fatalf("refs/heads/main = %s, want unchanged %s (TTL should skip sync)", got, oldHash)
	}
}

func TestPushToRemoteWithoutDestinationFuncIsNoop(t *testing.T) {
	root := t.TempDir()
	_, localPath := initSourceRepo(t, root, "local")

	m := mirror.NewMirror()
	if err := m.PushToRemote(context.Background(), localPath, "org/repo", nil); err != nil {
		t.Fatalf("push to remote: %v", err)
	}
}

func TestPushToRemoteNotAPushMirrorIsNoop(t *testing.T) {
	root := t.TempDir()
	_, localPath := initSourceRepo(t, root, "local")

	m := mirror.NewMirror(mirror.WithMirrorDestinationFunc(
		func(ctx context.Context, repoName string) (string, bool, error) {
			return "", false, nil
		}))
	if err := m.PushToRemote(context.Background(), localPath, "org/repo", nil); err != nil {
		t.Fatalf("push to remote: %v", err)
	}
}

func TestPushToRemotePushesAllRefs(t *testing.T) {
	root := t.TempDir()
	local, localPath := initSourceRepo(t, root, "local")
	addCommit(t, local, "dev", "dev.txt", "dev\n")

	destPath := filepath.Join(root, "dest.git")
	if _, err := repository.Init(context.Background(), osfs.Default, destPath, "main"); err != nil {
		t.Fatalf("init dest repo: %v", err)
	}

	m := mirror.NewMirror(mirror.WithMirrorDestinationFunc(staticDestination(destPath)))
	if err := m.PushToRemote(context.Background(), localPath, "org/repo", nil); err != nil {
		t.Fatalf("push to remote: %v", err)
	}

	localRefs, err := local.Refs()
	if err != nil {
		t.Fatalf("local refs: %v", err)
	}
	destRefs := refsAt(t, destPath)
	for ref, want := range localRefs {
		if destRefs[ref] != want {
			t.Fatalf("ref %s = %s, want %s (all: %v)", ref, destRefs[ref], want, destRefs)
		}
	}
}

func TestPushToRemoteSpecificRefsOnly(t *testing.T) {
	root := t.TempDir()
	local, localPath := initSourceRepo(t, root, "local")
	addCommit(t, local, "dev", "dev.txt", "dev\n")

	destPath := filepath.Join(root, "dest.git")
	if _, err := repository.Init(context.Background(), osfs.Default, destPath, "main"); err != nil {
		t.Fatalf("init dest repo: %v", err)
	}

	m := mirror.NewMirror(mirror.WithMirrorDestinationFunc(staticDestination(destPath)))
	err := m.PushToRemote(context.Background(), localPath, "org/repo",
		&mirror.PushOptions{Refs: []string{"refs/heads/main"}})
	if err != nil {
		t.Fatalf("push to remote: %v", err)
	}

	destRefs := refsAt(t, destPath)
	if _, ok := destRefs["refs/heads/main"]; !ok {
		t.Fatalf("expected refs/heads/main to be pushed, got %v", destRefs)
	}
	if _, ok := destRefs["refs/heads/dev"]; ok {
		t.Fatalf("refs/heads/dev must not be pushed, got %v", destRefs)
	}
}
