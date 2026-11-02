package e2e_test

import (
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"

	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// TestMaintenanceMatrix pins background maintenance after each write path and a repository's first sight; TestMain runs it under local and S3 storage.
func TestMaintenanceMatrix(t *testing.T) {
	for _, row := range []struct {
		name string
		run  func(t *testing.T)
	}{
		{"HTTPPush", func(t *testing.T) { mtPushRow(t, false) }},
		{"SSHPush", func(t *testing.T) { mtPushRow(t, true) }},
		{"HFAPICommit", mtAPICommitRow},
		{"MirrorSync", mtMirrorSyncRow},
		{"FirstSight", mtFirstSightRow},
		{"Native", mtNativeRow},
	} {
		t.Run(row.name, row.run)
	}
}

// mtPushRow pushes over HTTP or SSH once what creating the repository called for is flushed.
func mtPushRow(t *testing.T, ssh bool) {
	const repoID = "maint-org/pushed"
	opts := []e2eOption{withGoGit(), withMaintenance()}
	if ssh {
		opts = append(opts, withSSH())
	}
	s := newE2EServer(t, opts...)
	s.createRepo(t, "maint-org", "pushed")
	remote, env := s.httpRemote(repoID)
	if ssh {
		remote, env = s.sshRemote(repoID)
	}
	mtSettle(t, s, remote, env)
	testPushCommit(t, s, remote, env)
	pushed := mtMain(t, s, repoID)
	s.flushMaintenance(t)
	mtRequireReusable(t, s, repoID, pushed)
	if loose, packs := gitObjectFiles(t, s.storage.RepositoriesFS(), repository.ResolvePath(repoID)); loose != 0 || packs != 1 {
		t.Fatalf("objects/ holds %d loose objects and %d packs after maintenance, want the reusable pack alone", loose, packs)
	}
	mtCloneAndPush(t, remote, env, pushed)
}

// mtAPICommitRow commits through the HF API once what creating the repository called for is flushed.
func mtAPICommitRow(t *testing.T) {
	const repoID = "maint-org/committed"
	s := newE2EServer(t, withGoGit(), withMaintenance())
	s.createRepo(t, "maint-org", "committed")
	remote, env := s.httpRemote(repoID)
	mtSettle(t, s, remote, env)
	commitGCOperation(t, s.httpURL+"/api/models/"+repoID+"/commit/main", "file", map[string]any{
		"path": "README.md", "content": "# committed\n", "encoding": "utf-8",
	})
	committed := mtMain(t, s, repoID)
	s.flushMaintenance(t)
	mtRequireReusable(t, s, repoID, committed)
	mtCloneAndPush(t, remote, env, committed)
}

// mtMirrorSyncRow syncs a new upstream commit into a pull mirror whose first sync is already flushed.
func mtMirrorSyncRow(t *testing.T) {
	const repoID = "maint-org/mirrored"
	upstream, work, upEnv := seedMirrorUpstream(t, repoID, "# upstream\n")
	proxy := newE2EServer(t, withGoGit(), withMaintenance(), withMirrorSource(upstream.httpURL))
	remote, env := proxy.httpRemote(repoID)
	mtSettle(t, proxy, remote, env)
	if err := os.WriteFile(filepath.Join(work, "synced.txt"), []byte("synced\n"), 0o644); err != nil {
		t.Fatalf("write synced.txt: %v", err)
	}
	runGit(t, work, upEnv, "add", "synced.txt")
	runGit(t, work, upEnv, "commit", "-m", "Commit to sync")
	runGit(t, work, upEnv, "push", "origin", "main")
	synced := mtHead(t, work, upEnv)
	// The clone's pre-open sync fetches the commit, so only its post-receive can call for maintenance.
	mtClone(t, remote, env, synced)
	proxy.flushMaintenance(t)
	mtRequireReusable(t, proxy, repoID, synced)
	mtClone(t, remote, env, synced)
}

// mtFirstSightRow clones a repository written behind the server's back, so only its first sight can call for maintenance.
func mtFirstSightRow(t *testing.T) {
	const repoID = "maint-org/seeded"
	s := newE2EServer(t, withGoGit(), withMaintenance())
	fs, repoPath := s.storage.RepositoriesFS(), repository.ResolvePath(repoID)
	if err := fs.MkdirAll(filepath.Dir(repoPath), 0o755); err != nil {
		t.Fatalf("create the directory of %s: %v", repoID, err)
	}
	repo, err := repository.Init(t.Context(), fs, repoPath, "main")
	if err != nil {
		t.Fatalf("init %s: %v", repoID, err)
	}
	seeded, err := repo.CreateCommit(t.Context(), "main", "Seed", "Test User", "test@test.com",
		[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: "README.md", Content: []byte("# seeded\n")}}, "")
	if err != nil {
		t.Fatalf("seed %s: %v", repoID, err)
	}
	if rp, err := repo.ReusablePack(t.Context()); err != nil || rp != nil {
		t.Fatalf("ReusablePack of the seeded %s = %v, %v; want no manifest before any request", repoID, rp, err)
	}
	remote, env := s.httpRemote(repoID)
	mtClone(t, remote, env, seeded)
	s.flushMaintenance(t)
	mtRequireReusable(t, s, repoID, seeded)
	mtCloneAndPush(t, remote, env, seeded)
}

// mtNativeRow requires maintenance to leave a pushed native-engine repository as written.
func mtNativeRow(t *testing.T) {
	if testS3Client != nil {
		t.Skip("S3 storage is served by go-git alone")
	}
	const repoID = "maint-org/native"
	s := newE2EServer(t, withMaintenance())
	s.createRepo(t, "maint-org", "native")
	remote, env := s.httpRemote(repoID)
	testPushCommit(t, s, remote, env)
	fs, repoPath := s.storage.RepositoriesFS(), repository.ResolvePath(repoID)
	before := snapshotRepository(t, fs, repoPath)
	s.flushMaintenance(t)
	if rp, err := mtOpen(t, s, repoID).ReusablePack(t.Context()); err != nil || rp != nil {
		t.Fatalf("ReusablePack of the native-engine %s after flushMaintenance = %v, %v; want nil, nil", repoID, rp, err)
	}
	if !maps.Equal(before, snapshotRepository(t, fs, repoPath)) {
		t.Fatal("maintenance changed the files of a native-engine repository, want them as written")
	}
}

// mtSettle uses up the repository's first sight through remote and flushes, so the next run answers later writes alone.
func mtSettle(t *testing.T, s *e2eServer, remote string, env []string) {
	t.Helper()
	runGit(t, "", env, "ls-remote", remote)
	s.flushMaintenance(t)
}

func mtOpen(t *testing.T, s *e2eServer, repoID string) *repository.Repository {
	t.Helper()
	repo, err := repository.Open(s.storage.RepositoriesFS(), repository.ResolvePath(repoID))
	if err != nil {
		t.Fatalf("open %s: %v", repoID, err)
	}
	return repo
}

// mtMain returns refs/heads/main of repoID on the server.
func mtMain(t *testing.T, s *e2eServer, repoID string) string {
	t.Helper()
	refs, err := mtOpen(t, s, repoID).Refs()
	if err != nil || refs["refs/heads/main"] == "" {
		t.Fatalf("refs of %s = %v, %v; want refs/heads/main", repoID, refs, err)
	}
	return refs["refs/heads/main"]
}

// mtRequireReusable requires the reusable pack of repoID to close over commit.
func mtRequireReusable(t *testing.T, s *e2eServer, repoID, commit string) {
	t.Helper()
	rp, err := mtOpen(t, s, repoID).ReusablePack(t.Context())
	if err != nil || rp == nil {
		t.Fatalf("ReusablePack of %s after flushMaintenance = %v, %v; want the pack background maintenance laid out", repoID, rp, err)
	}
	if !slices.Contains(rp.Tips, plumbing.NewHash(commit)) {
		t.Fatalf("ReusablePack.Tips of %s = %v, want them to hold %s", repoID, rp.Tips, commit)
	}
}

// mtHead returns the commit checked out in dir.
func mtHead(t *testing.T, dir string, env []string) string {
	t.Helper()
	out, stderr, err := gitCmd(t, dir, env, "rev-parse", "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD in %s: %v\n%s", dir, err, stderr)
	}
	return strings.TrimSpace(out)
}

// mtClone requires a fresh clone through remote to check out want and returns its directory.
func mtClone(t *testing.T, remote string, env []string, want string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "clone")
	runGit(t, "", env, "clone", remote, dir)
	if head := mtHead(t, dir, env); head != want {
		t.Fatalf("a clone through %s checked out %s, want %s", remote, head, want)
	}
	return dir
}

// mtCloneAndPush requires a fresh clone to check out want, then pushes a further commit from it.
func mtCloneAndPush(t *testing.T, remote string, env []string, want string) {
	t.Helper()
	dir := mtClone(t, remote, env, want)
	runGit(t, dir, env, "config", "user.email", "test@test.com")
	runGit(t, dir, env, "config", "user.name", "Test User")
	if err := os.WriteFile(filepath.Join(dir, "after.txt"), []byte("after maintenance\n"), 0o644); err != nil {
		t.Fatalf("write after.txt: %v", err)
	}
	runGit(t, dir, env, "add", "after.txt")
	runGit(t, dir, env, "commit", "-m", "Commit after maintenance")
	runGit(t, dir, env, "push", "origin", "main")
}
