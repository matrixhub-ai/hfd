package repository

// Tests for Repository.GC: unreachable objects go, everything a ref or HEAD
// reaches stays, and the cached handle keeps working afterwards.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/storage/filesystem"
)

// gcFixture is a bare repository with live branches, tag and detached HEAD plus loose and packed garbage.
type gcFixture struct {
	bare   string
	work   string
	repo   *Repository
	live   map[string]string
	loose  map[string]string
	packed map[string]string
	keep   []byte // keep.bin, reachable only through refs/heads/keep
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func revParse(t *testing.T, dir, name string) string {
	t.Helper()
	return strings.TrimSpace(gitOut(t, dir, "rev-parse", name))
}

// commitObjects returns the hashes of commit, its tree and the blob at file, labelled for assertions.
func commitObjects(t *testing.T, dir, commit, file string) map[string]string {
	t.Helper()
	return map[string]string{
		commit + " commit": revParse(t, dir, commit),
		commit + " tree":   revParse(t, dir, commit+"^{tree}"),
		commit + " blob":   revParse(t, dir, commit+":"+file),
	}
}

// pushBranch commits content on a new branch off main and pushes it, which lands as its own pack.
func (f *gcFixture) pushBranch(t *testing.T, name string, content []byte) map[string]string {
	t.Helper()
	runGit(t, f.work, "checkout", "-q", "-b", name, "main")
	commitFile(t, f.work, name+".bin", string(content), name)
	runGit(t, f.work, "push", "-q", "origin", name)
	return commitObjects(t, f.work, name, name+".bin")
}

// orphanPack pushes a branch and deletes it again, leaving its objects as packed garbage.
func (f *gcFixture) orphanPack(t *testing.T, name string) map[string]string {
	t.Helper()
	objects := f.pushBranch(t, name, randomBytes(t, 128<<10))
	runGit(t, f.bare, "update-ref", "-d", "refs/heads/"+name)
	return objects
}

// orphanLoose commits through hfd, which writes loose objects, then rolls main back past them.
func (f *gcFixture) orphanLoose(t *testing.T, name string) map[string]string {
	t.Helper()
	main := revParse(t, f.bare, "refs/heads/main")
	orphan, err := f.repo.CreateCommit(t.Context(), "main", name, "Test", "test@example.com",
		[]CommitOperation{{Type: CommitOperationAdd, Path: name + ".bin", Content: randomBytes(t, 128<<10)}}, main)
	if err != nil {
		t.Fatalf("create %s commit: %v", name, err)
	}
	runGit(t, f.bare, "update-ref", "refs/heads/main", main)
	return commitObjects(t, f.bare, orphan, name+".bin")
}

func buildGCFixture(t *testing.T) *gcFixture {
	t.Helper()
	root := t.TempDir()
	f := &gcFixture{bare: filepath.Join(root, "repo.git"), work: filepath.Join(root, "work"), keep: randomBytes(t, 64<<10)}
	runGit(t, "", "init", "--bare", "--initial-branch=main", f.bare)
	// Every push must land as its own pack and receive-pack must not gc behind the test.
	runGit(t, f.bare, "config", "receive.unpackLimit", "1")
	runGit(t, f.bare, "config", "gc.auto", "0")
	initParityWork(t, f.work)
	runGit(t, f.work, "remote", "add", "origin", f.bare)
	commitFile(t, f.work, "file.txt", "one\n", "c1")
	runGit(t, f.work, "push", "-q", "origin", "main")
	f.live = commitObjects(t, f.work, "main", "file.txt")
	maps.Copy(f.live, f.pushBranch(t, "keep", f.keep))
	runGit(t, f.work, "tag", "-a", "v1", "-m", "v1", "main")
	runGit(t, f.work, "push", "-q", "origin", "v1")
	f.live["v1 tag"] = revParse(t, f.work, "v1")
	maps.Copy(f.live, f.pushBranch(t, "detached", randomBytes(t, 32<<10)))
	runGit(t, f.bare, "update-ref", "--no-deref", "HEAD", revParse(t, f.work, "detached"))
	runGit(t, f.bare, "update-ref", "-d", "refs/heads/detached")
	f.packed = f.orphanPack(t, "tmp")

	repo, err := Open(osfs.Default, f.bare)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	f.repo = repo
	f.loose = f.orphanLoose(t, "orphan")
	return f
}

// fileState pins a file's metadata and content for byte-for-byte comparisons.
type fileState struct {
	mode     fs.FileMode
	size     int64
	modified int64
	digest   [sha256.Size]byte
}

// snapshotFiles records every file and directory under path on fs.
func snapshotFiles(t *testing.T, fs billy.Filesystem, path string) map[string]fileState {
	t.Helper()
	files := map[string]fileState{}
	err := util.Walk(fs, path, func(name string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		state := fileState{mode: info.Mode(), size: info.Size(), modified: info.ModTime().UnixNano()}
		if info.Mode().IsRegular() {
			content, err := util.ReadFile(fs, name)
			if err != nil {
				return err
			}
			state.digest = sha256.Sum256(content)
		}
		files[name] = state
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// gcWant is the result GC must report for removing exactly these objects, sized by native git.
func gcWant(t *testing.T, bare string, objects ...map[string]string) GCResult {
	t.Helper()
	var want GCResult
	for _, m := range objects {
		for _, hash := range m {
			size, err := strconv.ParseInt(strings.TrimSpace(gitOut(t, bare, "cat-file", "-s", hash)), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			want.DeletedObjects = append(want.DeletedObjects, plumbing.NewHash(hash))
			want.DeletedBytes += size
		}
	}
	plumbing.HashesSort(want.DeletedObjects)
	return want
}

// requireGCResult compares deleted objects and their bytes; ReclaimedBytes depends on packing and is checked by callers.
func requireGCResult(t *testing.T, label string, got, want GCResult) {
	t.Helper()
	if !slices.Equal(got.DeletedObjects, want.DeletedObjects) || got.DeletedBytes != want.DeletedBytes {
		t.Errorf("%s deleted %v (%d bytes), want %v (%d bytes)", label, got.DeletedObjects, got.DeletedBytes, want.DeletedObjects, want.DeletedBytes)
	}
}

// requireNothingDeleted asserts a successful GC that found nothing to remove.
func requireNothingDeleted(t *testing.T, label string, res GCResult, err error) {
	t.Helper()
	if err != nil || len(res.DeletedObjects) != 0 || res.DeletedBytes != 0 || res.ReclaimedBytes != 0 {
		t.Fatalf("%s = %+v, %v; want nothing deleted, nil", label, res, err)
	}
}

// memfsGCRepo builds a repository off the OS filesystem with one live commit on main and one orphaned commit.
func memfsGCRepo(t *testing.T, fs billy.Filesystem) (repo *Repository, live, orphan string) {
	t.Helper()
	ctx := t.Context()
	repo, err := Init(ctx, fs, "/repo.git", "main")
	if err != nil {
		t.Fatalf("init repository: %v", err)
	}
	add := func(rev, name string, content []byte, parent string) string {
		t.Helper()
		commit, err := repo.CreateCommit(ctx, rev, name, "Test", "test@example.com",
			[]CommitOperation{{Type: CommitOperationAdd, Path: name, Content: content}}, parent)
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		return commit
	}
	live = add("main", "file.txt", []byte("one\n"), "")
	orphan = add("main", "orphan.bin", randomBytes(t, 128<<10), live)
	if err := repo.repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), plumbing.NewHash(live))); err != nil {
		t.Fatal(err)
	}
	return repo, live, orphan
}

// A dry run needs no temporary directory: it predicts the deletions from the repository alone.
func TestGCDryRunWithoutTemporaryDirectory(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		fixture := buildGCFixture(t)
		requireGitMode(t, fixture.repo, native)
		want := gcWant(t, fixture.bare, fixture.loose, fixture.packed)
		unavailable := filepath.Join(t.TempDir(), "not-a-directory")
		if err := os.WriteFile(unavailable, []byte("unavailable"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("TMPDIR", unavailable)
		before := snapshotFiles(t, osfs.Default, fixture.bare)
		preview, err := fixture.repo.GC(t.Context(), time.Time{}, true)
		if err != nil {
			t.Fatal(err)
		}
		requireGCResult(t, "read-only preview", preview, want)
		if !maps.Equal(before, snapshotFiles(t, osfs.Default, fixture.bare)) {
			t.Fatal("dry-run GC changed repository files")
		}
	})
}

// A dry run reports exactly the objects the following GC removes, sized by native git, with no disk saving yet; the GC then reports the same objects and a smaller objects directory.
func TestGCDryRun(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		f := buildGCFixture(t)
		requireGitMode(t, f.repo, native)
		want := gcWant(t, f.bare, f.loose, f.packed)
		if len(want.DeletedObjects) != 6 {
			t.Fatalf("fixture has %d garbage objects, want 6", len(want.DeletedObjects))
		}
		before := snapshotFiles(t, osfs.Default, f.bare)
		preview, err := f.repo.GC(t.Context(), time.Time{}, true)
		if err != nil {
			t.Fatalf("dry-run GC: %v", err)
		}
		requireGCResult(t, "dry-run GC", preview, want)
		if preview.ReclaimedBytes != 0 {
			t.Errorf("dry-run GC reclaimed %d bytes, want 0 without a repack", preview.ReclaimedBytes)
		}
		if !maps.Equal(before, snapshotFiles(t, osfs.Default, f.bare)) {
			t.Fatal("dry-run GC changed repository files")
		}
		requireObjects(t, f, f.loose, true)
		requireObjects(t, f, f.packed, true)
		requireObjects(t, f, f.live, true)

		got, err := f.repo.GC(t.Context(), time.Time{}, false)
		if err != nil {
			t.Fatalf("GC: %v", err)
		}
		requireGCResult(t, "GC", got, want)
		if got.ReclaimedBytes <= 0 {
			t.Errorf("GC reclaimed %d bytes, want a smaller objects directory", got.ReclaimedBytes)
		}
		requireObjects(t, f, f.loose, false)
		requireObjects(t, f, f.packed, false)
		requireObjects(t, f, f.live, true)
		requireBlob(t, f.repo, "keep", "keep.bin", f.keep)
	})

	// Off the OS filesystem the preview runs go-git's model even though a git binary is configured, and never writes.
	t.Run("memfs", func(t *testing.T) {
		setGitBinary(t, filepath.Join(t.TempDir(), "no-such-git"))
		fs := memfs.New()
		repo, live, orphan := memfsGCRepo(t, fs)
		repo.fs = readOnlyFS{Filesystem: fs}
		commit, err := repo.repo.CommitObject(plumbing.NewHash(orphan))
		if err != nil {
			t.Fatal(err)
		}
		tree, err := commit.Tree()
		if err != nil {
			t.Fatal(err)
		}
		file, err := tree.File("orphan.bin")
		if err != nil {
			t.Fatal(err)
		}
		st := newStorer(fs, "/repo.git", cache.NewObjectLRUDefault())
		var want GCResult
		for _, h := range []plumbing.Hash{commit.Hash, tree.Hash, file.Hash} {
			size, err := st.EncodedObjectSize(h)
			if err != nil {
				t.Fatal(err)
			}
			want.DeletedObjects = append(want.DeletedObjects, h)
			want.DeletedBytes += size
		}
		plumbing.HashesSort(want.DeletedObjects)

		before := snapshotFiles(t, fs, "/repo.git")
		preview, err := repo.GC(t.Context(), time.Time{}, true)
		if err != nil {
			t.Fatalf("dry-run GC: %v", err)
		}
		requireGCResult(t, "dry-run GC", preview, want)
		if preview.ReclaimedBytes != 0 || !maps.Equal(before, snapshotFiles(t, fs, "/repo.git")) {
			t.Fatalf("dry-run GC reclaimed %d bytes or changed repository files", preview.ReclaimedBytes)
		}
		if !hasObject(t, fs, "/repo.git", orphan) {
			t.Fatalf("dry-run GC removed orphan commit %s", orphan)
		}

		repo.fs = fs
		got, err := repo.GC(t.Context(), time.Time{}, false)
		if err != nil {
			t.Fatalf("GC: %v", err)
		}
		requireGCResult(t, "GC", got, want)
		if got.ReclaimedBytes <= 0 {
			t.Errorf("GC reclaimed %d bytes, want a smaller objects directory", got.ReclaimedBytes)
		}
		if hasObject(t, fs, "/repo.git", orphan) {
			t.Errorf("orphan commit %s survived GC", orphan)
		}
		if !hasObject(t, fs, "/repo.git", live) {
			t.Errorf("live commit %s lost by GC", live)
		}
		requireBlob(t, repo, "main", "file.txt", []byte("one\n"))
	})
}

// readOnlyFS fails every mutating call, so a preview that writes fails instead of covering its tracks.
type readOnlyFS struct {
	billy.Filesystem
}

var errReadOnlyFS = errors.New("write to read-only filesystem")

func (readOnlyFS) Create(string) (billy.File, error)           { return nil, errReadOnlyFS }
func (readOnlyFS) Remove(string) error                         { return errReadOnlyFS }
func (readOnlyFS) Rename(string, string) error                 { return errReadOnlyFS }
func (readOnlyFS) MkdirAll(string, fs.FileMode) error          { return errReadOnlyFS }
func (readOnlyFS) Symlink(string, string) error                { return errReadOnlyFS }
func (readOnlyFS) TempFile(string, string) (billy.File, error) { return nil, errReadOnlyFS }

func (ro readOnlyFS) OpenFile(name string, flag int, perm fs.FileMode) (billy.File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND) != 0 {
		return nil, errReadOnlyFS
	}
	return ro.Filesystem.OpenFile(name, flag, perm)
}

// A native preview runs exactly a configuration check and one rev-list, with cutoff's grace fed in as roots; never gc, repack, prune or pack-objects.
func TestGCDryRunNativeCommands(t *testing.T) {
	for _, cutoff := range []time.Time{{}, time.Now().Add(-time.Hour)} {
		t.Run(gitExpiry(cutoff), func(t *testing.T) {
			bin, log := recordingGit(t)
			setGitBinary(t, bin)
			f := buildGCFixture(t)
			requireGitMode(t, f.repo, true)
			if _, err := f.repo.GC(t.Context(), cutoff, true); err != nil {
				t.Fatalf("dry-run GC: %v", err)
			}
			want := []string{
				"-C " + f.bare + " config --get-regexp " + previewBlockingConfigPattern,
				"-C " + f.bare + " rev-list --objects --no-object-names --all --reflog --indexed-objects --stdin",
			}
			if calls := strings.Split(strings.TrimSuffix(recordedCalls(t, log), "\n"), "\n"); !slices.Equal(calls, want) {
				t.Fatalf("git calls = %q, want %q", calls, want)
			}
		})
	}
}

// A promisor repository fetches missing reachable objects during any reachability walk, so the preview refuses before asking native git anything else; neither repository nor remote may change.
func TestGCDryRunPromisorRepository(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	setGitBinary(t, gitPath)
	root := t.TempDir()
	remote, bare, work := filepath.Join(root, "remote.git"), filepath.Join(root, "repo.git"), filepath.Join(root, "work")
	runGit(t, "", "init", "--bare", "--initial-branch=main", remote)
	runGit(t, "", "init", "--bare", "--initial-branch=main", bare)
	initParityWork(t, work)
	commitFile(t, work, "file.txt", "one\n", "c1")
	runGit(t, work, "push", "-q", remote, "main")
	runGit(t, work, "push", "-q", bare, "main")
	tree := commitObjects(t, work, "main", "file.txt")["main tree"]
	garbage := filepath.Join(root, "garbage.bin")
	if err := os.WriteFile(garbage, randomBytes(t, 8<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, bare, "hash-object", "-w", garbage)
	// The root commit stays, so the walk reads its tree from the promisor remote; the remote is a plain repository that can serve it.
	if err := os.Remove(filepath.Join(bare, "objects", tree[:2], tree[2:])); err != nil {
		t.Fatal(err)
	}
	runGit(t, bare, "config", "remote.origin.url", remote)
	runGit(t, bare, "config", "remote.origin.promisor", "true")
	if hasObject(t, osfs.Default, bare, tree) || len(packFiles(t, osfs.Default, bare)) != 0 || gitTry(t, remote, "cat-file", "-e", tree) != nil {
		t.Fatalf("fixture: tree %s must be missing from the loose-only repository and served by the remote", tree)
	}
	repo, err := Open(osfs.Default, bare)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	requireGitMode(t, repo, true)
	before, remoteBefore := snapshotFiles(t, osfs.Default, bare), snapshotFiles(t, osfs.Default, remote)
	if _, err := repo.GC(t.Context(), time.Time{}, true); !errors.Is(err, errGCPreviewUnsupported) {
		t.Fatalf("dry-run GC = %v, want %v", err, errGCPreviewUnsupported)
	}
	if packs := packFiles(t, osfs.Default, bare); len(packs) != 0 {
		t.Fatalf("dry-run GC fetched packs %v into the repository", packs)
	}
	if !maps.Equal(before, snapshotFiles(t, osfs.Default, bare)) {
		t.Fatal("dry-run GC changed repository files")
	}
	if !maps.Equal(remoteBefore, snapshotFiles(t, osfs.Default, remote)) {
		t.Fatal("dry-run GC changed the promisor remote")
	}
}

func TestGCDryRunBigPackThreshold(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	setGitBinary(t, gitPath)
	fixture := buildGCFixture(t)
	runGit(t, fixture.bare, "config", "gc.bigPackThreshold", "1")
	want := gcWant(t, fixture.bare, fixture.loose)
	before := snapshotFiles(t, osfs.Default, fixture.bare)
	if _, err := fixture.repo.GC(t.Context(), time.Time{}, true); !errors.Is(err, errGCPreviewUnsupported) {
		t.Fatalf("configured pack retention preview = %v, want %v", err, errGCPreviewUnsupported)
	}
	if !maps.Equal(before, snapshotFiles(t, osfs.Default, fixture.bare)) {
		t.Fatal("dry-run GC changed repository files")
	}
	result, err := fixture.repo.GC(t.Context(), time.Time{}, false)
	if err != nil {
		t.Fatal(err)
	}
	requireGCResult(t, "GC with pack retention", result, want)
	requireObjects(t, fixture, fixture.packed, true)
	requireObjects(t, fixture, fixture.live, true)
}

// Config read failures must not be interpreted as an ordinary repository.
func TestGCPreviewBlockedGit(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	setGitBinary(t, gitPath)
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, bare string)
		want  bool
	}{
		{"Plain", func(*testing.T, string) {}, false},
		{"NearMiss", func(t *testing.T, bare string) {
			runGit(t, bare, "config", "remote.origin.promisorx", "true")
			runGit(t, bare, "config", "remote.origin.partialCloneFilter", "blob:none")
			runGit(t, bare, "config", "promisor.acceptFromServer", "all")
			runGit(t, bare, "config", "gc.pruneExpire", "1.hour.ago")
		}, false},
		{"RemotePromisor", func(t *testing.T, bare string) { runGit(t, bare, "config", "remote.Origin.promisor", "true") }, true},
		{"PartialCloneExtension", func(t *testing.T, bare string) { runGit(t, bare, "config", "extensions.partialClone", "origin") }, true},
		{"RecentObjectsHook", func(t *testing.T, bare string) { runGit(t, bare, "config", "gc.recentObjectsHook", "/bin/true") }, true},
		{"Included", func(t *testing.T, bare string) {
			inc := filepath.Join(t.TempDir(), "promisor.inc")
			if err := os.WriteFile(inc, []byte("[remote \"origin\"]\n\tpromisor = true\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			runGit(t, bare, "config", "include.path", inc)
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bare := filepath.Join(t.TempDir(), "repo.git")
			runGit(t, "", "init", "--bare", bare)
			tc.setup(t, bare)
			if got, err := previewBlockedGit(t.Context(), bare); err != nil || got != tc.want {
				t.Fatalf("previewBlockedGit = %t, %v; want %t, nil", got, err, tc.want)
			}
		})
	}
	t.Run("BrokenConfig", func(t *testing.T) {
		bare := filepath.Join(t.TempDir(), "repo.git")
		runGit(t, "", "init", "--bare", bare)
		appendConfig(t, bare, "[\n")
		if got, err := previewBlockedGit(t.Context(), bare); err == nil {
			t.Fatalf("previewBlockedGit = %t, nil; want git's config error", got)
		}
	})
	t.Run("Cancelled", func(t *testing.T) {
		bare := filepath.Join(t.TempDir(), "repo.git")
		runGit(t, "", "init", "--bare", bare)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := previewBlockedGit(ctx, bare); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled previewBlockedGit = %v, want context.Canceled", err)
		}
	})
}

// A rev-list line that is not an object hash fails the preview rather than being guessed at.
func TestGCDryRunNativeRevListOutput(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	f := buildGCFixture(t)
	bin := filepath.Join(t.TempDir(), "git")
	script := fmt.Sprintf("#!/bin/sh\nif [ \"$3\" = rev-list ]; then\n  echo 'not an object'\n  exit 0\nfi\nexec %q \"$@\"\n", gitPath)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	setGitBinary(t, bin)
	requireGitMode(t, f.repo, true)
	if _, err := f.repo.GC(t.Context(), time.Time{}, true); err == nil || !strings.Contains(err.Error(), "unexpected line") {
		t.Fatalf("dry-run GC = %v, want an unexpected line error", err)
	}
	requireObjects(t, f, f.loose, true)
}

// A fresh unreachable commit on top of the expired orphan: native git keeps everything the recent commit reaches, go-git prunes it; each preview matches its engine.
func TestGCGraceRecentParent(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		f := buildGCFixture(t)
		requireGitMode(t, f.repo, native)
		ageObjects(t, f.bare, time.Now().Add(-2*time.Hour))
		main := revParse(t, f.bare, "refs/heads/main")
		for label, hash := range f.loose {
			if strings.HasSuffix(label, " commit") {
				runGit(t, f.bare, "update-ref", "refs/heads/main", hash)
			}
		}
		fresh := f.orphanLoose(t, "fresh")
		runGit(t, f.bare, "update-ref", "refs/heads/main", main)
		cutoff := time.Now().Add(-time.Hour)
		want := gcWant(t, f.bare, f.packed)
		if !native {
			want = gcWant(t, f.bare, f.loose, f.packed)
		}
		preview, err := f.repo.GC(t.Context(), cutoff, true)
		if err != nil {
			t.Fatalf("dry-run GC: %v", err)
		}
		requireGCResult(t, "dry-run GC", preview, want)
		requireObjects(t, f, f.loose, true)
		got, err := f.repo.GC(t.Context(), cutoff, false)
		if err != nil {
			t.Fatalf("GC: %v", err)
		}
		requireGCResult(t, "GC", got, want)
		requireObjects(t, f, f.loose, native)
		requireObjects(t, f, fresh, true)
		requireObjects(t, f, f.live, true)
	})
}

// Two objects whose hashes share a first byte fill one fanout bucket of a pack index; go-git's in-memory index iterator pads every entry but the bucket's last with the following entry's bytes, and a preview keyed on such hashes calls live objects deleted.
func TestGCPackIndexSharedFanoutBucket(t *testing.T) {
	setGitBinary(t, "")
	root := t.TempDir()
	bare, work := filepath.Join(root, "repo.git"), filepath.Join(root, "work")
	runGit(t, "", "init", "--bare", "--initial-branch=main", bare)
	runGit(t, bare, "config", "receive.unpackLimit", "1")
	initParityWork(t, work)
	runGit(t, work, "remote", "add", "origin", bare)
	firstByte := func(content []byte) byte {
		sum := sha1.Sum(append([]byte("blob "+strconv.Itoa(len(content))+"\x00"), content...))
		return sum[0]
	}
	first, second := []byte("one\n"), []byte("two 0\n")
	for i := 1; firstByte(second) != firstByte(first); i++ {
		second = fmt.Appendf(nil, "two %d\n", i)
	}
	for name, content := range map[string][]byte{"first.txt": first, "second.txt": second} {
		if err := os.WriteFile(filepath.Join(work, name), content, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-q", "-m", "shared bucket")
	runGit(t, work, "push", "-q", "origin", "main")
	packs, err := packInfos(t.Context(), osfs.Default, bare)
	if err != nil || len(packs) != 1 {
		t.Fatalf("packInfos = %v, %v; want the single pushed pack", packs, err)
	}
	for _, h := range packs[0].objects {
		if h != plumbing.NewHash(h.String()) {
			t.Errorf("packInfos listed %s in a form plumbing.NewHash does not produce", h)
		}
	}
	repo, err := Open(osfs.Default, bare)
	if err != nil {
		t.Fatal(err)
	}
	requireGitMode(t, repo, false)
	for _, dry := range []bool{true, false} {
		res, err := repo.GC(t.Context(), time.Time{}, dry)
		requireNothingDeleted(t, fmt.Sprintf("GC(dryRun=%t)", dry), res, err)
	}
}

// go-git leaves a lone pack without loose objects alone, so a preview must not promise the garbage inside it.
func TestGCDryRunSinglePackSkip(t *testing.T) {
	setGitBinary(t, "")
	f := buildGCFixture(t)
	requireGitMode(t, f.repo, false)
	if _, err := f.repo.GC(t.Context(), time.Time{}, false); err != nil {
		t.Fatalf("GC: %v", err)
	}
	if packs := packFiles(t, osfs.Default, f.bare); len(packs) != 1 {
		t.Fatalf("packs after GC = %v, want one", packs)
	}
	runGit(t, f.bare, "update-ref", "-d", "refs/heads/keep")
	for _, dry := range []bool{true, false} {
		res, err := f.repo.GC(t.Context(), time.Time{}, dry)
		requireNothingDeleted(t, fmt.Sprintf("GC(dryRun=%t)", dry), res, err)
	}
	requireObjects(t, f, f.live, true)
}

// States whose outcome only a real GC could tell are refused with errGCPreviewUnsupported, before anything is written; go-git ignores git's markers and configuration.
func TestGCDryRunUnsupported(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cutoff time.Time
		goGit  bool // refused by the go-git engine too
		setup  func(t *testing.T, f *gcFixture)
	}{
		{"Alternates", time.Time{}, true, func(t *testing.T, f *gcFixture) {
			if err := os.WriteFile(filepath.Join(f.bare, "objects", "info", "alternates"), []byte(t.TempDir()+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"Reflogs", time.Time{}, false, func(t *testing.T, f *gcFixture) {
			runGit(t, f.bare, "config", "core.logAllRefUpdates", "true")
			main := revParse(t, f.bare, "refs/heads/main")
			runGit(t, f.bare, "update-ref", "refs/heads/main", f.live["keep commit"])
			runGit(t, f.bare, "update-ref", "refs/heads/main", main)
			if _, err := os.Stat(filepath.Join(f.bare, "logs", "refs", "heads", "main")); err != nil {
				t.Fatal(err)
			}
		}},
		{"RecentObjectsHook", time.Time{}, false, func(t *testing.T, f *gcFixture) {
			runGit(t, f.bare, "config", "gc.recentObjectsHook", "/bin/true")
		}},
		{"KeepWithGrace", time.Now().Add(-time.Hour), false, func(t *testing.T, f *gcFixture) { markPack(t, f, "keep") }},
		{"CruftWithGrace", time.Now().Add(-time.Hour), false, func(t *testing.T, f *gcFixture) { markPack(t, f, "mtimes") }},
		{"PromisorPack", time.Time{}, false, func(t *testing.T, f *gcFixture) { markPack(t, f, "promisor") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			forEachGitMode(t, func(t *testing.T, native bool) {
				f := buildGCFixture(t)
				requireGitMode(t, f.repo, native)
				tc.setup(t, f)
				before := snapshotFiles(t, osfs.Default, f.bare)
				_, err := f.repo.GC(t.Context(), tc.cutoff, true)
				if native || tc.goGit {
					if !errors.Is(err, errGCPreviewUnsupported) {
						t.Fatalf("dry-run GC = %v, want %v", err, errGCPreviewUnsupported)
					}
				} else if err != nil {
					t.Fatalf("dry-run GC: %v", err)
				}
				if !maps.Equal(before, snapshotFiles(t, osfs.Default, f.bare)) {
					t.Fatal("dry-run GC changed repository files")
				}
			})
		})
	}
}

// packContaining returns the objects/pack/pack-<hash> base path of the fixture pack holding object.
func packContaining(t *testing.T, f *gcFixture, object string) string {
	t.Helper()
	packs, err := packInfos(t.Context(), osfs.Default, f.bare)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range packs {
		if slices.Contains(p.objects, plumbing.NewHash(object)) {
			return filepath.Join(f.bare, "objects", "pack", "pack-"+p.hash.String())
		}
	}
	t.Fatalf("no pack holds %s", object)
	return ""
}

// markPack writes git's marker ext beside the pack holding the orphaned tmp branch.
func markPack(t *testing.T, f *gcFixture, ext string) {
	t.Helper()
	if err := os.WriteFile(packContaining(t, f, f.packed["tmp commit"])+"."+ext, nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

// Native gc never rewrites a .keep pack: without grace everything in it survives, garbage another pack duplicates included, while the loose orphan goes; the preview says the same.
func TestGCKeepPack(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	setGitBinary(t, gitPath)
	f := buildGCFixture(t)
	requireGitMode(t, f.repo, true)
	keep := packContaining(t, f, f.packed["tmp commit"])
	// One more pack holding every reachable object, tmp's included, then only the original tmp pack is kept.
	runGit(t, f.bare, "update-ref", "refs/heads/tmp", f.packed["tmp commit"])
	runGit(t, f.bare, "repack", "-a", "-q")
	runGit(t, f.bare, "update-ref", "-d", "refs/heads/tmp")
	if err := os.WriteFile(keep+".keep", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	want := gcWant(t, f.bare, f.loose)
	for _, dry := range []bool{true, false} {
		res, err := f.repo.GC(t.Context(), time.Time{}, dry)
		if err != nil {
			t.Fatalf("GC(dryRun=%t): %v", dry, err)
		}
		requireGCResult(t, fmt.Sprintf("GC(dryRun=%t)", dry), res, want)
	}
	requireObjects(t, f, f.packed, true)
	requireObjects(t, f, f.loose, false)
	requireObjects(t, f, f.live, true)
	if _, err := os.Stat(keep + ".pack"); err != nil {
		t.Fatalf("kept pack after GC: %v", err)
	}
	gitFsck(t, f.bare)
}

// ageObjects backdates every file under objects/ so an age guard sees the garbage present now as expired.
func ageObjects(t *testing.T, bare string, when time.Time) {
	t.Helper()
	err := filepath.WalkDir(filepath.Join(bare, "objects"), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		return os.Chtimes(path, when, when)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// hasObject reads hash through a fresh storer so no cached copy can stand in for the on-disk object.
func hasObject(t *testing.T, fs billy.Filesystem, path, hash string) bool {
	t.Helper()
	_, err := newStorer(fs, path, cache.NewObjectLRUDefault()).EncodedObject(plumbing.AnyObject, plumbing.NewHash(hash))
	if err != nil && !errors.Is(err, plumbing.ErrObjectNotFound) {
		t.Fatalf("read object %s: %v", hash, err)
	}
	return err == nil
}

// requireObjects asserts every object is present (or gone) for both a fresh go-git storer and native git.
func requireObjects(t *testing.T, f *gcFixture, objects map[string]string, present bool) {
	t.Helper()
	for label, hash := range objects {
		if got := hasObject(t, osfs.Default, f.bare, hash); got != present {
			t.Errorf("%s %s: go-git present = %t, want %t", label, hash, got, present)
		}
		if got := gitTry(t, f.bare, "cat-file", "-e", hash) == nil; got != present {
			t.Errorf("%s %s: git present = %t, want %t", label, hash, got, present)
		}
	}
}

func TestGCLooseOrphans(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		f := buildGCFixture(t)
		requireGitMode(t, f.repo, native)
		refs := gitLocalRefs(t, f.bare)
		res, err := f.repo.GC(t.Context(), time.Time{}, false)
		if err != nil {
			t.Fatalf("GC: %v", err)
		}
		if res.ReclaimedBytes < 100<<10 {
			t.Errorf("reclaimed %d bytes, want at least the 128 KiB orphan blob", res.ReclaimedBytes)
		}
		requireObjects(t, f, f.loose, false)
		requireObjects(t, f, f.live, true)
		requireSameRefs(t, "refs after GC", gitLocalRefs(t, f.bare), refs)
		gitFsck(t, f.bare)
	})

	// Off the OS filesystem only go-git can run, whatever binary is configured.
	t.Run("memfs", func(t *testing.T) {
		setGitBinary(t, filepath.Join(t.TempDir(), "no-such-git"))
		fs := memfs.New()
		repo, live, orphan := memfsGCRepo(t, fs)
		res, err := repo.GC(t.Context(), time.Time{}, false)
		if err != nil {
			t.Fatalf("GC: %v", err)
		}
		if res.ReclaimedBytes < 100<<10 {
			t.Errorf("reclaimed %d bytes, want at least the 128 KiB orphan blob", res.ReclaimedBytes)
		}
		if hasObject(t, fs, "/repo.git", orphan) {
			t.Errorf("orphan commit %s survived GC", orphan)
		}
		if !hasObject(t, fs, "/repo.git", live) {
			t.Errorf("live commit %s lost by GC", live)
		}
		requireBlob(t, repo, "main", "file.txt", []byte("one\n"))
	})
}

// packFiles lists the packfiles of the bare repository at path on fs.
func packFiles(t *testing.T, fs billy.Filesystem, path string) []string {
	t.Helper()
	entries, err := fs.ReadDir(filepath.Join(path, "objects", "pack"))
	if err != nil {
		t.Fatal(err)
	}
	var packs []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".pack") {
			packs = append(packs, e.Name())
		}
	}
	return packs
}

func TestGCPackedGarbage(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		f := buildGCFixture(t)
		requireGitMode(t, f.repo, native)
		if n := len(packFiles(t, osfs.Default, f.bare)); n < 4 {
			t.Fatalf("fixture has %d packs, want one per push", n)
		}
		head := strings.TrimSpace(gitOut(t, f.bare, "rev-parse", "HEAD"))
		res, err := f.repo.GC(t.Context(), time.Time{}, false)
		if err != nil {
			t.Fatalf("GC: %v", err)
		}
		if res.ReclaimedBytes < 200<<10 {
			t.Errorf("reclaimed %d bytes, want at least the 256 KiB of orphaned blobs", res.ReclaimedBytes)
		}
		requireObjects(t, f, f.packed, false)
		requireObjects(t, f, f.live, true)
		if packs := packFiles(t, osfs.Default, f.bare); len(packs) != 1 {
			t.Errorf("packs after GC = %v, want a single consolidated pack", packs)
		}
		if got := strings.TrimSpace(gitOut(t, f.bare, "rev-parse", "HEAD")); got != head {
			t.Errorf("detached HEAD = %s, want %s", got, head)
		}
		gitFsck(t, f.bare)
	})
}

// A positive cutoff expires only garbage older than it; a zero cutoff on the same handle then removes the rest. Each preview names exactly what its GC then removes.
func TestGCGrace(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		f := buildGCFixture(t)
		requireGitMode(t, f.repo, native)
		ageObjects(t, f.bare, time.Now().Add(-2*time.Hour))
		freshLoose := f.orphanLoose(t, "fresh-orphan")
		freshPacked := f.orphanPack(t, "fresh-tmp")
		cutoff := time.Now().Add(-time.Hour)
		want := gcWant(t, f.bare, f.loose, f.packed)
		preview, err := f.repo.GC(t.Context(), cutoff, true)
		if err != nil {
			t.Fatalf("dry-run GC with grace: %v", err)
		}
		requireGCResult(t, "dry-run GC with grace", preview, want)
		requireObjects(t, f, f.loose, true)
		requireObjects(t, f, f.packed, true)
		got, err := f.repo.GC(t.Context(), cutoff, false)
		if err != nil {
			t.Fatalf("GC with grace: %v", err)
		}
		requireGCResult(t, "GC with grace", got, want)
		requireObjects(t, f, f.loose, false)
		requireObjects(t, f, f.packed, false)
		requireObjects(t, f, freshLoose, true)
		requireObjects(t, f, freshPacked, true)
		requireObjects(t, f, f.live, true)
		gitFsck(t, f.bare)
		// Native gc parks the fresh garbage in a cruft pack, which the zero-grace preview must then read like any other.
		if cruft, _ := filepath.Glob(filepath.Join(f.bare, "objects", "pack", "*.mtimes")); (len(cruft) != 0) != native {
			t.Fatalf("cruft packs after GC with grace = %v, want one iff native", cruft)
		}

		want = gcWant(t, f.bare, freshLoose, freshPacked)
		preview, err = f.repo.GC(t.Context(), time.Time{}, true)
		if err != nil {
			t.Fatalf("dry-run GC without grace: %v", err)
		}
		requireGCResult(t, "dry-run GC without grace", preview, want)
		requireObjects(t, f, freshLoose, true)
		requireObjects(t, f, freshPacked, true)
		got, err = f.repo.GC(t.Context(), time.Time{}, false)
		if err != nil {
			t.Fatalf("GC without grace: %v", err)
		}
		requireGCResult(t, "GC without grace", got, want)
		requireObjects(t, f, freshLoose, false)
		requireObjects(t, f, freshPacked, false)
		requireObjects(t, f, f.live, true)
		if packs := packFiles(t, osfs.Default, f.bare); len(packs) != 1 {
			t.Errorf("packs after GC = %v, want a single consolidated pack", packs)
		}
		gitFsck(t, f.bare)
	})
}

// requireBlob reads rev:path through the hfd API and compares its content.
func requireBlob(t *testing.T, repo *Repository, rev, path string, want []byte) {
	t.Helper()
	b, err := repo.Blob(rev, path)
	if err != nil {
		t.Fatalf("Blob(%s, %s): %v", rev, path, err)
	}
	rc, err := b.NewReader()
	if err != nil {
		t.Fatalf("Blob(%s, %s) reader: %v", rev, path, err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("Blob(%s, %s) read: %v", rev, path, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Blob(%s, %s) = %d bytes, want %d bytes of the pushed content", rev, path, len(got), len(want))
	}
}

// The handle GC ran on keeps serving objects the deleted packs held, cached beforehand or first read after, and Open still returns it.
func TestGCSameHandle(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		f := buildGCFixture(t)
		requireGitMode(t, f.repo, native)
		requireBlob(t, f.repo, "main", "file.txt", []byte("one\n"))
		if _, err := f.repo.GC(t.Context(), time.Time{}, false); err != nil {
			t.Fatalf("GC: %v", err)
		}
		requireBlob(t, f.repo, "main", "file.txt", []byte("one\n"))
		requireBlob(t, f.repo, "keep", "keep.bin", f.keep)
		repo, err := Open(osfs.Default, f.bare)
		if err != nil {
			t.Fatalf("open after GC: %v", err)
		}
		if repo != f.repo {
			t.Fatal("GC evicted its handle from the cache")
		}
	})
}

// Native gc packs every ref; hfd must still list them and commit on top of one.
func TestGCPackedRefs(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	setGitBinary(t, gitPath)
	f := buildGCFixture(t)
	requireGitMode(t, f.repo, true)
	refs := gitLocalRefs(t, f.bare)
	if _, err := f.repo.GC(t.Context(), time.Time{}, false); err != nil {
		t.Fatalf("GC: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.bare, "packed-refs")); err != nil {
		t.Fatalf("packed-refs after native gc: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.bare, "refs", "heads", "keep")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("loose refs/heads/keep after native gc: %v", err)
	}
	got, err := f.repo.Refs()
	if err != nil {
		t.Fatalf("Refs: %v", err)
	}
	requireSameRefs(t, "Refs after GC", got, refs)
	branches, err := f.repo.Branches()
	if err != nil {
		t.Fatalf("Branches: %v", err)
	}
	slices.Sort(branches)
	if want := []string{"keep", "main"}; !slices.Equal(branches, want) {
		t.Fatalf("Branches = %v, want %v", branches, want)
	}
	commit, err := f.repo.CreateCommit(t.Context(), "keep", "after gc", "Test", "test@example.com",
		[]CommitOperation{{Type: CommitOperationAdd, Path: "after.txt", Content: []byte("after\n")}}, refs["refs/heads/keep"])
	if err != nil {
		t.Fatalf("CreateCommit on packed branch: %v", err)
	}
	if got := revParse(t, f.bare, "refs/heads/keep"); got != commit {
		t.Fatalf("git sees refs/heads/keep = %s, want %s", got, commit)
	}
	requireBlob(t, f.repo, "keep", "keep.bin", f.keep)
	gitFsck(t, f.bare)
}

// An init-only repository with an unborn HEAD is left alone and stays writable.
func TestGCEmptyRepository(t *testing.T) {
	empty := func(t *testing.T, fs billy.Filesystem, native bool) {
		t.Helper()
		ctx := t.Context()
		repo, err := Init(ctx, fs, "/empty.git", "main")
		if err != nil {
			t.Fatalf("init repository: %v", err)
		}
		requireGitMode(t, repo, native)
		for _, dry := range []bool{true, false} {
			res, err := repo.GC(ctx, time.Time{}, dry)
			requireNothingDeleted(t, fmt.Sprintf("GC(dryRun=%t)", dry), res, err)
		}
		if packs := packFiles(t, fs, "/empty.git"); len(packs) != 0 {
			t.Fatalf("GC wrote packs %v into an empty repository", packs)
		}
		if _, err := repo.CreateCommit(ctx, "main", "first", "Test", "test@example.com",
			[]CommitOperation{{Type: CommitOperationAdd, Path: "file.txt", Content: []byte("one\n")}}, ""); err != nil {
			t.Fatalf("CreateCommit after GC: %v", err)
		}
		requireBlob(t, repo, "main", "file.txt", []byte("one\n"))
	}
	forEachGitMode(t, func(t *testing.T, native bool) { empty(t, osfs.New(t.TempDir()), native) })
	t.Run("memfs", func(t *testing.T) { empty(t, memfs.New(), false) })
}

// Cancellation and a failing git leave every object in place and are reported, for previews too.
func TestGCCancelled(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		f := buildGCFixture(t)
		requireGitMode(t, f.repo, native)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		for _, dry := range []bool{true, false} {
			if _, err := f.repo.GC(ctx, time.Time{}, dry); !errors.Is(err, context.Canceled) {
				t.Fatalf("GC(dryRun=%t) = %v, want context.Canceled", dry, err)
			}
		}
		requireObjects(t, f, f.loose, true)
		requireObjects(t, f, f.packed, true)
	})

	// A git that never finishes must be killed as soon as the context ends: the preview's rev-list or the real gc.
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	for _, tc := range []struct {
		name  string
		dry   bool
		stage string
	}{{"NativeDuringGC", false, "gc"}, {"NativeDuringPreview", true, "rev-list"}} {
		t.Run(tc.name, func(t *testing.T) {
			f := buildGCFixture(t)
			dir := t.TempDir()
			started := filepath.Join(dir, "started")
			bin := filepath.Join(dir, "git")
			script := fmt.Sprintf("#!/bin/sh\nif [ \"$3\" = %s ]; then\n  touch %q\n  exec sleep 60\nfi\nexec %q \"$@\"\n", tc.stage, started, gitPath)
			if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			setGitBinary(t, bin)
			requireGitMode(t, f.repo, true)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := f.repo.GC(ctx, time.Time{}, tc.dry)
				done <- err
			}()
			for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
				if _, err := os.Stat(started); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("git %s was not started", tc.stage)
				}
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("GC = %v, want context.Canceled", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("GC did not return after cancellation")
			}
			requireObjects(t, f, f.loose, true)
			requireObjects(t, f, f.packed, true)
		})
	}

	t.Run("NativeGitFails", func(t *testing.T) {
		f := buildGCFixture(t)
		setGitBinary(t, filepath.Join(t.TempDir(), "no-such-git"))
		requireGitMode(t, f.repo, true)
		for _, dry := range []bool{true, false} {
			if _, err := f.repo.GC(t.Context(), time.Time{}, dry); err == nil {
				t.Fatalf("GC(dryRun=%t) with a missing git binary succeeded", dry)
			}
		}
		requireObjects(t, f, f.loose, true)
		requireObjects(t, f, f.packed, true)
		requireObjects(t, f, f.live, true)
	})
}

// packCloseCanceller cancels its context as go-git closes the repack's new pack, which go-git itself never checks.
type packCloseCanceller struct {
	*filesystem.Storage
	cancel   context.CancelFunc
	closeErr error
}

func (s *packCloseCanceller) PackfileWriter() (io.WriteCloser, error) {
	wc, err := s.Storage.PackfileWriter()
	if err != nil {
		return nil, err
	}
	return &cancelCloser{WriteCloser: wc, cancel: s.cancel, err: s.closeErr}, nil
}

type cancelCloser struct {
	io.WriteCloser
	cancel context.CancelFunc
	err    error
}

func (c *cancelCloser) Close() error {
	c.cancel()
	return errors.Join(c.WriteCloser.Close(), c.err)
}

// A cancellation arriving inside the repack is reported even though go-git finishes, together with any engine error.
func TestGCCancelledDuringRepack(t *testing.T) {
	errClose := errors.New("close failed")
	for _, tc := range []struct {
		name     string
		closeErr error
	}{{"CleanClose", nil}, {"FailedClose", errClose}} {
		t.Run(tc.name, func(t *testing.T) {
			setGitBinary(t, "")
			f := buildGCFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			c := cache.NewObjectLRUDefault()
			st := &packCloseCanceller{Storage: newStorer(osfs.Default, f.bare, c), cancel: cancel, closeErr: tc.closeErr}
			gr, err := git.Open(st, nil)
			if err != nil {
				t.Fatal(err)
			}
			r := &Repository{repo: gr, objectCache: c, fs: osfs.Default, repoPath: f.bare, localDir: localDir(osfs.Default, f.bare)}
			requireGitMode(t, r, false)
			_, err = r.GC(ctx, time.Time{}, false)
			if !errors.Is(err, context.Canceled) || (tc.closeErr != nil && !errors.Is(err, tc.closeErr)) {
				t.Fatalf("GC = %v, want context.Canceled and %v", err, tc.closeErr)
			}
			requireObjects(t, f, f.live, true)
			requireBlob(t, r, "keep", "keep.bin", f.keep)
			gitFsck(t, f.bare)
		})
	}
}

// go-git's object walker has no blob case, so a tag on a blob or a symlink entry fails GC closed; native git handles both.
func TestGCTagToBlobAndSymlink(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, f *gcFixture) map[string]string
	}{
		{"TagToBlob", func(t *testing.T, f *gcFixture) map[string]string {
			if err := os.WriteFile(filepath.Join(f.work, "blob.bin"), randomBytes(t, 8<<10), 0o644); err != nil {
				t.Fatal(err)
			}
			blob := strings.TrimSpace(gitOut(t, f.work, "hash-object", "-w", "blob.bin"))
			runGit(t, f.work, "tag", "-a", "blob-tag", "-m", "blob tag", blob)
			runGit(t, f.work, "push", "-q", "origin", "blob-tag")
			return map[string]string{"blob-tag tag": revParse(t, f.work, "blob-tag"), "blob-tag blob": blob}
		}},
		{"Symlink", func(t *testing.T, f *gcFixture) map[string]string {
			runGit(t, f.work, "checkout", "-q", "-b", "symlink", "main")
			if err := os.Symlink("file.txt", filepath.Join(f.work, "link")); err != nil {
				t.Fatal(err)
			}
			runGit(t, f.work, "add", "link")
			runGit(t, f.work, "commit", "-q", "-m", "symlink")
			runGit(t, f.work, "push", "-q", "origin", "symlink")
			return commitObjects(t, f.work, "symlink", "link")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			forEachGitMode(t, func(t *testing.T, native bool) {
				f := buildGCFixture(t)
				requireGitMode(t, f.repo, native)
				live := tc.setup(t, f)
				// The push landed a pack behind the handle, which hfd reindexes after every native write.
				if err := f.repo.reindex(); err != nil {
					t.Fatal(err)
				}
				refs := gitLocalRefs(t, f.bare)
				_, err := f.repo.GC(t.Context(), time.Time{}, false)
				if (err == nil) != native || errors.Is(err, plumbing.ErrObjectNotFound) {
					t.Fatalf("GC = %v, want failure = %t on the blob itself", err, !native)
				}
				requireObjects(t, f, f.loose, !native)
				requireObjects(t, f, f.packed, !native)
				requireObjects(t, f, f.live, true)
				requireObjects(t, f, live, true)
				requireSameRefs(t, "refs after GC", gitLocalRefs(t, f.bare), refs)
				gitFsck(t, f.bare)
			})
		})
	}
}
