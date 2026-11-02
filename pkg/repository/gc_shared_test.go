package repository

// Tests for Repository.GC on repositories borrowing objects through alternates, the shared pool above all.

import (
	"errors"
	"maps"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
)

// sharedGCRequireEmptyPreview asserts a dry run of repo deletes nothing, runs no git and leaves every file under dir on fs alone.
func sharedGCRequireEmptyPreview(t *testing.T, repo *Repository, native bool, fs billy.Filesystem, dir string) {
	t.Helper()
	bin, log := recordingGit(t)
	if native {
		setGitBinary(t, bin)
	}
	requireGitMode(t, repo, native)
	before := snapshotFiles(t, fs, dir)
	res, err := repo.GC(t.Context(), time.Time{}, true)
	requireNothingDeleted(t, "dry-run GC of a repository with alternates", res, err)
	if calls := recordedCalls(t, log); calls != "" {
		t.Errorf("dry-run GC of a repository with alternates ran git:\n%s", calls)
	}
	if !maps.Equal(before, snapshotFiles(t, fs, dir)) {
		t.Error("dry-run GC of a repository with alternates changed files")
	}
}

// sharedGCPlantGarbage stores an unreachable blob in the bound repository at repoPath itself, bypassing the pool, and returns what GC reports for deleting it.
func sharedGCPlantGarbage(t *testing.T, dataFS billy.Filesystem, repoPath string) GCResult {
	t.Helper()
	const content = "garbage no pool holds\n"
	h := storeBlob(t, newStorer(dataFS, path.Join("/repositories", repoPath), cache.NewObjectLRUDefault()), content)
	return GCResult{DeletedObjects: []plumbing.Hash{h}, DeletedBytes: int64(len(content))}
}

// errPoolIO is the failure statFailure injects.
var errPoolIO = errors.New("injected pool I/O failure")

// statFailure fails Stat, Lstat and Open of path with err while err is set, also through its chroots, which is how go-git reaches an alternate.
type statFailure struct {
	billy.Filesystem
	path string
	err  error
}

func (f *statFailure) fail(name string) error {
	if name == f.path {
		return f.err
	}
	return nil
}

func (f *statFailure) Stat(name string) (os.FileInfo, error) {
	if err := f.fail(name); err != nil {
		return nil, err
	}
	return f.Filesystem.Stat(name)
}

func (f *statFailure) Lstat(name string) (os.FileInfo, error) {
	if err := f.fail(name); err != nil {
		return nil, err
	}
	return f.Filesystem.Lstat(name)
}

func (f *statFailure) Open(name string) (billy.File, error) {
	if err := f.fail(name); err != nil {
		return nil, err
	}
	return f.Filesystem.Open(name)
}

func (f *statFailure) Chroot(dir string) (billy.Filesystem, error) {
	return chroot.New(f, dir), nil
}

func TestGCSharedObjects(t *testing.T) {
	const repoPath = "/repo.git" // where memfsGCRepo puts its repository

	t.Run("DryRunIsEmpty", func(t *testing.T) {
		forEachGitMode(t, func(t *testing.T, native bool) {
			dataFS := osfs.New(t.TempDir())
			repo, _, _ := memfsGCRepo(t, sharedTestBind(dataFS))
			sharedGCRequireEmptyPreview(t, repo, native, dataFS, "/")
		})
	})

	// Membership is the alternates file alone, whatever filesystem the repository is on.
	t.Run("ForeignAlternatesDryRunIsEmpty", func(t *testing.T) {
		forEachGitMode(t, func(t *testing.T, native bool) {
			f := buildGCFixture(t)
			if err := os.WriteFile(filepath.Join(f.bare, "objects", "info", "alternates"), []byte(t.TempDir()+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			sharedGCRequireEmptyPreview(t, f.repo, native, osfs.Default, f.bare)
		})
	})

	t.Run("LiveGoGitPrunesLocalOnly", func(t *testing.T) {
		setGitBinary(t, "")
		root := t.TempDir()
		dataFS := osfs.New(root)
		bound := sharedTestBind(dataFS)
		repo, _, orphan := memfsGCRepo(t, bound)
		requireGitMode(t, repo, false)
		want := sharedGCPlantGarbage(t, dataFS, repoPath)
		pooled, err := util.ReadFile(dataFS, sharedTestObjectPath(orphan))
		if err != nil {
			t.Fatalf("read pooled orphan %s: %v", orphan, err)
		}
		if err := util.WriteFile(dataFS, path.Join("/repositories", repoPath, "objects", orphan[:2], orphan[2:]), pooled, 0o444); err != nil {
			t.Fatal(err)
		}
		ageObjects(t, filepath.Join(root, "repositories", "repo.git"), time.Now().Add(-2*time.Hour))
		pool := sharedTestFiles(t, dataFS, sharedTestPool)
		res, err := repo.GC(t.Context(), time.Now().Add(-time.Hour), false)
		if err != nil {
			t.Fatalf("go-git GC of a pool member: %v", err)
		}
		// The local copy of orphan goes too, but the pool still serves orphan, so it is not deleted.
		requireGCResult(t, "go-git GC of a pool member", res, want)
		if res.ReclaimedBytes <= 0 {
			t.Errorf("go-git GC of a pool member reclaimed %d bytes, want the expired local objects' space", res.ReclaimedBytes)
		}
		if got := sharedTestLocalFiles(t, dataFS, repoPath); !slices.Equal(got, sharedTestOnlyAlternates) {
			t.Errorf("repository objects after go-git GC = %q, want %q: expired local objects pruned, no pack written", got, sharedTestOnlyAlternates)
		}
		if got := sharedTestFiles(t, dataFS, sharedTestPool); !slices.Equal(got, pool) {
			t.Errorf("go-git GC changed the pool from %q to %q", pool, got)
		}
		if !hasObject(t, bound, repoPath, orphan) {
			t.Errorf("pooled orphan %s unreadable after GC dropped its local copy", orphan)
		}
		requireBlob(t, sharedTestReopen(t, bound, repoPath), "main", "file.txt", []byte("one\n"))
	})

	t.Run("LiveNativeKeepsPool", func(t *testing.T) {
		gitPath, err := exec.LookPath("git")
		if err != nil {
			t.Fatalf("git binary required: %v", err)
		}
		setGitBinary(t, gitPath)
		root := t.TempDir()
		dataFS := osfs.New(root)
		repo, live, _ := memfsGCRepo(t, sharedTestBind(dataFS))
		requireGitMode(t, repo, true)
		host := filepath.Join(root, "repositories", "repo.git")
		local := path.Join("/repositories", repoPath)
		env := sharedTestGitEnv(t)
		// Without -l, repack copies every reachable pooled object into a pack of the repository's own.
		sharedTestGit(t, env, host, "repack", "-a", "-d", "-q")
		packs, err := packInfos(t.Context(), dataFS, local)
		if err != nil || len(packs) != 1 || !slices.Contains(packs[0].objects, plumbing.NewHash(live)) {
			t.Fatalf("fixture: local packs %v, %v; want one copying pooled commit %s", packs, err, live)
		}
		copies := "pack-" + packs[0].hash.String() + ".pack"
		want := sharedGCPlantGarbage(t, dataFS, repoPath)
		ageObjects(t, host, time.Now().Add(-2*time.Hour))
		pool := sharedTestFiles(t, dataFS, sharedTestPool)
		res, err := repo.GC(t.Context(), time.Now().Add(-time.Hour), false)
		if err != nil {
			t.Fatalf("native GC of a pool member: %v", err)
		}
		requireGCResult(t, "native GC of a pool member", res, want)
		sharedTestGit(t, env, host, "fsck", "--full", "--strict")
		if got := sharedTestFiles(t, dataFS, sharedTestPool); !slices.Equal(got, pool) {
			t.Errorf("native GC changed the pool from %q to %q", pool, got)
		}
		if slices.Contains(packFiles(t, dataFS, local), copies) {
			t.Errorf("local pack %s of pooled copies survived native GC", copies)
		}
		requireBlob(t, repo, "main", "file.txt", []byte("one\n"))
	})

	// Membership GC cannot establish is not taken for its absence: neither engine runs.
	t.Run("AlternatesStatErrorAborts", func(t *testing.T) {
		forEachGitMode(t, func(t *testing.T, native bool) {
			dataFS := osfs.New(t.TempDir())
			repo, _, _ := memfsGCRepo(t, sharedTestBind(dataFS))
			// A file where objects/info belongs makes stat-ing alternates fail with ENOTDIR.
			info := path.Join("/repositories", repoPath, "objects", "info")
			if err := util.RemoveAll(dataFS, info); err != nil {
				t.Fatal(err)
			}
			if err := util.WriteFile(dataFS, info, []byte("not a directory\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			bin, log := recordingGit(t)
			if native {
				setGitBinary(t, bin)
			}
			requireGitMode(t, repo, native)
			before := snapshotFiles(t, dataFS, "/")
			for _, dryRun := range []bool{true, false} {
				if res, err := repo.GC(t.Context(), time.Time{}, dryRun); !errors.Is(err, syscall.ENOTDIR) || !reflect.DeepEqual(res, GCResult{}) {
					t.Errorf("GC(dryRun=%t) with objects/info a file = %+v, %v; want GCResult{} and the ENOTDIR of stat-ing alternates, not os.ErrNotExist", dryRun, res, err)
				}
			}
			if calls := recordedCalls(t, log); calls != "" {
				t.Errorf("GC ran git although stat-ing alternates failed:\n%s", calls)
			}
			if !maps.Equal(before, snapshotFiles(t, dataFS, "/")) {
				t.Error("GC changed files although stat-ing alternates failed")
			}
		})
	})

	// A pool lookup that fails does not prove an object the repository dropped is gone.
	t.Run("PoolReadErrorAborts", func(t *testing.T) {
		setGitBinary(t, "")
		dataFS := &statFailure{Filesystem: osfs.New(t.TempDir())}
		repo, _, _ := memfsGCRepo(t, sharedTestBind(dataFS))
		requireGitMode(t, repo, false)
		garbage := sharedGCPlantGarbage(t, dataFS, repoPath).DeletedObjects[0].String()
		// Only GC's pool check of the garbage it pruned reads this path.
		dataFS.path, dataFS.err = sharedTestObjectPath(garbage), errPoolIO
		if res, err := repo.GC(t.Context(), time.Time{}, false); !errors.Is(err, errPoolIO) || !reflect.DeepEqual(res, GCResult{}) {
			t.Errorf("live GC failing to stat %s in the pool = %+v, %v; want GCResult{} and errPoolIO", garbage, res, err)
		}
		dataFS.err = nil
		if _, err := repo.GC(t.Context(), time.Time{}, false); err != nil {
			t.Errorf("live GC once the pool reads again: %v", err)
		}
	})

	t.Run("UnboundUnchanged", func(t *testing.T) {
		forEachGitMode(t, func(t *testing.T, native bool) {
			plain := osfs.New(t.TempDir())
			repo, _, orphan := memfsGCRepo(t, plain)
			requireGitMode(t, repo, native)
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
			st := newStorer(plain, repoPath, cache.NewObjectLRUDefault())
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
			preview, err := repo.GC(t.Context(), time.Time{}, true)
			if err != nil {
				t.Fatalf("dry-run GC off the pool: %v", err)
			}
			requireGCResult(t, "dry-run GC off the pool", preview, want)
			if preview.ReclaimedBytes != 0 {
				t.Errorf("dry-run GC off the pool reclaimed %d bytes, want 0", preview.ReclaimedBytes)
			}
		})
	})
}
