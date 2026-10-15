package gc

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage/filesystem"
	xetclient "github.com/wzshiming/xet/client"
	xetstorage "github.com/wzshiming/xet/storage"
	xetlocal "github.com/wzshiming/xet/storage/local"

	"github.com/matrixhub-ai/hfd/pkg/gc/xetstore"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

const objectSize = 64 * 1024

type fixture struct {
	st    *storage.Storage
	root  string
	xs    *xetlocal.Storage
	xsDir string
	m     *mirror.Mirror
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	root := t.TempDir()
	xsDir := filepath.Join(root, "xet", "storage")
	xs, err := xetlocal.NewStorage(xetlocal.WithBasePath(xsDir))
	if err != nil {
		t.Fatalf("new xet storage: %v", err)
	}
	st, err := storage.NewStorage(storage.WithRootDir(root), storage.WithXETStorage(xs))
	if err != nil {
		t.Fatalf("new storage: %v", err)
	}
	m, err := mirror.NewMirror(mirror.WithXETStorage(xs), mirror.WithXETCache(xetclient.NewCache(st.XETMirrorDir(), 0, 0)), mirror.WithDataDir(st.XETDir()))
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}
	t.Cleanup(m.Wait)
	return &fixture{st: st, root: root, xs: xs, xsDir: xsDir, m: m}
}

// ageFiles moves every file under dir into the past so it falls outside any positive grace window.
func ageFiles(t *testing.T, dir string, d time.Duration) {
	t.Helper()
	old := time.Now().Add(-d)
	err := filepath.WalkDir(dir, func(path string, e fs.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		return os.Chtimes(path, old, old)
	})
	if err != nil {
		t.Fatalf("age %s: %v", dir, err)
	}
}

// age backdates every stored xet object.
func (f *fixture) age(t *testing.T, d time.Duration) {
	t.Helper()
	ageFiles(t, f.xsDir, d)
}

// ageGit backdates every Git object of the repository at name.
func (f *fixture) ageGit(t *testing.T, name string, d time.Duration) {
	t.Helper()
	ageFiles(t, filepath.Join(f.root, "repositories", filepath.FromSlash(repository.ResolvePath(name)), "objects"), d)
}

func (f *fixture) gc() *GC {
	return NewGC(f.st.RepositoriesFS(), xetstore.New(f.xs))
}

func (f *fixture) put(t *testing.T, seed string) string {
	t.Helper()
	data := bytes.Repeat([]byte(seed), objectSize/len(seed)+1)[:objectSize]
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])
	if err := f.m.PutObject(context.Background(), oid, bytes.NewReader(data), objectSize); err != nil {
		t.Fatalf("put object %s: %v", seed, err)
	}
	return oid
}

func pointer(oid string, size int) []byte {
	return fmt.Appendf(nil, "version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, size)
}

func (f *fixture) commitPointer(t *testing.T, name, oid string) {
	t.Helper()
	ctx := context.Background()
	repo, err := repository.Init(ctx, f.st.RepositoriesFS(), repository.ResolvePath(name), "main")
	if err != nil {
		t.Fatalf("init %s: %v", name, err)
	}
	_, err = repo.CreateCommit(ctx, "main", "add model", "Test", "test@test.com",
		[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: "model.bin", Content: pointer(oid, objectSize)}}, "")
	if err != nil {
		t.Fatalf("commit %s: %v", name, err)
	}
}

// storer opens a fresh go-git storer on the repository at name, reading the shared pool through its alternates, so no cached object can stand in for the stored ones.
func (f *fixture) storer(name string) *filesystem.Storage {
	return filesystem.NewStorageWithOptions(chroot.New(f.st.RepositoriesFS(), repository.ResolvePath(name)), cache.NewObjectLRUDefault(),
		filesystem.Options{AlternatesFS: f.st.FS()})
}

// orphanPointer commits a pointer for oid plus 64 KiB of random filler on a throwaway branch and deletes the branch,
// returning the commit, tree and blobs this leaves as garbage with their payload sizes.
func (f *fixture) orphanPointer(t *testing.T, name, oid string, size int) map[plumbing.Hash]int64 {
	t.Helper()
	ctx := context.Background()
	repo, err := repository.Open(f.st.RepositoriesFS(), repository.ResolvePath(name))
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	filler := make([]byte, 64<<10)
	if _, err := rand.Read(filler); err != nil {
		t.Fatal(err)
	}
	commit, err := repo.CreateCommit(ctx, "orphan", "orphan", "Test", "test@test.com", []repository.CommitOperation{
		{Type: repository.CommitOperationAdd, Path: "dead.bin", Content: pointer(oid, size)},
		{Type: repository.CommitOperationAdd, Path: "filler.bin", Content: filler},
	}, "")
	if err != nil {
		t.Fatalf("commit orphan in %s: %v", name, err)
	}
	if err := repo.DeleteBranch("orphan"); err != nil {
		t.Fatalf("delete orphan branch in %s: %v", name, err)
	}
	st := f.storer(name)
	c, err := object.GetCommit(st, plumbing.NewHash(commit))
	if err != nil {
		t.Fatal(err)
	}
	tree, err := c.Tree()
	if err != nil {
		t.Fatal(err)
	}
	objects := map[plumbing.Hash]int64{}
	for _, h := range []plumbing.Hash{c.Hash, tree.Hash, tree.Entries[0].Hash, tree.Entries[1].Hash} {
		if objects[h], err = st.EncodedObjectSize(h); err != nil {
			t.Fatal(err)
		}
	}
	return objects
}

// requireObjects asserts every object is stored (or gone) in the repository at name, read through a fresh storer.
func (f *fixture) requireObjects(t *testing.T, name string, objects map[plumbing.Hash]int64, present bool) {
	t.Helper()
	for h := range objects {
		_, err := f.storer(name).EncodedObject(plumbing.AnyObject, h)
		if err != nil && !errors.Is(err, plumbing.ErrObjectNotFound) {
			t.Fatalf("read object %s: %v", h, err)
		}
		if got := err == nil; got != present {
			t.Errorf("%s object %s: present = %t, want %t", name, h, got, present)
		}
	}
}

// requirePointer reads main:path through the hfd API and checks it is a pointer for oid.
func (f *fixture) requirePointer(t *testing.T, name, path, oid string) {
	t.Helper()
	repo, err := repository.Open(f.st.RepositoriesFS(), repository.ResolvePath(name))
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	b, err := repo.Blob("main", path)
	if err != nil {
		t.Fatalf("blob %s:%s: %v", name, path, err)
	}
	ptr, err := b.LFSPointer()
	if err != nil || ptr == nil || ptr.OID() != oid {
		t.Fatalf("%s:%s pointer = %v, %v; want oid %s", name, path, ptr, err, oid)
	}
}

// snapshot records every file under the repositories root with its mode, size, mtime and content digest.
func (f *fixture) snapshot(t *testing.T) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := util.Walk(f.st.RepositoriesFS(), "/", func(name string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		state := fmt.Sprintf("%v %d %d", info.Mode(), info.Size(), info.ModTime().UnixNano())
		if info.Mode().IsRegular() {
			content, err := util.ReadFile(f.st.RepositoriesFS(), name)
			if err != nil {
				return err
			}
			sum := sha256.Sum256(content)
			state += " " + hex.EncodeToString(sum[:])
		}
		files[name] = state
		return nil
	})
	if err != nil {
		t.Fatalf("snapshot repositories: %v", err)
	}
	return files
}

// requireNoGitStats asserts res deleted no Git object and reclaimed nothing: hfd writes every object to the shared pool, which GC never collects.
func requireNoGitStats(t *testing.T, label string, res *PruneResult) {
	t.Helper()
	if res.DeletedGitObjects != 0 || res.DeletedGitBytes != 0 || res.ReclaimedBytes != 0 {
		t.Fatalf("%s: deleted %d objects, %d bytes, reclaimed %d; want none while the shared pool holds the garbage",
			label, res.DeletedGitObjects, res.DeletedGitBytes, res.ReclaimedBytes)
	}
}

func (f *fixture) stored(t *testing.T, oid string) bool {
	t.Helper()
	raw, err := hex.DecodeString(oid)
	if err != nil {
		t.Fatalf("decode oid: %v", err)
	}
	_, err = f.xs.GetFileHashBySHA256(context.Background(), [32]byte(raw))
	return err == nil
}

func (f *fixture) counts(t *testing.T) (shards, xorbs int) {
	t.Helper()
	if err := f.xs.WalkShards(context.Background(), func(string, int64, time.Time) error {
		shards++
		return nil
	}); err != nil {
		t.Fatalf("walk shards: %v", err)
	}
	if err := f.xs.WalkXorbs(context.Background(), func(string, int64, time.Time) error {
		xorbs++
		return nil
	}); err != nil {
		t.Fatalf("walk xorbs: %v", err)
	}
	return shards, xorbs
}

func TestPrune(t *testing.T) {
	ctx := context.Background()
	t.Run("LeavesDataForSweep", func(t *testing.T) {
		f := newFixture(t)
		c := f.gc()
		live, dead := f.put(t, "live-j "), f.put(t, "dead-j ")
		f.commitPointer(t, "org/repo", live)
		shards, xorbs := f.counts(t)
		if shards <= 0 || xorbs <= 0 {
			t.Fatalf("empty storage: shards=%d xorbs=%d", shards, xorbs)
		}
		res, err := c.Prune(ctx, PruneOptions{Grace: -1})
		if err != nil {
			t.Fatalf("prune: %v", err)
		}
		if !slices.Equal(res.Unlinked, []string{dead}) || f.stored(t, dead) || !f.stored(t, live) {
			t.Fatalf("unexpected result: %+v live stored=%v dead stored=%v", res, f.stored(t, live), f.stored(t, dead))
		}
		if gotShards, gotXorbs := f.counts(t); gotShards != shards || gotXorbs != xorbs {
			t.Fatalf("data changed during unlink: shards=%d xorbs=%d, want shards=%d xorbs=%d", gotShards, gotXorbs, shards, xorbs)
		}
		sweep, err := xetstorage.NewGC(f.xs).SweepStep(ctx, xetstorage.SweepOptions{Anchor: xetstorage.AnchorSHA256, Grace: -1})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if !sweep.Done || len(sweep.SweptShards) == 0 || len(sweep.SweptXorbs) == 0 || !f.stored(t, live) {
			t.Fatalf("unexpected sweep: %+v live stored=%v", sweep, f.stored(t, live))
		}
		if gotShards, gotXorbs := f.counts(t); gotShards >= shards || gotXorbs >= xorbs {
			t.Fatalf("data not reclaimed: shards=%d xorbs=%d, want fewer than shards=%d xorbs=%d", gotShards, gotXorbs, shards, xorbs)
		}
	})
	t.Run("UnlinksUnreferenced", func(t *testing.T) {
		f := newFixture(t)
		live, dead := f.put(t, "live-a "), f.put(t, "dead-a ")
		f.commitPointer(t, "org/repo", live)
		res, err := f.gc().Prune(ctx, PruneOptions{Grace: -1})
		if err != nil {
			t.Fatalf("prune: %v", err)
		}
		if res.Repositories != 1 || res.LiveObjects != 1 || !slices.Equal(res.Unlinked, []string{dead}) || res.SkippedInGrace != 0 {
			t.Fatalf("unexpected result: %+v", res)
		}
		if !f.stored(t, live) || f.stored(t, dead) {
			t.Fatalf("live stored=%v dead stored=%v", f.stored(t, live), f.stored(t, dead))
		}
	})
	t.Run("GraceShieldsFresh", func(t *testing.T) {
		f := newFixture(t)
		live, dead := f.put(t, "live-b "), f.put(t, "dead-b ")
		f.commitPointer(t, "org/repo", live)
		res, err := f.gc().Prune(ctx, PruneOptions{})
		if err != nil {
			t.Fatalf("prune: %v", err)
		}
		if len(res.Unlinked) != 0 || res.SkippedInGrace != 1 || !f.stored(t, dead) {
			t.Fatalf("unexpected result: %+v stored=%v", res, f.stored(t, dead))
		}
	})
	t.Run("GraceUnlinksStale", func(t *testing.T) {
		f := newFixture(t)
		live, dead := f.put(t, "live-f "), f.put(t, "dead-f ")
		f.commitPointer(t, "org/repo", live)
		f.age(t, 2*time.Hour)
		res, err := f.gc().Prune(ctx, PruneOptions{Grace: time.Hour})
		if err != nil {
			t.Fatalf("prune: %v", err)
		}
		if !slices.Equal(res.Unlinked, []string{dead}) || res.SkippedInGrace != 0 {
			t.Fatalf("unexpected result: %+v", res)
		}
		if !f.stored(t, live) || f.stored(t, dead) {
			t.Fatalf("live stored=%v dead stored=%v", f.stored(t, live), f.stored(t, dead))
		}
	})
	t.Run("DryRunDeletesNothing", func(t *testing.T) {
		f := newFixture(t)
		live, dead := f.put(t, "live-c "), f.put(t, "dead-c ")
		f.commitPointer(t, "org/repo", live)
		shards, xorbs := f.counts(t)
		res, err := f.gc().Prune(ctx, PruneOptions{Grace: -1, DryRun: true})
		if err != nil {
			t.Fatalf("prune: %v", err)
		}
		if !res.DryRun || !slices.Equal(res.Unlinked, []string{dead}) || !f.stored(t, dead) {
			t.Fatalf("unexpected result: %+v stored=%v", res, f.stored(t, dead))
		}
		if gotShards, gotXorbs := f.counts(t); gotShards != shards || gotXorbs != xorbs {
			t.Fatalf("data changed during dry run: shards=%d xorbs=%d, want shards=%d xorbs=%d", gotShards, gotXorbs, shards, xorbs)
		}
	})
	t.Run("Layouts", func(t *testing.T) {
		// Every layout hfd's own APIs can produce: root, namespaced, type-prefixed, deeper than three
		// components (create/move do not bound slashes), and a .git-suffixed namespace.
		names := []string{"repo", "org/repo", "datasets/org/ds", "spaces/org/sp", "org/a/b/c", "ns.git/repo"}
		f := newFixture(t)
		oids := make([]string, len(names))
		for i, name := range names {
			oids[i] = f.put(t, fmt.Sprintf("live-%d ", i))
			f.commitPointer(t, name, oids[i])
		}
		res, err := f.gc().Prune(ctx, PruneOptions{Grace: -1})
		if err != nil {
			t.Fatalf("prune: %v", err)
		}
		if res.Repositories != len(names) || res.LiveObjects != len(names) || len(res.Unlinked) != 0 {
			t.Fatalf("unexpected result: %+v", res)
		}
		for i, oid := range oids {
			if !f.stored(t, oid) {
				t.Errorf("%s: live object %s deleted", names[i], oid)
			}
		}
	})
	t.Run("BrokenRepoAborts", func(t *testing.T) {
		f := newFixture(t)
		live, dead := f.put(t, "live-e "), f.put(t, "dead-e ")
		f.commitPointer(t, "org/repo", live)
		// The scan reads only what refs reach, so the ref sends it into objects/, where a file makes the read fail.
		for file, data := range map[string]string{"HEAD": "ref: refs/heads/main\n", "refs/heads/main": strings.Repeat("1", 40) + "\n", "objects": ""} {
			if err := util.WriteFile(f.st.RepositoriesFS(), "/org/broken.git/"+file, []byte(data), 0o644); err != nil {
				t.Fatalf("write %s: %v", file, err)
			}
		}
		if _, err := f.gc().Prune(ctx, PruneOptions{Grace: -1}); err == nil || !strings.Contains(err.Error(), "/org/broken.git") {
			t.Fatalf("prune: expected error naming the broken repository, got %v", err)
		}
		if !f.stored(t, dead) {
			t.Fatal("dead object unlinked despite aborted prune")
		}
	})
	t.Run("MissingRoot", func(t *testing.T) {
		f := newFixture(t)
		res, err := f.gc().Prune(ctx, PruneOptions{Grace: -1})
		if err != nil {
			t.Fatalf("prune: %v", err)
		}
		if res.Repositories != 0 || res.LiveObjects != 0 || len(res.Unlinked) != 0 {
			t.Fatalf("unexpected result: %+v", res)
		}
	})
}

// Prune runs Git GC in every repository before marking, so an orphaned pointer stops keeping its object alive.
func TestPruneGitGC(t *testing.T) {
	ctx := context.Background()
	// A dry run reports the unlink the following run then performs, without touching Git or xet files; neither reports the orphan's Git objects, which the shared pool keeps.
	t.Run("PreviewMatchesRun", func(t *testing.T) {
		f := newFixture(t)
		c := f.gc()
		live, dead := f.put(t, "live-g "), f.put(t, "dead-g ")
		f.commitPointer(t, "org/repo", live)
		garbage := f.orphanPointer(t, "org/repo", dead, objectSize)
		before := f.snapshot(t)
		shards, xorbs := f.counts(t)
		preview, err := c.Prune(ctx, PruneOptions{Grace: -1, DryRun: true})
		if err != nil {
			t.Fatalf("dry-run prune: %v", err)
		}
		requireNoGitStats(t, "dry-run prune", preview)
		if !preview.DryRun || len(preview.Failed) != 0 || !slices.Equal(preview.Unlinked, []string{dead}) || preview.Repositories != 1 || preview.LiveObjects != 1 {
			t.Fatalf("unexpected dry-run result: %+v", preview)
		}
		if !maps.Equal(before, f.snapshot(t)) {
			t.Fatal("dry run changed repository files")
		}
		if gotShards, gotXorbs := f.counts(t); gotShards != shards || gotXorbs != xorbs || !f.stored(t, dead) {
			t.Fatalf("dry run changed xet data: shards=%d xorbs=%d dead stored=%v", gotShards, gotXorbs, f.stored(t, dead))
		}
		f.requireObjects(t, "org/repo", garbage, true)

		res, err := c.Prune(ctx, PruneOptions{Grace: -1})
		if err != nil {
			t.Fatalf("prune: %v", err)
		}
		requireNoGitStats(t, "prune", res)
		if res.DryRun || len(res.Failed) != 0 || !slices.Equal(res.Unlinked, []string{dead}) || res.Repositories != 1 || res.LiveObjects != 1 {
			t.Fatalf("unexpected result: %+v", res)
		}
		if f.stored(t, dead) || !f.stored(t, live) {
			t.Fatalf("live stored=%v dead stored=%v", f.stored(t, live), f.stored(t, dead))
		}
		f.requireObjects(t, "org/repo", garbage, true)
		f.requirePointer(t, "org/repo", "model.bin", live)
	})
	// One grace window shields fresh xet data in the preview and in the run; the Git garbage stays in the shared pool.
	t.Run("Grace", func(t *testing.T) {
		f := newFixture(t)
		c := f.gc()
		live, oldDead, oldLoose := f.put(t, "live-k "), f.put(t, "dead-k "), f.put(t, "loose-k ")
		f.commitPointer(t, "org/repo", live)
		old := f.orphanPointer(t, "org/repo", oldDead, objectSize)
		f.age(t, 2*time.Hour)
		f.ageGit(t, "org/repo", 2*time.Hour)
		freshDead, freshLoose := f.put(t, "dead-l "), f.put(t, "loose-l ")
		fresh := f.orphanPointer(t, "org/repo", freshDead, objectSize)
		want := []string{oldDead, oldLoose}
		slices.Sort(want)
		for _, dry := range []bool{true, false} {
			res, err := c.Prune(ctx, PruneOptions{Grace: time.Hour, DryRun: dry})
			if err != nil {
				t.Fatalf("prune(dry_run=%t): %v", dry, err)
			}
			requireNoGitStats(t, fmt.Sprintf("prune(dry_run=%t)", dry), res)
			// The fresh orphan's pointer survives GC but no ref reaches it, so its object waits in grace like the fresh loose one.
			if !slices.Equal(res.Unlinked, want) || res.SkippedInGrace != 2 || res.LiveObjects != 1 || len(res.Failed) != 0 {
				t.Fatalf("prune(dry_run=%t): unexpected result %+v", dry, res)
			}
			f.requireObjects(t, "org/repo", old, true)
			f.requireObjects(t, "org/repo", fresh, true)
		}
		if !f.stored(t, live) || !f.stored(t, freshDead) || !f.stored(t, freshLoose) || f.stored(t, oldDead) || f.stored(t, oldLoose) {
			t.Fatalf("stored: live=%v freshDead=%v freshLoose=%v oldDead=%v oldLoose=%v",
				f.stored(t, live), f.stored(t, freshDead), f.stored(t, freshLoose), f.stored(t, oldDead), f.stored(t, oldLoose))
		}
	})
	// A repository whose GC fails is reported and keeps its garbage and reachable pointers, while the others are still collected and unlinked.
	t.Run("FailedRepository", func(t *testing.T) {
		f := newFixture(t)
		c := f.gc()
		live, brokenLive, dead, unrelated := f.put(t, "live-n "), f.put(t, "broken-n "), f.put(t, "dead-n "), f.put(t, "unrelated-n ")
		f.commitPointer(t, "org/repo", live)
		garbage := f.orphanPointer(t, "org/repo", dead, objectSize)
		f.commitPointer(t, "org/broken", brokenLive)
		// The orphan names the OID main keeps live, so no outcome hinges on an unreachable pointer.
		brokenGarbage := f.orphanPointer(t, "org/broken", brokenLive, objectSize+1)
		// An undecodable loose object fails the object inventory a run starts from, while every reachable object stays readable from the pool.
		if err := util.WriteFile(f.st.RepositoriesFS(), "/org/broken.git/objects/11/"+strings.Repeat("1", 38), []byte("garbage"), 0o644); err != nil {
			t.Fatal(err)
		}
		want := []string{dead, unrelated}
		slices.Sort(want)
		// A pool member's preview reports nothing, so the failure shows in the run.
		res, err := c.Prune(ctx, PruneOptions{Grace: -1})
		if err == nil || !strings.Contains(err.Error(), "git gc failed in 1 repositories") || res == nil {
			t.Fatalf("prune = %+v, %v; want the partial result and the GC failure", res, err)
		}
		requireNoGitStats(t, "prune", res)
		if len(res.Failed) != 1 || res.Failed["/org/broken.git"] == "" || !slices.Equal(res.Unlinked, want) || res.Repositories != 2 || res.LiveObjects != 2 {
			t.Fatalf("prune: unexpected result %+v", res)
		}
		f.requireObjects(t, "org/repo", garbage, true)
		f.requireObjects(t, "org/broken", brokenGarbage, true)
		if !f.stored(t, live) || !f.stored(t, brokenLive) || f.stored(t, dead) || f.stored(t, unrelated) {
			t.Fatalf("stored: live=%v brokenLive=%v dead=%v unrelated=%v", f.stored(t, live), f.stored(t, brokenLive), f.stored(t, dead), f.stored(t, unrelated))
		}
		f.requirePointer(t, "org/broken", "model.bin", brokenLive)
	})
	// An OID another blob or repository still names stays live, in the preview and the run, while the shared pool keeps the orphans.
	t.Run("SharedOIDSurvivesPreviewDeletion", func(t *testing.T) {
		f := newFixture(t)
		c := f.gc()
		shared, cross, dead := f.put(t, "shared-m "), f.put(t, "cross-m "), f.put(t, "dead-m ")
		f.commitPointer(t, "org/a", shared)
		f.commitPointer(t, "org/b", cross)
		// org/a's garbage: a second blob naming shared, a pointer for cross that org/b keeps live, and the only pointer for dead.
		garbage := f.orphanPointer(t, "org/a", shared, objectSize+1)
		maps.Copy(garbage, f.orphanPointer(t, "org/a", cross, objectSize))
		maps.Copy(garbage, f.orphanPointer(t, "org/a", dead, objectSize))
		if len(garbage) != 12 {
			t.Fatalf("orphans left %d objects, want 12", len(garbage))
		}
		for _, dry := range []bool{true, false} {
			res, err := c.Prune(ctx, PruneOptions{Grace: -1, DryRun: dry})
			if err != nil {
				t.Fatalf("prune(dry_run=%t): %v", dry, err)
			}
			requireNoGitStats(t, fmt.Sprintf("prune(dry_run=%t)", dry), res)
			if !slices.Equal(res.Unlinked, []string{dead}) || res.Repositories != 2 || res.LiveObjects != 2 || len(res.Failed) != 0 {
				t.Fatalf("prune(dry_run=%t): unexpected result %+v", dry, res)
			}
			f.requireObjects(t, "org/a", garbage, true)
		}
		if !f.stored(t, shared) || !f.stored(t, cross) || f.stored(t, dead) {
			t.Fatalf("stored: shared=%v cross=%v dead=%v", f.stored(t, shared), f.stored(t, cross), f.stored(t, dead))
		}
	})
	// A cancellation reported by GC aborts the run before any unlink instead of counting as a failed repository.
	t.Run("CancelledDuringGC", func(t *testing.T) {
		f := newFixture(t)
		dead := f.put(t, "dead-o ")
		f.commitPointer(t, "org/repo", f.put(t, "live-o "))
		garbage := f.orphanPointer(t, "org/repo", dead, objectSize)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		c := NewGC(&cancelOnHEAD{Filesystem: f.st.RepositoriesFS(), cancel: cancel}, xetstore.New(f.xs))
		res, err := c.Prune(ctx, PruneOptions{Grace: -1})
		if !errors.Is(err, context.Canceled) || res == nil || len(res.Failed) != 0 || len(res.Unlinked) != 0 || res.Repositories != 0 {
			t.Fatalf("prune = %+v, %v; want context.Canceled with no failed repository and no unlink", res, err)
		}
		f.requireObjects(t, "org/repo", garbage, true)
		if !f.stored(t, dead) {
			t.Fatal("dead object unlinked after cancellation")
		}
	})
}

// A ref to a missing object fails the pointer scan, which aborts the prune before anything is unlinked.
func TestPruneDanglingRefAborts(t *testing.T) {
	ctx := context.Background()
	repos := newRepos(t)
	for _, name := range []string{"/org/broken.git", "/org/repo.git"} {
		repo, err := repository.Init(ctx, repos, name, "main")
		if err != nil {
			t.Fatalf("init %s: %v", name, err)
		}
		if _, err := repo.CreateCommit(ctx, "main", "add model", "Test", "test@test.com",
			[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: "model.bin", Content: pointer(strings.Repeat("a", 64), 1)}}, ""); err != nil {
			t.Fatalf("commit %s: %v", name, err)
		}
	}
	if err := util.WriteFile(repos, "/org/broken.git/refs/heads/broken", []byte(strings.Repeat("1", 40)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{objects: []fakeObject{{strings.Repeat("d", 64), time.Now().Add(-2 * time.Hour)}}}
	res, err := NewGC(repos, store).Prune(ctx, PruneOptions{Grace: -1})
	if err == nil || !strings.Contains(err.Error(), "scan /org/broken.git:") || res == nil || len(res.Unlinked) != 0 || len(store.unlinked) != 0 {
		t.Fatalf("prune = %+v, %v; unlinked %v; want the scan error of /org/broken.git and nothing unlinked", res, err, store.unlinked)
	}
}

// Pointer blobs hfd writes live only in the shared pool; Prune reads them there and keeps their OID, previewing and running alike.
func TestPruneKeepsPooledPointers(t *testing.T) {
	ctx := context.Background()
	repos := newRepos(t)
	live, dead := strings.Repeat("a", 64), strings.Repeat("d", 64)
	for _, name := range []string{"/org/a.git", "/datasets/org/b.git"} {
		repo, err := repository.Init(ctx, repos, name, "main")
		if err != nil {
			t.Fatalf("init %s: %v", name, err)
		}
		if _, err := repo.CreateCommit(ctx, "main", "add model", "Test", "test@test.com",
			[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: "model.bin", Content: pointer(live, 1)}}, ""); err != nil {
			t.Fatalf("commit %s: %v", name, err)
		}
		b, err := repo.Blob("main", "model.bin")
		if err != nil {
			t.Fatalf("blob %s:model.bin: %v", name, err)
		}
		h := b.Hash().String()
		if _, err := repos.Stat(name + "/objects/" + h[:2] + "/" + h[2:]); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("stat %s's own copy of pointer blob %s: %v; want the blob only in the shared pool", name, h, err)
		}
	}
	old := time.Now().Add(-2 * time.Hour)
	for _, dry := range []bool{true, false} {
		store := &fakeStore{objects: []fakeObject{{live, old}, {dead, old}}}
		res, err := NewGC(repos, store).Prune(ctx, PruneOptions{Grace: -1, DryRun: dry})
		if err != nil || len(res.Failed) != 0 || res.Repositories != 2 || res.LiveObjects != 1 || !slices.Equal(res.Unlinked, []string{dead}) {
			t.Fatalf("prune(dry_run=%t) = %+v, %v; want both repositories collected, %s live and only %s unlinked", dry, res, err, live, dead)
		}
	}
}

// A pointer blob a repository also holds itself stays live when GC drops that copy, since the shared pool still stores the blob.
func TestPruneKeepsPointerDuplicatedInLocalPack(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	const name = "org/repo"
	live, dead := strings.Repeat("a", 64), strings.Repeat("d", 64)
	f.commitPointer(t, name, live)
	repoPath := repository.ResolvePath(name)
	repo, err := repository.Open(f.st.RepositoriesFS(), repoPath)
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	b, err := repo.Blob("main", "model.bin")
	if err != nil {
		t.Fatalf("blob %s:model.bin: %v", name, err)
	}
	blob := b.Hash()
	// local reports whether the repository stores blob itself, as GC's object inventory sees it.
	local := func() bool {
		t.Helper()
		st := f.storer(name)
		defer func() { _ = st.Close() }()
		iter, err := st.IterEncodedObjects(plumbing.BlobObject)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		if err := iter.ForEach(func(o plumbing.EncodedObject) error {
			found = found || o.Hash() == blob
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return found
	}
	// The repository package picks native GC by this same lookup.
	gitPath, lookErr := exec.LookPath("git")
	native := lookErr == nil
	if native {
		// Without -l, repack copies every pooled object a ref reaches into a pack of the repository's own.
		cmd := exec.CommandContext(ctx, gitPath, "-C", filepath.Join(f.root, "repositories", filepath.FromSlash(repoPath)), "repack", "-a", "-d", "-q")
		cmd.Env = append(slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "GIT_") }),
			"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git repack -a -d: %v\n%s", err, out)
		}
	} else {
		// Without git, a loose copy of the pooled blob stands in for the pack.
		hex := blob.String()
		pooled, err := util.ReadFile(f.st.FS(), "/git/sha1/objects/"+hex[:2]+"/"+hex[2:])
		if err != nil {
			t.Fatalf("read pooled pointer blob %s: %v", hex, err)
		}
		if err := util.WriteFile(f.st.RepositoriesFS(), repoPath+"/objects/"+hex[:2]+"/"+hex[2:], pooled, 0o444); err != nil {
			t.Fatal(err)
		}
	}
	if !local() {
		t.Fatalf("fixture: %s stores no copy of pointer blob %s itself", name, blob)
	}
	old := time.Now().Add(-2 * time.Hour)
	store := &fakeStore{objects: []fakeObject{{live, old}, {dead, old}}}
	res, err := NewGC(f.st.RepositoriesFS(), store).Prune(ctx, PruneOptions{Grace: -1})
	if err != nil || len(res.Failed) != 0 || res.LiveObjects != 1 || !slices.Equal(res.Unlinked, []string{dead}) || !slices.Equal(store.unlinked, []string{dead}) {
		t.Fatalf("prune = %+v, %v; unlinked %v; want %s live and only %s unlinked", res, err, store.unlinked, live, dead)
	}
	if res.DeletedGitObjects != 0 || res.DeletedGitBytes != 0 {
		t.Errorf("prune deleted %d Git objects of %d bytes; want none while the pool stores every object", res.DeletedGitObjects, res.DeletedGitBytes)
	}
	// Native gc repacks with -l, dropping the copy the pool makes redundant; go-git only prunes unreachable loose objects.
	if got := local(); got == native {
		t.Errorf("%s stores its own copy of pointer blob %s after GC = %t, want %t", name, blob, got, !native)
	}
	f.requireObjects(t, name, map[plumbing.Hash]int64{blob: 0}, true)
}

// cancelOnHEAD cancels the run as Walk stats a repository's HEAD, so the first GC itself sees the cancellation.
type cancelOnHEAD struct {
	billy.Filesystem
	cancel context.CancelFunc
}

func (c *cancelOnHEAD) Stat(name string) (os.FileInfo, error) {
	if filepath.Base(name) == "HEAD" {
		c.cancel()
	}
	return c.Filesystem.Stat(name)
}

// fakeObject is one object a fakeStore holds.
type fakeObject struct {
	oid      string
	storedAt time.Time
}

// fakeStore is an LFSStore over fixed objects that records every unlink.
type fakeStore struct {
	objects  []fakeObject // in walk order
	unlinked []string
	gone     map[string]bool // unlinks of these report the object already absent
	failOn   string
	failErr  error
	walkErr  error
	entered  chan struct{} // with gate set, Walk signals entered and waits for gate to close
	gate     chan struct{}
}

func (s *fakeStore) Walk(_ context.Context, fn func(string, time.Time) error) error {
	if s.gate != nil {
		s.entered <- struct{}{}
		<-s.gate
	}
	if s.walkErr != nil {
		return s.walkErr
	}
	for _, o := range s.objects {
		if err := fn(o.oid, o.storedAt); err != nil {
			return err
		}
	}
	return nil
}

func (s *fakeStore) Unlink(_ context.Context, oid string) (bool, error) {
	s.unlinked = append(s.unlinked, oid)
	if oid == s.failOn {
		return false, s.failErr
	}
	return !s.gone[oid], nil
}

// newRepos returns a repositories filesystem of its own, holding no repository yet.
func newRepos(t *testing.T) billy.Filesystem {
	t.Helper()
	st, err := storage.NewStorage(storage.WithRootDir(t.TempDir()))
	if err != nil {
		t.Fatalf("new storage: %v", err)
	}
	return st.RepositoriesFS()
}

// within receives from ch, failing the test after 5s so a stuck run cannot hang it.
func within[T any](t *testing.T, ch <-chan T, what string) (v T) {
	t.Helper()
	select {
	case v = <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
	return v
}

// TestPruneStore pins the prune pass against the LFSStore port alone, over repositories naming no OID.
func TestPruneStore(t *testing.T) {
	ctx := context.Background()
	old, recent := time.Now().Add(-2*time.Hour), time.Now().Add(-30*time.Minute)
	hex64 := func(digit string) string { return strings.Repeat(digit, 64) }
	// storeOf holds every oid, stored two hours ago.
	storeOf := func(oids ...string) *fakeStore {
		s := &fakeStore{}
		for _, oid := range oids {
			s.objects = append(s.objects, fakeObject{oid, old})
		}
		return s
	}
	t.Run("UnlinksSorted", func(t *testing.T) {
		store := storeOf(hex64("c"), hex64("a"), hex64("b"))
		res, err := NewGC(newRepos(t), store).Prune(ctx, PruneOptions{Grace: -1})
		want := []string{hex64("a"), hex64("b"), hex64("c")}
		if err != nil || !slices.Equal(res.Unlinked, want) || !slices.Equal(store.unlinked, want) || res.SkippedInGrace != 0 {
			t.Fatalf("prune = %+v, %v; unlinked %v; want %v unlinked in that order", res, err, store.unlinked, want)
		}
	})
	t.Run("PartialFailure", func(t *testing.T) {
		injected := errors.New("injected unlink failure")
		store := storeOf(hex64("3"), hex64("1"), hex64("2"))
		store.failOn, store.failErr = hex64("2"), injected
		res, err := NewGC(newRepos(t), store).Prune(ctx, PruneOptions{Grace: -1})
		if !errors.Is(err, injected) || !strings.Contains(err.Error(), "unlink "+hex64("2")) {
			t.Fatalf("prune error = %v; want the injected failure naming %s", err, hex64("2"))
		}
		if res == nil || !slices.Equal(res.Unlinked, []string{hex64("1")}) || len(store.unlinked) != 2 {
			t.Fatalf("prune = %+v; unlinked %v; want the first unlinked and the third never tried", res, store.unlinked)
		}
	})
	t.Run("DryRunUnlinksNothing", func(t *testing.T) {
		store := storeOf(hex64("e"), hex64("d"))
		res, err := NewGC(newRepos(t), store).Prune(ctx, PruneOptions{Grace: -1, DryRun: true})
		if err != nil || !res.DryRun || !slices.Equal(res.Unlinked, []string{hex64("d"), hex64("e")}) || len(store.unlinked) != 0 {
			t.Fatalf("dry run = %+v, %v; unlinked %v; want both listed and none unlinked", res, err, store.unlinked)
		}
	})
	t.Run("ZeroStoredAtInGrace", func(t *testing.T) {
		for _, tc := range []struct {
			grace   time.Duration
			skipped int
			want    []string
		}{{time.Hour, 1, []string{}}, {-1, 0, []string{hex64("f")}}} {
			store := &fakeStore{objects: []fakeObject{{hex64("f"), time.Time{}}}}
			res, err := NewGC(newRepos(t), store).Prune(ctx, PruneOptions{Grace: tc.grace})
			if err != nil || res.SkippedInGrace != tc.skipped || !slices.Equal(res.Unlinked, tc.want) || !slices.Equal(store.unlinked, tc.want) {
				t.Fatalf("grace %v: prune = %+v, %v; unlinked %v; want %d in grace and %v unlinked", tc.grace, res, err, store.unlinked, tc.skipped, tc.want)
			}
		}
	})
	t.Run("Busy", func(t *testing.T) {
		store := storeOf(hex64("8"))
		store.entered, store.gate = make(chan struct{}, 1), make(chan struct{})
		release := sync.OnceFunc(func() { close(store.gate) })
		t.Cleanup(release)
		c := NewGC(newRepos(t), store)
		type outcome struct {
			res *PruneResult
			err error
		}
		prune := func() <-chan outcome {
			done := make(chan outcome, 1)
			go func() {
				res, err := c.Prune(ctx, PruneOptions{Grace: -1})
				done <- outcome{res, err}
			}()
			return done
		}
		first := prune()
		within(t, store.entered, "the first prune to walk the store")
		if got := within(t, prune(), "the concurrent prune"); !errors.Is(got.err, ErrBusy) || got.res != nil {
			t.Fatalf("concurrent prune = %+v, %v; want nil, ErrBusy", got.res, got.err)
		}
		release()
		if got := within(t, first, "the first prune"); got.err != nil || !slices.Equal(got.res.Unlinked, []string{hex64("8")}) {
			t.Fatalf("first prune = %+v, %v; want %s unlinked", got.res, got.err, hex64("8"))
		}
	})
	t.Run("DefaultGrace", func(t *testing.T) {
		if DefaultGrace != xetstorage.DefaultSweepGrace {
			t.Fatalf("DefaultGrace = %v, want xet's sweep default %v", DefaultGrace, xetstorage.DefaultSweepGrace)
		}
		store := &fakeStore{objects: []fakeObject{{hex64("4"), recent}, {hex64("5"), old}}}
		res, err := NewGC(newRepos(t), store).Prune(ctx, PruneOptions{})
		if err != nil || res.SkippedInGrace != 1 || !slices.Equal(res.Unlinked, []string{hex64("5")}) || !slices.Equal(store.unlinked, []string{hex64("5")}) {
			t.Fatalf("prune with zero grace = %+v, %v; unlinked %v; want the 30-minute-old object in grace and the two-hour-old one unlinked", res, err, store.unlinked)
		}
	})
	t.Run("NilStore", func(t *testing.T) {
		repos := newRepos(t)
		if _, err := repository.Init(ctx, repos, "/org/repo.git", "main"); err != nil {
			t.Fatalf("init: %v", err)
		}
		if res, err := NewGC(repos, storeOf()).Prune(ctx, PruneOptions{Grace: -1, DryRun: true}); err != nil || res.Repositories != 1 {
			t.Fatalf("prune with a store = %+v, %v; want the repository walked", res, err)
		}
		c := NewGC(repos, nil)
		for _, dry := range []bool{false, true} {
			res, err := c.Prune(ctx, PruneOptions{Grace: -1, DryRun: dry})
			if err == nil || res == nil || res.Repositories != 0 || len(res.Failed) != 0 {
				t.Fatalf("prune(dry_run=%t) without a store = %+v, %v; want an error before any Git GC", dry, res, err)
			}
		}
	})
	t.Run("WalkError", func(t *testing.T) {
		injected := errors.New("injected walk failure")
		store := storeOf(hex64("9"))
		store.walkErr = injected
		res, err := NewGC(newRepos(t), store).Prune(ctx, PruneOptions{Grace: -1})
		if !errors.Is(err, injected) || !strings.HasPrefix(err.Error(), "walk objects:") || res == nil || len(res.Unlinked) != 0 || len(store.unlinked) != 0 {
			t.Fatalf("prune = %+v, %v; unlinked %v; want the walk error and nothing unlinked", res, err, store.unlinked)
		}
	})
	t.Run("VanishedNotReported", func(t *testing.T) {
		store := storeOf(hex64("6"), hex64("7"))
		store.gone = map[string]bool{hex64("6"): true}
		res, err := NewGC(newRepos(t), store).Prune(ctx, PruneOptions{Grace: -1})
		if err != nil || !slices.Equal(store.unlinked, []string{hex64("6"), hex64("7")}) || !slices.Equal(res.Unlinked, []string{hex64("7")}) {
			t.Fatalf("prune = %+v, %v; unlinked %v; want both tried and only %s reported", res, err, store.unlinked, hex64("7"))
		}
	})
}
