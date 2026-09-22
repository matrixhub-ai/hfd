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
	"path/filepath"
	"slices"
	"strings"
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
	client, err := xetclient.NewClient(xetclient.WithCacheDir(filepath.Join(st.XETDir(), "chunks")))
	if err != nil {
		t.Fatalf("new xet client: %v", err)
	}
	m, err := mirror.NewMirror(mirror.WithXETStorage(xs), mirror.WithXETClient(client), mirror.WithDataDir(st.XETDir()))
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

func (f *fixture) collector() *Collector {
	return NewCollector(f.st.RepositoriesFS(), f.xs)
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

// storer opens a fresh go-git storer on the repository at name, so no cached object can stand in for the stored ones.
func (f *fixture) storer(name string) *filesystem.Storage {
	return filesystem.NewStorage(chroot.New(f.st.RepositoriesFS(), repository.ResolvePath(name)), cache.NewObjectLRUDefault())
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

// requireGitStats asserts res reports exactly the garbage objects and their payload bytes, plus a smaller objects directory unless it was a dry run, which measures none.
func requireGitStats(t *testing.T, label string, res *PruneResult, garbage map[plumbing.Hash]int64) {
	t.Helper()
	var size int64
	for _, n := range garbage {
		size += n
	}
	if res.DeletedGitObjects != len(garbage) || res.DeletedGitBytes != size || (res.ReclaimedBytes > 0) == res.DryRun {
		t.Fatalf("%s: deleted %d objects, %d bytes, reclaimed %d; want %d objects, %d bytes, reclaimed > 0 unless dry run (%t)",
			label, res.DeletedGitObjects, res.DeletedGitBytes, res.ReclaimedBytes, len(garbage), size, res.DryRun)
	}
}

func (f *fixture) stored(t *testing.T, oid string) bool {
	t.Helper()
	raw, err := hex.DecodeString(oid)
	if err != nil {
		t.Fatalf("decode oid: %v", err)
	}
	_, err = f.xs.GetFileHashBySHA256(context.Background(), "default", [32]byte(raw))
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
	if err := f.xs.WalkXorbs(context.Background(), "default", func(string, int64, time.Time) error {
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
		c := f.collector()
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
		sweep, err := c.SweepStep(ctx, Options{Grace: -1})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if !sweep.Done || sweep.SweptShards == 0 || sweep.SweptXorbs == 0 || !f.stored(t, live) {
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
		res, err := f.collector().Prune(ctx, PruneOptions{Grace: -1})
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
		res, err := f.collector().Prune(ctx, PruneOptions{})
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
		res, err := f.collector().Prune(ctx, PruneOptions{Grace: time.Hour})
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
		res, err := f.collector().Prune(ctx, PruneOptions{Grace: -1, DryRun: true})
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
	t.Run("BoundedSweepContinues", func(t *testing.T) {
		f := newFixture(t)
		c := f.collector()
		dead := []string{f.put(t, "dead-h "), f.put(t, "dead-i ")}
		slices.Sort(dead)
		res, err := c.Prune(ctx, PruneOptions{Grace: -1})
		if err != nil {
			t.Fatalf("prune: %v", err)
		}
		if !slices.Equal(res.Unlinked, dead) {
			t.Fatalf("unexpected result: %+v", res)
		}
		// Prune is unbounded; the cap stops the sweep after one shard.
		sweep, err := c.SweepStep(ctx, Options{Grace: -1, MaxDeletes: 1})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if sweep.Done || sweep.SweptShards != 1 || sweep.RemainingShards == 0 {
			t.Fatalf("unexpected sweep: %+v", sweep)
		}
		sweep, err = c.SweepStep(ctx, Options{Grace: -1})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if !sweep.Done || sweep.SweptShards == 0 || sweep.SweptXorbs == 0 {
			t.Fatalf("unexpected sweep: %+v", sweep)
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
		res, err := f.collector().Prune(ctx, PruneOptions{Grace: -1})
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
		// go-git opens a HEAD-only dir as an empty repo; a file where objects/ belongs makes the scan fail.
		for _, file := range []string{"HEAD", "objects"} {
			if err := util.WriteFile(f.st.RepositoriesFS(), "/org/broken.git/"+file, []byte("ref: refs/heads/main\n"), 0o644); err != nil {
				t.Fatalf("write %s: %v", file, err)
			}
		}
		if _, err := f.collector().Prune(ctx, PruneOptions{Grace: -1}); err == nil || !strings.Contains(err.Error(), "/org/broken.git") {
			t.Fatalf("prune: expected error naming the broken repository, got %v", err)
		}
		if !f.stored(t, dead) {
			t.Fatal("dead object unlinked despite aborted prune")
		}
	})
	t.Run("MissingRoot", func(t *testing.T) {
		f := newFixture(t)
		res, err := f.collector().Prune(ctx, PruneOptions{Grace: -1})
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
	// A dry run reports the objects, bytes and unlink the following run then performs, without touching Git or xet files.
	t.Run("PreviewMatchesRun", func(t *testing.T) {
		f := newFixture(t)
		c := f.collector()
		live, dead := f.put(t, "live-g "), f.put(t, "dead-g ")
		f.commitPointer(t, "org/repo", live)
		garbage := f.orphanPointer(t, "org/repo", dead, objectSize)
		before := f.snapshot(t)
		shards, xorbs := f.counts(t)
		preview, err := c.Prune(ctx, PruneOptions{Grace: -1, DryRun: true})
		if err != nil {
			t.Fatalf("dry-run prune: %v", err)
		}
		requireGitStats(t, "dry-run prune", preview, garbage)
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
		requireGitStats(t, "prune", res, garbage)
		if res.DryRun || len(res.Failed) != 0 || !slices.Equal(res.Unlinked, []string{dead}) || res.Repositories != 1 || res.LiveObjects != 1 {
			t.Fatalf("unexpected result: %+v", res)
		}
		if f.stored(t, dead) || !f.stored(t, live) {
			t.Fatalf("live stored=%v dead stored=%v", f.stored(t, live), f.stored(t, dead))
		}
		f.requireObjects(t, "org/repo", garbage, false)
		f.requirePointer(t, "org/repo", "model.bin", live)
	})
	// One grace window shields fresh Git garbage and fresh xet data alike, in the preview and in the run.
	t.Run("Grace", func(t *testing.T) {
		f := newFixture(t)
		c := f.collector()
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
			requireGitStats(t, fmt.Sprintf("prune(dry_run=%t)", dry), res, old)
			// The fresh orphan's pointer survives GC, so its object is live rather than in grace; only the fresh loose object is.
			if !slices.Equal(res.Unlinked, want) || res.SkippedInGrace != 1 || res.LiveObjects != 2 || len(res.Failed) != 0 {
				t.Fatalf("prune(dry_run=%t): unexpected result %+v", dry, res)
			}
			f.requireObjects(t, "org/repo", old, dry)
			f.requireObjects(t, "org/repo", fresh, true)
		}
		if !f.stored(t, live) || !f.stored(t, freshDead) || !f.stored(t, freshLoose) || f.stored(t, oldDead) || f.stored(t, oldLoose) {
			t.Fatalf("stored: live=%v freshDead=%v freshLoose=%v oldDead=%v oldLoose=%v",
				f.stored(t, live), f.stored(t, freshDead), f.stored(t, freshLoose), f.stored(t, oldDead), f.stored(t, oldLoose))
		}
	})
	// A repository whose GC fails is reported and keeps every pointer blob live, while the others are still collected and unlinked.
	t.Run("FailedRepository", func(t *testing.T) {
		f := newFixture(t)
		c := f.collector()
		live, brokenDead, dead, unrelated := f.put(t, "live-n "), f.put(t, "broken-n "), f.put(t, "dead-n "), f.put(t, "unrelated-n ")
		f.commitPointer(t, "org/repo", live)
		garbage := f.orphanPointer(t, "org/repo", dead, objectSize)
		f.commitPointer(t, "org/broken", live)
		brokenGarbage := f.orphanPointer(t, "org/broken", brokenDead, objectSize)
		// A ref to a missing object fails both engines before they delete anything, while every blob stays readable.
		if err := util.WriteFile(f.st.RepositoriesFS(), "/org/broken.git/refs/heads/dangling", []byte(strings.Repeat("1", 40)+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		want := []string{dead, unrelated}
		slices.Sort(want)
		for _, dry := range []bool{true, false} {
			res, err := c.Prune(ctx, PruneOptions{Grace: -1, DryRun: dry})
			if err == nil || !strings.Contains(err.Error(), "git gc failed in 1 repositories") || res == nil {
				t.Fatalf("prune(dry_run=%t) = %+v, %v; want the partial result and the GC failure", dry, res, err)
			}
			requireGitStats(t, fmt.Sprintf("prune(dry_run=%t)", dry), res, garbage)
			if len(res.Failed) != 1 || res.Failed["/org/broken.git"] == "" || !slices.Equal(res.Unlinked, want) || res.Repositories != 2 || res.LiveObjects != 2 {
				t.Fatalf("prune(dry_run=%t): unexpected result %+v", dry, res)
			}
			f.requireObjects(t, "org/repo", garbage, dry)
			f.requireObjects(t, "org/broken", brokenGarbage, true)
		}
		if !f.stored(t, live) || !f.stored(t, brokenDead) || f.stored(t, dead) || f.stored(t, unrelated) {
			t.Fatalf("stored: live=%v brokenDead=%v dead=%v unrelated=%v", f.stored(t, live), f.stored(t, brokenDead), f.stored(t, dead), f.stored(t, unrelated))
		}
		f.requirePointer(t, "org/broken", "model.bin", live)
	})
	// Preview-deleted pointer blobs are skipped by Git hash, so an OID another blob or repository still names stays live.
	t.Run("SharedOIDSurvivesPreviewDeletion", func(t *testing.T) {
		f := newFixture(t)
		c := f.collector()
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
			requireGitStats(t, fmt.Sprintf("prune(dry_run=%t)", dry), res, garbage)
			if !slices.Equal(res.Unlinked, []string{dead}) || res.Repositories != 2 || res.LiveObjects != 2 || len(res.Failed) != 0 {
				t.Fatalf("prune(dry_run=%t): unexpected result %+v", dry, res)
			}
			f.requireObjects(t, "org/a", garbage, dry)
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
		c := NewCollector(&cancelOnHEAD{Filesystem: f.st.RepositoriesFS(), cancel: cancel}, f.xs)
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

// TestSweepSharesLock pins that SweepStep and Prune exclude each other, so the store has one sweeper.
func TestSweepSharesLock(t *testing.T) {
	f := newFixture(t)
	c := f.collector()
	c.mu.Lock()
	if _, err := c.SweepStep(context.Background(), Options{Grace: -1}); !errors.Is(err, xetstorage.ErrGCBusy) {
		t.Fatalf("sweep during prune: got %v, want ErrGCBusy", err)
	}
	if _, err := c.Prune(context.Background(), PruneOptions{Grace: -1}); !errors.Is(err, xetstorage.ErrGCBusy) {
		t.Fatalf("prune during sweep: got %v, want ErrGCBusy", err)
	}
	c.mu.Unlock()
	res, err := c.SweepStep(context.Background(), Options{Grace: -1})
	if err != nil || !res.Done {
		t.Fatalf("sweep: err=%v result=%+v", err, res)
	}
}

// TestSweepStepAnchorsSHA256 pins that hfd sweeps sha256-anchored: once Prune drops the OID, the files entry alone keeps nothing alive.
func TestSweepStepAnchorsSHA256(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	c := f.collector()
	oid := f.put(t, "anchor ")
	if res, err := c.Prune(ctx, PruneOptions{Grace: -1}); err != nil || !slices.Equal(res.Unlinked, []string{oid}) {
		t.Fatalf("prune: result=%+v err=%v", res, err)
	}
	res, err := c.SweepStep(ctx, Options{Grace: -1})
	if err != nil {
		t.Fatalf("sweep step: %v", err)
	}
	if !res.Done || res.SweptShards == 0 || res.SweptXorbs == 0 || res.ReclaimedBytes <= 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if f.stored(t, oid) {
		t.Fatal("unlinked object still stored after sweep")
	}
}

func TestList(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	c := f.collector()
	a, b := f.put(t, "list-a "), f.put(t, "list-b ")
	objects, err := c.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	oids := make([]string, 0, len(objects))
	for _, o := range objects {
		if o.Size != objectSize {
			t.Fatalf("object %s: size %d, want %d", o.OID, o.Size, objectSize)
		}
		oids = append(oids, o.OID)
	}
	slices.Sort(oids)
	want := []string{a, b}
	slices.Sort(want)
	if !slices.Equal(oids, want) {
		t.Fatalf("list: got %v, want %v", oids, want)
	}
}

func TestParseOIDRejectsInvalid(t *testing.T) {
	for _, bad := range []string{"zz", strings.Repeat("0", 64)} {
		if _, err := parseOID(bad); !errors.Is(err, ErrInvalidOID) {
			t.Fatalf("parseOID %q: got %v, want ErrInvalidOID", bad, err)
		}
	}
	if _, err := parseOID(strings.Repeat("1", 64)); err != nil {
		t.Fatalf("parseOID valid digest: %v", err)
	}
}
