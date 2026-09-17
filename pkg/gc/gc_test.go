package gc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-billy/v6/util"
	xetclient "github.com/wzshiming/xet/client"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

const objectSize = 64 * 1024

type fixture struct {
	st    *storage.Storage
	root  string
	xs    *xetstorage.FileStorage
	xsDir string
	m     *mirror.Mirror
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "xet")
	client, err := xetclient.NewClient(xetclient.WithCacheDir(filepath.Join(dataDir, "chunks")))
	if err != nil {
		t.Fatalf("new xet client: %v", err)
	}
	xsDir := filepath.Join(dataDir, "storage")
	xs, err := xetstorage.NewFileStorage(xetstorage.WithBasePath(xsDir))
	if err != nil {
		t.Fatalf("new xet storage: %v", err)
	}
	m, err := mirror.NewMirror(mirror.WithXETStorage(xs), mirror.WithXETClient(client), mirror.WithDataDir(dataDir))
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}
	t.Cleanup(m.Wait)
	root := t.TempDir()
	return &fixture{st: storage.NewStorage(storage.WithRootDir(root)), root: root, xs: xs, xsDir: xsDir, m: m}
}

func (f *fixture) repo(t *testing.T, name string) *repository.Repository {
	t.Helper()
	repo, err := repository.Init(context.Background(), f.st.RepositoriesFS(), repository.ResolvePath(name), "main")
	if err != nil {
		t.Fatalf("init %s: %v", name, err)
	}
	return repo
}

func (f *fixture) commit(t *testing.T, repo *repository.Repository, branch, path, content string) string {
	t.Helper()
	hash, err := repo.CreateCommit(context.Background(), branch, "add "+path, "Test", "test@test.com",
		[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: path, Content: []byte(content)}}, "")
	if err != nil {
		t.Fatalf("commit %s: %v", path, err)
	}
	return hash
}

// gitObjects lists the shared store's loose objects with their on-disk sizes, independently of the walker.
func (f *fixture) gitObjects(t *testing.T) map[string]int64 {
	t.Helper()
	got := map[string]int64{}
	root := filepath.Join(f.root, "git", "sha1", "objects")
	dirs, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read %s: %v", root, err)
	}
	for _, dir := range dirs {
		if !dir.IsDir() || len(dir.Name()) != 2 {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(root, dir.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", dir.Name(), err)
		}
		for _, e := range entries {
			info, err := e.Info()
			if err != nil {
				t.Fatalf("stat %s: %v", e.Name(), err)
			}
			got[dir.Name()+e.Name()] = info.Size()
		}
	}
	return got
}

// orphan writes a well-named raw file into the shared store, like a write aborted before its ref landed.
func (f *fixture) orphan(t *testing.T, seed string) string {
	t.Helper()
	hex := seed + strings.Repeat("0", 40-len(seed))
	path := filepath.Join(f.root, "git", "sha1", "objects", hex[:2], hex[2:])
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir orphan: %v", err)
	}
	if err := os.WriteFile(path, []byte("orphan "+seed), 0o644); err != nil {
		t.Fatalf("write orphan: %v", err)
	}
	return hex
}

// diff returns the objects of after that before lacks.
func diff(before, after map[string]int64) map[string]int64 {
	out := map[string]int64{}
	for hash, size := range after {
		if _, ok := before[hash]; !ok {
			out[hash] = size
		}
	}
	return out
}

func sum(objects map[string]int64) (total int64) {
	for _, size := range objects {
		total += size
	}
	return total
}

// age moves every stored xet and git object's mtime into the past so it falls outside any positive grace window.
func (f *fixture) age(t *testing.T, d time.Duration) {
	t.Helper()
	old := time.Now().Add(-d)
	for _, dir := range []string{f.xsDir, filepath.Join(f.root, "git", "sha1")} {
		err := filepath.WalkDir(dir, func(path string, e fs.DirEntry, err error) error {
			if err != nil || e.IsDir() {
				return err
			}
			return os.Chtimes(path, old, old)
		})
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("age %s: %v", dir, err)
		}
	}
}

func (f *fixture) collector() *Collector {
	return NewCollector(f.st.RepositoriesFS(), f.xs)
}

// readBlob checks the content of path on main and returns the blob's hash.
func readBlob(t *testing.T, repo *repository.Repository, path, want string) string {
	t.Helper()
	blob, err := repo.Blob("main", path)
	if err != nil {
		t.Fatalf("blob %s: %v", path, err)
	}
	r, err := blob.NewReader()
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	data, err := io.ReadAll(r)
	r.Close()
	if err != nil || string(data) != want {
		t.Fatalf("read %s: %q, %v", path, data, err)
	}
	return blob.Hash().String()
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

func (f *fixture) commitPointer(t *testing.T, name, oid string) {
	t.Helper()
	ctx := context.Background()
	repo, err := repository.Init(ctx, f.st.RepositoriesFS(), repository.ResolvePath(name), "main")
	if err != nil {
		t.Fatalf("init %s: %v", name, err)
	}
	ptr := fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, objectSize)
	_, err = repo.CreateCommit(ctx, "main", "add model", "Test", "test@test.com",
		[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: "model.bin", Content: []byte(ptr)}}, "")
	if err != nil {
		t.Fatalf("commit %s: %v", name, err)
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

func TestSweepStepGitObjects(t *testing.T) {
	ctx := context.Background()
	t.Run("DeletesUnreachable", func(t *testing.T) {
		f := newFixture(t)
		repo := f.repo(t, "org/repo")
		f.commit(t, repo, "main", "a.txt", "a\n")
		live := f.gitObjects(t)
		f.commit(t, repo, "tmp", "tmp.txt", "tmp\n")
		f.orphan(t, "ab")
		dead := diff(live, f.gitObjects(t))
		if len(dead) != 4 {
			t.Fatalf("fixture: dead objects %v, want commit, tree, blob and orphan", dead)
		}
		if err := repo.DeleteBranch("tmp"); err != nil {
			t.Fatalf("delete branch: %v", err)
		}
		res, err := f.collector().SweepStep(ctx, Options{Grace: -1})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if !res.Done || res.SweptGitObjects != len(dead) || res.RemainingGitObjects != 0 || res.ReclaimedBytes != sum(dead) || res.SkippedInGrace != 0 {
			t.Fatalf("unexpected result: %+v, want %d git objects and %d bytes", res, len(dead), sum(dead))
		}
		if got := f.gitObjects(t); !maps.Equal(got, live) {
			t.Fatalf("shared store after sweep: %v, want %v", got, live)
		}
	})
	t.Run("KeepsReachable", func(t *testing.T) {
		f := newFixture(t)
		a := f.repo(t, "org/a")
		first := f.commit(t, a, "main", "a.txt", "shared\n")
		f.commit(t, a, "main", "b.txt", "b\n")
		if err := a.CreateTag("v1", first); err != nil {
			t.Fatalf("tag: %v", err)
		}
		if err := a.CreateBranch("feature", "main"); err != nil {
			t.Fatalf("branch: %v", err)
		}
		live := f.gitObjects(t)
		b := f.repo(t, "org/b")
		bCommit := f.commit(t, b, "main", "copy.txt", "shared\n")
		dead := diff(live, f.gitObjects(t))
		if _, ok := dead[bCommit]; !ok || len(dead) != 2 {
			t.Fatalf("fixture: repository b added %v, want only its commit %s and tree", dead, bCommit)
		}
		if err := b.Remove(); err != nil {
			t.Fatalf("remove b: %v", err)
		}
		res, err := f.collector().SweepStep(ctx, Options{Grace: -1})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if !res.Done || res.SweptGitObjects != 2 || res.ReclaimedBytes != sum(dead) {
			t.Fatalf("unexpected result: %+v", res)
		}
		if got := f.gitObjects(t); !maps.Equal(got, live) {
			t.Fatalf("shared store after sweep: %v, want %v", got, live)
		}
		for _, rev := range []string{"main", "v1", "feature"} {
			blob, err := a.Blob(rev, "a.txt")
			if err != nil {
				t.Fatalf("blob at %s: %v", rev, err)
			}
			r, err := blob.NewReader()
			if err != nil {
				t.Fatalf("read at %s: %v", rev, err)
			}
			data, err := io.ReadAll(r)
			r.Close()
			if err != nil || string(data) != "shared\n" {
				t.Fatalf("read at %s: %q, %v", rev, data, err)
			}
		}
	})
	t.Run("KeepsSharedAcrossRepos", func(t *testing.T) {
		f := newFixture(t)
		c := f.collector()
		a := f.repo(t, "org/a")
		f.commit(t, a, "main", "shared.txt", "shared\n")
		f.commit(t, a, "main", "a.txt", "only a\n")
		aObjects := f.gitObjects(t)
		b := f.repo(t, "org/b")
		f.commit(t, b, "main", "copy.txt", "shared\n")
		f.commit(t, b, "main", "b.txt", "only b\n")
		live := f.gitObjects(t)
		shared := readBlob(t, a, "shared.txt", "shared\n")
		if _, ok := aObjects[shared]; !ok || readBlob(t, b, "copy.txt", "shared\n") != shared {
			t.Fatalf("fixture: shared content is not one blob %s written by a", shared)
		}
		orphan := f.orphan(t, "ab")
		all := f.gitObjects(t)
		res, err := c.SweepStep(ctx, Options{Grace: -1})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if !res.Done || res.SweptGitObjects != 1 || res.ReclaimedBytes != all[orphan] || res.SkippedInGrace != 0 {
			t.Fatalf("unexpected result: %+v, want only the orphan swept", res)
		}
		if got := f.gitObjects(t); !maps.Equal(got, live) {
			t.Fatalf("shared store after sweep: %v, want %v", got, live)
		}
		readBlob(t, a, "a.txt", "only a\n")
		readBlob(t, a, "shared.txt", "shared\n")
		readBlob(t, b, "b.txt", "only b\n")
		readBlob(t, b, "copy.txt", "shared\n")
		// Removing a leaves b's own objects and the blob both repositories name.
		want := diff(aObjects, live)
		want[shared] = aObjects[shared]
		if err := a.Remove(); err != nil {
			t.Fatalf("remove a: %v", err)
		}
		res, err = c.SweepStep(ctx, Options{Grace: -1})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if !res.Done || res.SweptGitObjects != len(aObjects)-1 || res.ReclaimedBytes != sum(aObjects)-aObjects[shared] {
			t.Fatalf("unexpected result: %+v, want %d objects of a swept", res, len(aObjects)-1)
		}
		if got := f.gitObjects(t); !maps.Equal(got, want) {
			t.Fatalf("shared store after removing a: %v, want %v", got, want)
		}
		readBlob(t, b, "b.txt", "only b\n")
		readBlob(t, b, "copy.txt", "shared\n")
	})
	t.Run("KeepsGitInternals", func(t *testing.T) {
		// foo.git/HEAD and bar.git/HEAD are ref files shaped like a repository's HEAD; dir.git/HEAD is a ref directory.
		f := newFixture(t)
		repo := f.repo(t, "org/repo")
		f.commit(t, repo, "main", "a.txt", "a\n")
		for _, name := range []string{"foo.git/HEAD", "dir.git/HEAD/leaf"} {
			if err := repo.CreateBranch(name, "main"); err != nil {
				t.Fatalf("branch %s: %v", name, err)
			}
		}
		if err := repo.CreateTag("bar.git/HEAD", "main"); err != nil {
			t.Fatalf("tag: %v", err)
		}
		live := f.gitObjects(t)
		orphan := f.orphan(t, "ab")
		all := f.gitObjects(t)
		bare := filepath.Join(f.root, "repositories", "org", "repo.git")
		refFiles := func() map[string]string {
			got := map[string]string{}
			err := filepath.WalkDir(filepath.Join(bare, "refs"), func(path string, e fs.DirEntry, err error) error {
				if err != nil || e.IsDir() {
					return err
				}
				data, err := os.ReadFile(path)
				got[path] = string(data)
				return err
			})
			if err != nil {
				t.Fatalf("read refs: %v", err)
			}
			return got
		}
		refs := refFiles()
		if len(refs) != 4 {
			t.Fatalf("fixture: ref files %v, want main, foo.git/HEAD, dir.git/HEAD/leaf and bar.git/HEAD", refs)
		}
		var visited []string
		err := repository.Walk(ctx, f.st.RepositoriesFS(), "/", func(path string) error {
			visited = append(visited, path)
			return nil
		})
		if err != nil || !slices.Equal(visited, []string{"/org/repo.git"}) {
			t.Fatalf("Walk yielded %v, %v; want only /org/repo.git", visited, err)
		}
		c := f.collector()
		if res, err := c.Prune(ctx, PruneOptions{Grace: -1}); err != nil || res.Repositories != 1 {
			t.Fatalf("prune: %+v, %v; want 1 repository", res, err)
		}
		for i, want := range []int{1, 0} {
			res, err := c.SweepStep(ctx, Options{Grace: -1})
			if err != nil {
				t.Fatalf("sweep %d: %v", i, err)
			}
			if !res.Done || res.SweptGitObjects != want || res.ReclaimedBytes != int64(want)*all[orphan] {
				t.Fatalf("sweep %d: %+v, want %d swept", i, res, want)
			}
		}
		if got := f.gitObjects(t); !maps.Equal(got, live) {
			t.Fatalf("shared store after sweep: %v, want %v", got, live)
		}
		if got := refFiles(); !maps.Equal(got, refs) {
			t.Fatalf("refs after sweep: %v, want %v", got, refs)
		}
		readBlob(t, repo, "a.txt", "a\n")
		for _, args := range [][]string{{"-C", bare, "fsck", "--full", "--strict"}, {"clone", "-q", "--no-local", bare, filepath.Join(t.TempDir(), "clone")}} {
			cmd := exec.CommandContext(t.Context(), "git", args...)
			cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=safe.bareRepository", "GIT_CONFIG_VALUE_0=all")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
			}
		}
	})
	t.Run("Grace", func(t *testing.T) {
		f := newFixture(t)
		c := f.collector()
		repo := f.repo(t, "org/repo")
		f.commit(t, repo, "main", "a.txt", "a\n")
		live := f.gitObjects(t)
		f.commit(t, repo, "tmp", "tmp.txt", "tmp\n")
		if err := repo.DeleteBranch("tmp"); err != nil {
			t.Fatalf("delete branch: %v", err)
		}
		dead := diff(live, f.gitObjects(t))
		res, err := c.SweepStep(ctx, Options{})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if !res.Done || res.SweptGitObjects != 0 || res.SkippedInGrace != len(dead) {
			t.Fatalf("default grace: %+v, want %d skipped", res, len(dead))
		}
		f.age(t, 2*time.Hour)
		fresh := f.orphan(t, "ab")
		want := f.gitObjects(t)
		for hash := range dead {
			delete(want, hash)
		}
		res, err = c.SweepStep(ctx, Options{Grace: time.Hour})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if !res.Done || res.SweptGitObjects != len(dead) || res.SkippedInGrace != 1 || res.ReclaimedBytes != sum(dead) {
			t.Fatalf("aged sweep: %+v, want %d swept, 1 skipped", res, len(dead))
		}
		if got := f.gitObjects(t); !maps.Equal(got, want) {
			t.Fatalf("shared store after sweep: %v, want %v", got, want)
		}
		res, err = c.SweepStep(ctx, Options{Grace: -1})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if !res.Done || res.SweptGitObjects != 1 || res.ReclaimedBytes != want[fresh] {
			t.Fatalf("disabled grace: %+v", res)
		}
		if got := f.gitObjects(t); !maps.Equal(got, live) {
			t.Fatalf("shared store after sweep: %v, want %v", got, live)
		}
	})
	t.Run("GraceBoundary", func(t *testing.T) {
		f := newFixture(t)
		f.commit(t, f.repo(t, "org/repo"), "main", "a.txt", "a\n")
		live := f.gitObjects(t)
		// The clock has a fractional second: without the cutoff's Truncate, equal and after would fall before it.
		start := time.Date(2026, 1, 2, 3, 4, 5, 700_000_000, time.UTC)
		now = func() time.Time { return start }
		t.Cleanup(func() { now = time.Now })
		cutoff := start.Add(-time.Hour).Truncate(time.Second)
		before, equal, after, zero := f.orphan(t, "ab"), f.orphan(t, "cd"), f.orphan(t, "ef"), f.orphan(t, "00")
		for hash, mtime := range map[string]time.Time{before: cutoff.Add(-time.Second), equal: cutoff, after: cutoff.Add(500 * time.Millisecond), zero: cutoff.Add(-time.Second)} {
			if err := os.Chtimes(filepath.Join(f.root, "git", "sha1", "objects", hash[:2], hash[2:]), mtime, mtime); err != nil {
				t.Fatalf("chtimes %s: %v", hash, err)
			}
		}
		all := f.gitObjects(t)
		c := NewCollector(repository.BindSharedObjects(&zeroModTimeFS{Filesystem: osfs.New(f.root), hex: zero}, "/repositories", "/git/sha1"), f.xs)
		res, err := c.SweepStep(ctx, Options{Grace: time.Hour})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if !res.Done || res.SweptGitObjects != 1 || res.SkippedInGrace != 3 || res.ReclaimedBytes != all[before] {
			t.Fatalf("boundary sweep: %+v, want only %s swept and 3 skipped", res, before)
		}
		want := maps.Clone(all)
		delete(want, before)
		if got := f.gitObjects(t); !maps.Equal(got, want) {
			t.Fatalf("shared store after boundary sweep: %v, want %v", got, want)
		}
		res, err = c.SweepStep(ctx, Options{Grace: -1})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if !res.Done || res.SweptGitObjects != 3 || res.ReclaimedBytes != sum(want)-sum(live) {
			t.Fatalf("disabled grace: %+v, want the 3 shielded orphans swept", res)
		}
		if got := f.gitObjects(t); !maps.Equal(got, live) {
			t.Fatalf("shared store after sweep: %v, want %v", got, live)
		}
	})
	t.Run("DryRun", func(t *testing.T) {
		f := newFixture(t)
		f.repo(t, "org/repo")
		f.orphan(t, "ab")
		f.orphan(t, "cd")
		before := f.gitObjects(t)
		res, err := f.collector().SweepStep(ctx, Options{Grace: -1, DryRun: true, MaxDeletes: 1, Budget: time.Nanosecond})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if !res.DryRun || !res.Done || res.SweptGitObjects != 2 || res.RemainingGitObjects != 0 || res.ReclaimedBytes != sum(before) {
			t.Fatalf("unexpected result: %+v", res)
		}
		if got := f.gitObjects(t); !maps.Equal(got, before) {
			t.Fatalf("dry run changed the store: %v, want %v", got, before)
		}
	})
	t.Run("MaxDeletes", func(t *testing.T) {
		f := newFixture(t)
		c := f.collector()
		f.repo(t, "org/repo")
		for _, seed := range []string{"ab", "cd", "ef"} {
			f.orphan(t, seed)
		}
		for step, want := range []struct{ swept, remaining int }{{1, 2}, {1, 1}, {1, 0}, {0, 0}} {
			res, err := c.SweepStep(ctx, Options{Grace: -1, MaxDeletes: 1})
			if err != nil {
				t.Fatalf("step %d: %v", step, err)
			}
			if res.SweptGitObjects != want.swept || res.RemainingGitObjects != want.remaining || res.Done != (want.remaining == 0) {
				t.Fatalf("step %d: %+v, want %+v", step, res, want)
			}
			if got := f.gitObjects(t); len(got) != want.remaining {
				t.Fatalf("step %d: store holds %v, want %d objects", step, got, want.remaining)
			}
		}
	})
	t.Run("Budget", func(t *testing.T) {
		f := newFixture(t)
		c := f.collector()
		f.repo(t, "org/repo")
		f.orphan(t, "ab")
		f.orphan(t, "cd")
		res, err := c.SweepStep(ctx, Options{Grace: -1, Budget: time.Nanosecond})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if res.Done || res.SweptGitObjects != 1 || res.RemainingGitObjects != 1 || len(f.gitObjects(t)) != 1 {
			t.Fatalf("unexpected result: %+v, store %v", res, f.gitObjects(t))
		}
		res, err = c.SweepStep(ctx, Options{Grace: -1, Budget: time.Nanosecond})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if !res.Done || res.SweptGitObjects != 1 || res.RemainingGitObjects != 0 || len(f.gitObjects(t)) != 0 {
			t.Fatalf("unexpected result: %+v, store %v", res, f.gitObjects(t))
		}
	})
	// withDeadXet returns a fixture whose xet store holds one unlinked object and whose git store holds two orphans,
	// with the xet object's shard+xorb count and the git store's live and dead sets.
	withDeadXet := func(t *testing.T) (f *fixture, xetDead int, live, dead map[string]int64) {
		t.Helper()
		f = newFixture(t)
		oid := f.put(t, "dead-g ")
		if res, err := f.collector().Prune(ctx, PruneOptions{Grace: -1}); err != nil || !slices.Equal(res.Unlinked, []string{oid}) {
			t.Fatalf("prune: result=%+v err=%v", res, err)
		}
		shards, xorbs := f.counts(t)
		f.repo(t, "org/repo")
		live = f.gitObjects(t)
		f.orphan(t, "ab")
		f.orphan(t, "cd")
		return f, shards + xorbs, live, diff(live, f.gitObjects(t))
	}
	t.Run("XetIncompleteSkipsGit", func(t *testing.T) {
		f, _, live, dead := withDeadXet(t)
		res, err := f.collector().SweepStep(ctx, Options{Grace: -1, MaxDeletes: 1})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if res.Done || res.SweptShards != 1 || res.SweptGitObjects != 0 || res.RemainingGitObjects != 0 {
			t.Fatalf("unexpected result: %+v", res)
		}
		if got := f.gitObjects(t); len(got) != len(live)+len(dead) {
			t.Fatalf("git store touched while xet incomplete: %v", got)
		}
	})
	t.Run("MaxDeletesSpansStores", func(t *testing.T) {
		f, xetDead, live, dead := withDeadXet(t)
		c := f.collector()
		res, err := c.SweepStep(ctx, Options{Grace: -1, MaxDeletes: xetDead})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if res.Done || res.SweptShards+res.SweptXorbs != xetDead || res.SweptGitObjects != 0 || res.RemainingGitObjects != 2 {
			t.Fatalf("max=%d: %+v", xetDead, res)
		}
		if shards, xorbs := f.counts(t); shards+xorbs != 0 {
			t.Fatalf("xet store not drained: shards=%d xorbs=%d", shards, xorbs)
		}
		if got := f.gitObjects(t); len(got) != len(live)+len(dead) {
			t.Fatalf("git store touched at the shared cap: %v", got)
		}
		f, xetDead, live, dead = withDeadXet(t)
		c = f.collector()
		res, err = c.SweepStep(ctx, Options{Grace: -1, MaxDeletes: xetDead + 1})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if res.Done || res.SweptShards+res.SweptXorbs != xetDead || res.SweptGitObjects != 1 || res.RemainingGitObjects != 1 {
			t.Fatalf("max=%d: %+v", xetDead+1, res)
		}
		if left := diff(live, f.gitObjects(t)); len(left) != 1 || len(diff(dead, left)) != 0 {
			t.Fatalf("git store after one deletion: %v, want one of %v", left, dead)
		}
		res, err = c.SweepStep(ctx, Options{Grace: -1})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if !res.Done || res.SweptGitObjects != 1 || res.RemainingGitObjects != 0 {
			t.Fatalf("drain: %+v", res)
		}
		if got := f.gitObjects(t); !maps.Equal(got, live) {
			t.Fatalf("shared store after drain: %v, want %v", got, live)
		}
	})
	t.Run("BudgetSpansStores", func(t *testing.T) {
		f, _, live, dead := withDeadXet(t)
		c := f.collector()
		shards, _ := f.counts(t)
		// Leave exactly one dead xorb so the next xet pass deletes once and still finishes.
		if res, err := c.SweepStep(ctx, Options{Grace: -1, MaxDeletes: shards}); err != nil || res.Done || res.SweptShards != shards {
			t.Fatalf("shard sweep: result=%+v err=%v", res, err)
		}
		if shards, xorbs := f.counts(t); shards != 0 || xorbs != 1 {
			t.Fatalf("fixture: shards=%d xorbs=%d, want one dead xorb left", shards, xorbs)
		}
		res, err := c.SweepStep(ctx, Options{Grace: -1, Budget: time.Nanosecond})
		if err != nil {
			t.Fatalf("sweep step: %v", err)
		}
		if res.Done || res.SweptXorbs != 1 || res.SweptGitObjects != 0 || res.RemainingGitObjects != 2 {
			t.Fatalf("budget after xet deletion: %+v", res)
		}
		if got := f.gitObjects(t); len(got) != len(live)+len(dead) {
			t.Fatalf("git store touched with the budget spent: %v", got)
		}
	})
	t.Run("MarkUncharged", func(t *testing.T) {
		// now is read at sweep start, mark start and mark end, then per candidate.
		for name, tc := range map[string]struct {
			clock          []time.Duration
			swept, remains int
		}{
			"UpstreamCharged": {clock: []time.Duration{0, time.Hour}, swept: 0, remains: 2},
			"MarkingFree":     {clock: []time.Duration{0, 0, time.Hour}, swept: 2, remains: 0},
		} {
			t.Run(name, func(t *testing.T) {
				f, xetDead, _, _ := withDeadXet(t)
				now = fakeClock(tc.clock...)
				t.Cleanup(func() { now = time.Now })
				res, err := f.collector().SweepStep(ctx, Options{Grace: -1, Budget: time.Minute})
				if err != nil {
					t.Fatalf("sweep step: %v", err)
				}
				if res.SweptShards+res.SweptXorbs != xetDead || res.SweptGitObjects != tc.swept || res.RemainingGitObjects != tc.remains || res.Done != (tc.remains == 0) {
					t.Fatalf("unexpected result: %+v, want %d git objects swept, %d remaining", res, tc.swept, tc.remains)
				}
			})
		}
	})
	t.Run("Aborts", func(t *testing.T) {
		// Every case starts with one dead orphan that must survive the failed sweep.
		setup := func(t *testing.T) (*fixture, map[string]int64) {
			t.Helper()
			f := newFixture(t)
			f.repo(t, "org/repo")
			f.orphan(t, "ab")
			return f, f.gitObjects(t)
		}
		t.Run("BrokenRepo", func(t *testing.T) {
			f, before := setup(t)
			for _, file := range []string{"HEAD", "objects"} {
				if err := util.WriteFile(f.st.RepositoriesFS(), "/org/broken.git/"+file, []byte("ref: refs/heads/main\n"), 0o644); err != nil {
					t.Fatalf("write %s: %v", file, err)
				}
			}
			if _, err := f.collector().SweepStep(ctx, Options{Grace: -1}); err == nil || !strings.Contains(err.Error(), "/org/broken.git") {
				t.Fatalf("sweep: expected error naming the broken repository, got %v", err)
			}
			if got := f.gitObjects(t); !maps.Equal(got, before) {
				t.Fatalf("git objects deleted despite aborted sweep: %v", got)
			}
		})
		t.Run("MalformedRef", func(t *testing.T) {
			f, before := setup(t)
			if err := util.WriteFile(f.st.RepositoriesFS(), "/org/repo.git/refs/heads/broken", []byte(strings.Repeat("a", 40)+"\n"), 0o644); err != nil {
				t.Fatalf("write ref: %v", err)
			}
			if _, err := f.collector().SweepStep(ctx, Options{Grace: -1}); err == nil || !strings.Contains(err.Error(), "/org/repo.git") {
				t.Fatalf("sweep: expected error naming the repository, got %v", err)
			}
			if got := f.gitObjects(t); !maps.Equal(got, before) {
				t.Fatalf("git objects deleted despite aborted sweep: %v", got)
			}
		})
		t.Run("MissingObjectAfterIntactRepo", func(t *testing.T) {
			// org/repo marks cleanly before org/zzz, whose HEAD commit is gone from the shared store.
			f := newFixture(t)
			f.commit(t, f.repo(t, "org/repo"), "main", "a.txt", "a\n")
			f.orphan(t, "ab")
			hash := f.commit(t, f.repo(t, "org/zzz"), "main", "z.txt", "z\n")
			if err := os.Remove(filepath.Join(f.root, "git", "sha1", "objects", hash[:2], hash[2:])); err != nil {
				t.Fatalf("remove commit: %v", err)
			}
			before := f.gitObjects(t)
			_, err := f.collector().SweepStep(ctx, Options{Grace: -1})
			if err == nil || !strings.Contains(err.Error(), "/org/zzz.git") || !strings.Contains(err.Error(), hash) {
				t.Fatalf("sweep: expected error naming org/zzz and %s, got %v", hash, err)
			}
			if got := f.gitObjects(t); !maps.Equal(got, before) {
				t.Fatalf("git objects deleted despite aborted sweep: %v", got)
			}
		})
		t.Run("HEADStatError", func(t *testing.T) {
			// org/repo marks cleanly before org/zzz, whose HEAD cannot be stat'ed.
			f := newFixture(t)
			f.commit(t, f.repo(t, "org/repo"), "main", "a.txt", "a\n")
			f.orphan(t, "ab")
			f.commit(t, f.repo(t, "org/zzz"), "main", "z.txt", "z\n")
			before := f.gitObjects(t)
			fault := &headFaultFS{Filesystem: osfs.New(f.root), head: "/repositories/org/zzz.git/HEAD", err: errors.New("unreadable")}
			_, err := NewCollector(repository.BindSharedObjects(fault, "/repositories", "/git/sha1"), f.xs).SweepStep(ctx, Options{Grace: -1})
			if !errors.Is(err, fault.err) || !strings.Contains(err.Error(), "/org/zzz.git/HEAD") {
				t.Fatalf("sweep: expected the HEAD stat error, got %v", err)
			}
			if got := f.gitObjects(t); !maps.Equal(got, before) {
				t.Fatalf("git objects deleted despite aborted sweep: %v", got)
			}
		})
		t.Run("Canceled", func(t *testing.T) {
			f, before := setup(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			c := NewCollector(f.st.RepositoriesFS(), &cancelStore{FileStorage: f.xs, cancel: cancel})
			if _, err := c.SweepStep(ctx, Options{Grace: -1}); !errors.Is(err, context.Canceled) {
				t.Fatalf("sweep: got %v, want context.Canceled", err)
			}
			if got := f.gitObjects(t); !maps.Equal(got, before) {
				t.Fatalf("git objects deleted despite cancellation: %v", got)
			}
		})
		t.Run("RemoveError", func(t *testing.T) {
			if os.Getuid() == 0 {
				t.Skip("root ignores directory permissions")
			}
			f, before := setup(t)
			dir := filepath.Join(f.root, "git", "sha1", "objects", "ab")
			if err := os.Chmod(dir, 0o555); err != nil {
				t.Fatalf("chmod: %v", err)
			}
			t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
			if _, err := f.collector().SweepStep(ctx, Options{Grace: -1}); err == nil || !strings.Contains(err.Error(), "ab"+strings.Repeat("0", 38)) {
				t.Fatalf("sweep: expected error naming the object, got %v", err)
			}
			if got := f.gitObjects(t); !maps.Equal(got, before) {
				t.Fatalf("git objects changed: %v", got)
			}
		})
		t.Run("Unbound", func(t *testing.T) {
			f, before := setup(t)
			res, err := NewCollector(osfs.New(filepath.Join(f.root, "repositories")), f.xs).SweepStep(ctx, Options{Grace: -1})
			if err != nil {
				t.Fatalf("sweep step: %v", err)
			}
			if !res.Done || res.SweptGitObjects != 0 || res.RemainingGitObjects != 0 {
				t.Fatalf("unexpected result: %+v", res)
			}
			if got := f.gitObjects(t); !maps.Equal(got, before) {
				t.Fatalf("unbound collector deleted git objects: %v", got)
			}
		})
	})
}

// fakeClock returns base plus each offset in turn, then the last one forever.
func fakeClock(offsets ...time.Duration) func() time.Time {
	base, calls := time.Now(), 0
	return func() time.Time {
		d := offsets[min(calls, len(offsets)-1)]
		calls++
		return base.Add(d)
	}
}

// cancelStore cancels the sweep context once xet's last mark walk returns, so only the git phase sees it.
type cancelStore struct {
	*xetstorage.FileStorage
	cancel context.CancelFunc
}

func (s *cancelStore) WalkXorbs(ctx context.Context, fn func(string, int64, time.Time) error) error {
	defer s.cancel()
	return s.FileStorage.WalkXorbs(ctx, fn)
}

// zeroModTimeFS lists the loose object hex with a zero mtime, like an object store listing without a timestamp.
type zeroModTimeFS struct {
	billy.Filesystem
	hex string
}

func (z *zeroModTimeFS) Chroot(path string) (billy.Filesystem, error) {
	return chroot.New(z, path), nil
}

func (z *zeroModTimeFS) ReadDir(path string) ([]fs.DirEntry, error) {
	entries, err := z.Filesystem.ReadDir(path)
	for i, e := range entries {
		if filepath.Base(path) == z.hex[:2] && e.Name() == z.hex[2:] {
			entries[i] = zeroModTimeEntry{e}
		}
	}
	return entries, err
}

type zeroModTimeEntry struct{ fs.DirEntry }

func (e zeroModTimeEntry) Info() (fs.FileInfo, error) {
	info, err := e.DirEntry.Info()
	if err != nil {
		return nil, err
	}
	return zeroModTimeInfo{info}, nil
}

type zeroModTimeInfo struct{ fs.FileInfo }

func (zeroModTimeInfo) ModTime() time.Time { return time.Time{} }

// headFaultFS fails Stat on one HEAD with a non-NotExist error, staying in the chroot chain like zeroModTimeFS.
type headFaultFS struct {
	billy.Filesystem
	head string
	err  error
}

func (h *headFaultFS) Chroot(path string) (billy.Filesystem, error) {
	return chroot.New(h, path), nil
}

func (h *headFaultFS) Stat(name string) (os.FileInfo, error) {
	if name == h.head {
		return nil, &os.PathError{Op: "stat", Path: name, Err: h.err}
	}
	return h.Filesystem.Stat(name)
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
