package internalapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/util"
	xetclient "github.com/wzshiming/xet/client"
	xetmirror "github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/mirror/spool"
	xetstorage "github.com/wzshiming/xet/storage"
	xetlocal "github.com/wzshiming/xet/storage/local"

	"github.com/matrixhub-ai/hfd/pkg/gc"
	"github.com/matrixhub-ai/hfd/pkg/gc/xetstore"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

const objectSize = 64 * 1024

// fixture is a real local xet store under hfd storage, with hfd's mirror to upload LFS objects into it.
type fixture struct {
	st *storage.Storage
	xs *xetlocal.Storage
	m  *mirror.Mirror
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	root := t.TempDir()
	xs, err := xetlocal.NewStorage(xetlocal.WithBasePath(filepath.Join(root, "xet", "storage")))
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
	return &fixture{st: st, xs: xs, m: m}
}

// handler serves the fixture's repositories and xet store with grace disabled unless a request sets it; opts come last.
func (f *fixture) handler(opts ...Option) *Handler {
	return NewHandler(append([]Option{WithGC(gc.NewGC(f.st.RepositoriesFS(), xetstore.New(f.xs))), WithStorage(f.xs), WithGCGrace(-1)}, opts...)...)
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
	content := fmt.Appendf(nil, "version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, objectSize)
	_, err = repo.CreateCommit(ctx, "main", "add model", "Test", "test@test.com",
		[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: "model.bin", Content: content}}, "")
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

// storedBytes sums the sizes of every stored shard and xorb.
func (f *fixture) storedBytes(t *testing.T) int64 {
	t.Helper()
	var total int64
	add := func(_ string, size int64, _ time.Time) error {
		total += size
		return nil
	}
	if err := f.xs.WalkShards(context.Background(), add); err != nil {
		t.Fatalf("walk shards: %v", err)
	}
	if err := f.xs.WalkXorbs(context.Background(), add); err != nil {
		t.Fatalf("walk xorbs: %v", err)
	}
	return total
}

// newSpool builds a spool over the fixture's xet store in a directory of its own.
func (f *fixture) newSpool(t *testing.T) (*spool.Spool, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "spool")
	sp, err := spool.NewSpool(dir, f.xs)
	if err != nil {
		t.Fatalf("new spool: %v", err)
	}
	return sp, dir
}

// newEngine builds a xet mirror engine over the fixture's store sharing sp, returning it with its index directory.
func (f *fixture) newEngine(t *testing.T, sp *spool.Spool) (*xetmirror.Mirror, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "mirror")
	engine, err := xetmirror.NewMirror(xetmirror.WithStorage(f.xs), xetmirror.WithCacheDir(dir), xetmirror.WithSpool(sp))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return engine, filepath.Join(dir, "index")
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

// plant writes content to dir/name, creating dir.
func plant(t *testing.T, dir, name, content string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// plantIndexTemp leaves the temp file of an interrupted manifest write in the engine index.
func plantIndexTemp(t *testing.T, indexDir string) string {
	t.Helper()
	return plant(t, filepath.Join(indexDir, "repo", "commits"), "."+strings.Repeat("c", 40)+".json.tmp", "{}")
}

func fileExists(t *testing.T, p string) bool {
	t.Helper()
	_, err := os.Stat(p)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stat %s: %v", p, err)
	}
	return err == nil
}

// gcStep posts one GC step with the query and decodes the 200 response, returning it with its body.
func gcStep(t *testing.T, h http.Handler, query string) (gcResponse, string) {
	t.Helper()
	rec := do(h, http.MethodPost, "/internal/gc?"+query)
	return decodeResult(t, rec, http.StatusOK), rec.Body.String()
}

// TestHandlerGCReclaims pins that one step unlinks and reclaims sha256-anchored (the files entry alone keeps nothing alive),
// and that a dry run previews both and changes nothing.
func TestHandlerGCReclaims(t *testing.T) {
	t.Run("DryRun", func(t *testing.T) {
		f := newFixture(t)
		oid := f.put(t, "collect-b ")
		shards, xorbs := f.counts(t)
		res, body := gcStep(t, f.handler(), "dry_run=true&grace=0")
		if !res.Done || !res.DryRun || len(res.Errors) != 0 || !res.Prune.DryRun || !slices.Equal(res.Prune.Unlinked, []string{oid}) {
			t.Fatalf("unexpected result: %s", body)
		}
		if res.Storage == nil || !res.Storage.DryRun || !f.stored(t, oid) {
			t.Fatalf("unexpected storage pass: %s stored=%v", body, f.stored(t, oid))
		}
		if gotShards, gotXorbs := f.counts(t); gotShards != shards || gotXorbs != xorbs {
			t.Fatalf("dry run changed data: shards=%d xorbs=%d, want shards=%d xorbs=%d", gotShards, gotXorbs, shards, xorbs)
		}
	})
	t.Run("Run", func(t *testing.T) {
		f := newFixture(t)
		live, dead := f.put(t, "live-a "), f.put(t, "collect-a ")
		f.commitPointer(t, "org/repo", live)
		res, body := gcStep(t, f.handler(), "grace=0")
		t.Logf("gc JSON: %s", body)
		if !res.Done || len(res.Errors) != 0 || !slices.Equal(res.Prune.Unlinked, []string{dead}) {
			t.Fatalf("unexpected result: %s", body)
		}
		if res.Storage == nil || res.Storage.SweptShards == 0 || res.Storage.SweptXorbs == 0 || res.Storage.ReclaimedBytes <= 0 || f.stored(t, dead) || !f.stored(t, live) {
			t.Fatalf("unexpected storage pass: %s dead stored=%v live stored=%v", body, f.stored(t, dead), f.stored(t, live))
		}
	})
}

// A prune that fails in one repository keeps its section and errors entry, and the sweep still reclaims what it unlinked.
func TestHandlerGCFailedPruneStillSweeps(t *testing.T) {
	f := newFixture(t)
	live, dead := f.put(t, "live-p "), f.put(t, "dead-p ")
	f.commitPointer(t, "org/broken", live)
	if err := util.WriteFile(f.st.RepositoriesFS(), "/org/broken.git/refs/heads/dangling", []byte(strings.Repeat("1", 40)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	res, body := gcStep(t, f.handler(), "grace=0")
	t.Logf("failed-prune gc JSON: %s", body)
	if res.Done || len(res.Errors) != 1 || !strings.HasPrefix(res.Errors[0], "prune: git gc failed in 1 repositories") {
		t.Fatalf("unexpected result: %s", body)
	}
	if res.Prune.Failed["/org/broken.git"] == "" || !slices.Equal(res.Prune.Unlinked, []string{dead}) {
		t.Fatalf("unexpected prune section: %s", body)
	}
	if res.Storage == nil || !res.Storage.Done || res.Storage.SweptShards == 0 || f.stored(t, dead) || !f.stored(t, live) {
		t.Fatalf("sweep did not run after the failed prune: %s live stored=%v", body, f.stored(t, live))
	}
}

func TestHandlerGCSpools(t *testing.T) {
	f := newFixture(t)
	sp, dir := f.newSpool(t)
	const content = "spooled bytes"
	idle := plant(t, dir, "idle.spool", content)
	other := plant(t, dir, "notaspool.txt", content)
	ageFiles(t, dir, 2*time.Hour)
	h := f.handler(WithSpool(sp))

	// The spool is past the default 1h window, so only a forwarded 24h grace spares it.
	res, body := gcStep(t, h, "grace=24h")
	if !res.Done || res.Spools == nil || *res.Spools != (spool.SweepResult{Done: true}) || res.Mirror != nil || !fileExists(t, idle) {
		t.Fatalf("step within grace: %s, want nothing swept", body)
	}
	for _, dry := range []bool{true, false} {
		res, body := gcStep(t, h, fmt.Sprintf("grace=0&dry_run=%t", dry))
		want := spool.SweepResult{DryRun: dry, SweptSpools: 1, ReclaimedBytes: int64(len(content)), Done: true}
		if !res.Done || res.Spools == nil || *res.Spools != want || res.Mirror != nil {
			t.Fatalf("step(dry_run=%t): %s, want spools %+v", dry, body, want)
		}
		if fileExists(t, idle) != dry {
			t.Fatalf("step(dry_run=%t): idle spool present = %t", dry, !dry)
		}
	}
	if res, body := gcStep(t, h, "grace=0"); res.Spools == nil || *res.Spools != (spool.SweepResult{Done: true}) {
		t.Fatalf("step after reclaim: %s, want none swept", body)
	}
	if !fileExists(t, other) {
		t.Fatal("step removed a file without the .spool suffix")
	}
}

func TestHandlerGCIndex(t *testing.T) {
	f := newFixture(t)
	sp, _ := f.newSpool(t)
	engine, indexDir := f.newEngine(t, sp)
	tmp := plantIndexTemp(t, indexDir)
	ageFiles(t, indexDir, 2*time.Hour)
	h := f.handler(WithMirror(engine))

	// The temp file is past the default 1h window, so only a forwarded 24h grace spares it.
	res, body := gcStep(t, h, "grace=24h")
	if !res.Done || res.Mirror == nil || *res.Mirror != (xetmirror.SweepResult{Done: true}) || res.Spools != nil || !fileExists(t, tmp) {
		t.Fatalf("step within grace: %s, want nothing removed", body)
	}
	for _, dry := range []bool{true, false} {
		res, body := gcStep(t, h, fmt.Sprintf("grace=0&dry_run=%t", dry))
		want := xetmirror.SweepResult{DryRun: dry, RemovedTempFiles: 1, Done: true}
		if !res.Done || res.Mirror == nil || *res.Mirror != want || fileExists(t, tmp) != dry {
			t.Fatalf("step(dry_run=%t): %s, want mirror %+v", dry, body, want)
		}
	}

	// An engine-less deployment still sweeps its spool.
	res, body = gcStep(t, f.handler(WithSpool(sp), WithMirror(nil)), "grace=0")
	if !res.Done || res.Spools == nil || res.Mirror != nil {
		t.Fatalf("engine-less step: %s", body)
	}
}

func TestHandlerGCSweepsSpoolsAndIndexOnceStorageEnded(t *testing.T) {
	f := newFixture(t)
	sp, spoolDir := f.newSpool(t)
	engine, indexDir := f.newEngine(t, sp)
	idle := plant(t, spoolDir, "idle.spool", "idle")
	tmp := plantIndexTemp(t, indexDir)
	h := f.handler(WithSpool(sp), WithMirror(engine))
	f.put(t, "dead-s ")
	f.put(t, "dead-t ")

	// The cap stops the storage pass after one shard, so neither follow-up pass may run yet.
	res, body := gcStep(t, h, "grace=0&max=1")
	t.Logf("bounded step: %s", body)
	if res.Done || len(res.Prune.Unlinked) != 2 || res.Storage == nil || res.Storage.Done || res.Storage.SweptShards != 1 ||
		strings.Contains(body, `"spools"`) || strings.Contains(body, `"mirror"`) || !fileExists(t, idle) || !fileExists(t, tmp) {
		t.Fatalf("bounded step: %s, want an unfinished storage pass with the spool and index untouched", body)
	}
	shardsBefore, xorbsBefore := f.counts(t)
	bytesBefore := f.storedBytes(t)
	res, body = gcStep(t, h, "grace=0")
	t.Logf("finishing step JSON: %s", body)
	if !res.Done || fileExists(t, idle) || fileExists(t, tmp) {
		t.Fatalf("finishing step: %s, want the idle spool and the index temp file removed", body)
	}
	shardsAfter, xorbsAfter := f.counts(t)
	bytesAfter := f.storedBytes(t)
	st := res.Storage
	if st == nil || st.DryRun || !st.Done || st.SkippedInGrace != 0 || st.RemainingShards != 0 || st.RemainingXorbs != 0 {
		t.Fatalf("finishing step: %s, want a finished real storage pass", body)
	}
	if st.SweptShards != shardsBefore-shardsAfter || st.SweptXorbs != xorbsBefore-xorbsAfter || st.ReclaimedBytes != bytesBefore-bytesAfter {
		t.Fatalf("finishing step: %s, want the store's deltas: %d shards, %d xorbs, %d bytes",
			body, shardsBefore-shardsAfter, xorbsBefore-xorbsAfter, bytesBefore-bytesAfter)
	}
	if shardsAfter != 0 || xorbsAfter != 0 || st.SweptShards != 1 || st.SweptXorbs == 0 || st.ReclaimedBytes <= 0 {
		t.Fatalf("finishing step: %s, left %d shards and %d xorbs, want both dead objects swept", body, shardsAfter, xorbsAfter)
	}
	if !strings.Contains(body, `"dangling":[],"unreadable_shards":[]`) {
		t.Errorf("finishing step body %s: empty lists must encode as []", body)
	}
	for _, want := range []string{
		`"spools":{"dry_run":false,"swept_spools":1,"reclaimed_bytes":4,"done":true,"remaining_spools":0}`,
		`"mirror":{"dry_run":false,"dropped_entries":0,"removed_manifests":0,"removed_temp_files":1,"done":true,"remaining_temp_files":0,"remaining_manifests":0}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("finishing step body %s: missing %s", body, want)
		}
	}
}

func TestHandlerGCWithoutSpoolOrMirror(t *testing.T) {
	f := newFixture(t)
	for name, h := range map[string]*Handler{
		"unset": f.handler(),
		"nil":   f.handler(WithSpool(nil), WithMirror(nil)),
	} {
		res, body := gcStep(t, h, "grace=0")
		if !res.Done || strings.Contains(body, `"spools"`) || strings.Contains(body, `"mirror"`) {
			t.Errorf("%s: step %s, want a finished step without spools or mirror", name, body)
		}
		rec := do(h, http.MethodGet, "/internal/usage")
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"Spool":{"Count":0,"Bytes":0},"Mirror":{"Count":0,"Bytes":0}`) {
			t.Errorf("%s: usage %d %s, want zero Spool and Mirror sections", name, rec.Code, rec.Body)
		}
	}
}

func TestHandlerGCBudgetHandsDown(t *testing.T) {
	f := newFixture(t)
	sp, spoolDir := f.newSpool(t)
	engine, indexDir := f.newEngine(t, sp)
	spools := []string{plant(t, spoolDir, "a.spool", "a"), plant(t, spoolDir, "b.spool", "b")}
	tmp := plantIndexTemp(t, indexDir)
	ageFiles(t, spoolDir, 2*time.Hour)
	h := f.handler(WithSpool(sp), WithMirror(engine))
	const query = "grace=0&budget=1ns"

	// The storage pass sweeps nothing, so the spool pass gets a token budget and stops once it swept one spool.
	res, body := gcStep(t, h, query)
	t.Logf("first budgeted step: %s", body)
	if res.Done || res.Spools == nil || res.Spools.SweptSpools < 1 || len(res.Errors) != 0 ||
		res.Spools.Done && res.Mirror != nil {
		t.Fatalf("first step: %s, want progress with the spool pass cut short or the mirror pass deferred", body)
	}
	for i := 0; i < 10 && !res.Done; i++ {
		res, body = gcStep(t, h, query)
	}
	if !res.Done || res.Spools == nil || !res.Spools.Done || res.Mirror == nil || !res.Mirror.Done || len(res.Errors) != 0 {
		t.Fatalf("last step: %s, want every pass finished", body)
	}
	for _, p := range append(spools, tmp) {
		if fileExists(t, p) {
			t.Errorf("%s survived the budgeted steps", p)
		}
	}
}

// xorbWalkFailure fails the storage sweep's xorb walk, which the prune pass never takes.
type xorbWalkFailure struct {
	xetstorage.Storage
}

func (xorbWalkFailure) WalkXorbs(context.Context, func(string, int64, time.Time) error) error {
	return errors.New("injected xorb walk failure")
}

func TestHandlerGCStorageFailureStillSweepsSpoolAndMirror(t *testing.T) {
	f := newFixture(t)
	sp, spoolDir := f.newSpool(t)
	engine, indexDir := f.newEngine(t, sp)
	idle := plant(t, spoolDir, "idle.spool", "idle")
	tmp := plantIndexTemp(t, indexDir)
	f.put(t, "stored ")
	h := f.handler(WithStorage(xorbWalkFailure{Storage: f.xs}), WithSpool(sp), WithMirror(engine))

	res, body := gcStep(t, h, "grace=0")
	if res.Done || res.Storage != nil || len(res.Errors) != 1 || !strings.HasPrefix(res.Errors[0], "storage: ") ||
		strings.Contains(body, `"storage"`) || !strings.Contains(body, `"errors":["storage: `) {
		t.Fatalf("step: %s, want an unfinished step without a storage section listing the storage error", body)
	}
	if res.Spools == nil || !res.Spools.Done || res.Mirror == nil || !res.Mirror.Done || fileExists(t, idle) || fileExists(t, tmp) {
		t.Fatalf("step: %s, want both passes run despite the storage failure", body)
	}
}

func TestHandlerGCDryRunIgnoresBudget(t *testing.T) {
	f := newFixture(t)
	sp, spoolDir := f.newSpool(t)
	var spools []string
	for _, name := range []string{"a", "b", "c"} {
		spools = append(spools, plant(t, spoolDir, name+".spool", name))
	}
	ageFiles(t, spoolDir, 2*time.Hour)

	res, body := gcStep(t, f.handler(WithSpool(sp)), "dry_run=true&grace=0&budget=1ns")
	if !res.Done || res.Spools == nil || res.Spools.SweptSpools != 3 || !res.Spools.Done {
		t.Fatalf("dry run: %s, want a finished pass reporting all three spools", body)
	}
	for _, p := range spools {
		if !fileExists(t, p) {
			t.Errorf("dry run removed %s", p)
		}
	}
}

// A spool or index directory the walk cannot read fails that pass, listed in the step's errors, and the usage report.
func TestHandlerGCSpoolOrIndexWalkError(t *testing.T) {
	for _, broken := range []string{"spool", "mirror"} {
		t.Run(broken, func(t *testing.T) {
			f := newFixture(t)
			sp, spoolDir := f.newSpool(t)
			engine, indexDir := f.newEngine(t, sp)
			dir := map[string]string{"spool": spoolDir, "mirror": indexDir}[broken]
			// A regular file where the directory belongs fails ReadDir with an error other than ErrNotExist.
			if err := os.Remove(dir); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(dir, []byte("not a directory"), 0o644); err != nil {
				t.Fatal(err)
			}
			h := f.handler(WithSpool(sp), WithMirror(engine))
			prefix := map[string]string{"spool": "spools: ", "mirror": "mirror: "}[broken]
			res, body := gcStep(t, h, "grace=0")
			if res.Done || len(res.Errors) != 1 || !strings.HasPrefix(res.Errors[0], prefix) || res.Storage == nil || !res.Storage.Done || res.Storage.RemainingShards != 0 {
				t.Errorf("step: %s; want an unfinished step listing one %q error after a finished storage pass", body, prefix)
			}
			rec := do(h, http.MethodGet, "/internal/usage")
			if rec.Code != http.StatusInternalServerError || !strings.HasPrefix(rec.Body.String(), "Usage failed: "+broken+" usage: ") {
				t.Errorf("usage: %d %q; want 500 with the %s usage error", rec.Code, rec.Body, broken)
			}
		})
	}
}

func TestHandlerUsageSpoolAndMirror(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	sp, spoolDir := f.newSpool(t)
	engine, indexDir := f.newEngine(t, sp)
	const content = "spooled usage"
	plant(t, spoolDir, "usage.spool", content)
	plant(t, spoolDir, "notaspool.txt", content)
	plantIndexTemp(t, indexDir)
	f.commitPointer(t, "org/a", f.put(t, "usage-x "))
	index, err := engine.Usage(ctx)
	if err != nil || index.Count != 1 {
		t.Fatalf("engine usage = %+v, %v; want the one index file", index, err)
	}
	xet, err := f.xs.Usage(ctx)
	if err != nil || xet.Xorbs.Count == 0 || xet.SHA256Index.Count == 0 {
		t.Fatalf("xet usage = %+v, %v; want stored xorbs and sha256 entries", xet, err)
	}
	git, err := gc.NewGC(f.st.RepositoriesFS(), xetstore.New(f.xs)).Usage(ctx)
	if err != nil || git.Objects.Count == 0 {
		t.Fatalf("repository usage = %+v, %v; want the committed objects", git, err)
	}

	rec := do(f.handler(WithSpool(sp), WithMirror(engine)), http.MethodGet, "/internal/usage")
	t.Logf("usage JSON: %s", rec.Body)
	want := usageResponse{Repository: git, Xet: xet, Spool: xetstorage.ObjectUsage{Count: 1, Bytes: int64(len(content))}, Mirror: index}
	var got usageResponse
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &got) != nil || got != want {
		t.Fatalf("usage %d %s; want %+v", rec.Code, rec.Body, want)
	}
	for _, key := range []string{
		fmt.Sprintf(`"Repository":{"Objects":{"Count":%d,`, git.Objects.Count),
		fmt.Sprintf(`"Spool":{"Count":1,"Bytes":%d}`, len(content)),
		fmt.Sprintf(`"Mirror":{"Count":1,"Bytes":%d}`, index.Bytes),
	} {
		if !strings.Contains(rec.Body.String(), key) {
			t.Errorf("usage body %s: missing %s", rec.Body, key)
		}
	}
}

// getUsage decodes GET /internal/usage, requiring 200.
func getUsage(t *testing.T, h *Handler) usageResponse {
	t.Helper()
	rec := do(h, http.MethodGet, "/internal/usage")
	var got usageResponse
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &got) != nil {
		t.Fatalf("usage %d %s; want 200 JSON", rec.Code, rec.Body)
	}
	return got
}

// TestHandlerUsageStore pins the xet part of the report: it is read on every request, even without a repositories root,
// and a failing store fails the whole request whether or not there are repositories to walk.
func TestHandlerUsageStore(t *testing.T) {
	ctx := context.Background()
	t.Run("MissingRootReportsXet", func(t *testing.T) {
		f := newFixture(t)
		f.put(t, "usage-x ")
		xet, err := f.xs.Usage(ctx)
		if err != nil || xet.Xorbs.Count == 0 {
			t.Fatalf("xet usage = %+v, %v; want the stored object", xet, err)
		}
		if got := getUsage(t, f.handler()); got != (usageResponse{Xet: xet}) {
			t.Fatalf("usage without repositories = %+v; want only the xet section %+v", got, xet)
		}
	})
	t.Run("RefreshesAfterUpload", func(t *testing.T) {
		f := newFixture(t)
		f.commitPointer(t, "org/a", f.put(t, "usage-x "))
		h := f.handler()
		before := getUsage(t, h)
		f.put(t, "usage-y ")
		xet, err := f.xs.Usage(ctx)
		if err != nil || xet == before.Xet {
			t.Fatalf("second upload left xet usage at %+v (%v)", xet, err)
		}
		if after := getUsage(t, h); after != (usageResponse{Repository: before.Repository, Xet: xet}) {
			t.Fatalf("usage after an xet-only upload = %+v; want the repositories unchanged and xet at %+v", after, xet)
		}
	})
	t.Run("StoreErrorAborts", func(t *testing.T) {
		for _, populated := range []bool{false, true} {
			f := newFixture(t)
			if populated {
				f.commitPointer(t, "org/a", f.put(t, "usage-h "))
			}
			store := &usageStore{Storage: f.xs, err: errors.New("injected xet usage failure")}
			h := NewHandler(WithGC(gc.NewGC(f.st.RepositoriesFS(), xetstore.New(store))), WithStorage(store))
			rec := do(h, http.MethodGet, "/internal/usage")
			if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "xet usage: injected xet usage failure") || strings.Contains(rec.Body.String(), `"Repository"`) {
				t.Errorf("populated=%t: usage %d %q; want 500 with the store error and no partial report", populated, rec.Code, rec.Body)
			}
		}
	})
}
