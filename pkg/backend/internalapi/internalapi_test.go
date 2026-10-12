package internalapi

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/util"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/gc"
	"github.com/matrixhub-ai/hfd/pkg/gc/xetstore"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

func newStorage(t *testing.T) xetstorage.Storage {
	t.Helper()
	st, err := storage.NewStorage(storage.WithRootDir(t.TempDir()))
	if err != nil {
		t.Fatalf("new storage: %v", err)
	}
	return st.XETStorage()
}

func newRepos(t *testing.T) billy.Filesystem {
	t.Helper()
	st, err := storage.NewStorage(storage.WithRootDir(t.TempDir()))
	if err != nil {
		t.Fatalf("new storage: %v", err)
	}
	return st.RepositoriesFS()
}

func newHandler(t *testing.T, repos billy.Filesystem, store xetstorage.Storage) *Handler {
	t.Helper()
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	return NewHandler(WithGC(gc.NewGC(repos, xetstore.New(store))), WithStorage(store), WithGCGrace(-1), WithNext(next))
}

// orphanPointer creates name with a live pointer on main and an orphaned pointer commit of four objects: commit, tree, pointer blob and 64 KiB filler.
func orphanPointer(t *testing.T, repos billy.Filesystem, name string) {
	t.Helper()
	ctx := context.Background()
	repo, err := repository.Init(ctx, repos, name, "main")
	if err != nil {
		t.Fatalf("init %s: %v", name, err)
	}
	pointer := func(oid string) []byte {
		return fmt.Appendf(nil, "version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize 1\n", oid)
	}
	filler := make([]byte, 64<<10)
	if _, err := rand.Read(filler); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateCommit(ctx, "main", "live", "Test", "test@test.com",
		[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: "model.bin", Content: pointer(strings.Repeat("a", 64))}}, ""); err != nil {
		t.Fatalf("commit %s: %v", name, err)
	}
	if _, err := repo.CreateCommit(ctx, "orphan", "orphan", "Test", "test@test.com", []repository.CommitOperation{
		{Type: repository.CommitOperationAdd, Path: "dead.bin", Content: pointer(deadSHA)},
		{Type: repository.CommitOperationAdd, Path: "filler.bin", Content: filler},
	}, ""); err != nil {
		t.Fatalf("commit orphan in %s: %v", name, err)
	}
	if err := repo.DeleteBranch("orphan"); err != nil {
		t.Fatalf("delete orphan branch in %s: %v", name, err)
	}
}

func do(h http.Handler, method, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec
}

func TestHandler(t *testing.T) {
	h := newHandler(t, newRepos(t), newStorage(t))
	for _, tc := range []struct {
		method, target string
		want           int
	}{
		{http.MethodPost, "/internal/gc?grace=bogus", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc?grace=-5s", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc?grace=", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc?dry_run=maybe", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc?dry_run=", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc?dry_run=true;x=1", http.StatusBadRequest}, // r.URL.Query() would drop dry_run and collect for real
		{http.MethodPost, "/internal/gc?grace=-1s", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc?max=-1", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc?max=x", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc?budget=nope", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc?budget=-1s", http.StatusBadRequest},
		{http.MethodGet, "/internal/gc", http.StatusMethodNotAllowed},
		{http.MethodPost, "/internal/gc/prune", http.StatusTeapot},
		{http.MethodPost, "/internal/gc/sweep", http.StatusTeapot},
		{http.MethodDelete, "/internal/objects/" + deadSHA, http.StatusTeapot},
		{http.MethodGet, "/internal/objects", http.StatusOK},
		{http.MethodPost, "/internal/usage", http.StatusMethodNotAllowed},
		{http.MethodGet, "/internal/usage", http.StatusOK},
		{http.MethodGet, "/other", http.StatusTeapot},
	} {
		if rec := do(h, tc.method, tc.target); rec.Code != tc.want {
			t.Errorf("%s %s: got %d, want %d", tc.method, tc.target, rec.Code, tc.want)
		}
	}

	rec := do(h, http.MethodGet, "/internal/objects")
	if rec.Header().Get("Content-Type") != "application/json" || strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Fatalf("empty list: content-type %q, body %q", rec.Header().Get("Content-Type"), rec.Body)
	}

	rec = do(h, http.MethodPost, "/internal/gc?dry_run=true&grace=0")
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("dry run: status %d, content-type %q, body %s", rec.Code, rec.Header().Get("Content-Type"), rec.Body)
	}
	if !strings.Contains(rec.Body.String(), `"unlinked":[]`) {
		t.Fatalf("empty unlinked must encode as []: %s", rec.Body)
	}
	if strings.Contains(rec.Body.String(), `"errors"`) {
		t.Fatalf("a clean step must not list errors: %s", rec.Body)
	}
	var res gcResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil || !res.DryRun || !res.Done || res.Prune == nil || !res.Prune.DryRun || res.Storage == nil || !res.Storage.DryRun {
		t.Fatalf("dry run body %s: err=%v result=%+v", rec.Body, err, res)
	}

	rec = do(h, http.MethodPost, "/internal/gc?grace=0&max=1&budget=1s&dry_run=false")
	res = decodeResult(t, rec, http.StatusOK)
	if !res.Done || res.DryRun || res.Prune == nil || res.Prune.DryRun || res.Storage == nil || res.Storage.DryRun || len(res.Errors) != 0 {
		t.Fatalf("collect body %s: result=%+v", rec.Body, res)
	}
}

// TestHandlerListObjects pins that GET /internal/objects lists the stored objects that still have a sha256 index entry.
func TestHandlerListObjects(t *testing.T) {
	f := newFixture(t)
	a, b := f.put(t, "list-a "), f.put(t, "list-b ")
	h := f.handler()
	list := func() []string {
		t.Helper()
		rec := do(h, http.MethodGet, "/internal/objects")
		var objects []object
		if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" || json.Unmarshal(rec.Body.Bytes(), &objects) != nil {
			t.Fatalf("list: status %d, content-type %q, body %s; want 200 JSON", rec.Code, rec.Header().Get("Content-Type"), rec.Body)
		}
		oids := []string{}
		for _, o := range objects {
			if o.Size != objectSize {
				t.Fatalf("list body %s: object %s has size %d, want %d", rec.Body, o.OID, o.Size, objectSize)
			}
			oids = append(oids, o.OID)
		}
		slices.Sort(oids)
		return oids
	}
	want := []string{a, b}
	slices.Sort(want)
	if got := list(); !slices.Equal(got, want) {
		t.Fatalf("list = %v, want %v", got, want)
	}
	if removed, err := f.xs.DeleteSHA256IndexEntry(context.Background(), a); err != nil || !removed {
		t.Fatalf("delete the sha256 index entry of %s = %t, %v", a, removed, err)
	}
	if got := list(); !slices.Equal(got, []string{b}) {
		t.Fatalf("list after unindexing %s = %v, want only %s", a, got, b)
	}
}

// blockingStore parks the first xorb walk until released and counts the shard walks; prune walks shards, only the storage sweep walks xorbs.
type blockingStore struct {
	xetstorage.Storage
	once           sync.Once
	enter, release chan struct{}
	shardWalks     atomic.Int32
}

func (b *blockingStore) WalkShards(ctx context.Context, fn func(string, int64, time.Time) error) error {
	b.shardWalks.Add(1)
	return b.Storage.WalkShards(ctx, fn)
}

func (b *blockingStore) WalkXorbs(ctx context.Context, fn func(string, int64, time.Time) error) error {
	b.once.Do(func() { b.enter <- struct{}{}; <-b.release })
	return b.Storage.WalkXorbs(ctx, fn)
}

// TestHandlerBusy pins that the handler serializes whole GC steps: a step parked in its storage sweep, past the prune pass
// and the GC's lock, still turns the next request away before it runs a prune of its own.
func TestHandlerBusy(t *testing.T) {
	store := &blockingStore{Storage: newStorage(t), enter: make(chan struct{}), release: make(chan struct{})}
	h := newHandler(t, newRepos(t), store)
	first := make(chan int, 1)
	go func() { first <- do(h, http.MethodPost, "/internal/gc").Code }()
	<-store.enter
	walks := store.shardWalks.Load()
	if walks == 0 {
		t.Fatal("the first step reached its storage sweep without a prune pass")
	}
	if rec := do(h, http.MethodPost, "/internal/gc"); rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "GC already running") {
		t.Errorf("GC during GC: got %d %q, want 409", rec.Code, rec.Body)
	}
	if got := store.shardWalks.Load(); got != walks {
		t.Errorf("rejected step walked the shards %d more times, want none: it must not prune", got-walks)
	}
	close(store.release)
	if code := <-first; code != http.StatusOK {
		t.Fatalf("first GC: got %d, want 200", code)
	}
}

// TestHandlerGCStorageBusyKeepsPrune pins that a busy storage GC is reported as a storage error after the prune pass ran,
// not as a 409 that would drop the prune section.
func TestHandlerGCStorageBusyKeepsPrune(t *testing.T) {
	store := &blockingStore{Storage: newStorage(t), enter: make(chan struct{}), release: make(chan struct{})}
	h := newHandler(t, newRepos(t), store)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(store.release) }) }
	t.Cleanup(release)
	direct := make(chan error, 1)
	go func() {
		_, err := h.sweeper.SweepStep(context.Background(), xetstorage.SweepOptions{Anchor: xetstorage.AnchorSHA256, Grace: -1})
		direct <- err
	}()
	<-store.enter
	walks := store.shardWalks.Load()

	rec := do(h, http.MethodPost, "/internal/gc")
	if rec.Code != http.StatusOK {
		t.Fatalf("GC during a busy storage sweep: got %d %q, want 200", rec.Code, rec.Body)
	}
	var res gcResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode %s: %v", rec.Body, err)
	}
	if res.Prune == nil || res.Storage != nil || !slices.Equal(res.Errors, []string{"storage: gc already running"}) || res.Done {
		t.Fatalf("body %s: want the prune section, no storage section and a storage busy error", rec.Body)
	}
	if got := store.shardWalks.Load(); got != walks+1 {
		t.Fatalf("body %s: shard walks went %d -> %d, want one prune pass", rec.Body, walks, got)
	}

	release()
	if err := <-direct; err != nil {
		t.Fatalf("direct sweep: %v", err)
	}
}

// parkingStore parks the first sha256 index walk until released and lets later walks through, so a request that wrongly
// proceeds past the busy GC fails fast instead of deadlocking behind the parked prune.
type parkingStore struct {
	xetstorage.Storage
	parked         atomic.Bool
	enter, release chan struct{}
	shardWalks     atomic.Int32
}

func (p *parkingStore) WalkShards(ctx context.Context, fn func(string, int64, time.Time) error) error {
	p.shardWalks.Add(1)
	return p.Storage.WalkShards(ctx, fn)
}

func (p *parkingStore) WalkSHA256Index(ctx context.Context, fn func(string, string) error) error {
	if p.parked.CompareAndSwap(false, true) {
		p.enter <- struct{}{}
		<-p.release
	}
	return p.Storage.WalkSHA256Index(ctx, fn)
}

// TestHandlerGCBusy pins that a prune running on the GC outside the handler's lock turns the request away with 409
// before it walks the store or sweeps.
func TestHandlerGCBusy(t *testing.T) {
	store := &parkingStore{Storage: newStorage(t), enter: make(chan struct{}), release: make(chan struct{})}
	h := newHandler(t, newRepos(t), store)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(store.release) }) }
	t.Cleanup(release)
	direct := make(chan error, 1)
	go func() {
		_, err := h.gc.Prune(context.Background(), gc.PruneOptions{Grace: -1})
		direct <- err
	}()
	<-store.enter
	walks := store.shardWalks.Load()

	if rec := do(h, http.MethodPost, "/internal/gc"); rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "GC already running") {
		t.Errorf("GC during a direct prune: got %d %q, want 409", rec.Code, rec.Body)
	}
	if got := store.shardWalks.Load(); got != walks {
		t.Errorf("rejected step walked the shards %d more times, want none", got-walks)
	}

	release()
	if err := <-direct; err != nil {
		t.Fatalf("direct prune: %v", err)
	}
}

// partialStore accepts the first dead entry's unlink and fails the second.
type partialStore struct {
	xetstorage.Storage
}

const deadSHA = "1111111111111111111111111111111111111111111111111111111111111111"
const deadSHA2 = "2222222222222222222222222222222222222222222222222222222222222222"

func (p *partialStore) WalkSHA256Index(_ context.Context, fn func(string, string) error) error {
	if err := fn(deadSHA, "missing-shard"); err != nil {
		return err
	}
	return fn(deadSHA2, "missing-shard")
}

func (p *partialStore) DeleteSHA256IndexEntry(_ context.Context, oid string) (bool, error) {
	if oid == deadSHA {
		return true, nil
	}
	return false, errors.New("index delete failed")
}

func TestHandlerReportsUnlinksOnFailure(t *testing.T) {
	h := newHandler(t, newRepos(t), &partialStore{Storage: newStorage(t)})
	rec := do(h, http.MethodPost, "/internal/gc")
	res := decodeResult(t, rec, http.StatusOK)
	if res.Done || len(res.Errors) == 0 || !strings.HasPrefix(res.Errors[0], "prune: ") || !strings.Contains(res.Errors[0], "index delete failed") {
		t.Fatalf("body %s: want an unfinished step listing the prune error first", rec.Body)
	}
	if res.Prune == nil || !slices.Equal(res.Prune.Unlinked, []string{deadSHA}) || res.Storage == nil {
		t.Fatalf("body %s: want the applied unlinks and the storage pass that still ran", rec.Body)
	}
}

// usageStore answers Usage with fixed values, records the contexts it was asked with, and runs onUsage first when set.
type usageStore struct {
	xetstorage.Storage
	usage   xetstorage.Usage
	err     error
	ctxs    []context.Context
	onUsage func()
}

func (s *usageStore) Usage(ctx context.Context) (xetstorage.Usage, error) {
	s.ctxs = append(s.ctxs, ctx)
	if s.onUsage != nil {
		s.onUsage()
	}
	return s.usage, s.err
}

func TestHandlerUsage(t *testing.T) {
	ctx := context.Background()
	const empty = `{"Repository":{"Objects":{"Count":0,"Bytes":0},"Other":{"Count":0,"Bytes":0}},"Xet":{"Xorbs":{"Count":0,"Bytes":0},"Shards":{"Count":0,"Bytes":0},"FileIndex":{"Count":0,"Bytes":0},"ChunkIndex":{"Count":0,"Bytes":0},"SHA256Index":{"Count":0,"Bytes":0}},"Spool":{"Count":0,"Bytes":0},"Mirror":{"Count":0,"Bytes":0}}`
	rec := do(newHandler(t, newRepos(t), newStorage(t)), http.MethodGet, "/internal/usage")
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" || strings.TrimSpace(rec.Body.String()) != empty {
		t.Fatalf("empty usage: status %d, content-type %q, body %s", rec.Code, rec.Header().Get("Content-Type"), rec.Body)
	}

	repos := newRepos(t)
	if _, err := repository.Init(ctx, repos, "/org/repo.git", "main"); err != nil {
		t.Fatalf("init: %v", err)
	}
	store := &usageStore{Storage: newStorage(t), usage: xetstorage.Usage{
		Xorbs:       xetstorage.ObjectUsage{Count: 1, Bytes: 10},
		Shards:      xetstorage.ObjectUsage{Count: 2, Bytes: 20},
		FileIndex:   xetstorage.ObjectUsage{Count: 3, Bytes: 30},
		ChunkIndex:  xetstorage.ObjectUsage{Count: 4, Bytes: 40},
		SHA256Index: xetstorage.ObjectUsage{Count: 5, Bytes: 50},
	}}
	g := gc.NewGC(repos, xetstore.New(store))
	gitUsage, err := g.Usage(ctx)
	if err != nil || gitUsage.Other.Count == 0 {
		t.Fatalf("fixture usage = %+v, %v; want git metadata", gitUsage, err)
	}
	want := usageResponse{Repository: gitUsage, Xet: store.usage}
	h := NewHandler(WithGC(g), WithStorage(store))
	get := func(ctx context.Context) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/usage", nil).WithContext(ctx))
		return rec
	}

	store.ctxs = nil
	reqCtx, cancel := context.WithCancel(ctx)
	rec = get(reqCtx)
	cancel()
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("usage: status %d, content-type %q, body %s", rec.Code, rec.Header().Get("Content-Type"), rec.Body)
	}
	var got usageResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || got != want {
		t.Fatalf("usage body %s: err=%v, want %+v", rec.Body, err, want)
	}
	if len(store.ctxs) != 1 || store.ctxs[0].Err() == nil {
		t.Fatalf("store saw %d Usage calls, want exactly one carrying the request context", len(store.ctxs))
	}
	if after, err := g.Usage(ctx); err != nil || after != gitUsage {
		t.Fatalf("usage after request = %+v, %v; want unchanged %+v", after, err, gitUsage)
	}

	store.err = errors.New("disk on fire")
	rec = get(ctx)
	if rec.Code != http.StatusInternalServerError || !strings.HasPrefix(rec.Body.String(), "Usage failed: xet usage: ") || !strings.Contains(rec.Body.String(), "disk on fire") {
		t.Fatalf("store failure: status %d, body %q", rec.Code, rec.Body)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	rec = get(canceled)
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), context.Canceled.Error()) {
		t.Fatalf("canceled request: status %d, body %q", rec.Code, rec.Body)
	}

	// A request canceled while the store answers fails after that success too, with or without repositories to walk.
	store.err = nil
	for _, populated := range []bool{false, true} {
		repos := newRepos(t)
		if populated {
			if _, err := repository.Init(ctx, repos, "/org/repo.git", "main"); err != nil {
				t.Fatalf("init: %v", err)
			}
		}
		mid, cancelMid := context.WithCancel(ctx)
		t.Cleanup(cancelMid)
		store.onUsage = cancelMid
		h := NewHandler(WithGC(gc.NewGC(repos, xetstore.New(store))), WithStorage(store))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/usage", nil).WithContext(mid))
		if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), context.Canceled.Error()) || strings.Contains(rec.Body.String(), `"Xet"`) {
			t.Fatalf("populated=%t, canceled during the store call: status %d, body %q; want 500 and no partial report", populated, rec.Code, rec.Body)
		}
	}
}

// decodeResult decodes a GC response body, requiring the status and JSON content type.
func decodeResult(t *testing.T, rec *httptest.ResponseRecorder, status int) gcResponse {
	t.Helper()
	if rec.Code != status || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("status %d, content-type %q, body %s; want %d JSON", rec.Code, rec.Header().Get("Content-Type"), rec.Body, status)
	}
	var res gcResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode %s: %v", rec.Body, err)
	}
	if res.Prune == nil {
		t.Fatalf("body %s: missing the prune section", rec.Body)
	}
	return res
}

// A GC preview reports the Git objects and bytes the run then deletes, with no disk saving measured, and deletes nothing itself.
func TestHandlerPruneGitGC(t *testing.T) {
	repos := newRepos(t)
	orphanPointer(t, repos, "/org/repo.git")
	h := newHandler(t, repos, newStorage(t))
	var preview *gc.PruneResult
	// A second identical preview proves the first deleted nothing.
	for i := range 2 {
		rec := do(h, http.MethodPost, "/internal/gc?dry_run=true&grace=0")
		preview = decodeResult(t, rec, http.StatusOK).Prune
		if !preview.DryRun || preview.Repositories != 1 || preview.DeletedGitObjects != 4 || preview.DeletedGitBytes <= 64<<10 || preview.ReclaimedBytes != 0 {
			t.Fatalf("preview %d: %+v; want 4 objects, more than the 64 KiB filler and no reclaimed bytes", i, preview)
		}
		if body := rec.Body.String(); !strings.Contains(body, `"deleted_git_objects":4`) || !strings.Contains(body, `"deleted_git_bytes":`) || !strings.Contains(body, `"reclaimed_bytes":`) || strings.Contains(body, `"failed"`) {
			t.Fatalf("preview body %s: want the Git fields and no failed map", body)
		}
	}
	res := decodeResult(t, do(h, http.MethodPost, "/internal/gc?grace=0"), http.StatusOK).Prune
	if res.DryRun || res.DeletedGitObjects != preview.DeletedGitObjects || res.DeletedGitBytes != preview.DeletedGitBytes || res.ReclaimedBytes <= 0 {
		t.Fatalf("prune %+v, want the preview's %d objects and %d bytes", res, preview.DeletedGitObjects, preview.DeletedGitBytes)
	}
	if after := decodeResult(t, do(h, http.MethodPost, "/internal/gc?dry_run=true&grace=0"), http.StatusOK).Prune; after.DeletedGitObjects != 0 || after.DeletedGitBytes != 0 {
		t.Fatalf("preview after prune %+v, want nothing left to delete", after)
	}
}

// A repository whose GC fails answers 200 with the partial prune section: the failure, the other repositories' stats and the error in errors.
func TestHandlerReportsGitGCFailure(t *testing.T) {
	repos := newRepos(t)
	orphanPointer(t, repos, "/org/repo.git")
	orphanPointer(t, repos, "/org/broken.git")
	if err := util.WriteFile(repos, "/org/broken.git/refs/heads/dangling", []byte(strings.Repeat("1", 40)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	h := newHandler(t, repos, newStorage(t))
	rec := do(h, http.MethodPost, "/internal/gc?dry_run=true&grace=0")
	res := decodeResult(t, rec, http.StatusOK)
	if res.Done || len(res.Errors) != 1 || !strings.Contains(res.Errors[0], "prune: git gc failed in 1 repositories") || res.Storage == nil {
		t.Fatalf("body %s: want an unfinished step listing the prune error and the storage pass", rec.Body)
	}
	if p := res.Prune; p.Failed["/org/broken.git"] == "" || len(p.Failed) != 1 || p.Repositories != 2 || p.DeletedGitObjects != 4 || p.DeletedGitBytes <= 0 {
		t.Fatalf("failure body must carry the failed repository and the healthy repository's stats: %s", rec.Body)
	}
}
