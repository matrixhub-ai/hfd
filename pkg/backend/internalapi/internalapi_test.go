package internalapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/gc"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

func newStorage(t *testing.T) *xetstorage.FileStorage {
	t.Helper()
	xs, err := xetstorage.NewFileStorage(xetstorage.WithBasePath(filepath.Join(t.TempDir(), "xet")))
	if err != nil {
		t.Fatalf("new xet storage: %v", err)
	}
	return xs
}

func newHandler(t *testing.T, store xetstorage.GCStore) *Handler {
	t.Helper()
	repos := storage.NewStorage(storage.WithRootDir(t.TempDir())).RepositoriesFS()
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	return NewHandler(WithCollector(gc.NewCollector(repos, store)), WithGCGrace(-1), WithNext(next))
}

func do(h http.Handler, method, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec
}

func TestHandler(t *testing.T) {
	h := newHandler(t, newStorage(t))
	for _, tc := range []struct {
		method, target string
		want           int
	}{
		{http.MethodPost, "/internal/gc?grace=bogus", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc?grace=-5s", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc?grace=", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc?dry_run=maybe", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc?dry_run=", http.StatusBadRequest},
		{http.MethodGet, "/other", http.StatusTeapot},
	} {
		if rec := do(h, tc.method, tc.target); rec.Code != tc.want {
			t.Errorf("%s %s: got %d, want %d", tc.method, tc.target, rec.Code, tc.want)
		}
	}

	rec := do(h, http.MethodPost, "/internal/gc?dry_run=true&grace=0")
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("dry run: status %d, content-type %q, body %s", rec.Code, rec.Header().Get("Content-Type"), rec.Body)
	}
	if !strings.Contains(rec.Body.String(), `"unlinked":[]`) {
		t.Fatalf("empty unlinked must encode as []: %s", rec.Body)
	}
	var res gc.Result
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil || !res.DryRun || res.Sweep != nil {
		t.Fatalf("dry run body %s: err=%v result=%+v", rec.Body, err, res)
	}
	if rec := do(h, http.MethodPost, "/internal/gc"); rec.Code != http.StatusOK {
		t.Fatalf("collect: got %d, body %s", rec.Code, rec.Body)
	}
}

// blockingStore parks the first shard walk until released so a concurrent request observes the busy collector; the sweep's later walks pass through.
type blockingStore struct {
	*xetstorage.FileStorage
	once           sync.Once
	enter, release chan struct{}
}

func (b *blockingStore) WalkShards(ctx context.Context, fn func(string, int64, time.Time) error) error {
	b.once.Do(func() { b.enter <- struct{}{}; <-b.release })
	return b.FileStorage.WalkShards(ctx, fn)
}

func TestHandlerBusy(t *testing.T) {
	store := &blockingStore{FileStorage: newStorage(t), enter: make(chan struct{}), release: make(chan struct{})}
	h := newHandler(t, store)
	first := make(chan int, 1)
	go func() { first <- do(h, http.MethodPost, "/internal/gc").Code }()
	<-store.enter
	if rec := do(h, http.MethodPost, "/internal/gc"); rec.Code != http.StatusConflict {
		t.Fatalf("busy: got %d, want 409", rec.Code)
	}
	close(store.release)
	if code := <-first; code != http.StatusOK {
		t.Fatalf("first collect: got %d, want 200", code)
	}
}
