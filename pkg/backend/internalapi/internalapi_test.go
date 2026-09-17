package internalapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
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
		{http.MethodPost, "/internal/gc/prune?grace=bogus", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc/prune?grace=-5s", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc/prune?grace=", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc/prune?dry_run=maybe", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc/prune?dry_run=", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc/prune?dry_run=true;x=1", http.StatusBadRequest}, // r.URL.Query() would drop dry_run and prune for real
		{http.MethodPost, "/internal/gc/sweep?dry_run=true;x=1", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc/sweep?grace=-1s", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc/sweep?max=-1", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc/sweep?budget=nope", http.StatusBadRequest},
		{http.MethodPost, "/internal/gc", http.StatusTeapot},
		{http.MethodDelete, "/internal/objects/" + deadSHA, http.StatusTeapot},
		{http.MethodGet, "/internal/objects", http.StatusOK},
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

	rec = do(h, http.MethodPost, "/internal/gc/prune?dry_run=true&grace=0")
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("dry run: status %d, content-type %q, body %s", rec.Code, rec.Header().Get("Content-Type"), rec.Body)
	}
	if !strings.Contains(rec.Body.String(), `"unlinked":[]`) {
		t.Fatalf("empty unlinked must encode as []: %s", rec.Body)
	}
	if strings.Contains(rec.Body.String(), `"sweep"`) {
		t.Fatalf("prune body must not contain sweep: %s", rec.Body)
	}
	var res gc.PruneResult
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil || !res.DryRun {
		t.Fatalf("dry run body %s: err=%v result=%+v", rec.Body, err, res)
	}
	rec = do(h, http.MethodPost, "/internal/gc/prune?grace=0")
	if rec.Code != http.StatusOK {
		t.Fatalf("prune: got %d, body %s", rec.Code, rec.Body)
	}
	var prune gc.PruneResult
	if err := json.Unmarshal(rec.Body.Bytes(), &prune); err != nil || prune.DryRun {
		t.Fatalf("prune body %s: err=%v result=%+v", rec.Body, err, prune)
	}

	rec = do(h, http.MethodPost, "/internal/gc/sweep?grace=0&max=1&budget=1s&dry_run=false")
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("sweep: status %d, content-type %q, body %s", rec.Code, rec.Header().Get("Content-Type"), rec.Body)
	}
	var sweep gc.SweepResult
	if err := json.Unmarshal(rec.Body.Bytes(), &sweep); err != nil || !sweep.Done || sweep.DryRun {
		t.Fatalf("sweep body %s: err=%v result=%+v", rec.Body, err, sweep)
	}
	for _, field := range []string{`"swept_git_objects":0`, `"remaining_git_objects":0`} {
		if !strings.Contains(rec.Body.String(), field) {
			t.Fatalf("sweep body must report %s: %s", field, rec.Body)
		}
	}
}

// blockingStore parks the first shard walk until released; later walks pass through.
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
	for _, endpoint := range []string{"prune", "sweep"} {
		t.Run(endpoint, func(t *testing.T) {
			store := &blockingStore{FileStorage: newStorage(t), enter: make(chan struct{}), release: make(chan struct{})}
			h := newHandler(t, store)
			first := make(chan int, 1)
			go func() { first <- do(h, http.MethodPost, "/internal/gc/"+endpoint).Code }()
			<-store.enter
			if rec := do(h, http.MethodPost, "/internal/gc/prune"); rec.Code != http.StatusConflict {
				t.Errorf("prune during %s: got %d, want 409", endpoint, rec.Code)
			}
			if endpoint == "prune" {
				if rec := do(h, http.MethodPost, "/internal/gc/sweep"); rec.Code != http.StatusConflict {
					t.Errorf("sweep during prune: got %d, want 409", rec.Code)
				}
			}
			close(store.release)
			if code := <-first; code != http.StatusOK {
				t.Fatalf("first %s: got %d, want 200", endpoint, code)
			}
		})
	}
}

// partialStore accepts the first dead entry's unlink and fails the second.
type partialStore struct {
	*xetstorage.FileStorage
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
	h := newHandler(t, &partialStore{FileStorage: newStorage(t)})
	rec := do(h, http.MethodPost, "/internal/gc/prune")
	if rec.Code != http.StatusInternalServerError || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("status %d, content-type %q, body %s", rec.Code, rec.Header().Get("Content-Type"), rec.Body)
	}
	var res gc.PruneResult
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode %s: %v", rec.Body, err)
	}
	if !slices.Equal(res.Unlinked, []string{deadSHA}) || !strings.Contains(res.Error, "index delete failed") {
		t.Fatalf("failure body must list the applied unlinks and the error: %+v", res)
	}
}
