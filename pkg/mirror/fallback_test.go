package mirror

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	xetclient "github.com/wzshiming/xet/client"
	xetmirror "github.com/wzshiming/xet/mirror"

	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// TestPrefetchFallsBackToLFSBatch covers sources without the hub resolve API:
// the xet ingest fails, and the object arrives through the git-lfs batch API
// into the LFS storage.
func TestPrefetchFallsBackToLFSBatch(t *testing.T) {
	data := bytes.Repeat([]byte("lfs batch fallback bytes. "), 2048)
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])

	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	// No /resolve/ routes: the mirror ingest gets a 404 from this upstream.
	mux.HandleFunc("POST /org/repo.git/info/lfs/objects/batch", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"transfer": "basic",
			"objects": []map[string]any{{
				"oid":  oid,
				"size": len(data),
				"actions": map[string]any{
					"download": map[string]any{"href": srv.URL + "/data/" + oid},
				},
			}},
		})
	})
	mux.HandleFunc("GET /data/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		_, _ = w.Write(data)
	})

	lfsSrv := srv // batch and content live on the same server
	m := newFallbackMirror(t, srv.URL)

	commit := strings.Repeat("a", 40)
	targets := map[string]resolveTarget{
		oid: {repoName: "org/repo", commit: commit, path: "model.bin", size: int64(len(data))},
	}
	m.prefetchLFS(lfsSrv.URL+"/org/repo", []string{oid}, targets)

	ctx := context.Background()
	deadline := time.Now().Add(30 * time.Second)
	for !m.HasObject(ctx, oid) {
		if time.Now().After(deadline) {
			t.Fatal("object never arrived via the LFS batch fallback")
		}
		time.Sleep(20 * time.Millisecond)
	}

	rs, size, err := m.OpenObject(ctx, oid)
	if err != nil {
		t.Fatalf("open fallback object: %v", err)
	}
	defer rs.Close()
	if size != int64(len(data)) {
		t.Fatalf("size = %d, want %d", size, len(data))
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rs); err != nil {
		t.Fatalf("read fallback object: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatal("fallback object bytes mismatch")
	}
}

// TestPrefetchConcurrency pins how many scanned objects one pull prefetches at
// once. The source holds each download until that many are in flight, so a
// prefetch running more at once, or fewer, shows in the peak.
func TestPrefetchConcurrency(t *testing.T) {
	const objects = 5
	cases := []struct {
		concurrency, wantPeak int
	}{
		{concurrency: 0, wantPeak: 1},
		{concurrency: 1, wantPeak: 1},
		{concurrency: 3, wantPeak: 3},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("concurrency=%d", tc.concurrency), func(t *testing.T) {
			blobs := map[string][]byte{}
			var oids []string
			targets := map[string]resolveTarget{}
			for i := range objects {
				data := bytes.Repeat([]byte(fmt.Sprintf("prefetch object %d. ", i)), 2048)
				sum := sha256.Sum256(data)
				oid := hex.EncodeToString(sum[:])
				blobs[oid] = data
				oids = append(oids, oid)
				targets[oid] = resolveTarget{repoName: "org/repo", commit: strings.Repeat("a", 40), path: fmt.Sprintf("model-%d.bin", i), size: int64(len(data))}
			}

			var mu sync.Mutex
			var started, inflight, peak int
			mux := http.NewServeMux()
			srv := httptest.NewServer(mux)
			t.Cleanup(srv.Close)
			// No /resolve/ routes: every ingest falls back to the git-lfs batch API.
			mux.HandleFunc("POST /org/repo.git/info/lfs/objects/batch", func(w http.ResponseWriter, r *http.Request) {
				var req struct {
					Objects []struct {
						Oid string `json:"oid"`
					} `json:"objects"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				var objs []map[string]any
				for _, o := range req.Objects {
					objs = append(objs, map[string]any{
						"oid":     o.Oid,
						"size":    len(blobs[o.Oid]),
						"actions": map[string]any{"download": map[string]any{"href": srv.URL + "/data/" + o.Oid}},
					})
				}
				w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
				_ = json.NewEncoder(w).Encode(map[string]any{"transfer": "basic", "objects": objs})
			})
			mux.HandleFunc("GET /data/{oid}", func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				started++
				inflight++
				peak = max(peak, inflight)
				mu.Unlock()
				defer func() {
					mu.Lock()
					inflight--
					mu.Unlock()
				}()
				for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
					mu.Lock()
					saturated := inflight >= tc.wantPeak || started == objects
					mu.Unlock()
					if saturated {
						break
					}
				}
				data := blobs[r.PathValue("oid")]
				w.Header().Set("Content-Length", fmt.Sprint(len(data)))
				_, _ = w.Write(data)
			})

			m := newFallbackMirror(t, srv.URL, WithPrefetchConcurrency(tc.concurrency))
			m.prefetchLFS(srv.URL+"/org/repo", oids, targets)
			m.Wait()

			for _, oid := range oids {
				if !m.HasObject(context.Background(), oid) {
					t.Errorf("object %s was not prefetched", oid)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if peak != tc.wantPeak {
				t.Errorf("peak concurrent source downloads = %d, want %d", peak, tc.wantPeak)
			}
		})
	}
}

// newFallbackMirror builds a pull mirror of sourceURL over fresh xet storage.
func newFallbackMirror(t *testing.T, sourceURL string, opts ...Option) *Mirror {
	t.Helper()
	// Removed best-effort: the engine's ingest finalize can outlive the test
	// body, and strict t.TempDir cleanup races it.
	dataDir, err := os.MkdirTemp("", "mirror-xet-data")
	if err != nil {
		t.Fatalf("create xet data dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	st, err := storage.NewStorage(storage.WithRootDir(dataDir))
	if err != nil {
		t.Fatalf("new storage: %v", err)
	}
	xs := st.XETStorage()
	sp := newTestSpool(t, st)
	engine, err := xetmirror.NewMirror(
		xetmirror.WithStorage(xs),
		xetmirror.WithCacheDir(st.XETMirrorDir()),
		xetmirror.WithSpool(sp),
	)
	if err != nil {
		t.Fatalf("new xet mirror engine: %v", err)
	}
	m, err := NewMirror(append([]Option{
		WithXETStorage(xs),
		WithXETCache(xetclient.NewCache(st.XETMirrorDir(), 0, 0)),
		WithXETMirror(engine),
		WithMirrorSourceFunc(func(ctx context.Context, repoName string) (string, bool, error) {
			return sourceURL + "/" + repoName, true, nil
		}),
		WithDataDir(st.XETDir()),
	}, opts...)...)
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}
	t.Cleanup(m.Wait)
	return m
}
