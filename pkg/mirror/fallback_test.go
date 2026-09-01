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
	"path/filepath"
	"strings"
	"testing"
	"time"

	xetclient "github.com/wzshiming/xet/client"
	xetmirror "github.com/wzshiming/xet/mirror"
	xetstorage "github.com/wzshiming/xet/storage"
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

	// Removed best-effort: the engine's ingest finalize can outlive the test
	// body, and strict t.TempDir cleanup races it.
	dataDir, err := os.MkdirTemp("", "mirror-xet-data")
	if err != nil {
		t.Fatalf("create xet data dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	client, err := xetclient.NewClient(xetclient.WithCacheDir(filepath.Join(dataDir, "chunks")))
	if err != nil {
		t.Fatalf("new xet client: %v", err)
	}
	xs, err := xetstorage.NewFileStorage(
		xetstorage.WithBasePath(filepath.Join(dataDir, "storage")),
	)
	if err != nil {
		t.Fatalf("new xet storage: %v", err)
	}
	engine, err := xetmirror.NewMirror(
		xetmirror.WithStorage(xs),
		xetmirror.WithUpstream(srv.URL),
		xetmirror.WithCacheDir(filepath.Join(dataDir, "mirror")),
		xetmirror.WithClient(client),
	)
	if err != nil {
		t.Fatalf("new xet mirror engine: %v", err)
	}
	m, err := NewMirror(
		WithXETStorage(xs),
		WithXETClient(client),
		WithXETMirror(engine),
		WithDataDir(dataDir),
	)
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}
	t.Cleanup(m.Wait)

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
