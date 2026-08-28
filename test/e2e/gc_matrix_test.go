package e2e_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"testing"
	"time"

	xetinternalapi "github.com/wzshiming/xet/server/internalapi"
	xetstorage "github.com/wzshiming/xet/storage"

	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
)

// gcFileEntry carries the storage.FileListEntry fields the test asserts on.
type gcFileEntry struct {
	SHA256       string   `json:"sha256"`
	FileHashes   []string `json:"file_hashes"`
	OriginalSize uint64   `json:"original_size"`
}

// gcSweepResult carries the storage.SweepResult fields the test asserts on.
type gcSweepResult struct {
	SweptShards []json.RawMessage `json:"swept_shards"`
	SweptXorbs  []json.RawMessage `json:"swept_xorbs"`
	Done        bool              `json:"done"`
}

// TestGCLifecycle drives the /internal/ management API end to end over the
// assembled hfd chain: a pull-through mirror ingests an LFS object from an
// upstream hfd server, /internal/files lists it, an anchored sweep leaves it
// alone, unlinking both anchors lets the next sweep reclaim the bytes, and
// the next resolve self-heals by re-ingesting from upstream. The internal
// API wraps the chain outermost with the same options as cmd/hfd's
// wrapInternalAPI. Library-level GC semantics stay covered upstream in xet;
// this test pins the hfd wiring. TestMain runs it under local and S3
// storage; both xet storages implement GCStore, so the GC endpoints never
// answer 501.
func TestGCLifecycle(t *testing.T) {
	if _, err := exec.LookPath("git-lfs"); err != nil {
		t.Skip("git-lfs not available, skipping GC lifecycle test")
	}

	const repoID = "gc-org/gc-repo"
	upstream := newE2EServer(t)
	upstream.createRepo(t, "gc-org", "gc-repo")
	data := makeBinaryData(128*1024, 77)
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])
	pushViaXetBatch(t, upstream, repoID, data)

	// Pull-through proxy of upstream, assembled like newE2EServer except the
	// xet engine ingests from the real upstream (the harness points it at an
	// always-404 server), so resolve can re-ingest after GC.
	dataDir := newDataDir(t, "e2e-gc-proxy")
	st := newTestStorage(t, dataDir)
	proxyMirror, xet := newTestMirror(t, dataDir, upstream.httpURL, testS3Client != nil,
		mirror.WithRepositoriesFS(st.RepositoriesFS()),
		mirror.WithMirrorSourceFunc(newMirrorSourceFunc(upstream.httpURL)),
	)
	preOpen := newMirrorPreOpenHook(proxyMirror)
	var handler http.Handler
	handler = backendhf.NewHandler(
		backendhf.WithStorage(st),
		backendhf.WithMirror(proxyMirror),
		backendhf.WithNext(xet.tail),
		backendhf.WithPreOpenHookFunc(preOpen),
	)
	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(st),
		backendlfs.WithNext(handler),
		backendlfs.WithMirror(proxyMirror),
	)
	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(st),
		backendhttp.WithNext(handler),
		backendhttp.WithMirror(proxyMirror),
		backendhttp.WithPreOpenHookFunc(preOpen),
	)
	// The wiring under test: the internal management API wraps the whole
	// chain outermost, with the options cmd/hfd's wrapInternalAPI uses.
	handler = xetinternalapi.NewHandler(
		xetinternalapi.WithStorage(xet.xs),
		xetinternalapi.WithGCGrace(1*time.Hour),
		xetinternalapi.WithGCAnchor(xetstorage.AnchorBoth),
		xetinternalapi.WithNext(handler),
	)
	proxy := httptest.NewServer(handler)
	t.Cleanup(proxy.Close)

	resolveURL := proxy.URL + "/" + repoID + "/resolve/main/" + transferMatrixFile

	// step runs the lifecycle stages in order and stops at the first failure,
	// since every stage depends on the previous one's state.
	step := func(name string, fn func(t *testing.T)) {
		t.Helper()
		if !t.Run(name, fn) {
			t.FailNow()
		}
	}

	mustGet := func(t *testing.T, url string) []byte {
		t.Helper()
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("GET %s: %v", url, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read GET %s body: %v", url, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200 (body %q)", url, resp.StatusCode, body)
		}
		return body
	}

	listFiles := func(t *testing.T) []gcFileEntry {
		t.Helper()
		var entries []gcFileEntry
		if err := json.Unmarshal(mustGet(t, proxy.URL+"/internal/files"), &entries); err != nil {
			t.Fatalf("decode /internal/files: %v", err)
		}
		return entries
	}

	findEntry := func(entries []gcFileEntry) *gcFileEntry {
		for i := range entries {
			if entries[i].SHA256 == oid {
				return &entries[i]
			}
		}
		return nil
	}

	sweep := func(t *testing.T) gcSweepResult {
		t.Helper()
		resp, err := http.Post(proxy.URL+"/internal/gc/sweep?grace=0", "application/json", nil)
		if err != nil {
			t.Fatalf("POST sweep: %v", err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read sweep body: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("sweep status = %d, want 200 (body %q)", resp.StatusCode, body)
		}
		var res gcSweepResult
		if err := json.Unmarshal(body, &res); err != nil {
			t.Fatalf("decode sweep result: %v", err)
		}
		return res
	}

	del := func(t *testing.T, path string) int {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, proxy.URL+path, nil)
		if err != nil {
			t.Fatalf("build DELETE %s: %v", path, err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("DELETE %s: %v", path, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}

	waitIngested := func(t *testing.T) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for !proxyMirror.HasObject(t.Context(), oid) {
			if time.Now().After(deadline) {
				t.Fatalf("object %s never fully ingested into the proxy xet storage", oid)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	countStored := func(t *testing.T) (shards, xorbs int) {
		t.Helper()
		gcs, ok := xet.xs.(xetstorage.GCStore)
		if !ok {
			t.Fatalf("xet storage %T does not implement GCStore", xet.xs)
		}
		if err := gcs.WalkShards(t.Context(), func(string, int64, time.Time) error { shards++; return nil }); err != nil {
			t.Fatalf("walk shards: %v", err)
		}
		if err := gcs.WalkXorbs(t.Context(), func(string, int64, time.Time) error { xorbs++; return nil }); err != nil {
			t.Fatalf("walk xorbs: %v", err)
		}
		return shards, xorbs
	}

	var fileHashes []string

	step("MirrorIngest", func(t *testing.T) {
		// The first resolve mirrors the repo via the pre-open hook and pulls
		// the bytes through the proxy — streaming, or via the bridge redirect
		// when the background prefetch wins the race; either way the object
		// must land fully ingested. Stream-while-ingest itself is pinned
		// deterministically by pkg/backend/lfs's gated test.
		if got := mustGet(t, resolveURL); !bytes.Equal(got, data) {
			t.Fatalf("resolve bytes mismatch: got %d bytes, want %d", len(got), len(data))
		}
		waitIngested(t)
	})

	step("ListFiles", func(t *testing.T) {
		entry := findEntry(listFiles(t))
		if entry == nil {
			t.Fatalf("GET /internal/files misses sha256 %s", oid)
		}
		if entry.OriginalSize != uint64(len(data)) {
			t.Fatalf("original_size = %d, want %d", entry.OriginalSize, len(data))
		}
		if len(entry.FileHashes) == 0 {
			t.Fatal("entry lists no xet file hashes")
		}
		fileHashes = entry.FileHashes
	})

	step("SweepAnchoredKeepsObject", func(t *testing.T) {
		res := sweep(t)
		if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
			t.Fatalf("anchored sweep reclaimed shards=%d xorbs=%d, want none", len(res.SweptShards), len(res.SweptXorbs))
		}
		if got := mustGet(t, proxy.URL+"/objects/"+oid); !bytes.Equal(got, data) {
			t.Fatalf("object download after anchored sweep mismatch: got %d bytes, want %d", len(got), len(data))
		}
	})

	step("UnlinkAnchors", func(t *testing.T) {
		for _, fh := range fileHashes {
			if code := del(t, "/internal/files/xet/"+fh); code != http.StatusOK {
				t.Fatalf("DELETE /internal/files/xet/%s status = %d, want 200", fh, code)
			}
		}
		if code := del(t, "/internal/files/sha256/"+oid); code != http.StatusOK {
			t.Fatalf("DELETE /internal/files/sha256/%s status = %d, want 200", oid, code)
		}
		if code := del(t, "/internal/files/xet/"+fileHashes[0]); code != http.StatusNotFound {
			t.Fatalf("second DELETE xet status = %d, want 404", code)
		}
		if code := del(t, "/internal/files/sha256/"+oid); code != http.StatusNotFound {
			t.Fatalf("second DELETE sha256 status = %d, want 404", code)
		}
	})

	step("SweepReclaims", func(t *testing.T) {
		res := sweep(t)
		if len(res.SweptShards) == 0 || len(res.SweptXorbs) == 0 {
			t.Fatalf("unanchored sweep reclaimed shards=%d xorbs=%d, want both non-empty", len(res.SweptShards), len(res.SweptXorbs))
		}
		if !res.Done {
			t.Fatal("unbounded sweep step did not finish the cycle")
		}
		// Storage-direct checks only: probing the HTTP paths here would
		// trigger the self-heal re-ingest that the next step covers.
		if proxyMirror.HasObject(t.Context(), oid) {
			t.Fatalf("object %s still resolvable in xet storage after sweep", oid)
		}
		if shards, xorbs := countStored(t); shards != 0 || xorbs != 0 {
			t.Fatalf("storage still holds %d shards and %d xorbs after sweep", shards, xorbs)
		}
	})

	step("ResolveSelfHeals", func(t *testing.T) {
		// The pull-scan OID index still knows the object, so resolve delegates
		// to the hub front end, which drops the stale ready entry and
		// re-ingests from upstream.
		if got := mustGet(t, resolveURL); !bytes.Equal(got, data) {
			t.Fatalf("resolve after GC bytes mismatch: got %d bytes, want %d", len(got), len(data))
		}
		waitIngested(t)
		if findEntry(listFiles(t)) == nil {
			t.Fatalf("GET /internal/files misses sha256 %s after re-ingest", oid)
		}
	})
}
