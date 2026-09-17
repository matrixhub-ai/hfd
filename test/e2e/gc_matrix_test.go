package e2e_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	xetstorage "github.com/wzshiming/xet/storage"

	backendcas "github.com/matrixhub-ai/hfd/pkg/backend/cas"
	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendinternalapi "github.com/matrixhub-ai/hfd/pkg/backend/internalapi"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	"github.com/matrixhub-ai/hfd/pkg/gc"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/permission"
)

// gcObject carries the gc.Object fields the test asserts on.
type gcObject struct {
	OID  string `json:"oid"`
	Size uint64 `json:"size"`
}

// gcSweepResult carries the gc.SweepResult fields the test asserts on.
type gcSweepResult struct {
	DryRun              bool  `json:"dry_run"`
	SweptShards         int   `json:"swept_shards"`
	SweptXorbs          int   `json:"swept_xorbs"`
	SweptGitObjects     int   `json:"swept_git_objects"`
	ReclaimedBytes      int64 `json:"reclaimed_bytes"`
	Done                bool  `json:"done"`
	RemainingShards     int   `json:"remaining_shards"`
	RemainingXorbs      int   `json:"remaining_xorbs"`
	RemainingGitObjects int   `json:"remaining_git_objects"`
}

// gcPruneResult carries the gc.PruneResult fields the test asserts on.
type gcPruneResult struct {
	DryRun         bool     `json:"dry_run"`
	Repositories   int      `json:"repositories"`
	LiveObjects    int      `json:"live_objects"`
	Unlinked       []string `json:"unlinked"`
	SkippedInGrace int      `json:"skipped_in_grace"`
}

// mustGet follows redirects and returns the 200 body, failing the test otherwise.
func mustGet(t *testing.T, url string) []byte {
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

// postSweep runs one unshielded sweep step against baseURL and returns the decoded 200 body, failing the test otherwise.
func postSweep(t *testing.T, baseURL string, query ...string) gcSweepResult {
	t.Helper()
	resp, err := http.Post(baseURL+"/internal/gc/sweep?grace=0&"+strings.Join(query, "&"), "application/json", nil)
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

// postPrune runs one prune with query against baseURL and returns the decoded 200 body, failing the test otherwise.
func postPrune(t *testing.T, baseURL, query string) gcPruneResult {
	t.Helper()
	resp, err := http.Post(baseURL+"/internal/gc/prune"+query, "application/json", nil)
	if err != nil {
		t.Fatalf("POST /internal/gc/prune%s: %v", query, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read prune body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("prune status = %d, want 200 (body %q)", resp.StatusCode, body)
	}
	var res gcPruneResult
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatalf("decode prune result: %v", err)
	}
	return res
}

// TestGCLifecycle drives the /internal/ management API end to end over the
// assembled hfd chain: a pull-through mirror ingests an LFS object from an
// upstream hfd server, /internal/objects lists it, and prune keeps the
// referenced object. Deleting the mirrored repo lets prune unlink the OID
// and sweep reclaim the bytes; the next resolve self-heals by mirroring
// the repo and re-ingesting from upstream. The internal API wraps the chain
// outermost with the same options as cmd/hfd's internalAPI. Library-level
// GC semantics stay covered upstream in xet; this test pins the hfd wiring.
// TestMain runs it under local and S3 storage; both xet storages implement
// GCStore, so the GC endpoints never answer 501.
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
		backendhf.WithNext(backendcas.NewHandler(
			backendcas.WithMirror(proxyMirror),
			backendcas.WithNext(http.NotFoundHandler()),
		)),
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
		backendhttp.WithPermissionHookFunc(permission.PullMirrorReadOnly(proxyMirror)),
		backendhttp.WithPreOpenHookFunc(preOpen),
	)
	handler = xet.casServer(handler)
	// The wiring under test: the internal management API wraps the whole
	// chain outermost, the way cmd/hfd's internalAPI does.
	gcs, ok := xet.xs.(xetstorage.GCStore)
	if !ok {
		t.Fatalf("xet storage %T does not implement GCStore", xet.xs)
	}
	handler = backendinternalapi.NewHandler(
		backendinternalapi.WithCollector(gc.NewCollector(st.RepositoriesFS(), gcs)),
		backendinternalapi.WithGCGrace(time.Hour),
		backendinternalapi.WithNext(handler),
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

	listObjects := func(t *testing.T) []gcObject {
		t.Helper()
		var objects []gcObject
		if err := json.Unmarshal(mustGet(t, proxy.URL+"/internal/objects"), &objects); err != nil {
			t.Fatalf("decode /internal/objects: %v", err)
		}
		return objects
	}

	findObject := func(objects []gcObject) *gcObject {
		for i := range objects {
			if objects[i].OID == oid {
				return &objects[i]
			}
		}
		return nil
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

	step("ListObjects", func(t *testing.T) {
		obj := findObject(listObjects(t))
		if obj == nil {
			t.Fatalf("GET /internal/objects misses oid %s", oid)
		}
		if obj.Size != uint64(len(data)) {
			t.Fatalf("size = %d, want %d", obj.Size, len(data))
		}
	})

	step("PruneKeepsReferenced", func(t *testing.T) {
		prune := postPrune(t, proxy.URL, "?grace=0")
		if len(prune.Unlinked) != 0 || prune.LiveObjects < 1 {
			t.Fatalf("prune = %+v, want a live object and nothing unlinked", prune)
		}
		res := postSweep(t, proxy.URL)
		if res.SweptShards != 0 || res.SweptXorbs != 0 {
			t.Fatalf("anchored sweep reclaimed shards=%d xorbs=%d, want none", res.SweptShards, res.SweptXorbs)
		}
		if got := mustGet(t, proxy.URL+"/objects/"+oid); !bytes.Equal(got, data) {
			t.Fatalf("object download after anchored sweep mismatch: got %d bytes, want %d", len(got), len(data))
		}
	})

	step("DeleteMirrorRepo", func(t *testing.T) {
		deleteRepoAt(t, proxy.URL, "gc-org", "gc-repo")
	})

	step("PruneUnlinks", func(t *testing.T) {
		res := postPrune(t, proxy.URL, "?grace=0")
		if !slices.Equal(res.Unlinked, []string{oid}) || res.Repositories != 0 {
			t.Fatalf("prune = %+v, want [%s] unlinked and no repositories", res, oid)
		}
	})

	step("SweepReclaims", func(t *testing.T) {
		res := postSweep(t, proxy.URL)
		if res.SweptShards == 0 || res.SweptXorbs == 0 {
			t.Fatalf("unanchored sweep reclaimed shards=%d xorbs=%d, want both non-zero", res.SweptShards, res.SweptXorbs)
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
		// The pre-open hook re-mirrors the deleted repo, and resolve re-ingests the missing object from upstream.
		if got := mustGet(t, resolveURL); !bytes.Equal(got, data) {
			t.Fatalf("resolve after GC bytes mismatch: got %d bytes, want %d", len(got), len(data))
		}
		waitIngested(t)
		if findObject(listObjects(t)) == nil {
			t.Fatalf("GET /internal/objects misses oid %s after re-ingest", oid)
		}
	})
}

func assertGCObjects(t *testing.T, baseURL string, want ...gcObject) {
	t.Helper()
	var objects []gcObject
	if err := json.Unmarshal(mustGet(t, baseURL+"/internal/objects"), &objects); err != nil {
		t.Fatalf("decode /internal/objects: %v", err)
	}
	compare := func(left, right gcObject) int { return strings.Compare(left.OID, right.OID) }
	slices.SortFunc(objects, compare)
	slices.SortFunc(want, compare)
	if !slices.Equal(objects, want) {
		t.Fatalf("/internal/objects = %+v, want %+v", objects, want)
	}
}

func assertGCNotFound(t *testing.T, url string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read GET %s: %v", url, err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET %s status = %d, want 404 (body %q)", url, resp.StatusCode, body)
	}
}

func commitGCOperation(t *testing.T, url, key string, value map[string]any) {
	t.Helper()
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	for _, operation := range []map[string]any{
		{"key": "header", "value": map[string]any{"summary": "update GC references"}},
		{"key": key, "value": value},
	} {
		if err := encoder.Encode(operation); err != nil {
			t.Fatalf("encode commit: %v", err)
		}
	}
	resp, err := http.Post(url, "application/x-ndjson", &body)
	if err != nil {
		t.Fatalf("POST commit: %v", err)
	}
	defer resp.Body.Close()
	result, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read commit: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("commit status = %d, want 200 (body %q)", resp.StatusCode, result)
	}
}

// TestGCSharedReferences keeps a shared OID until its last repository is deleted.
func TestGCSharedReferences(t *testing.T) {
	s := newE2EServer(t, withInternalAPI())
	data := makeBinaryData(64*1024, 31)
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])
	object := gcObject{OID: oid, Size: uint64(len(data))}
	step := func(name string, fn func(t *testing.T)) {
		t.Helper()
		if !t.Run(name, fn) {
			t.FailNow()
		}
	}

	step("PushBoth", func(t *testing.T) {
		s.createRepo(t, "gc-org", "shared-a")
		s.createRepo(t, "gc-org", "shared-b")
		pushViaXetBatch(t, s, "gc-org/shared-a", data)
		commitGCOperation(t, s.httpURL+"/api/models/gc-org/shared-b/commit/main", "lfsFile", map[string]any{
			"path": transferMatrixFile, "oid": oid, "size": len(data),
		})
		for _, repo := range []string{"shared-a", "shared-b"} {
			if got := mustGet(t, s.httpURL+"/gc-org/"+repo+"/resolve/main/"+transferMatrixFile); !bytes.Equal(got, data) {
				t.Fatalf("%s resolve: got %d bytes, want original %d bytes", repo, len(got), len(data))
			}
		}
	})
	step("ListOnce", func(t *testing.T) {
		assertGCObjects(t, s.httpURL, object)
	})
	step("DeleteFirst", func(t *testing.T) {
		s.deleteRepo(t, "gc-org", "shared-a")
	})
	step("KeepShared", func(t *testing.T) {
		prune := postPrune(t, s.httpURL, "?grace=0")
		if prune.DryRun || prune.Repositories != 1 || prune.LiveObjects != 1 || prune.SkippedInGrace != 0 || !slices.Equal(prune.Unlinked, []string{}) {
			t.Fatalf("prune = %+v, want 1 repository, 1 live object, nothing unlinked or skipped", prune)
		}
		// shared-a's own git objects are dead now; the xet content stays live through shared-b.
		sweep := postSweep(t, s.httpURL)
		if !sweep.Done || sweep.SweptShards != 0 || sweep.SweptXorbs != 0 || sweep.SweptGitObjects <= 0 || sweep.ReclaimedBytes <= 0 || sweep.RemainingShards != 0 || sweep.RemainingXorbs != 0 || sweep.RemainingGitObjects != 0 {
			t.Fatalf("sweep = %+v, want done with only shared-a's git objects reclaimed, nothing remaining", sweep)
		}
		assertGCObjects(t, s.httpURL, object)
		if got := mustGet(t, s.httpURL+"/gc-org/shared-b/resolve/main/"+transferMatrixFile); !bytes.Equal(got, data) {
			t.Fatalf("prune = %+v, sweep = %+v, shared-b resolve: got %d bytes, want original %d bytes", prune, sweep, len(got), len(data))
		}
	})
	step("DeleteLast", func(t *testing.T) {
		s.deleteRepo(t, "gc-org", "shared-b")
	})
	step("PruneLast", func(t *testing.T) {
		res := postPrune(t, s.httpURL, "?grace=0")
		if res.DryRun || res.Repositories != 0 || res.LiveObjects != 0 || res.SkippedInGrace != 0 || !slices.Equal(res.Unlinked, []string{oid}) {
			t.Fatalf("prune = %+v, want no repositories or live objects, [%s] unlinked, none skipped", res, oid)
		}
		assertGCObjects(t, s.httpURL)
	})
	step("SweepLast", func(t *testing.T) {
		res := postSweep(t, s.httpURL)
		if !res.Done || res.SweptShards <= 0 || res.SweptXorbs <= 0 || res.ReclaimedBytes <= 0 || res.RemainingShards != 0 || res.RemainingXorbs != 0 {
			t.Fatalf("sweep = %+v, want done with shards, xorbs and bytes reclaimed, nothing remaining", res)
		}
		assertGCObjects(t, s.httpURL)
		assertGCNotFound(t, s.httpURL+"/objects/"+oid)
	})
}

// TestGCHistoryReferences keeps pointers in main's history and on a non-default branch.
func TestGCHistoryReferences(t *testing.T) {
	s := newE2EServer(t, withInternalAPI())
	historyData, branchData := makeBinaryData(64*1024, 41), makeBinaryData(64*1024, 42)
	historySum, branchSum := sha256.Sum256(historyData), sha256.Sum256(branchData)
	historyOID, branchOID := hex.EncodeToString(historySum[:]), hex.EncodeToString(branchSum[:])
	step := func(name string, fn func(t *testing.T)) {
		t.Helper()
		if !t.Run(name, fn) {
			t.FailNow()
		}
	}

	step("RemoveFromMain", func(t *testing.T) {
		s.createRepo(t, "gc-org", "history")
		pushViaXetBatch(t, s, "gc-org/history", historyData)
		if got := mustGet(t, s.httpURL+"/gc-org/history/resolve/main/"+transferMatrixFile); !bytes.Equal(got, historyData) {
			t.Fatalf("main resolve: got %d bytes, want original %d bytes", len(got), len(historyData))
		}
		commitGCOperation(t, s.httpURL+"/api/models/gc-org/history/commit/main", "deletedFile", map[string]any{"path": transferMatrixFile})
		assertGCNotFound(t, s.httpURL+"/gc-org/history/resolve/main/"+transferMatrixFile)
	})
	step("BranchOnlyPointer", func(t *testing.T) {
		resp, err := http.Post(s.httpURL+"/api/models/gc-org/history/branch/side", "application/json", strings.NewReader(`{"startingPoint":"main"}`))
		if err != nil {
			t.Fatalf("create branch: %v", err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read create branch: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("create branch status = %d, want 200 (body %q)", resp.StatusCode, body)
		}
		s.createRepo(t, "gc-org", "staging")
		pushViaXetBatch(t, s, "gc-org/staging", branchData)
		commitGCOperation(t, s.httpURL+"/api/models/gc-org/history/commit/side", "lfsFile", map[string]any{
			"path": transferMatrixFile, "oid": branchOID, "size": len(branchData),
		})
		s.deleteRepo(t, "gc-org", "staging")
		assertGCNotFound(t, s.httpURL+"/gc-org/history/resolve/main/"+transferMatrixFile)
		if got := mustGet(t, s.httpURL+"/gc-org/history/resolve/side/"+transferMatrixFile); !bytes.Equal(got, branchData) {
			t.Fatalf("side resolve: got %d bytes, want original %d bytes", len(got), len(branchData))
		}
	})
	step("KeepHistoryAndBranch", func(t *testing.T) {
		prune := postPrune(t, s.httpURL, "?grace=0")
		if prune.DryRun || prune.Repositories != 1 || prune.LiveObjects != 2 || prune.SkippedInGrace != 0 || !slices.Equal(prune.Unlinked, []string{}) {
			t.Fatalf("prune = %+v, want 1 repository, 2 live objects, nothing unlinked or skipped", prune)
		}
		// staging's own git objects are dead now; both pointers stay reachable from history's main and side.
		sweep := postSweep(t, s.httpURL)
		if !sweep.Done || sweep.SweptShards != 0 || sweep.SweptXorbs != 0 || sweep.SweptGitObjects <= 0 || sweep.ReclaimedBytes <= 0 || sweep.RemainingShards != 0 || sweep.RemainingXorbs != 0 || sweep.RemainingGitObjects != 0 {
			t.Fatalf("sweep = %+v, want done with only staging's git objects reclaimed, nothing remaining", sweep)
		}
		assertGCObjects(t, s.httpURL, gcObject{OID: historyOID, Size: uint64(len(historyData))}, gcObject{OID: branchOID, Size: uint64(len(branchData))})
		for oid, data := range map[string][]byte{historyOID: historyData, branchOID: branchData} {
			if got := mustGet(t, s.httpURL+"/objects/"+oid); !bytes.Equal(got, data) {
				t.Fatalf("prune = %+v, sweep = %+v, object %s: got %d bytes, want original %d bytes", prune, sweep, oid, len(got), len(data))
			}
		}
	})
}

// TestGCBoundedSweep resumes a deletion-limited sweep after unlinking two dead objects.
func TestGCBoundedSweep(t *testing.T) {
	s := newE2EServer(t, withInternalAPI())
	firstData, secondData := makeBinaryData(64*1024, 51), makeBinaryData(64*1024, 52)
	firstSum, secondSum := sha256.Sum256(firstData), sha256.Sum256(secondData)
	oids := []string{hex.EncodeToString(firstSum[:]), hex.EncodeToString(secondSum[:])}
	slices.Sort(oids)
	step := func(name string, fn func(t *testing.T)) {
		t.Helper()
		if !t.Run(name, fn) {
			t.FailNow()
		}
	}
	step("UnlinkTwo", func(t *testing.T) {
		s.createRepo(t, "gc-org", "bounded")
		pushViaXetBatch(t, s, "gc-org/bounded", firstData)
		pushViaXetBatch(t, s, "gc-org/bounded", secondData)
		assertGCObjects(t, s.httpURL, gcObject{OID: oids[0], Size: uint64(len(firstData))}, gcObject{OID: oids[1], Size: uint64(len(secondData))})
		s.deleteRepo(t, "gc-org", "bounded")
		res := postPrune(t, s.httpURL, "?grace=0")
		if res.DryRun || res.Repositories != 0 || res.LiveObjects != 0 || res.SkippedInGrace != 0 || !slices.Equal(res.Unlinked, oids) {
			t.Fatalf("prune = %+v, want no repositories or live objects, %v unlinked, none skipped", res, oids)
		}
		assertGCObjects(t, s.httpURL)
	})
	var bounded gcSweepResult
	step("OneDeletion", func(t *testing.T) {
		bounded = postSweep(t, s.httpURL, "max=1")
		if bounded.Done || bounded.SweptShards != 1 || bounded.SweptXorbs != 0 || bounded.RemainingShards <= 0 || bounded.RemainingXorbs != 0 || bounded.ReclaimedBytes <= 0 {
			t.Fatalf("bounded sweep = %+v, want unfinished, 1 shard and 0 xorbs swept, positive remaining shards, xorbs not yet judged", bounded)
		}
		assertGCObjects(t, s.httpURL)
	})
	step("FinishCycle", func(t *testing.T) {
		res := postSweep(t, s.httpURL)
		if !res.Done || res.SweptShards != bounded.RemainingShards || res.SweptXorbs <= 0 || res.ReclaimedBytes <= 0 || res.RemainingShards != 0 || res.RemainingXorbs != 0 {
			t.Fatalf("sweep = %+v after bounded = %+v, want all remaining shards and xorbs reclaimed, done", res, bounded)
		}
		assertGCObjects(t, s.httpURL)
		for _, oid := range oids {
			assertGCNotFound(t, s.httpURL+"/objects/"+oid)
		}
	})
}

// TestGCPrune drives POST /internal/gc/prune then /internal/gc/sweep over the assembled chain: a deleted repository's LFS object is unlinked once outside the grace window, and only the separate sweep reclaims its data.
func TestGCPrune(t *testing.T) {
	s := newE2EServer(t, withInternalAPI())
	s.createRepo(t, "gc-org", "keep")
	s.createRepo(t, "gc-org", "drop")
	keepData, dropData := makeBinaryData(128*1024, 11), makeBinaryData(128*1024, 22)
	keepSum, dropSum := sha256.Sum256(keepData), sha256.Sum256(dropData)
	keepOID, dropOID := hex.EncodeToString(keepSum[:]), hex.EncodeToString(dropSum[:])
	pushViaXetBatch(t, s, "gc-org/keep", keepData)
	pushViaXetBatch(t, s, "gc-org/drop", dropData)

	step := func(name string, fn func(t *testing.T)) {
		t.Helper()
		if !t.Run(name, fn) {
			t.FailNow()
		}
	}

	prune := func(t *testing.T, query string) gcPruneResult {
		t.Helper()
		return postPrune(t, s.httpURL, query)
	}

	listed := func(t *testing.T) map[string]bool {
		t.Helper()
		var objects []gcObject
		if err := json.Unmarshal(mustGet(t, s.httpURL+"/internal/objects"), &objects); err != nil {
			t.Fatalf("decode /internal/objects: %v", err)
		}
		set := map[string]bool{}
		for _, o := range objects {
			set[o.OID] = true
		}
		return set
	}

	step("DryRunKeepsAll", func(t *testing.T) {
		res := prune(t, "?dry_run=true")
		if !res.DryRun || res.Repositories != 2 || res.LiveObjects != 2 || len(res.Unlinked) != 0 {
			t.Fatalf("dry run = %+v, want 2 repositories, 2 live objects, nothing unlinked", res)
		}
	})

	step("DeleteRepo", func(t *testing.T) {
		s.deleteRepo(t, "gc-org", "drop")
	})

	step("GraceShields", func(t *testing.T) {
		res := prune(t, "")
		if len(res.Unlinked) != 0 || res.SkippedInGrace != 1 || res.Repositories != 1 {
			t.Fatalf("prune in grace = %+v, want 1 repository, nothing unlinked, 1 skipped", res)
		}
		if !listed(t)[dropOID] {
			t.Fatalf("/internal/objects lost %s inside the grace window", dropOID)
		}
	})

	step("DryRunListsDead", func(t *testing.T) {
		res := prune(t, "?dry_run=true&grace=0")
		if !res.DryRun || res.Repositories != 1 || res.LiveObjects != 1 || res.SkippedInGrace != 0 || !slices.Equal(res.Unlinked, []string{dropOID}) {
			t.Fatalf("dry run = %+v, want 1 repository, 1 live object, [%s] unlinked, none skipped", res, dropOID)
		}
		files := listed(t)
		if len(files) != 2 || !files[dropOID] || !files[keepOID] {
			t.Fatalf("dry run = %+v, /internal/objects = %v, want both %s and %s", res, files, dropOID, keepOID)
		}
		if got := mustGet(t, s.httpURL+"/objects/"+dropOID); !bytes.Equal(got, dropData) {
			t.Fatalf("dry run = %+v, drop download: got %d bytes, want original %d bytes", res, len(got), len(dropData))
		}
	})

	step("Prune", func(t *testing.T) {
		// The fresh upload sits inside the default 1h grace window; grace=0 disables it.
		res := prune(t, "?grace=0")
		if !slices.Equal(res.Unlinked, []string{dropOID}) {
			t.Fatalf("unlinked = %v, want [%s]", res.Unlinked, dropOID)
		}
		files := listed(t)
		if files[dropOID] || !files[keepOID] {
			t.Fatalf("/internal/objects lists drop=%v keep=%v, want false/true", files[dropOID], files[keepOID])
		}
		if got := mustGet(t, s.httpURL+"/gc-org/keep/resolve/main/"+transferMatrixFile); !bytes.Equal(got, keepData) {
			t.Fatalf("keep resolve after prune: got %d bytes, want %d", len(got), len(keepData))
		}
	})

	step("Sweep", func(t *testing.T) {
		res := postSweep(t, s.httpURL)
		if !res.Done || res.SweptShards == 0 || res.SweptXorbs == 0 {
			t.Fatalf("sweep = %+v, want a finished sweep reclaiming shards and xorbs", res)
		}
		if got := mustGet(t, s.httpURL+"/gc-org/keep/resolve/main/"+transferMatrixFile); !bytes.Equal(got, keepData) {
			t.Fatalf("keep resolve after sweep: got %d bytes, want %d", len(got), len(keepData))
		}
	})

	step("Idempotent", func(t *testing.T) {
		res := prune(t, "?grace=0")
		if len(res.Unlinked) != 0 {
			t.Fatalf("second prune = %+v, want nothing unlinked", res)
		}
		if sweep := postSweep(t, s.httpURL); sweep.SweptShards != 0 || sweep.SweptXorbs != 0 {
			t.Fatalf("second sweep = %+v, want nothing swept", sweep)
		}
	})

	step("LegacyRouteGone", func(t *testing.T) {
		resp, err := http.Post(s.httpURL+"/internal/gc", "application/json", nil)
		if err != nil {
			t.Fatalf("POST legacy GC: %v", err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read legacy GC body: %v", err)
		}
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("legacy GC status = %d, want 404 (body %q)", resp.StatusCode, body)
		}
	})
}

// TestGCGitObjects sweeps a deleted repository's shared loose objects while the repository sharing a blob with it stays intact.
func TestGCGitObjects(t *testing.T) {
	const shared = "shared text\n"
	s := newE2EServer(t, withInternalAPI())
	fs := s.storage.FS()
	step := func(name string, fn func(t *testing.T)) {
		t.Helper()
		if !t.Run(name, fn) {
			t.FailNow()
		}
	}
	snapshot := func(t *testing.T) map[string]int64 {
		t.Helper()
		objects := map[string]int64{}
		dirs, err := fs.ReadDir("/git/sha1/objects")
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("read shared objects: %v", err)
		}
		for _, dir := range dirs {
			if _, err := hex.DecodeString(dir.Name()); err != nil || !dir.IsDir() || len(dir.Name()) != 2 {
				continue
			}
			entries, err := fs.ReadDir("/git/sha1/objects/" + dir.Name())
			if err != nil {
				t.Fatalf("read shared objects %s: %v", dir.Name(), err)
			}
			for _, entry := range entries {
				if _, err := hex.DecodeString(entry.Name()); err != nil || entry.IsDir() || len(entry.Name()) != 38 {
					continue
				}
				info, err := entry.Info()
				if err != nil {
					t.Fatalf("stat shared object %s%s: %v", dir.Name(), entry.Name(), err)
				}
				objects[dir.Name()+entry.Name()] = info.Size()
			}
		}
		return objects
	}
	total := func(objects map[string]int64) int64 {
		var sum int64
		for _, size := range objects {
			sum += size
		}
		return sum
	}
	push := func(t *testing.T, repo, unique string) (string, []string) {
		t.Helper()
		s.createRepo(t, "gc-org", repo)
		remote, env := s.httpRemote("gc-org/" + repo)
		dir := filepath.Join(t.TempDir(), repo)
		runGit(t, "", env, "clone", remote, dir)
		runGit(t, dir, env, "config", "user.email", "test@test.com")
		runGit(t, dir, env, "config", "user.name", "Test User")
		for name, content := range map[string]string{"shared.txt": shared, "unique.txt": unique} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
				t.Fatalf("write %s: %v", name, err)
			}
		}
		runGit(t, dir, env, "add", ".")
		runGit(t, dir, env, "commit", "-m", "Add "+repo)
		runGit(t, dir, env, "push", "origin", "main")
		return dir, env
	}
	resolve := func(t *testing.T, repo, name, want string) {
		t.Helper()
		if got := mustGet(t, s.httpURL+"/gc-org/"+repo+"/resolve/main/"+name); string(got) != want {
			t.Fatalf("%s resolve %s = %q, want %q", repo, name, got, want)
		}
	}

	var initial, live map[string]int64
	step("PushBoth", func(t *testing.T) {
		push(t, "loose-a", "only in loose-a\n")
		dir, env := push(t, "loose-b", "only in loose-b\n")
		initial = snapshot(t)
		live = map[string]int64{}
		for _, line := range strings.Split(strings.TrimSpace(runGit(t, dir, env, "rev-list", "--objects", "--all")), "\n") {
			hash, _, _ := strings.Cut(line, " ")
			size, ok := initial[hash]
			if !ok {
				t.Fatalf("loose-b object %s missing from the shared store %v", hash, initial)
			}
			live[hash] = size
		}
		blob := strings.TrimSpace(runGit(t, dir, env, "rev-parse", "HEAD:shared.txt"))
		if _, ok := live[blob]; !ok || len(initial) <= len(live) {
			t.Fatalf("shared store %v, loose-b reaches %v, want the shared blob %s reachable and loose-a's own objects on top", initial, live, blob)
		}
		resolve(t, "loose-a", "shared.txt", shared)
		resolve(t, "loose-b", "shared.txt", shared)
	})
	step("LiveSweep", func(t *testing.T) {
		sweep := postSweep(t, s.httpURL)
		if !sweep.Done || sweep.SweptShards != 0 || sweep.SweptXorbs != 0 || sweep.SweptGitObjects != 0 || sweep.ReclaimedBytes != 0 || sweep.RemainingShards != 0 || sweep.RemainingXorbs != 0 || sweep.RemainingGitObjects != 0 {
			t.Fatalf("sweep = %+v, want done with nothing reclaimed or remaining", sweep)
		}
		if got := snapshot(t); !maps.Equal(got, initial) {
			t.Fatalf("shared store = %v after a live sweep, want %v", got, initial)
		}
		resolve(t, "loose-a", "unique.txt", "only in loose-a\n")
		resolve(t, "loose-b", "unique.txt", "only in loose-b\n")
	})
	var dry, bounded gcSweepResult
	step("DryRun", func(t *testing.T) {
		s.deleteRepo(t, "gc-org", "loose-a")
		dry = postSweep(t, s.httpURL, "dry_run=true", "max=1", "budget=1ns")
		dead, deadBytes := len(initial)-len(live), total(initial)-total(live)
		if !dry.DryRun || !dry.Done || dry.SweptShards != 0 || dry.SweptXorbs != 0 || dry.SweptGitObjects != dead || dead < 2 || dry.ReclaimedBytes != deadBytes || dry.RemainingShards != 0 || dry.RemainingXorbs != 0 || dry.RemainingGitObjects != 0 {
			t.Fatalf("dry run = %+v, want done with all %d dead objects (%d bytes) reported despite the limits, nothing remaining", dry, dead, deadBytes)
		}
		if got := snapshot(t); !maps.Equal(got, initial) {
			t.Fatalf("shared store = %v after a dry run, want %v", got, initial)
		}
		resolve(t, "loose-b", "shared.txt", shared)
	})
	step("OneDeletion", func(t *testing.T) {
		bounded = postSweep(t, s.httpURL, "max=1")
		if bounded.DryRun || bounded.Done || bounded.SweptShards != 0 || bounded.SweptXorbs != 0 || bounded.SweptGitObjects != 1 || bounded.RemainingGitObjects != dry.SweptGitObjects-1 {
			t.Fatalf("bounded sweep = %+v after dry run %+v, want unfinished with 1 object swept and the rest remaining", bounded, dry)
		}
		got := snapshot(t)
		if len(got) != len(initial)-1 || total(initial)-total(got) != bounded.ReclaimedBytes {
			t.Fatalf("shared store = %v after bounded sweep %+v, want %v minus one object", got, bounded, initial)
		}
		for hash, size := range got {
			if initial[hash] != size {
				t.Fatalf("shared store gained %s (%d bytes) missing from %v", hash, size, initial)
			}
		}
		for hash, size := range live {
			if got[hash] != size {
				t.Fatalf("shared store = %v lost loose-b's object %s (%d bytes)", got, hash, size)
			}
		}
	})
	step("Drain", func(t *testing.T) {
		drain := postSweep(t, s.httpURL)
		if !drain.Done || drain.SweptShards != 0 || drain.SweptXorbs != 0 || drain.SweptGitObjects != bounded.RemainingGitObjects || drain.ReclaimedBytes != dry.ReclaimedBytes-bounded.ReclaimedBytes || drain.RemainingGitObjects != 0 {
			t.Fatalf("sweep = %+v after bounded = %+v and dry run = %+v, want the remaining objects and bytes reclaimed, done", drain, bounded, dry)
		}
		if got := snapshot(t); !maps.Equal(got, live) {
			t.Fatalf("shared store = %v after draining, want exactly loose-b's objects %v", got, live)
		}
		remote, env := s.httpRemote("gc-org/loose-b")
		dir := filepath.Join(t.TempDir(), "verify")
		runGit(t, "", env, "clone", remote, dir)
		runGit(t, dir, env, "fsck")
		for name, want := range map[string]string{"shared.txt": shared, "unique.txt": "only in loose-b\n"} {
			if got, err := os.ReadFile(filepath.Join(dir, name)); err != nil || string(got) != want {
				t.Fatalf("clone %s = %q (%v), want %q", name, got, err, want)
			}
		}
	})
	step("DeleteLast", func(t *testing.T) {
		s.deleteRepo(t, "gc-org", "loose-b")
		last := postSweep(t, s.httpURL)
		if !last.Done || last.SweptShards != 0 || last.SweptXorbs != 0 || last.SweptGitObjects != len(live) || last.ReclaimedBytes != total(live) || last.RemainingGitObjects != 0 {
			t.Fatalf("sweep = %+v after deleting the last repository, want all %d objects (%d bytes) reclaimed, done", last, len(live), total(live))
		}
		if got := snapshot(t); len(got) != 0 {
			t.Fatalf("shared store = %v with no repositories left, want empty", got)
		}
		if again := postSweep(t, s.httpURL); !again.Done || again.SweptGitObjects != 0 || again.ReclaimedBytes != 0 || again.RemainingGitObjects != 0 {
			t.Fatalf("second sweep = %+v, want done with nothing reclaimed or remaining", again)
		}
	})
}
