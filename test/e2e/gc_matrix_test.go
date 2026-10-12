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
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/storage/filesystem"

	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendinternalapi "github.com/matrixhub-ai/hfd/pkg/backend/internalapi"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	"github.com/matrixhub-ai/hfd/pkg/gc"
	"github.com/matrixhub-ai/hfd/pkg/gc/xetstore"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// gcObject carries the /internal/objects fields the test asserts on.
type gcObject struct {
	OID  string `json:"oid"`
	Size uint64 `json:"size"`
}

// gcResult carries the GC response fields the test asserts on.
type gcResult struct {
	DryRun bool `json:"dry_run"`
	Done   bool `json:"done"`
	Prune  *struct {
		DryRun            bool              `json:"dry_run"`
		Repositories      int               `json:"repositories"`
		DeletedGitObjects int               `json:"deleted_git_objects"`
		DeletedGitBytes   int64             `json:"deleted_git_bytes"`
		ReclaimedBytes    int64             `json:"reclaimed_bytes"`
		Failed            map[string]string `json:"failed"`
		LiveObjects       int               `json:"live_objects"`
		Unlinked          []string          `json:"unlinked"`
		SkippedInGrace    int               `json:"skipped_in_grace"`
	} `json:"prune"`
	Storage *struct {
		DryRun          bool  `json:"dry_run"`
		SweptShards     int   `json:"swept_shards"`
		SweptXorbs      int   `json:"swept_xorbs"`
		ReclaimedBytes  int64 `json:"reclaimed_bytes"`
		Done            bool  `json:"done"`
		RemainingShards int   `json:"remaining_shards"`
		RemainingXorbs  int   `json:"remaining_xorbs"`
	} `json:"storage"`
	// Spools and mirror are present once the storage pass ended, as the budget allows.
	Spools *struct {
		SweptSpools int  `json:"swept_spools"`
		Done        bool `json:"done"`
	} `json:"spools"`
	Mirror *struct {
		DroppedEntries int  `json:"dropped_entries"`
		Done           bool `json:"done"`
	} `json:"mirror"`
	Errors []string `json:"errors"`
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

// postGC runs one unshielded GC step (grace=0 plus query) against baseURL and returns the decoded 200 body, failing the test otherwise.
func postGC(t *testing.T, baseURL string, query ...string) gcResult {
	t.Helper()
	return postGCQuery(t, baseURL, strings.Join(append([]string{"grace=0"}, query...), "&"))
}

// postGCQuery runs one GC step with the raw query against baseURL and returns the decoded 200 body with its prune and storage sections, failing the test otherwise.
func postGCQuery(t *testing.T, baseURL, rawQuery string) gcResult {
	t.Helper()
	resp, err := http.Post(baseURL+"/internal/gc?"+rawQuery, "application/json", nil)
	if err != nil {
		t.Fatalf("POST /internal/gc?%s: %v", rawQuery, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read GC body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GC status = %d, want 200 (body %q)", resp.StatusCode, body)
	}
	var res gcResult
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatalf("decode GC result: %v", err)
	}
	if res.Prune == nil || res.Storage == nil {
		t.Fatalf("GC body %s: want the prune and storage sections", body)
	}
	return res
}

// TestGCLifecycle drives the /internal/ management API end to end over the
// assembled hfd chain: a pull-through mirror ingests an LFS object from an
// upstream hfd server, /internal/objects lists it, and a GC step keeps the
// referenced object. Deleting the mirrored repo lets the next GC step unlink
// the OID and reclaim the bytes; the next resolve self-heals by mirroring
// the repo and re-ingesting from upstream. The internal API wraps the chain
// outermost with the same options as cmd/hfd's internalAPI. Library-level
// GC semantics stay covered upstream in xet; this test pins the hfd wiring.
// TestMain runs it under local and S3 storage.
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

	// Pull-through proxy of upstream, assembled like newE2EServer with a
	// mirror source so the xet engine can re-ingest from the real upstream
	// after GC.
	dataDir := newDataDir(t, "e2e-gc-proxy")
	st := newTestStorage(t, dataDir)
	proxyMirror, xet := newTestMirror(t, st, true,
		mirror.WithRepositoriesFS(st.RepositoriesFS()),
		mirror.WithMirrorSourceFunc(newMirrorSourceFunc(upstream.httpURL)),
	)
	preOpen := newMirrorPreOpenHook(proxyMirror)
	var handler http.Handler
	handler = backendhf.NewHandler(append(catalogOptions(st),
		backendhf.WithStorage(st),
		backendhf.WithMirror(proxyMirror),
		backendhf.WithNext(http.NotFoundHandler()),
		backendhf.WithPreOpenHookFunc(preOpen),
	)...)
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
	handler = backendinternalapi.NewHandler(
		backendinternalapi.WithGC(gc.NewGC(st.RepositoriesFS(), xetstore.New(xet.xs))),
		backendinternalapi.WithStorage(xet.xs),
		backendinternalapi.WithSpool(xet.spool),
		backendinternalapi.WithMirror(xet.engine),
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
		if err := xet.xs.WalkShards(t.Context(), func(string, int64, time.Time) error { shards++; return nil }); err != nil {
			t.Fatalf("walk shards: %v", err)
		}
		if err := xet.xs.WalkXorbs(t.Context(), func(string, int64, time.Time) error { xorbs++; return nil }); err != nil {
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
		res := postGC(t, proxy.URL)
		if len(res.Prune.Unlinked) != 0 || res.Prune.LiveObjects < 1 {
			t.Fatalf("prune = %+v, want a live object and nothing unlinked", *res.Prune)
		}
		if res.Storage.SweptShards != 0 || res.Storage.SweptXorbs != 0 {
			t.Fatalf("anchored sweep reclaimed shards=%d xorbs=%d, want none", res.Storage.SweptShards, res.Storage.SweptXorbs)
		}
		if !res.Done || res.Errors != nil {
			t.Fatalf("GC = %+v, want a finished step without errors", res)
		}
		if got := mustGet(t, proxy.URL+"/objects/"+oid); !bytes.Equal(got, data) {
			t.Fatalf("object download after anchored sweep mismatch: got %d bytes, want %d", len(got), len(data))
		}
	})

	step("DeleteMirrorRepo", func(t *testing.T) {
		deleteRepoAt(t, proxy.URL, "gc-org", "gc-repo")
	})

	// The unlinked-but-stored state between the passes is pinned by pkg/gc's TestPrune/LeavesDataForSweep.
	step("CollectUnlinksAndReclaims", func(t *testing.T) {
		res := postGC(t, proxy.URL)
		if !slices.Equal(res.Prune.Unlinked, []string{oid}) || res.Prune.Repositories != 0 {
			t.Fatalf("prune = %+v, want [%s] unlinked and no repositories", *res.Prune, oid)
		}
		if res.Storage.SweptShards == 0 || res.Storage.SweptXorbs == 0 {
			t.Fatalf("unanchored sweep reclaimed shards=%d xorbs=%d, want both non-zero", res.Storage.SweptShards, res.Storage.SweptXorbs)
		}
		if !res.Done || res.Errors != nil {
			t.Fatalf("GC = %+v, want an unbounded step to finish the cycle without errors", res)
		}
		if res.Spools == nil || res.Mirror == nil {
			t.Fatalf("GC = %+v, want the finishing step to report the spool and engine index passes", res)
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
		res := postGC(t, s.httpURL)
		if p := res.Prune; p.DryRun || p.Repositories != 1 || p.LiveObjects != 1 || p.SkippedInGrace != 0 || !slices.Equal(p.Unlinked, []string{}) {
			t.Fatalf("prune = %+v, want 1 repository, 1 live object, nothing unlinked or skipped", *p)
		}
		if st := res.Storage; !res.Done || res.Errors != nil || st.SweptShards != 0 || st.SweptXorbs != 0 || st.ReclaimedBytes != 0 || st.RemainingShards != 0 || st.RemainingXorbs != 0 {
			t.Fatalf("GC = %+v, storage = %+v, want done with nothing reclaimed or remaining", res, *st)
		}
		assertGCObjects(t, s.httpURL, object)
		if got := mustGet(t, s.httpURL+"/gc-org/shared-b/resolve/main/"+transferMatrixFile); !bytes.Equal(got, data) {
			t.Fatalf("prune = %+v, storage = %+v, shared-b resolve: got %d bytes, want original %d bytes", *res.Prune, *res.Storage, len(got), len(data))
		}
	})
	step("DeleteLast", func(t *testing.T) {
		s.deleteRepo(t, "gc-org", "shared-b")
	})
	step("CollectLast", func(t *testing.T) {
		res := postGC(t, s.httpURL)
		if p := res.Prune; p.DryRun || p.Repositories != 0 || p.LiveObjects != 0 || p.SkippedInGrace != 0 || !slices.Equal(p.Unlinked, []string{oid}) {
			t.Fatalf("prune = %+v, want no repositories or live objects, [%s] unlinked, none skipped", *p, oid)
		}
		if st := res.Storage; !res.Done || res.Errors != nil || st.SweptShards <= 0 || st.SweptXorbs <= 0 || st.ReclaimedBytes <= 0 || st.RemainingShards != 0 || st.RemainingXorbs != 0 {
			t.Fatalf("GC = %+v, storage = %+v, want done with shards, xorbs and bytes reclaimed, nothing remaining", res, *st)
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
		res := postGC(t, s.httpURL)
		if p := res.Prune; p.DryRun || p.Repositories != 1 || p.LiveObjects != 2 || p.SkippedInGrace != 0 || !slices.Equal(p.Unlinked, []string{}) {
			t.Fatalf("prune = %+v, want 1 repository, 2 live objects, nothing unlinked or skipped", *p)
		}
		if st := res.Storage; !res.Done || res.Errors != nil || st.SweptShards != 0 || st.SweptXorbs != 0 || st.ReclaimedBytes != 0 || st.RemainingShards != 0 || st.RemainingXorbs != 0 {
			t.Fatalf("GC = %+v, storage = %+v, want done with nothing reclaimed or remaining", res, *st)
		}
		assertGCObjects(t, s.httpURL, gcObject{OID: historyOID, Size: uint64(len(historyData))}, gcObject{OID: branchOID, Size: uint64(len(branchData))})
		for oid, data := range map[string][]byte{historyOID: historyData, branchOID: branchData} {
			if got := mustGet(t, s.httpURL+"/objects/"+oid); !bytes.Equal(got, data) {
				t.Fatalf("prune = %+v, storage = %+v, object %s: got %d bytes, want original %d bytes", *res.Prune, *res.Storage, oid, len(got), len(data))
			}
		}
	})
}

// TestGCBoundedSweep resumes a deletion-limited GC step after unlinking two dead objects.
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
	step("PushAndDelete", func(t *testing.T) {
		s.createRepo(t, "gc-org", "bounded")
		pushViaXetBatch(t, s, "gc-org/bounded", firstData)
		pushViaXetBatch(t, s, "gc-org/bounded", secondData)
		assertGCObjects(t, s.httpURL, gcObject{OID: oids[0], Size: uint64(len(firstData))}, gcObject{OID: oids[1], Size: uint64(len(secondData))})
		s.deleteRepo(t, "gc-org", "bounded")
	})
	var bounded gcResult
	step("OneDeletion", func(t *testing.T) {
		// The prune pass is unbounded and unlinks both; max=1 caps only the storage pass.
		bounded = postGC(t, s.httpURL, "max=1")
		if p := bounded.Prune; p.DryRun || p.Repositories != 0 || p.LiveObjects != 0 || p.SkippedInGrace != 0 || !slices.Equal(p.Unlinked, oids) {
			t.Fatalf("prune = %+v, want no repositories or live objects, %v unlinked, none skipped", *p, oids)
		}
		st := bounded.Storage
		if bounded.Done || bounded.Errors != nil || st.Done || st.SweptShards != 1 || st.SweptXorbs != 0 || st.RemainingShards <= 0 || st.RemainingXorbs != 0 || st.ReclaimedBytes <= 0 {
			t.Fatalf("bounded GC = %+v, storage = %+v, want unfinished, 1 shard and 0 xorbs swept, positive remaining shards, xorbs not yet judged", bounded, *st)
		}
		if bounded.Spools != nil || bounded.Mirror != nil {
			t.Fatalf("bounded GC = %+v, want the spool and mirror passes deferred to the finishing step", bounded)
		}
		assertGCObjects(t, s.httpURL)
	})
	step("FinishCycle", func(t *testing.T) {
		res := postGC(t, s.httpURL)
		if len(res.Prune.Unlinked) != 0 {
			t.Fatalf("prune = %+v, want nothing left to unlink", *res.Prune)
		}
		if st := res.Storage; !res.Done || res.Errors != nil || !st.Done || st.SweptShards != bounded.Storage.RemainingShards || st.SweptXorbs <= 0 || st.ReclaimedBytes <= 0 || st.RemainingShards != 0 || st.RemainingXorbs != 0 {
			t.Fatalf("GC = %+v, storage = %+v after bounded storage = %+v, want all remaining shards and xorbs reclaimed, done", res, *st, *bounded.Storage)
		}
		// No engine on this server, so only the spool pass reports.
		if res.Spools == nil || res.Mirror != nil {
			t.Fatalf("GC = %+v, want the spool pass on the finishing step and no index pass without an engine", res)
		}
		assertGCObjects(t, s.httpURL)
		for _, oid := range oids {
			assertGCNotFound(t, s.httpURL+"/objects/"+oid)
		}
	})
}

// TestGCPrune drives POST /internal/gc over the assembled chain: a deleted repository's LFS object is unlinked and its data reclaimed in one step once outside the grace window.
// The unlinked-but-stored state between the passes is pinned by pkg/gc's TestPrune/LeavesDataForSweep.
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
		res := postGC(t, s.httpURL, "dry_run=true")
		if p := res.Prune; !res.DryRun || !p.DryRun || p.Repositories != 2 || p.LiveObjects != 2 || len(p.Unlinked) != 0 {
			t.Fatalf("dry run = %+v, prune = %+v, want 2 repositories, 2 live objects, nothing unlinked", res, *p)
		}
		if st := res.Storage; !res.Done || res.Errors != nil || !st.DryRun || st.SweptShards != 0 || st.SweptXorbs != 0 {
			t.Fatalf("dry run = %+v, storage = %+v, want a finished preview sweeping nothing", res, *st)
		}
	})

	step("DeleteRepo", func(t *testing.T) {
		s.deleteRepo(t, "gc-org", "drop")
	})

	step("GraceShields", func(t *testing.T) {
		// No grace parameter: the server's default window shields both passes.
		res := postGCQuery(t, s.httpURL, "")
		if p := res.Prune; len(p.Unlinked) != 0 || p.SkippedInGrace != 1 || p.Repositories != 1 {
			t.Fatalf("prune in grace = %+v, want 1 repository, nothing unlinked, 1 skipped", *p)
		}
		if st := res.Storage; st.SweptShards != 0 || st.SweptXorbs != 0 {
			t.Fatalf("storage in grace = %+v, want nothing swept", *st)
		}
		if !listed(t)[dropOID] {
			t.Fatalf("/internal/objects lost %s inside the grace window", dropOID)
		}
	})

	step("DryRunListsDead", func(t *testing.T) {
		res := postGC(t, s.httpURL, "dry_run=true")
		if p := res.Prune; !p.DryRun || p.Repositories != 1 || p.LiveObjects != 1 || p.SkippedInGrace != 0 || !slices.Equal(p.Unlinked, []string{dropOID}) {
			t.Fatalf("dry run prune = %+v, want 1 repository, 1 live object, [%s] unlinked, none skipped", *p, dropOID)
		}
		if !res.Storage.DryRun || !res.Done || res.Errors != nil {
			t.Fatalf("dry run = %+v, storage = %+v, want a finished preview", res, *res.Storage)
		}
		files := listed(t)
		if len(files) != 2 || !files[dropOID] || !files[keepOID] {
			t.Fatalf("/internal/objects = %v after dry run, want both %s and %s", files, dropOID, keepOID)
		}
		if got := mustGet(t, s.httpURL+"/objects/"+dropOID); !bytes.Equal(got, dropData) {
			t.Fatalf("drop download after dry run: got %d bytes, want original %d bytes", len(got), len(dropData))
		}
	})

	step("Collect", func(t *testing.T) {
		// The fresh upload sits inside the default 1h grace window; grace=0 disables it.
		res := postGC(t, s.httpURL)
		if !slices.Equal(res.Prune.Unlinked, []string{dropOID}) {
			t.Fatalf("unlinked = %v, want [%s]", res.Prune.Unlinked, dropOID)
		}
		if st := res.Storage; !res.Done || res.Errors != nil || st.SweptShards == 0 || st.SweptXorbs == 0 {
			t.Fatalf("GC = %+v, storage = %+v, want a finished step reclaiming shards and xorbs", res, *st)
		}
		files := listed(t)
		if files[dropOID] || !files[keepOID] {
			t.Fatalf("/internal/objects lists drop=%v keep=%v, want false/true", files[dropOID], files[keepOID])
		}
		assertGCNotFound(t, s.httpURL+"/objects/"+dropOID)
		if got := mustGet(t, s.httpURL+"/gc-org/keep/resolve/main/"+transferMatrixFile); !bytes.Equal(got, keepData) {
			t.Fatalf("keep resolve after GC: got %d bytes, want %d", len(got), len(keepData))
		}
	})

	step("Idempotent", func(t *testing.T) {
		res := postGC(t, s.httpURL)
		if len(res.Prune.Unlinked) != 0 || res.Storage.SweptShards != 0 || res.Storage.SweptXorbs != 0 || !res.Done || res.Errors != nil {
			t.Fatalf("second GC = %+v, prune = %+v, storage = %+v, want a finished step unlinking and sweeping nothing", res, *res.Prune, *res.Storage)
		}
	})

	step("SplitRoutesGone", func(t *testing.T) {
		for _, route := range []string{"/internal/gc/prune", "/internal/gc/sweep"} {
			resp, err := http.Post(s.httpURL+route, "application/json", nil)
			if err != nil {
				t.Fatalf("POST %s: %v", route, err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatalf("read %s body: %v", route, err)
			}
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("POST %s status = %d, want 404 (body %q)", route, resp.StatusCode, body)
			}
		}
		if res := postGC(t, s.httpURL, "dry_run=true"); !res.Done {
			t.Fatalf("POST /internal/gc = %+v, want a finished step", res)
		}
	})
}

// TestGCGitObjects force-pushes main back two commits, leaving them, their LFS pointer and a 128 KiB blob unreachable, then drives
// POST /internal/gc over the assembled chain: the dry run reports exactly those objects and bytes without touching storage,
// the live step removes them, unlinks the orphaned pointer's LFS object and reclaims its data. The expected objects come from the
// clone's own object graph sized by native git, never from the server.
func TestGCGitObjects(t *testing.T) {
	const repoID = "gc-org/git-objects"
	s := newE2EServer(t, withInternalAPI())
	s.createRepo(t, "gc-org", "git-objects")
	repos, repoPath := s.storage.RepositoriesFS(), repository.ResolvePath(repoID)
	keepData, dropData := randomData(t, 64<<10, 61), randomData(t, 64<<10, 62)
	keepSum, dropSum := sha256.Sum256(keepData), sha256.Sum256(dropData)
	keepOID, dropOID := hex.EncodeToString(keepSum[:]), hex.EncodeToString(dropSum[:])
	keepObject, dropObject := gcObject{OID: keepOID, Size: uint64(len(keepData))}, gcObject{OID: dropOID, Size: uint64(len(dropData))}
	remote, env := s.httpRemote(repoID)
	env = append(env, "GIT_LFS_SKIP_SMUDGE=1")
	step := func(name string, fn func(t *testing.T)) {
		t.Helper()
		if !t.Run(name, fn) {
			t.FailNow()
		}
	}

	// all is every object the clone pushed, kept what main still reaches after the force push; the difference is the expected garbage.
	var all, kept map[string]int64
	var wantDeleted int
	var wantBytes int64
	step("ForcePush", func(t *testing.T) {
		pushViaXetBatch(t, s, repoID, keepData)
		pushViaXetBatch(t, s, repoID, dropData)
		dir := filepath.Join(t.TempDir(), "history")
		runGit(t, "", env, "clone", remote, dir)
		runGit(t, dir, env, "config", "user.email", "matrix@test.com")
		runGit(t, dir, env, "config", "user.name", "Matrix Test")
		if err := os.WriteFile(filepath.Join(dir, "orphan.raw"), randomData(t, 128<<10, 63), 0o644); err != nil {
			t.Fatalf("write orphan blob: %v", err)
		}
		runGit(t, dir, env, "add", ".")
		runGit(t, dir, env, "commit", "-m", "add orphan blob")
		runGit(t, dir, env, "push", "origin", "main")
		all, kept = gitObjects(t, dir, "HEAD"), gitObjects(t, dir, "HEAD~2")
		dropPointer := strings.TrimSpace(runGit(t, dir, env, "rev-parse", "HEAD~1:"+transferMatrixFile))
		orphan := strings.TrimSpace(runGit(t, dir, env, "rev-parse", "HEAD:orphan.raw"))
		garbage := maps.Clone(all)
		maps.DeleteFunc(garbage, func(hash string, _ int64) bool { _, ok := kept[hash]; return ok })
		for _, size := range garbage {
			wantBytes += size
		}
		wantDeleted = len(garbage)
		if _, ok := garbage[dropPointer]; !ok || garbage[orphan] != 128<<10 || len(garbage) != 6 || len(kept) != 6 {
			t.Fatalf("garbage = %v, kept = %v; want 6 objects each, garbage holding pointer %s and the 128 KiB blob %s", garbage, kept, dropPointer, orphan)
		}
		runGit(t, dir, env, "reset", "--hard", "HEAD~2")
		runGit(t, dir, env, "push", "--force", "origin", "main")
		if got := mustGet(t, s.httpURL+"/"+repoID+"/resolve/main/"+transferMatrixFile); !bytes.Equal(got, keepData) {
			t.Fatalf("main resolve after force push: got %d bytes, want the retained %d bytes", len(got), len(keepData))
		}
		assertGCNotFound(t, s.httpURL+"/"+repoID+"/resolve/main/orphan.raw")
		if stored := storedGitObjects(t, repos, repoPath); !maps.Equal(stored, all) {
			t.Fatalf("server stores %v, want every pushed object %v", stored, all)
		}
		// go-git repacks only around loose objects or several packs; native git always does.
		if loose, packs := gitObjectFiles(t, repos, repoPath); loose == 0 && packs <= 1 {
			t.Fatalf("objects/ holds %d loose objects and %d packs, want a layout the go-git repack acts on", loose, packs)
		}
		assertGCObjects(t, s.httpURL, keepObject, dropObject)
	})

	requirePrune := func(t *testing.T, label string, got gcResult, dryRun bool) {
		t.Helper()
		res := got.Prune
		// A preview measures no disk saving; the run must shrink objects/ by at least the 128 KiB orphan blob's worth.
		reclaimedOK := res.ReclaimedBytes == 0
		if !dryRun {
			reclaimedOK = res.ReclaimedBytes >= 100<<10
		}
		if res.DryRun != dryRun || res.Repositories != 1 || res.LiveObjects != 1 || res.SkippedInGrace != 0 || len(res.Failed) != 0 ||
			res.DeletedGitObjects != wantDeleted || res.DeletedGitBytes != wantBytes || !reclaimedOK || !slices.Equal(res.Unlinked, []string{dropOID}) {
			t.Fatalf("%s = %+v, want dry_run=%t, 1 repository, 1 live object, %d Git objects (%d bytes) deleted, at least 100 KiB reclaimed unless dry run (then 0), [%s] unlinked, nothing failed or skipped",
				label, *res, dryRun, wantDeleted, wantBytes, dropOID)
		}
		if !got.Done || got.Errors != nil || got.Storage.DryRun != dryRun {
			t.Fatalf("%s: GC = %+v, storage = %+v, want a finished step without errors, dry_run=%t", label, got, *got.Storage, dryRun)
		}
	}

	step("DryRun", func(t *testing.T) {
		before := snapshotRepository(t, repos, repoPath)
		first := postGC(t, s.httpURL, "dry_run=true")
		second := postGC(t, s.httpURL, "dry_run=true")
		requirePrune(t, "dry run", first, true)
		requirePrune(t, "repeated dry run", second, true)
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("repeated dry run = %+v, want %+v", second, first)
		}
		if !maps.Equal(before, snapshotRepository(t, repos, repoPath)) {
			t.Fatal("dry run changed the repository files")
		}
		if stored := storedGitObjects(t, repos, repoPath); !maps.Equal(stored, all) {
			t.Fatalf("server stores %v after dry run, want every pushed object %v", stored, all)
		}
		assertGCObjects(t, s.httpURL, keepObject, dropObject)
		for oid, data := range map[string][]byte{keepOID: keepData, dropOID: dropData} {
			if got := mustGet(t, s.httpURL+"/objects/"+oid); !bytes.Equal(got, data) {
				t.Fatalf("object %s after dry run: got %d bytes, want %d", oid, len(got), len(data))
			}
		}
	})

	step("Collect", func(t *testing.T) {
		res := postGC(t, s.httpURL)
		requirePrune(t, "GC", res, false)
		if st := res.Storage; st.SweptShards <= 0 || st.SweptXorbs <= 0 || st.ReclaimedBytes <= 0 || st.RemainingShards != 0 || st.RemainingXorbs != 0 {
			t.Fatalf("storage = %+v, want shards, xorbs and bytes reclaimed, nothing remaining", *st)
		}
		if stored := storedGitObjects(t, repos, repoPath); !maps.Equal(stored, kept) {
			t.Fatalf("server stores %v after GC, want exactly what main reaches %v", stored, kept)
		}
		if loose, packs := gitObjectFiles(t, repos, repoPath); loose != 0 || packs != 1 {
			t.Fatalf("objects/ holds %d loose objects and %d packs after GC, want one pack", loose, packs)
		}
		assertGCObjects(t, s.httpURL, keepObject)
		assertGCNotFound(t, s.httpURL+"/objects/"+dropOID)
		if got := mustGet(t, s.httpURL+"/objects/"+keepOID); !bytes.Equal(got, keepData) {
			t.Fatalf("retained object after GC: got %d bytes, want %d", len(got), len(keepData))
		}
		if got := mustGet(t, s.httpURL+"/"+repoID+"/resolve/main/"+transferMatrixFile); !bytes.Equal(got, keepData) {
			t.Fatalf("main resolve after GC: got %d bytes, want %d", len(got), len(keepData))
		}
	})

	step("CloneAfterPrune", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "after")
		runGit(t, "", env, "clone", remote, dir)
		runGit(t, dir, env, "fsck", "--strict")
		if got := gitObjects(t, dir, "HEAD"); !maps.Equal(got, kept) {
			t.Fatalf("fresh clone reaches %v, want %v", got, kept)
		}
		pointer, err := os.ReadFile(filepath.Join(dir, transferMatrixFile))
		if err != nil || !strings.Contains(string(pointer), "oid sha256:"+keepOID) {
			t.Fatalf("fresh clone %s = %q, %v; want the pointer to %s", transferMatrixFile, pointer, err, keepOID)
		}
	})

	step("Idempotent", func(t *testing.T) {
		for _, query := range []string{"dry_run=true", "dry_run=false"} {
			got := postGC(t, s.httpURL, query)
			res := got.Prune
			if res.Repositories != 1 || res.LiveObjects != 1 || res.DeletedGitObjects != 0 || res.DeletedGitBytes != 0 || res.ReclaimedBytes != 0 ||
				len(res.Failed) != 0 || len(res.Unlinked) != 0 || res.SkippedInGrace != 0 {
				t.Fatalf("GC?%s prune after GC = %+v, want 1 repository, 1 live object and nothing deleted, reclaimed or unlinked", query, *res)
			}
			if st := got.Storage; !got.Done || got.Errors != nil || st.SweptShards != 0 || st.SweptXorbs != 0 || st.ReclaimedBytes != 0 {
				t.Fatalf("GC?%s after GC = %+v, storage = %+v, want a finished step sweeping nothing", query, got, *st)
			}
		}
		if stored := storedGitObjects(t, repos, repoPath); !maps.Equal(stored, kept) {
			t.Fatalf("server stores %v after repeated prune, want %v", stored, kept)
		}
	})
}

// gitObjects maps every object rev reaches in the clone at dir to its payload size, by native git.
func gitObjects(t *testing.T, dir, rev string) map[string]int64 {
	t.Helper()
	list, _, err := gitCmd(t, dir, nil, "rev-list", "--objects", rev)
	if err != nil {
		t.Fatalf("rev-list --objects %s: %v", rev, err)
	}
	objects := map[string]int64{}
	for line := range strings.SplitSeq(strings.TrimSpace(list), "\n") {
		hash, _, _ := strings.Cut(line, " ")
		out, _, err := gitCmd(t, dir, nil, "cat-file", "-s", hash)
		if err != nil {
			t.Fatalf("cat-file -s %s: %v", hash, err)
		}
		size, err := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
		if err != nil {
			t.Fatalf("cat-file -s %s = %q: %v", hash, out, err)
		}
		objects[hash] = size
	}
	return objects
}

// storedGitObjects maps the objects the bare repository at repoPath on fs holds to their payload sizes through a fresh go-git storer;
// on the host filesystem native git must list the same.
func storedGitObjects(t *testing.T, fs billy.Filesystem, repoPath string) map[string]int64 {
	t.Helper()
	st := filesystem.NewStorage(chroot.New(fs, repoPath), cache.NewObjectLRUDefault())
	defer st.Close()
	iter, err := st.IterEncodedObjects(plumbing.AnyObject)
	if err != nil {
		t.Fatalf("iterate objects of %s: %v", repoPath, err)
	}
	objects := map[string]int64{}
	err = iter.ForEach(func(o plumbing.EncodedObject) error {
		objects[o.Hash().String()] = o.Size()
		return nil
	})
	if err != nil {
		t.Fatalf("iterate objects of %s: %v", repoPath, err)
	}
	host, ok := fs.(*osfs.BoundOS)
	if !ok {
		return objects
	}
	out, _, err := gitCmd(t, filepath.Join(host.Root(), repoPath), nil, "cat-file", "--batch-all-objects", "--batch-check=%(objectname) %(objectsize)")
	if err != nil {
		t.Fatalf("cat-file --batch-all-objects: %v", err)
	}
	native := map[string]int64{}
	for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		hash, size, _ := strings.Cut(line, " ")
		n, err := strconv.ParseInt(size, 10, 64)
		if err != nil {
			t.Fatalf("cat-file --batch-all-objects line %q: %v", line, err)
		}
		native[hash] = n
	}
	if !maps.Equal(objects, native) {
		t.Fatalf("go-git lists %v, native git lists %v", objects, native)
	}
	return objects
}

// gitObjectFiles counts the loose objects and packfiles under objects/ of the bare repository at repoPath on fs.
func gitObjectFiles(t *testing.T, fs billy.Filesystem, repoPath string) (loose, packs int) {
	t.Helper()
	err := util.Walk(fs, filepath.Join(repoPath, "objects"), func(name string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		switch {
		case strings.HasSuffix(name, ".pack"):
			packs++
		case len(filepath.Base(filepath.Dir(name))) == 2 && len(info.Name()) == 38:
			loose++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk objects of %s: %v", repoPath, err)
	}
	return loose, packs
}

// gitFileState pins one repository file for byte-for-byte comparisons across a preview.
type gitFileState struct {
	size     int64
	modified int64
	digest   [sha256.Size]byte
}

// snapshotRepository records every file under repoPath on fs.
func snapshotRepository(t *testing.T, fs billy.Filesystem, repoPath string) map[string]gitFileState {
	t.Helper()
	files := map[string]gitFileState{}
	err := util.Walk(fs, repoPath, func(name string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		content, err := util.ReadFile(fs, name)
		if err != nil {
			return err
		}
		files[name] = gitFileState{size: info.Size(), modified: info.ModTime().UnixNano(), digest: sha256.Sum256(content)}
		return nil
	})
	if err != nil {
		t.Fatalf("snapshot %s: %v", repoPath, err)
	}
	return files
}
