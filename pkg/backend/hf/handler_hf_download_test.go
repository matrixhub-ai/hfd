package hf

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	xetclient "github.com/wzshiming/xet/client"
	xetmirror "github.com/wzshiming/xet/mirror"
	xetserver "github.com/wzshiming/xet/server"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// newXETDataPlane assembles the xet data-plane pieces the way cmd/hfd does —
// file storage, client, token scheme, and the ingest engine when upstreamURL
// is set — and builds the mirror over them, returning the mirror and the
// CAS-server composition. wrap, when set, decorates the storage everything
// is built over.
func newXETDataPlane(t *testing.T, upstreamURL string, wrap func(xetstorage.Storage) xetstorage.Storage) (*mirror.Mirror, http.Handler) {
	t.Helper()
	st := newStorage(t, newXETDataDir(t))
	client, err := xetclient.NewClient(xetclient.WithCacheDir(filepath.Join(st.XETDir(), "chunks")))
	if err != nil {
		t.Fatalf("create xet client: %v", err)
	}
	xs := st.XETStorage()
	var wrapped xetstorage.Storage = xs
	if wrap != nil {
		wrapped = wrap(xs)
	}
	issuer, err := auth.NewIssuer(nil, time.Hour, nil)
	if err != nil {
		t.Fatalf("create issuer: %v", err)
	}
	var engine *xetmirror.Mirror
	if upstreamURL != "" {
		engine, err = xetmirror.NewMirror(
			xetmirror.WithStorage(wrapped),
			xetmirror.WithUpstream(upstreamURL),
			xetmirror.WithCacheDir(filepath.Join(st.XETDir(), "mirror")),
			xetmirror.WithClient(client),
		)
		if err != nil {
			t.Fatalf("create xet mirror engine: %v", err)
		}
	}
	cas := xetserver.NewHandler(
		xetserver.WithStorage(wrapped),
		xetserver.WithAuthorizer(issuer),
		xetserver.WithNext(http.NotFoundHandler()),
	)
	m, err := mirror.NewMirror(
		mirror.WithXETStorage(wrapped),
		mirror.WithXETClient(client),
		mirror.WithXETMirror(engine),
		mirror.WithDataDir(st.XETDir()),
		mirror.WithMintToken(issuer.Sign),
	)
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}
	// Background work must not outlive the temp data dir.
	t.Cleanup(m.Wait)
	return m, cas
}

// newXETDataDir returns a data dir removed best-effort: the engine's ingest
// finalize can outlive the test body, and strict t.TempDir cleanup races it.
func newXETDataDir(t *testing.T) string {
	t.Helper()
	dataDir, err := os.MkdirTemp("", "hf-xet-data")
	if err != nil {
		t.Fatalf("create xet data dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	return dataDir
}

// newLFSRepo creates org/repo in the storage with the given LFS pointer
// files committed on main, returning the storage and the head commit hash.
func newLFSRepo(t *testing.T, pointers map[string]string) (*storage.Storage, string) {
	t.Helper()
	st := newStorage(t, t.TempDir())
	repo, err := repository.Init(context.Background(), st.RepositoriesFS(), repository.ResolvePath("org/repo"), "main")
	if err != nil {
		t.Fatalf("init repo: %v", err)
	}
	var head string
	for file, content := range pointers {
		head, err = repo.CreateCommit(context.Background(), "main", "commit "+file, "Test", "test@test.com",
			[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: file, Content: []byte(content)}}, "")
		if err != nil {
			t.Fatalf("create commit: %v", err)
		}
	}
	return st, head
}

func hfLFSPointerText(oid string, size int) string {
	return fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, size)
}

// TestResolveLFSRedirectsIngested pins hub parity on the resolve route:
// fully ingested files answer with metadata plus a redirect to the sha256
// bridge, and un-ingested files are 404 without an ingest engine.
func TestResolveLFSRedirectsIngested(t *testing.T) {
	ctx := context.Background()

	data := bytes.Repeat([]byte("hf resolve redirect bytes. "), 2048)
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])
	missingOID := strings.Repeat("a", 64)

	st, _ := newLFSRepo(t, map[string]string{
		"model.bin":   hfLFSPointerText(oid, len(data)),
		"missing.bin": hfLFSPointerText(missingOID, 5),
	})

	m, cas := newXETDataPlane(t, "", nil)
	if err := m.PutObject(ctx, oid, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("put object: %v", err)
	}

	h := NewHandler(WithStorage(st), WithMirror(m))

	t.Run("Redirect", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/org/repo/resolve/main/model.bin", nil))
		if rec.Code != http.StatusFound {
			t.Fatalf("status = %d, want 302", rec.Code)
		}
		hd := rec.Result().Header
		loc := hd.Get("Location")
		if !strings.HasSuffix(loc, "/xet-bridge/"+oid) {
			t.Fatalf("Location = %q, want suffix /xet-bridge/%s", loc, oid)
		}
		if !strings.HasPrefix(loc, "http") {
			t.Fatalf("Location = %q, want absolute URL", loc)
		}
		if got := hd.Get("X-Linked-Etag"); got != `"`+oid+`"` {
			t.Fatalf("X-Linked-Etag = %q", got)
		}
		if got := hd.Get("X-Linked-Size"); got != fmt.Sprint(len(data)) {
			t.Fatalf("X-Linked-Size = %q, want %d", got, len(data))
		}
		if hd.Get("X-Repo-Commit") == "" {
			t.Fatal("X-Repo-Commit not set")
		}
		fileHash := hd.Get("X-Xet-Hash")
		wantLink := fmt.Sprintf("<http://example.com/api/models/org/repo/xet-read-token/main>; rel=\"xet-auth\", <http://example.com/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\"", fileHash)
		if got := hd.Get("Link"); fileHash == "" || got != wantLink {
			t.Fatalf("Link = %q, want %q", got, wantLink)
		}

		bridge := httptest.NewRecorder()
		cas.ServeHTTP(bridge, httptest.NewRequest(http.MethodGet, "/xet-bridge/"+oid, nil))
		if bridge.Code != http.StatusOK {
			t.Fatalf("bridge status = %d, want 200", bridge.Code)
		}
		if !bytes.Equal(bridge.Body.Bytes(), data) {
			t.Fatal("bridge bytes mismatch")
		}
	})

	t.Run("MissWithoutEngine", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/org/repo/resolve/main/missing.bin", nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", rec.Code)
		}
	})

	// huggingface_hub >= 1.30 follows same-host redirects on the metadata
	// HEAD, so the probe must carry the metadata itself instead of a 302.
	t.Run("HeadAnswersMetadata", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/org/repo/resolve/main/model.bin", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		hd := rec.Result().Header
		if hd.Get("Location") != "" {
			t.Fatalf("Location = %q, want none", hd.Get("Location"))
		}
		if hd.Get("X-Repo-Commit") == "" || hd.Get("X-Xet-Hash") == "" {
			t.Fatalf("metadata headers missing: X-Repo-Commit=%q X-Xet-Hash=%q", hd.Get("X-Repo-Commit"), hd.Get("X-Xet-Hash"))
		}
		if got := hd.Get("X-Linked-Etag"); got != `"`+oid+`"` {
			t.Fatalf("X-Linked-Etag = %q", got)
		}
		if got, want := hd.Get("X-Linked-Size"), fmt.Sprint(len(data)); got != want || hd.Get("Content-Length") != want {
			t.Fatalf("X-Linked-Size = %q, Content-Length = %q, want %s", got, hd.Get("Content-Length"), want)
		}
	})
}

// zeroSizeStorage fakes the index entry for one OID whose reconstruction is
// empty. The upload pipeline never indexes zero-entry files, so the handler's
// zero-size answer cannot be reached with bytes ingested through it.
type zeroSizeStorage struct {
	xetstorage.Storage
	digest [32]byte
}

type emptyFile struct{ *bytes.Reader }

func (emptyFile) Close() error { return nil }

func (s zeroSizeStorage) GetFileHashBySHA256(ctx context.Context, ns string, digest [32]byte) (xet.FileHash, error) {
	if digest == s.digest {
		return xet.FileHash{}, nil
	}
	return s.Storage.GetFileHashBySHA256(ctx, ns, digest)
}

func (s zeroSizeStorage) GetReconstructedFile(ctx context.Context, ns string, digest [32]byte) (io.ReadSeekCloser, error) {
	if digest == s.digest {
		return emptyFile{bytes.NewReader(nil)}, nil
	}
	return s.Storage.GetReconstructedFile(ctx, ns, digest)
}

// TestResolveLFSZeroSize pins the hub-parity answer for ingested zero-size
// files: 200 with an explicit zero Content-Length instead of a redirect.
func TestResolveLFSZeroSize(t *testing.T) {
	emptySum := sha256.Sum256(nil)
	emptyOID := hex.EncodeToString(emptySum[:])

	st, _ := newLFSRepo(t, map[string]string{
		"empty.bin": hfLFSPointerText(emptyOID, 0),
	})

	m, _ := newXETDataPlane(t, "", func(xs xetstorage.Storage) xetstorage.Storage {
		return zeroSizeStorage{Storage: xs, digest: emptySum}
	})

	h := NewHandler(WithStorage(st), WithMirror(m))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/org/repo/resolve/main/empty.bin", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Result().Header.Get("Content-Length"); got != "0" {
		t.Fatalf("Content-Length = %q, want 0", got)
	}
	if got := rec.Result().Header.Get("X-Linked-Size"); got != "0" {
		t.Fatalf("X-Linked-Size = %q, want 0", got)
	}
}

// TestResolveLFSStreamsFromEngine pins the ingest-on-miss path: a resolve
// for a not-yet-ingested object registers it and streams the upstream bytes
// straight off the ingest engine while they land in storage.
func TestResolveLFSStreamsFromEngine(t *testing.T) {
	data := []byte("hf resolve engine streaming bytes")
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])

	st, _ := newLFSRepo(t, map[string]string{
		"model.bin": hfLFSPointerText(oid, len(data)),
	})

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/resolve/") || !strings.HasSuffix(r.URL.Path, "/model.bin") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("ETag", `"`+oid+`"`)
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write(data)
	}))
	t.Cleanup(upstream.Close)

	m, _ := newXETDataPlane(t, upstream.URL, nil)

	h := NewHandler(WithStorage(st), WithMirror(m))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/org/repo/resolve/main/model.bin", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("body = %q, want upstream bytes", rec.Body.String())
	}
	if got := rec.Result().Header.Get("X-Linked-Size"); got != fmt.Sprint(len(data)) {
		t.Fatalf("X-Linked-Size = %q, want %d", got, len(data))
	}
}

// TestTreeDirectoryEntries pins the hub shape of the tree route across repo
// types: directory entries are {type: directory, path, oid, size: 0} listed
// in pre-order with no blanks, expand carries the directory's own last
// commit, malformed boolean flags are 400 before any hook runs, and a file
// path is 404.
func TestTreeDirectoryEntries(t *testing.T) {
	ctx := context.Background()
	st := storage.NewStorage(storage.WithRootDir(t.TempDir()))
	pointer := hfLFSPointerText(strings.Repeat("e", 64), 12345)
	commit := func(repo *repository.Repository, msg string, files map[string]string) string {
		t.Helper()
		var ops []repository.CommitOperation
		for p, content := range files {
			ops = append(ops, repository.CommitOperation{Type: repository.CommitOperationAdd, Path: p, Content: []byte(content)})
		}
		sha, err := repo.CreateCommit(ctx, "main", msg, "Test", "test@test.com", ops, "")
		if err != nil {
			t.Fatalf("commit %s: %v", msg, err)
		}
		return sha
	}
	repos := map[string]*repository.Repository{}
	shas := map[string][]string{}
	for _, name := range []string{"org/repo", "datasets/org/repo", "spaces/org/repo", "org/empty"} {
		repo, err := repository.Init(ctx, st.RepositoriesFS(), repository.ResolvePath(name), "main")
		if err != nil {
			t.Fatalf("init %s: %v", name, err)
		}
		repos[name] = repo
		shas[name] = []string{
			commit(repo, "Add README", map[string]string{"README.md": "r"}),
			commit(repo, "Add docs", map[string]string{"docs/a.txt": "aa", "docs/guide/intro.txt": "intro"}),
			commit(repo, "Add model", map[string]string{"model.bin": pointer}),
		}
	}
	if _, err := repos["org/empty"].CreateCommit(ctx, "main", "Empty", "Test", "test@test.com", []repository.CommitOperation{
		{Type: repository.CommitOperationDelete, Path: "README.md"},
		{Type: repository.CommitOperationDelete, Path: "docs/a.txt"},
		{Type: repository.CommitOperationDelete, Path: "docs/guide/intro.txt"},
		{Type: repository.CommitOperationDelete, Path: "model.bin"},
	}, ""); err != nil {
		t.Fatalf("empty commit: %v", err)
	}

	var checks, opened []string
	h := NewHandler(WithStorage(st),
		WithPermissionHookFunc(func(_ context.Context, op permission.Operation, name string, _ permission.Context) (bool, error) {
			checks = append(checks, op.String()+" "+name)
			return true, nil
		}),
		WithPreOpenHookFunc(func(_ context.Context, name string, _ bool) error {
			opened = append(opened, name)
			return nil
		}))
	get := func(t *testing.T, target string) *httptest.ResponseRecorder {
		t.Helper()
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
		return rec
	}
	list := func(t *testing.T, target string) []map[string]any {
		t.Helper()
		rec := get(t, target)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s: %d %s", target, rec.Code, rec.Body)
		}
		var items []map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &items); err != nil {
			t.Fatalf("GET %s: decode %v: %s", target, err, rec.Body)
		}
		return items
	}
	pathsOf := func(items []map[string]any) []string {
		var paths []string
		for _, item := range items {
			paths = append(paths, fmt.Sprint(item["path"]))
		}
		return paths
	}

	for apiType, name := range map[string]string{"models": "org/repo", "datasets": "datasets/org/repo", "spaces": "spaces/org/repo"} {
		want, err := repos[name].Tree("main", "", nil)
		if err != nil {
			t.Fatalf("Tree %s: %v", name, err)
		}
		got := list(t, "/api/"+apiType+"/org/repo/tree/main")
		if len(got) != 3 || len(want) != 3 {
			t.Fatalf("%s: %d entries %v, want 3", apiType, len(got), got)
		}
		for i, e := range want {
			if item := got[i]; item["path"] != e.Path() || item["oid"] != e.Hash().String() || item["type"] != string(e.Type()) {
				t.Errorf("%s entry %d = %v, want %s %s %s", apiType, i, item, e.Type(), e.Path(), e.Hash())
			}
		}
		if readme := got[0]; readme["type"] != "file" || readme["size"] != float64(1) || readme["lfs"] != nil {
			t.Errorf("%s README %v", apiType, readme)
		}
		if docs := got[1]; docs["type"] != "directory" || docs["path"] != "docs" || docs["size"] != float64(0) || docs["lfs"] != nil || docs["lastCommit"] != nil {
			t.Errorf("%s docs %v", apiType, docs)
		}
		bin := got[2]
		if lfs, _ := bin["lfs"].(map[string]any); bin["size"] != float64(12345) || lfs == nil || lfs["oid"] != strings.Repeat("e", 64) || lfs["size"] != float64(12345) || lfs["pointerSize"] != float64(len(pointer)) {
			t.Errorf("%s model.bin %v", apiType, bin)
		}
	}
	if rec := get(t, "/api/models/org/empty/tree/main"); rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Errorf("empty tree: %d %s", rec.Code, rec.Body)
	}

	all := []string{"README.md", "docs", "docs/a.txt", "docs/guide", "docs/guide/intro.txt", "model.bin"}
	for target, want := range map[string][]string{
		"/api/models/org/repo/tree/main?recursive=true":      all,
		"/api/models/org/repo/tree/main/docs?recursive=1":    all[2:5],
		"/api/models/org/repo/tree/main/docs?recursive=True": all[2:5],
		"/api/models/org/repo/tree/main/docs":                {"docs/a.txt", "docs/guide"},
		"/api/models/org/repo/tree/main?recursive=false":     {"README.md", "docs", "model.bin"},
		"/api/models/org/repo/tree/main?recursive=":          {"README.md", "docs", "model.bin"},
	} {
		if got := pathsOf(list(t, target)); !slices.Equal(got, want) {
			t.Errorf("%s: paths %v, want %v", target, got, want)
		}
	}

	entries, err := repos["org/repo"].Tree("main", "", &repository.TreeOptions{Recursive: true})
	if err != nil {
		t.Fatalf("Tree: %v", err)
	}
	sha := shas["org/repo"]
	wantCommit := map[string]string{"README.md": sha[0], "docs": sha[1], "docs/a.txt": sha[1], "docs/guide": sha[1], "docs/guide/intro.txt": sha[1], "model.bin": sha[2]}
	expanded := list(t, "/api/models/org/repo/tree/main?recursive=true&expand=true")
	if got := pathsOf(expanded); !slices.Equal(got, all) {
		t.Fatalf("expanded paths %v, want %v", got, all)
	}
	for i, e := range entries {
		item := expanded[i]
		lc, _ := item["lastCommit"].(map[string]any)
		if item["oid"] != e.Hash().String() || lc == nil || lc["id"] != wantCommit[e.Path()] || lc["date"] != e.LastCommit().Author().When().UTC().Format(repository.TimeFormat) {
			t.Errorf("%s expanded %v, want oid %s lastCommit %s", e.Path(), item, e.Hash(), wantCommit[e.Path()])
		}
	}
	if lc, _ := expanded[1]["lastCommit"].(map[string]any); lc["title"] != "Add docs" {
		t.Errorf("docs lastCommit %v", lc)
	}

	checks, opened = nil, nil
	for _, target := range []string{"/api/models/org/repo/tree/main?recursive=maybe", "/api/models/org/repo/tree/main?expand=2", "/api/models/org/repo/tree/main?recursive=true&expand=yes"} {
		if rec := get(t, target); rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `"error"`) {
			t.Errorf("GET %s: %d %s", target, rec.Code, rec.Body)
		}
	}
	if len(checks) != 0 || len(opened) != 0 {
		t.Errorf("invalid flags reached checks %v opened %v", checks, opened)
	}
	for _, target := range []string{"/api/models/org/repo/tree/main/README.md", "/api/models/org/repo/tree/main/nope", "/api/models/org/repo/tree/main/docs/nope"} {
		if rec := get(t, target); rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), `"error"`) {
			t.Errorf("GET %s: %d %s", target, rec.Code, rec.Body)
		}
	}
	if !slices.Contains(checks, "read_repo org/repo") || !slices.Contains(opened, "org/repo") {
		t.Errorf("checks %v opened %v", checks, opened)
	}
}
