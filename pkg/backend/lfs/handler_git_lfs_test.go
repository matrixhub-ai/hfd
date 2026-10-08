package lfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/osfs"
	"github.com/wzshiming/xet/auth"
	xetclient "github.com/wzshiming/xet/client"
	xetmirror "github.com/wzshiming/xet/mirror"
	xetserver "github.com/wzshiming/xet/server"
	xetshard "github.com/wzshiming/xet/shard"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// newXETDataPlane assembles the xet data-plane pieces the way cmd/hfd does —
// file storage, client options, token scheme, and the ingest engine fed by
// hubURL when set — and builds the mirror over them with gitOpts appended,
// returning the mirror and the CAS-server composition.
func newXETDataPlane(t *testing.T, hubURL string, gitOpts ...mirror.Option) (*mirror.Mirror, http.Handler) {
	t.Helper()
	st, err := storage.NewStorage(storage.WithRootDir(t.TempDir()))
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	xs := st.XETStorage()
	issuer, err := auth.NewIssuer(nil, time.Hour, nil)
	if err != nil {
		t.Fatalf("create issuer: %v", err)
	}
	opts := []mirror.Option{
		mirror.WithXETStorage(xs),
		mirror.WithXETCache(xetclient.NewCache(filepath.Join(st.XETDir(), "chunks"), 0, 0)),
		mirror.WithDataDir(st.XETDir()),
		mirror.WithMintToken(issuer.Sign),
	}
	if hubURL != "" {
		engine, err := xetmirror.NewMirror(
			xetmirror.WithStorage(xs),
			xetmirror.WithCacheDir(filepath.Join(st.XETDir(), "mirror")),
		)
		if err != nil {
			t.Fatalf("create xet mirror engine: %v", err)
		}
		opts = append(opts,
			mirror.WithXETMirror(engine),
			mirror.WithMirrorSourceFunc(func(ctx context.Context, repoName string) (string, bool, error) {
				return hubURL + "/" + repoName, true, nil
			}),
		)
	}
	cas := xetserver.NewHandler(
		xetserver.WithStorage(xs),
		xetserver.WithAuthorizer(issuer),
		xetserver.WithNext(http.NotFoundHandler()),
	)
	m, err := mirror.NewMirror(append(opts, gitOpts...)...)
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}
	// Background prefetches must not outlive the temp data dir.
	t.Cleanup(m.Wait)
	return m, cas
}

// initSourceRepo creates a bare repo with one commit on main. The path ends
// with .git because InitMirror normalizes source URLs to a .git suffix.
func initSourceRepo(t *testing.T, root, name string) (*repository.Repository, string) {
	t.Helper()
	path := filepath.Join(root, name+".git")
	repo, err := repository.Init(context.Background(), osfs.Default, path, "main")
	if err != nil {
		t.Fatalf("init source repo: %v", err)
	}
	addCommit(t, repo, "main", "README.md", "# src\n")
	return repo, path
}

func addCommit(t *testing.T, repo *repository.Repository, rev, file, content string) string {
	t.Helper()
	hash, err := repo.CreateCommit(context.Background(), rev, "commit "+file, "Test", "test@test.com",
		[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: file, Content: []byte(content)}}, "")
	if err != nil {
		t.Fatalf("create commit: %v", err)
	}
	return hash
}

func lfsPointerText(oid string, size int) string {
	return fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, size)
}

func TestBatchXETToken(t *testing.T) {
	issuer, err := auth.NewIssuer(nil, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	m, _ := newXETDataPlane(t, "", mirror.WithMintToken(func(grant auth.Grant) (string, int64, error) {
		calls++
		return issuer.Sign(grant)
	}))
	h := NewHandler(WithMirror(m))
	oids := []string{strings.Repeat("a", 64), strings.Repeat("b", 64)}
	body := fmt.Sprintf(`{"operation":"upload","transfers":["xet","basic"],"objects":[{"oid":%q,"size":1},{"oid":%q,"size":2},{"oid":"not-a-sha256","size":3}]}`, oids[0], oids[1])
	req := httptest.NewRequest(http.MethodPost, "/org/repo.git/info/lfs/objects/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", metaMediaType)
	req.Header.Set("Accept", metaMediaType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var batch lfsBatchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &batch); err != nil {
		t.Fatal(err)
	}
	if calls != 3 || batch.Transfer != "xet" || len(batch.Objects) != 3 {
		t.Fatalf("mint calls = %d, batch = %+v", calls, batch)
	}
	var previousToken string
	for index, oid := range oids {
		upload := batch.Objects[index].Actions["upload"]
		if upload == nil {
			t.Fatal("missing upload action")
		}
		token := upload.Header["X-Xet-Access-Token"]
		if grant, ok := issuer.Validate(token); !ok || grant != (auth.Grant{Permission: auth.Write, SHA256: oid}) {
			t.Fatalf("grant = %+v, valid = %v, want write bound to %s", grant, ok, oid)
		}
		if token == previousToken {
			t.Fatal("upload tokens must differ between objects")
		}
		previousToken = token
		if upload.Header["X-Xet-Cas-Url"] == "" || upload.Header["X-Xet-Token-Expiration"] == "" {
			t.Fatalf("missing CAS URL or token expiration: %+v", upload.Header)
		}
	}
	if object := batch.Objects[2]; object.Error != nil || object.Actions["upload"] == nil || object.Actions["upload"].Header["X-Xet-Access-Token"] != "" {
		t.Fatalf("malformed OID object = %+v, want a plain upload action without CAS credentials", object)
	}
}

func TestBatchXETTokenDenied(t *testing.T) {
	calls := 0
	m, _ := newXETDataPlane(t, "", mirror.WithMintToken(func(auth.Grant) (string, int64, error) {
		calls++
		return "", 0, nil
	}))
	deny := func(context.Context, permission.Operation, string, permission.Context) (bool, error) {
		return false, nil
	}
	h := NewHandler(WithMirror(m), WithPermissionHookFunc(deny))
	body := fmt.Sprintf(`{"operation":"upload","transfers":["xet","basic"],"objects":[{"oid":%q,"size":1}]}`, strings.Repeat("a", 64))
	req := httptest.NewRequest(http.MethodPost, "/org/repo.git/info/lfs/objects/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", metaMediaType)
	req.Header.Set("Accept", metaMediaType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden || calls != 0 || strings.Contains(rec.Body.String(), "X-Xet-Access-Token") {
		t.Fatalf("status = %d, mint calls = %d, body = %s; want 403 without minting", rec.Code, calls, rec.Body.String())
	}
}

func TestBatchXETTokenBoundToOID(t *testing.T) {
	ctx := context.Background()
	m, cas := newXETDataPlane(t, "")
	srv := httptest.NewServer(cas)
	defer srv.Close()
	h := NewHandler(WithMirror(m))
	dataA := bytes.Repeat([]byte("object A bytes. "), 1024)
	dataB := bytes.Repeat([]byte("object B bytes. "), 1024)
	sumA := sha256.Sum256(dataA)
	sumB := sha256.Sum256(dataB)
	oidA := hex.EncodeToString(sumA[:])
	oidB := hex.EncodeToString(sumB[:])
	body := fmt.Sprintf(`{"operation":"upload","transfers":["xet","basic"],"objects":[{"oid":%q,"size":%d}]}`, oidA, len(dataA))
	req := httptest.NewRequest(http.MethodPost, "/org/repo.git/info/lfs/objects/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", metaMediaType)
	req.Header.Set("Accept", metaMediaType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var batch lfsBatchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &batch); err != nil {
		t.Fatal(err)
	}
	if batch.Transfer != "xet" || len(batch.Objects) != 1 {
		t.Fatalf("batch = %+v, want xet with one object", batch)
	}
	upload := batch.Objects[0].Actions["upload"]
	if upload == nil {
		t.Fatal("missing upload action")
	}
	token := upload.Header["X-Xet-Access-Token"]
	if token == "" {
		t.Fatal("missing CAS token")
	}
	xc, err := xetclient.NewClient(xetclient.WithCache(xetclient.NewCache(t.TempDir(), 0, 0)), xetclient.WithUpstreamProvider(xetclient.StaticUpstreamProvider(srv.URL, token)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := xc.UploadFile(ctx, bytes.NewReader(dataB)); err == nil {
		t.Fatal("upload of other content succeeded with token bound to object A")
	}
	if m.HasObject(ctx, oidB) {
		t.Fatal("object B landed in storage with token bound to object A")
	}
	if _, err := xc.UploadFile(ctx, bytes.NewReader(dataA)); err != nil {
		t.Fatalf("upload object A: %v", err)
	}
	if !m.HasObject(ctx, oidA) {
		t.Fatal("object A missing from storage after upload")
	}
}

func TestGetContentServesIngested(t *testing.T) {
	m, _ := newXETDataPlane(t, "")

	data := bytes.Repeat([]byte("lfs object content bytes. "), 2048)
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])
	if err := m.PutObject(context.Background(), oid, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("put object: %v", err)
	}

	h := NewHandler(WithMirror(m))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/objects/"+oid, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatal("served bytes mismatch")
	}
	hd := rec.Result().Header
	if got := hd.Get("X-Linked-Etag"); got != `"`+oid+`"` {
		t.Fatalf("X-Linked-Etag = %q", got)
	}
	if got := hd.Get("X-Linked-Size"); got != fmt.Sprint(len(data)) {
		t.Fatalf("X-Linked-Size = %q, want %d", got, len(data))
	}
	if link := hd.Get("Link"); link != "" {
		t.Fatalf("Link = %q, want empty", link)
	}
	if hash := hd.Get("X-Xet-Hash"); hash != "" {
		t.Fatalf("X-Xet-Hash = %q, want empty", hash)
	}

	t.Run("Range", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/objects/"+oid, nil)
		req.Header.Set("Range", "bytes=0-9")
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusPartialContent {
			t.Fatalf("range status = %d, want 206", rec.Code)
		}
		if !bytes.Equal(rec.Body.Bytes(), data[:10]) {
			t.Fatal("range bytes mismatch")
		}
	})

	t.Run("Miss", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/objects/"+strings.Repeat("0", 64), nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("miss status = %d, want 404", rec.Code)
		}
	})
}

// TestGetContentStreamsWhileIngesting pins the pull-scan streaming path:
// objects the mirror knows but has not fully ingested stream from the
// in-flight ingest spool on this response. The hub gates the ingest download
// halfway so the object cannot become fully ingested before the request is
// answered.
func TestGetContentStreamsWhileIngesting(t *testing.T) {
	root := t.TempDir()
	src, srcPath := initSourceRepo(t, root, "src")

	data := bytes.Repeat([]byte("streaming ingest bytes. "), 2048)
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])
	addCommit(t, src, "main", "weights.bin", lfsPointerText(oid, len(data)))

	gateHit := make(chan struct{})
	gate := make(chan struct{})
	var hitOnce sync.Once
	openGate := sync.OnceFunc(func() { close(gate) })
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/resolve/") || !strings.HasSuffix(r.URL.Path, "/weights.bin") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("ETag", `"`+oid+`"`)
		w.Header().Set("X-Linked-Etag", `"`+oid+`"`)
		w.Header().Set("X-Linked-Size", fmt.Sprint(len(data)))
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			return
		}
		// Resumed ranged fetches after the gate opens serve normally.
		if r.Header.Get("Range") != "" {
			http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		half := len(data) / 2
		_, _ = w.Write(data[:half])
		w.(http.Flusher).Flush()
		hitOnce.Do(func() { close(gateHit) })
		<-gate
		_, _ = w.Write(data[half:])
	}))
	t.Cleanup(hub.Close)

	m, _ := newXETDataPlane(t, hub.URL)
	// Registered after m.Wait and hub.Close so it runs first (LIFO): a test
	// failing early must unblock the gated hub handler before those waits.
	t.Cleanup(openGate)

	destPath := filepath.Join(root, "dest.git")
	if err := m.PullFromRemote(context.Background(), destPath, "org/repo", &mirror.PullOptions{SourceURL: srcPath}); err != nil {
		t.Fatalf("pull from remote: %v", err)
	}

	// The prefetch ingest is stalled halfway at the hub gate, so the object
	// is provably not fully ingested when the request lands.
	select {
	case <-gateHit:
	case <-time.After(30 * time.Second):
		t.Fatal("ingest never reached the hub gate")
	}

	h := NewHandler(WithMirror(m))

	// Open the gate only once the response starts, after the handler has
	// committed to the hub streaming path.
	rec := httptest.NewRecorder()
	h.ServeHTTP(&openOnWrite{ResponseRecorder: rec, open: openGate},
		httptest.NewRequest(http.MethodGet, "/objects/"+oid, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (spool streaming path)", rec.Code)
	}
	if !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatal("served bytes mismatch")
	}
}

// openOnWrite runs open when the response is first written, marking the
// moment the handler has committed to a serving path.
type openOnWrite struct {
	*httptest.ResponseRecorder
	open func()
}

func (g *openOnWrite) WriteHeader(code int) {
	g.open()
	g.ResponseRecorder.WriteHeader(code)
}

func (g *openOnWrite) Write(p []byte) (int, error) {
	g.open()
	return g.ResponseRecorder.Write(p)
}

// gatedShardStorage holds shard writes until gate closes, so an accepted upload cannot finish its ingest before the test allows it.
type gatedShardStorage struct {
	xetstorage.Storage
	gate <-chan struct{}
}

func (g *gatedShardStorage) PutShard(ctx context.Context, s *xetshard.Shard) (bool, error) {
	select {
	case <-g.gate:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	return g.Storage.PutShard(ctx, s)
}

// TestPutContentAcknowledgesBeforeIngest pins the basic-transfer contract: the PUT answers while the ingest is held, and the object is verifiable and downloadable meanwhile.
func TestPutContentAcknowledgesBeforeIngest(t *testing.T) {
	st, err := storage.NewStorage(storage.WithRootDir(t.TempDir()))
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	gate := make(chan struct{})
	m, err := mirror.NewMirror(
		mirror.WithXETStorage(&gatedShardStorage{Storage: st.XETStorage(), gate: gate}),
		mirror.WithXETCache(xetclient.NewCache(filepath.Join(st.XETDir(), "chunks"), 0, 0)),
		mirror.WithDataDir(st.XETDir()),
	)
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}
	t.Cleanup(m.Wait)
	// Registered after m.Wait so it runs first (LIFO): a failing test must release the gate before waiting.
	openGate := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(openGate)

	h := NewHandler(WithMirror(m))
	data := bytes.Repeat([]byte("basic transfer upload bytes. "), 4096)
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])

	put := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/objects/"+oid, bytes.NewReader(data)))
		put <- rec
	}()
	select {
	case rec := <-put:
		if rec.Code != http.StatusOK {
			t.Fatalf("PUT status = %d: %s", rec.Code, rec.Body.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("PUT did not return while the ingest was held at the gate")
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/objects/"+oid+"/verify", strings.NewReader(fmt.Sprintf(`{"oid":%q,"size":%d}`, oid, len(data)))))
	if rec.Code != http.StatusOK {
		t.Fatalf("verify status = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	body := fmt.Sprintf(`{"operation":"download","objects":[{"oid":%q,"size":%d}]}`, oid, len(data))
	req := httptest.NewRequest(http.MethodPost, "/org/repo.git/info/lfs/objects/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", metaMediaType)
	req.Header.Set("Accept", metaMediaType)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("batch status = %d: %s", rec.Code, rec.Body.String())
	}
	var batch lfsBatchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &batch); err != nil {
		t.Fatal(err)
	}
	if len(batch.Objects) != 1 || batch.Objects[0].Error != nil || batch.Objects[0].Actions["download"] == nil {
		t.Fatalf("batch = %s, want a download action for the pending object", rec.Body.String())
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/objects/"+oid, nil))
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("GET status = %d, %d bytes; want 200 with the payload", rec.Code, rec.Body.Len())
	}
	if got := rec.Result().Header.Get("X-Linked-Size"); got != fmt.Sprint(len(data)) {
		t.Fatalf("X-Linked-Size = %q, want %d", got, len(data))
	}

	openGate()
	m.Wait()
	if !m.HasObject(context.Background(), oid) || m.FileHash(context.Background(), oid) == "" {
		t.Fatal("object not stored after the ingest finished")
	}
}

// The zero-length object, which xet never indexes, must still round-trip: PUT, verify and GET all answer 200.
func TestPutContentEmptyObjectRoundTrip(t *testing.T) {
	st, err := storage.NewStorage(storage.WithRootDir(t.TempDir()))
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	m, err := mirror.NewMirror(
		mirror.WithXETStorage(st.XETStorage()),
		mirror.WithXETCache(xetclient.NewCache(filepath.Join(st.XETDir(), "chunks"), 0, 0)),
		mirror.WithDataDir(st.XETDir()),
	)
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}
	t.Cleanup(m.Wait)
	h := NewHandler(WithMirror(m))
	sum := sha256.Sum256(nil)
	oid := hex.EncodeToString(sum[:])

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/objects/"+oid, bytes.NewReader(nil)))
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT status = %d: %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/objects/"+oid+"/verify", strings.NewReader(fmt.Sprintf(`{"oid":%q,"size":0}`, oid))))
	if rec.Code != http.StatusOK {
		t.Fatalf("verify status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/objects/"+oid, nil))
	hd := rec.Result().Header
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 || hd.Get("X-Linked-Size") != "0" || hd.Get("Content-Length") != "0" {
		t.Fatalf("GET status = %d, %d bytes, headers %v; want 200 with an empty body", rec.Code, rec.Body.Len(), hd)
	}
}
