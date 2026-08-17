package mirror_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	xetclient "github.com/wzshiming/xet/client"

	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/permission"
)

// fakeHub answers hub-style resolve requests for a single LFS object with the
// metadata headers the xet mirror probes for.
func fakeHub(t *testing.T, filename string, data []byte, oid string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/resolve/") || !strings.HasSuffix(r.URL.Path, "/"+filename) {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("ETag", `"`+oid+`"`)
		w.Header().Set("X-Linked-Etag", `"`+oid+`"`)
		w.Header().Set("X-Linked-Size", fmt.Sprint(len(data)))
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write(data)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func lfsPointerText(oid string, size int) string {
	return fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, size)
}

func TestPullMirrorLFSDataPlane(t *testing.T) {
	root := t.TempDir()
	src, srcPath := initSourceRepo(t, root, "src")

	data := bytes.Repeat([]byte("hfd xet mirror data plane! "), 4096)
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])
	addCommit(t, src, "main", "model.bin", lfsPointerText(oid, len(data)))

	hub := fakeHub(t, "model.bin", data, oid)

	m := newMirror(t,
		mirror.WithMirrorSourceFunc(staticSource(srcPath)),
		mirror.WithUpstream(hub.URL),
	)

	destPath := filepath.Join(root, "dest.git")
	if err := m.PullFromRemote(context.Background(), destPath, "org/repo", nil); err != nil {
		t.Fatalf("pull from remote: %v", err)
	}

	ctx := context.Background()
	if !m.KnowsObject(ctx, oid) {
		t.Fatalf("object %s not known after pull scan", oid)
	}

	deadline := time.Now().Add(30 * time.Second)
	for !m.HasObject(ctx, oid) {
		if time.Now().After(deadline) {
			t.Fatalf("object %s was never ingested", oid)
		}
		time.Sleep(50 * time.Millisecond)
	}

	rs, size, err := m.OpenObject(ctx, oid)
	if err != nil {
		t.Fatalf("open object: %v", err)
	}
	defer rs.Close()
	if size != int64(len(data)) {
		t.Fatalf("object size = %d, want %d", size, len(data))
	}
	got, err := io.ReadAll(rs)
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("object bytes mismatch")
	}

	t.Run("ServeObject", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/ignored", nil)
		if !m.ServeObject(rec, req, oid) {
			t.Fatal("ServeObject reported not served")
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if !bytes.Equal(rec.Body.Bytes(), data) {
			t.Fatal("served bytes mismatch")
		}
	})

	t.Run("ServeResolveAndBridge", func(t *testing.T) {
		commit := refsAt(t, destPath)["refs/heads/main"]

		// The ready entry may publish slightly after the sha256 index lands.
		var rec *httptest.ResponseRecorder
		for {
			rec = httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/unused", nil)
			if !m.ServeResolve(rec, req, "org/repo", commit, "model.bin") {
				t.Fatal("ServeResolve reported not served")
			}
			if rec.Code == http.StatusFound || time.Now().After(deadline) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if rec.Code != http.StatusFound {
			t.Fatalf("resolve status = %d, want 302", rec.Code)
		}
		loc := rec.Result().Header.Get("Location")
		if !strings.HasSuffix(loc, "/xet-bridge/"+oid) {
			t.Fatalf("Location = %q, want suffix /xet-bridge/%s", loc, oid)
		}
		if got := rec.Result().Header.Get("X-Linked-Size"); got != fmt.Sprint(len(data)) {
			t.Fatalf("X-Linked-Size = %q, want %d", got, len(data))
		}

		bridge := httptest.NewRecorder()
		m.DataPlane().ServeHTTP(bridge, httptest.NewRequest(http.MethodGet, "/xet-bridge/"+oid, nil))
		if bridge.Code != http.StatusOK {
			t.Fatalf("bridge status = %d, want 200", bridge.Code)
		}
		if !bytes.Equal(bridge.Body.Bytes(), data) {
			t.Fatal("bridge bytes mismatch")
		}
	})
}

func TestServeObjectStreamsWhileIngesting(t *testing.T) {
	root := t.TempDir()
	src, srcPath := initSourceRepo(t, root, "src")

	data := bytes.Repeat([]byte("streaming ingest bytes. "), 2048)
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])
	addCommit(t, src, "main", "weights.bin", lfsPointerText(oid, len(data)))

	hub := fakeHub(t, "weights.bin", data, oid)

	m := newMirror(t,
		mirror.WithMirrorSourceFunc(staticSource(srcPath)),
		mirror.WithUpstream(hub.URL),
	)

	destPath := filepath.Join(root, "dest.git")
	if err := m.PullFromRemote(context.Background(), destPath, "org/repo", nil); err != nil {
		t.Fatalf("pull from remote: %v", err)
	}

	// Served via the resolve delegation even before ingest completes.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ignored", nil)
	if !m.ServeObject(rec, req, oid) {
		t.Fatal("ServeObject reported not served")
	}
	body := rec.Body.Bytes()
	if rec.Code == http.StatusFound {
		bridge := httptest.NewRecorder()
		m.DataPlane().ServeHTTP(bridge, httptest.NewRequest(http.MethodGet, rec.Result().Header.Get("Location"), nil))
		if bridge.Code != http.StatusOK {
			t.Fatalf("bridge status = %d, want 200", bridge.Code)
		}
		body = bridge.Body.Bytes()
	} else if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 or 302", rec.Code)
	}
	if !bytes.Equal(body, data) {
		t.Fatal("served bytes mismatch")
	}
}

func TestXETUploadRoundTrip(t *testing.T) {
	// Data-plane-only mirror: no upstream, no destination.
	m := newMirror(t)
	if !m.XETUploadEnabled() {
		t.Fatal("xet upload not enabled on data-plane-only mirror")
	}

	srv := httptest.NewServer(m.DataPlane())
	t.Cleanup(srv.Close)

	_, token, expiresAt := m.MintXETToken(httptest.NewRequest(http.MethodGet, srv.URL+"/", nil))
	if token == "" || !expiresAt.After(time.Now()) {
		t.Fatalf("bad minted token %q (expires %v)", token, expiresAt)
	}

	data := bytes.Repeat([]byte("xet upload round trip. "), 4096)
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])

	xc, err := xetclient.NewClient(xetclient.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("create xet client: %v", err)
	}
	provider := xetclient.StaticAuthProvider(srv.URL, token)
	if _, err := xc.UploadFileWithAuthProvider(context.Background(), provider, bytes.NewReader(data)); err != nil {
		t.Fatalf("xet upload: %v", err)
	}

	ctx := context.Background()
	if !m.HasObject(ctx, oid) {
		t.Fatalf("uploaded object %s not found in xet storage", oid)
	}
	rs, size, err := m.OpenObject(ctx, oid)
	if err != nil {
		t.Fatalf("open uploaded object: %v", err)
	}
	defer rs.Close()
	if size != int64(len(data)) {
		t.Fatalf("size = %d, want %d", size, int64(len(data)))
	}
	got, err := io.ReadAll(rs)
	if err != nil {
		t.Fatalf("read uploaded object: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("uploaded bytes mismatch")
	}

	t.Run("ServeIngested", func(t *testing.T) {
		rec := httptest.NewRecorder()
		if !m.ServeIngested(rec, httptest.NewRequest(http.MethodGet, "/ignored", nil), oid) {
			t.Fatal("ServeIngested reported not served")
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if !bytes.Equal(rec.Body.Bytes(), data) {
			t.Fatal("served bytes mismatch")
		}
		h := rec.Result().Header
		if got := h.Get("X-Linked-Etag"); got != `"`+oid+`"` {
			t.Fatalf("X-Linked-Etag = %q", got)
		}
		if got := h.Get("X-Linked-Size"); got != fmt.Sprint(len(data)) {
			t.Fatalf("X-Linked-Size = %q, want %d", got, len(data))
		}
		if link := h.Get("Link"); !strings.Contains(link, `rel="xet-auth"`) || !strings.Contains(link, `rel="xet-reconstruction-info"`) {
			t.Fatalf("Link = %q, want xet-auth and xet-reconstruction-info", link)
		}
		if h.Get("X-Xet-Hash") == "" {
			t.Fatal("X-Xet-Hash not set")
		}

		miss := httptest.NewRecorder()
		if m.ServeIngested(miss, httptest.NewRequest(http.MethodGet, "/ignored", nil), strings.Repeat("0", 64)) {
			t.Fatal("ServeIngested served a missing object")
		}
	})

	t.Run("ServeIngestedRedirect", func(t *testing.T) {
		rec := httptest.NewRecorder()
		if !m.ServeIngestedRedirect(rec, httptest.NewRequest(http.MethodGet, "/unused", nil), oid) {
			t.Fatal("ServeIngestedRedirect reported not served")
		}
		if rec.Code != http.StatusFound {
			t.Fatalf("status = %d, want 302", rec.Code)
		}
		h := rec.Result().Header
		if loc := h.Get("Location"); !strings.HasSuffix(loc, "/xet-bridge/"+oid) {
			t.Fatalf("Location = %q, want suffix /xet-bridge/%s", loc, oid)
		}
		if got := h.Get("X-Linked-Etag"); got != `"`+oid+`"` {
			t.Fatalf("X-Linked-Etag = %q", got)
		}
		if got := h.Get("X-Linked-Size"); got != fmt.Sprint(len(data)) {
			t.Fatalf("X-Linked-Size = %q, want %d", got, len(data))
		}
		if link := h.Get("Link"); !strings.Contains(link, `rel="xet-auth"`) {
			t.Fatalf("Link = %q, want xet-auth", link)
		}

		bridge := httptest.NewRecorder()
		m.DataPlane().ServeHTTP(bridge, httptest.NewRequest(http.MethodGet, h.Get("Location"), nil))
		if bridge.Code != http.StatusOK {
			t.Fatalf("bridge status = %d, want 200", bridge.Code)
		}
		if !bytes.Equal(bridge.Body.Bytes(), data) {
			t.Fatal("bridge bytes mismatch")
		}

		miss := httptest.NewRecorder()
		if m.ServeIngestedRedirect(miss, httptest.NewRequest(http.MethodGet, "/unused", nil), strings.Repeat("0", 64)) {
			t.Fatal("ServeIngestedRedirect served a missing object")
		}
	})
}

// uploadToCAS pushes data into the mirror's CAS through the xet transfer and
// returns its sha256 OID.
func uploadToCAS(t *testing.T, m *mirror.Mirror, data []byte) string {
	t.Helper()
	srv := httptest.NewServer(m.DataPlane())
	defer srv.Close()

	_, token, _ := m.MintXETToken(httptest.NewRequest(http.MethodGet, srv.URL+"/", nil))
	xc, err := xetclient.NewClient(xetclient.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("create xet client: %v", err)
	}
	provider := xetclient.StaticAuthProvider(srv.URL, token)
	if _, err := xc.UploadFileWithAuthProvider(context.Background(), provider, bytes.NewReader(data)); err != nil {
		t.Fatalf("xet upload: %v", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestDataPlaneUserPathGate(t *testing.T) {
	allow := true
	var gotRepos []string
	hook := func(ctx context.Context, op permission.Operation, repoName string, _ permission.Context) (bool, error) {
		if op != permission.OperationReadRepo {
			t.Errorf("op = %v, want read_repo", op)
		}
		gotRepos = append(gotRepos, repoName)
		return allow, nil
	}
	m := newMirror(t, mirror.WithPermissionHookFunc(hook))

	data := bytes.Repeat([]byte("gated bytes. "), 1024)
	oid := uploadToCAS(t, m, data)

	get := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		m.DataPlane().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec
	}

	if rec := get("/xet-bridge/" + oid); rec.Code != http.StatusOK {
		t.Fatalf("allowed bridge status = %d, want 200", rec.Code)
	} else if !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatal("allowed bridge bytes mismatch")
	}

	allow = false
	gotRepos = nil
	for _, path := range []string{
		"/xet-bridge/" + oid,
		"/xet-token",
		"/api/models/org/repo/xet-read-token/main",
		"/api/datasets/org/repo/xet-read-token/main",
	} {
		if rec := get(path); rec.Code != http.StatusForbidden {
			t.Fatalf("denied %s status = %d, want 403", path, rec.Code)
		}
	}
	want := []string{"", "", "org/repo", "datasets/org/repo"}
	if fmt.Sprint(gotRepos) != fmt.Sprint(want) {
		t.Fatalf("hook repos = %v, want %v", gotRepos, want)
	}

	// CAS transfer routes authenticate with CAS tokens, not the hook.
	if rec := get("/v1/reconstructions/" + strings.Repeat("0", 64)); rec.Code == http.StatusForbidden {
		t.Fatalf("CAS route hit the permission gate (status %d)", rec.Code)
	}
}
