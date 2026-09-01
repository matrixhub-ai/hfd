package hf

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
	xetclient "github.com/wzshiming/xet/client"
	xetmirror "github.com/wzshiming/xet/mirror"
	xetserver "github.com/wzshiming/xet/server"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
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
	dataDir := filepath.Join(t.TempDir(), "xet")
	chunksDir := filepath.Join(dataDir, "chunks")
	if err := os.MkdirAll(chunksDir, 0755); err != nil {
		t.Fatalf("create xet chunk cache dir: %v", err)
	}
	client, err := xetclient.NewClient(xetclient.WithCacheDir(chunksDir))
	if err != nil {
		t.Fatalf("create xet client: %v", err)
	}
	xs, err := xetstorage.NewFileStorage(
		xetstorage.WithBasePath(filepath.Join(dataDir, "storage")),
	)
	if err != nil {
		t.Fatalf("create xet storage: %v", err)
	}
	var wrapped xetstorage.Storage = xs
	if wrap != nil {
		wrapped = wrap(xs)
	}
	mint, authFn, err := authenticate.NewXETTokenScheme(nil)
	if err != nil {
		t.Fatalf("create token scheme: %v", err)
	}
	var engine *xetmirror.Mirror
	if upstreamURL != "" {
		engine, err = xetmirror.NewMirror(
			xetmirror.WithStorage(wrapped),
			xetmirror.WithUpstream(upstreamURL),
			xetmirror.WithCacheDir(filepath.Join(dataDir, "mirror")),
			xetmirror.WithClient(client),
		)
		if err != nil {
			t.Fatalf("create xet mirror engine: %v", err)
		}
	}
	cas := xetserver.NewHandler(
		xetserver.WithStorage(wrapped),
		xetserver.WithAuthFunc(authFn),
		xetserver.WithNext(http.NotFoundHandler()),
	)
	m, err := mirror.NewMirror(
		mirror.WithXETStorage(wrapped),
		mirror.WithXETClient(client),
		mirror.WithXETMirror(engine),
		mirror.WithDataDir(dataDir),
		mirror.WithMintToken(mint),
	)
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}
	// Background work must not outlive the temp data dir.
	t.Cleanup(m.Wait)
	return m, cas
}

// newLFSRepo creates org/repo in the storage with the given LFS pointer
// files committed on main, returning the storage and the head commit hash.
func newLFSRepo(t *testing.T, pointers map[string]string) (*storage.Storage, string) {
	t.Helper()
	st := storage.NewStorage(storage.WithRootDir(t.TempDir()))
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
