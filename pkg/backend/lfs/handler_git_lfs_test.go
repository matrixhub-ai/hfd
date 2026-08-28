package lfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/osfs"
	xetclient "github.com/wzshiming/xet/client"
	xetmirror "github.com/wzshiming/xet/mirror"
	xetserver "github.com/wzshiming/xet/server"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// newXETDataPlane assembles the xet data-plane pieces the way cmd/hfd does —
// file storage, client, token scheme, and the ingest engine when hubURL is
// set — and builds the mirror over them with gitOpts appended, returning the
// mirror and the CAS-server composition.
func newXETDataPlane(t *testing.T, hubURL string, gitOpts ...mirror.Option) (*mirror.Mirror, http.Handler) {
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
	mint, authFn, err := authenticate.NewXETTokenScheme(nil)
	if err != nil {
		t.Fatalf("create token scheme: %v", err)
	}
	var engine *xetmirror.Mirror
	if hubURL != "" {
		engine, err = xetmirror.NewMirror(
			xetmirror.WithStorage(xs),
			xetmirror.WithUpstream(hubURL),
			xetmirror.WithCacheDir(filepath.Join(dataDir, "mirror")),
			xetmirror.WithClient(client),
		)
		if err != nil {
			t.Fatalf("create xet mirror engine: %v", err)
		}
	}
	cas := xetserver.NewHandler(
		xetserver.WithStorage(xs),
		xetserver.WithAuthFunc(authFn),
		xetserver.WithNext(http.NotFoundHandler()),
	)
	m, err := mirror.NewMirror(append([]mirror.Option{
		mirror.WithXETStorage(xs),
		mirror.WithXETClient(client),
		mirror.WithXETMirror(engine),
		mirror.WithDataDir(dataDir),
		mirror.WithMintToken(mint),
	}, gitOpts...)...)
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

func staticSource(path string) mirror.SourceFunc {
	return func(ctx context.Context, repoName string) (string, bool, error) {
		return path, true, nil
	}
}

func lfsPointerText(oid string, size int) string {
	return fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, size)
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
	if link := hd.Get("Link"); !strings.Contains(link, `rel="xet-auth"`) || !strings.Contains(link, `rel="xet-reconstruction-info"`) {
		t.Fatalf("Link = %q, want xet-auth and xet-reconstruction-info", link)
	}
	if hd.Get("X-Xet-Hash") == "" {
		t.Fatal("X-Xet-Hash not set")
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

	m, _ := newXETDataPlane(t, hub.URL,
		mirror.WithMirrorSourceFunc(staticSource(srcPath)),
	)
	// Registered after m.Wait and hub.Close so it runs first (LIFO): a test
	// failing early must unblock the gated hub handler before those waits.
	t.Cleanup(openGate)

	destPath := filepath.Join(root, "dest.git")
	if err := m.PullFromRemote(context.Background(), destPath, "org/repo", nil); err != nil {
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
