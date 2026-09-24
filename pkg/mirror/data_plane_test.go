package mirror_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	xetmirror "github.com/wzshiming/xet/mirror"

	"github.com/matrixhub-ai/hfd/pkg/mirror"
)

func TestMintXETToken(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	grant := auth.Grant{Permission: auth.Read, File: &xet.FileHash{1, 2, 3}}
	m := newMirror(t, "")
	if m.CanMintToken() {
		t.Fatal("unexpected mint configured")
	}
	if _, _, _, err := m.MintXETToken(request, grant); err == nil || err.Error() != "no CAS token mint configured" {
		t.Fatalf("nil mint error = %v", err)
	}
	mintErr := errors.New("mint failed")
	for _, wantErr := range []error{nil, mintErr} {
		m := newMirror(t, "", mirror.WithExternalURL("https://cas.example/"), mirror.WithMintToken(func(got auth.Grant) (string, int64, error) {
			if got != grant {
				t.Fatalf("grant = %+v, want %+v", got, grant)
			}
			return "token", 123, wantErr
		}))
		casURL, token, expiresAt, err := m.MintXETToken(request, grant)
		if !m.CanMintToken() || !errors.Is(err, wantErr) {
			t.Fatalf("CanMintToken = %v, error = %v, want %v", m.CanMintToken(), err, wantErr)
		}
		if wantErr == nil && (casURL != "https://cas.example" || token != "token" || expiresAt.Unix() != 123) {
			t.Fatalf("mint result = %q, %q, %v", casURL, token, expiresAt)
		}
	}
}

// fakeHub answers hub-style resolve requests for a single LFS object with the
// metadata headers the xet mirror probes for.
func fakeHub(t *testing.T, filename string, data []byte, oid string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(fakeHubHandler(filename, data, oid))
	t.Cleanup(srv.Close)
	return srv
}

func fakeHubHandler(filename string, data []byte, oid string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	})
}

// recorder collects strings from the engine's concurrent goroutines.
type recorder struct {
	mu   sync.Mutex
	seen []string
}

func (rec *recorder) add(s string) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.seen = append(rec.seen, s)
}

func (rec *recorder) all() []string {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]string(nil), rec.seen...)
}

// strictHub records request paths and, like huggingface.co, 404s .git or doubled-slash repo names.
func strictHub(t *testing.T, filename string, data []byte, oid string, paths *recorder) *httptest.Server {
	t.Helper()
	hub := fakeHubHandler(filename, data, oid)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths.add(r.URL.Path)
		if strings.Contains(r.URL.Path, ".git") || strings.HasPrefix(r.URL.Path, "//") {
			http.NotFound(w, r)
			return
		}
		hub.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// recordingUpstream sends every repo to hubURL and records the names the engine asks for.
func recordingUpstream(t *testing.T, hubURL string, repos *recorder) xetmirror.UpstreamFunc {
	t.Helper()
	u, err := url.Parse(hubURL)
	if err != nil {
		t.Fatalf("parse hub URL: %v", err)
	}
	return func(ctx context.Context, repo string) (*url.URL, string, error) {
		repos.add(repo)
		return u, "", nil
	}
}

// wantOnly fails unless got is non-empty and every entry equals want.
func wantOnly(t *testing.T, what string, got []string, want string) {
	t.Helper()
	if len(got) == 0 {
		t.Fatalf("%s: nothing recorded, want %q", what, want)
	}
	for _, g := range got {
		if g != want {
			t.Fatalf("%s = %q, want only %q", what, got, want)
		}
	}
}

func lfsPointerText(oid string, size int) string {
	return fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, size)
}

// An OID registered as /org/repo.git reaches the hub and the selector as org/repo.
func TestServeOIDCanonicalRepoName(t *testing.T) {
	data := bytes.Repeat([]byte("canonical repo name bytes. "), 2048)
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])
	var paths, repos recorder
	hub := strictHub(t, "model.bin", data, oid, &paths)
	m := newMirrorWithUpstream(t, recordingUpstream(t, hub.URL, &repos))

	commit := strings.Repeat("a", 40)
	m.RegisterObject(oid, "/org/repo.git", commit, "model.bin", int64(len(data)))
	rec := httptest.NewRecorder()
	if !m.ServeOID(rec, httptest.NewRequest(http.MethodGet, "/anything", nil), oid) {
		t.Fatalf("ServeOID = false, want true (hub paths %q, selector repos %q)", paths.all(), repos.all())
	}
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("ServeOID status = %d, body %d bytes, want 200 with %d bytes", rec.Code, rec.Body.Len(), len(data))
	}
	wantPath := "/org/repo/resolve/" + commit + "/model.bin"
	if got := paths.all(); !slices.ContainsFunc(got, func(p string) bool { return strings.HasPrefix(p, wantPath) }) {
		t.Fatalf("hub paths = %q, want one starting with %q", got, wantPath)
	}
	wantOnly(t, "selector repos", repos.all(), "org/repo")
}

// The git pull-through path hands the mirror the access-path name /org/repo.git.
func TestPullMirrorLFSCanonicalRepoName(t *testing.T) {
	root := t.TempDir()
	src, srcPath := initSourceRepo(t, root, "src")
	data := bytes.Repeat([]byte("canonical pull scan bytes. "), 2048)
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])
	addCommit(t, src, "main", "model.bin", lfsPointerText(oid, len(data)))

	var paths, repos recorder
	hub := strictHub(t, "model.bin", data, oid, &paths)
	m := newMirrorWithUpstream(t, recordingUpstream(t, hub.URL, &repos),
		mirror.WithMirrorSourceFunc(staticSource(srcPath)),
	)
	ctx := context.Background()
	if err := m.PullFromRemote(ctx, filepath.Join(root, "dest.git"), "/org/repo.git", nil); err != nil {
		t.Fatalf("pull from remote: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for !m.HasObject(ctx, oid) {
		if time.Now().After(deadline) {
			t.Fatalf("object %s was never ingested (hub paths %q, selector repos %q)", oid, paths.all(), repos.all())
		}
		time.Sleep(50 * time.Millisecond)
	}
	wantOnly(t, "selector repos", repos.all(), "org/repo")
}

// The most recently registered target is dead; the older registration still serves.
func TestServeOIDTriesEveryTarget(t *testing.T) {
	data := bytes.Repeat([]byte("multi target bytes. "), 2048)
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])
	live := fakeHub(t, "model.bin", data, oid)
	dead := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(dead.Close)
	hubs := map[string]string{"org/b": live.URL, "org/a": dead.URL}
	m := newMirrorWithUpstream(t, func(ctx context.Context, repo string) (*url.URL, string, error) {
		hub, ok := hubs[repo]
		if !ok {
			return nil, "", fmt.Errorf("unexpected repo %q", repo)
		}
		u, err := url.Parse(hub)
		return u, "", err
	})

	commit := strings.Repeat("b", 40)
	m.RegisterObject(oid, "org/b", commit, "model.bin", int64(len(data)))
	m.RegisterObject(oid, "org/a", commit, "model.bin", int64(len(data)))
	ctx := context.Background()
	if !m.KnowsObject(ctx, oid) {
		t.Fatal("KnowsObject = false before ingestion")
	}
	rec := httptest.NewRecorder()
	if !m.ServeOID(rec, httptest.NewRequest(http.MethodGet, "/anything", nil), oid) {
		t.Fatal("ServeOID = false, want the live target to serve after the dead one")
	}
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("ServeOID status = %d, body %d bytes, want 200 with %d bytes", rec.Code, rec.Body.Len(), len(data))
	}
}

func TestPullMirrorLFSDataPlane(t *testing.T) {
	root := t.TempDir()
	src, srcPath := initSourceRepo(t, root, "src")

	data := bytes.Repeat([]byte("hfd xet mirror data plane! "), 4096)
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])
	addCommit(t, src, "main", "model.bin", lfsPointerText(oid, len(data)))

	hub := fakeHub(t, "model.bin", data, oid)

	m := newMirror(t, hub.URL,
		mirror.WithMirrorSourceFunc(staticSource(srcPath)),
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
}
