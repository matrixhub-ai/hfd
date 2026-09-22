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
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"

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

// fakeHub answers hub-style resolve requests for LFS files by name with the
// metadata headers the xet mirror probes for, and reports the file names
// requested so far.
func fakeHub(t *testing.T, files map[string][]byte) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var requested []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := path.Base(r.URL.Path)
		data, ok := files[name]
		if !strings.Contains(r.URL.Path, "/resolve/") || !ok {
			http.NotFound(w, r)
			return
		}
		mu.Lock()
		requested = append(requested, name)
		mu.Unlock()
		oid := oidOf(data)
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
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(requested)
	}
}

func oidOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
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

	hub, _ := fakeHub(t, map[string][]byte{"model.bin": data})

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

// TestPullMirrorLFSIngestFilter pins the eager prefetch filter: it sees every
// scanned pointer with its ref tip commit, a rejected pointer stays registered
// and is not fetched until it is read, and reads are not filtered.
func TestPullMirrorLFSIngestFilter(t *testing.T) {
	root := t.TempDir()
	src, srcPath := initSourceRepo(t, root, "src")
	keep := bytes.Repeat([]byte("kept by the ingest filter. "), 2048)
	skip := bytes.Repeat([]byte("skipped by the ingest filter. "), 2048)
	addCommit(t, src, "main", "keep.bin", lfsPointerText(oidOf(keep), len(keep)))
	tip := addCommit(t, src, "main", "skip.bin", lfsPointerText(oidOf(skip), len(skip)))
	hub, requested := fakeHub(t, map[string][]byte{"keep.bin": keep, "skip.bin": skip})

	var calls []string
	m := newMirror(t, hub.URL,
		mirror.WithMirrorSourceFunc(staticSource(srcPath)),
		mirror.WithLFSIngestFilterFunc(func(ctx context.Context, repoName, revision, name string, size int64) bool {
			calls = append(calls, fmt.Sprintf("%s %s %s %d", repoName, revision, name, size))
			return name != "skip.bin"
		}),
	)
	if err := m.PullFromRemote(context.Background(), filepath.Join(root, "dest.git"), "org/repo", nil); err != nil {
		t.Fatalf("pull from remote: %v", err)
	}
	want := []string{
		fmt.Sprintf("org/repo %s keep.bin %d", tip, len(keep)),
		fmt.Sprintf("org/repo %s skip.bin %d", tip, len(skip)),
	}
	if !slices.Equal(calls, want) {
		t.Fatalf("filter calls = %q, want %q", calls, want)
	}

	ctx := context.Background()
	if !m.KnowsObject(ctx, oidOf(skip)) {
		t.Fatal("rejected object must stay registered for lazy reads")
	}
	m.Wait()
	if !m.HasObject(ctx, oidOf(keep)) {
		t.Fatal("accepted object was not prefetched")
	}
	if m.HasObject(ctx, oidOf(skip)) || slices.Contains(requested(), "skip.bin") {
		t.Fatalf("rejected object was prefetched; upstream requests %q", requested())
	}

	rec := httptest.NewRecorder()
	if !m.ServeOID(rec, httptest.NewRequest(http.MethodGet, "/", nil), oidOf(skip)) {
		t.Fatal("ServeOID of the rejected object failed")
	}
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), skip) {
		t.Fatalf("ServeOID status = %d, body %d bytes, want 200 with %d bytes", rec.Code, rec.Body.Len(), len(skip))
	}
}

// TestPullMirrorLFSIngestFilterSharedOID scans a rejected path before an
// admitted one holding the same object: the object is still prefetched, through
// the admitted path.
func TestPullMirrorLFSIngestFilterSharedOID(t *testing.T) {
	root := t.TempDir()
	src, srcPath := initSourceRepo(t, root, "src")
	data := bytes.Repeat([]byte("shared between two paths. "), 2048)
	pointer := lfsPointerText(oidOf(data), len(data))
	addCommit(t, src, "main", "excluded.bin", pointer)
	addCommit(t, src, "main", "included.bin", pointer)
	hub, requested := fakeHub(t, map[string][]byte{"excluded.bin": data, "included.bin": data})

	m := newMirror(t, hub.URL,
		mirror.WithMirrorSourceFunc(staticSource(srcPath)),
		mirror.WithLFSIngestFilterFunc(func(ctx context.Context, repoName, revision, name string, size int64) bool {
			return name != "excluded.bin"
		}),
	)
	if err := m.PullFromRemote(context.Background(), filepath.Join(root, "dest.git"), "org/repo", nil); err != nil {
		t.Fatalf("pull from remote: %v", err)
	}
	m.Wait()
	if !m.HasObject(context.Background(), oidOf(data)) {
		t.Fatal("object admitted through included.bin was not prefetched")
	}
	if got := requested(); slices.Contains(got, "excluded.bin") || !slices.Contains(got, "included.bin") {
		t.Fatalf("upstream requests = %q, want only included.bin", got)
	}
}
