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

	"github.com/matrixhub-ai/hfd/pkg/mirror"
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
