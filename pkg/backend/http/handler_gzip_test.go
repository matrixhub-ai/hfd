package backend_test

// Tests for gzip-compressed smart-HTTP request bodies: git clients compress
// POST bodies larger than 1KiB (remote-curl.c post_rpc) and git http-backend
// transparently inflates them, so the go-git handler must too.

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

func TestHTTPGitGzipRequestBody(t *testing.T) {
	root := t.TempDir()

	st := storage.NewStorage(storage.WithRootDir(root))
	repoPath := filepath.Join(st.RepositoriesDir(), "repo.git")
	if err := os.MkdirAll(filepath.Dir(repoPath), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	runGitCmd(t, "", "init", "--bare", "--initial-branch=main", repoPath)

	work := filepath.Join(root, "work")
	runGitCmd(t, "", "init", "--initial-branch=main", work)
	runGitCmd(t, work, "config", "user.email", "test@example.com")
	runGitCmd(t, work, "config", "user.name", "Test User")
	if err := os.WriteFile(filepath.Join(work, "file.txt"), []byte("content\n"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	runGitCmd(t, work, "add", ".")
	runGitCmd(t, work, "commit", "-m", "c1")
	runGitCmd(t, work, "push", repoPath, "main")
	mainHash := strings.TrimSpace(runGitCmd(t, repoPath, "rev-parse", "refs/heads/main"))

	srv := httptest.NewServer(backendhttp.NewHandler(backendhttp.WithStorage(st)))
	t.Cleanup(srv.Close)

	// A v0 upload-pack request equivalent to what git sends: want + done.
	var plain bytes.Buffer
	wantLine := fmt.Sprintf("want %s multi_ack_detailed\n", mainHash)
	fmt.Fprintf(&plain, "%04x%s", 4+len(wantLine), wantLine)
	plain.WriteString("0000")
	fmt.Fprintf(&plain, "%04x%s", 4+len("done\n"), "done\n")

	post := func(t *testing.T, body io.Reader, encoding string) *http.Response {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/repo.git/git-upload-pack", body)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
		if encoding != "" {
			req.Header.Set("Content-Encoding", encoding)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		t.Cleanup(func() { _ = resp.Body.Close() })
		return resp
	}

	requirePackResponse := func(t *testing.T, resp *http.Response) {
		t.Helper()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, body: %s", resp.StatusCode, data)
		}
		if !bytes.HasPrefix(data, []byte("0008NAK\n")) || !bytes.Contains(data, []byte("PACK")) {
			t.Fatalf("expected NAK followed by a packfile, got %q...", data[:min(32, len(data))])
		}
	}

	t.Run("PlainBody", func(t *testing.T) {
		requirePackResponse(t, post(t, bytes.NewReader(plain.Bytes()), ""))
	})

	t.Run("GzipBody", func(t *testing.T) {
		var compressed bytes.Buffer
		zw := gzip.NewWriter(&compressed)
		if _, err := zw.Write(plain.Bytes()); err != nil {
			t.Fatalf("gzip write: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("gzip close: %v", err)
		}
		requirePackResponse(t, post(t, bytes.NewReader(compressed.Bytes()), "gzip"))
	})

	t.Run("CorruptGzipBodyRejected", func(t *testing.T) {
		resp := post(t, strings.NewReader("not gzip at all"), "gzip")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("corrupt gzip body: status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
		}
	})
}
