package e2e_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	backendxet "github.com/matrixhub-ai/hfd/pkg/backend/xet"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/storage"
	pkgxet "github.com/matrixhub-ai/hfd/pkg/xet"
)

// setupXetTestServer creates a test server with xet CAS backend enabled.
// This mirrors setupTestServer but adds xet handler, xet-enabled LFS, and HF backends.
func setupXetTestServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "hf-xet-e2e-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	store := storage.NewStorage(storage.WithRootDir(dataDir))
	lfsStorage := lfs.NewLocal(store.LFSDir())
	xetStorage := pkgxet.NewStorage(filepath.Join(dataDir, "xet"))

	// Set up handler chain (same order as main.go with xet enabled)
	var handler http.Handler

	handler = backendhf.NewHandler(
		backendhf.WithStorage(store),
		backendhf.WithLFSStorage(lfsStorage),
		backendhf.WithXetEnabled(true),
	)

	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(store),
		backendlfs.WithNext(handler),
		backendlfs.WithLFSStorage(lfsStorage),
		backendlfs.WithXetEnabled(true),
	)

	handler = backendxet.NewHandler(
		backendxet.WithStorage(xetStorage),
		backendxet.WithNext(handler),
	)

	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(store),
		backendhttp.WithNext(handler),
	)

	server := httptest.NewServer(handler)
	t.Cleanup(func() { server.Close() })

	return server, dataDir
}

// TestXetHFDownload verifies that `hf download` works when the server has xet enabled.
// The xet backend enables xet CAS transfer for uploads, but downloads must still work
// via the standard basic LFS transfer protocol.
func TestXetHFDownload(t *testing.T) {
	if _, err := exec.LookPath("hf"); err != nil {
		t.Skip("hf CLI not available, skipping xet hf download test")
	}

	server, _ := setupXetTestServer(t)
	endpoint := server.URL

	// Upload text files via hf CLI
	uploadDir, err := os.MkdirTemp("", "xet-hf-upload")
	if err != nil {
		t.Fatalf("Failed to create upload dir: %v", err)
	}
	defer os.RemoveAll(uploadDir)

	files := []struct {
		path    string
		content string
	}{
		{"README.md", "# Xet Download Test\n"},
		{"config.json", `{"model_type": "bert"}` + "\n"},
	}
	for _, f := range files {
		fp := filepath.Join(uploadDir, f.path)
		if err := os.MkdirAll(filepath.Dir(fp), 0755); err != nil {
			t.Fatalf("Failed to create dir for %s: %v", f.path, err)
		}
		if err := os.WriteFile(fp, []byte(f.content), 0644); err != nil {
			t.Fatalf("Failed to write %s: %v", f.path, err)
		}
	}

	runHFCmd(t, endpoint, "upload", "xet-user/xet-model", uploadDir, ".", "--commit-message", "Upload for xet download test")

	// Verify files via resolve endpoint
	for _, f := range files {
		resp, err := http.Get(endpoint + "/xet-user/xet-model/resolve/main/" + f.path)
		if err != nil {
			t.Fatalf("Failed to resolve %s: %v", f.path, err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("Failed to read %s: %v", f.path, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200 for %s, got %d: %s", f.path, resp.StatusCode, body)
		}
		if string(body) != f.content {
			t.Errorf("Resolve content mismatch for %s: got %q, want %q", f.path, body, f.content)
		}
	}

	// Download via hf CLI and verify
	downloadDir, err := os.MkdirTemp("", "xet-hf-download")
	if err != nil {
		t.Fatalf("Failed to create download dir: %v", err)
	}
	defer os.RemoveAll(downloadDir)

	runHFCmd(t, endpoint, "download", "xet-user/xet-model", "--local-dir", downloadDir)

	for _, f := range files {
		content, err := os.ReadFile(filepath.Join(downloadDir, f.path))
		if err != nil {
			t.Fatalf("Failed to read downloaded %s: %v", f.path, err)
		}
		if string(content) != f.content {
			t.Errorf("Downloaded content mismatch for %s: got %q, want %q", f.path, content, f.content)
		}
	}
}

// TestXetHFDownloadLFSFile verifies that `hf download` correctly retrieves LFS-tracked
// binary files when xet is enabled. The HF CLI uploads LFS files via preupload/commit,
// and downloads them via the resolve endpoint which serves content through LFS basic transfer.
func TestXetHFDownloadLFSFile(t *testing.T) {
	if _, err := exec.LookPath("hf"); err != nil {
		t.Skip("hf CLI not available, skipping xet hf download LFS test")
	}

	server, _ := setupXetTestServer(t)
	endpoint := server.URL

	uploadDir, err := os.MkdirTemp("", "xet-hf-lfs-upload")
	if err != nil {
		t.Fatalf("Failed to create upload dir: %v", err)
	}
	defer os.RemoveAll(uploadDir)

	// Create a binary file large enough to be tracked as LFS (matches default .gitattributes *.bin pattern)
	binaryContent := make([]byte, 2048)
	for i := range binaryContent {
		binaryContent[i] = byte(i % 256)
	}
	if err := os.WriteFile(filepath.Join(uploadDir, "model.bin"), binaryContent, 0644); err != nil {
		t.Fatalf("Failed to write model.bin: %v", err)
	}

	// Also upload a text file
	if err := os.WriteFile(filepath.Join(uploadDir, "README.md"), []byte("# Xet LFS Test\n"), 0644); err != nil {
		t.Fatalf("Failed to write README.md: %v", err)
	}

	runHFCmd(t, endpoint, "upload", "xet-user/xet-lfs-model", uploadDir, ".", "--commit-message", "Upload LFS file with xet")

	// Verify the LFS file is resolvable
	resp, err := http.Get(endpoint + "/xet-user/xet-lfs-model/resolve/main/model.bin")
	if err != nil {
		t.Fatalf("Failed to resolve LFS file: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 resolving LFS file, got %d", resp.StatusCode)
	}
	downloaded, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read LFS file content: %v", err)
	}
	if !bytes.Equal(downloaded, binaryContent) {
		t.Fatalf("Resolved LFS content mismatch: got %d bytes, want %d", len(downloaded), len(binaryContent))
	}

	// Download via hf CLI
	downloadDir, err := os.MkdirTemp("", "xet-hf-lfs-download")
	if err != nil {
		t.Fatalf("Failed to create download dir: %v", err)
	}
	defer os.RemoveAll(downloadDir)

	runHFCmd(t, endpoint, "download", "xet-user/xet-lfs-model", "--local-dir", downloadDir)

	// Verify binary file
	content, err := os.ReadFile(filepath.Join(downloadDir, "model.bin"))
	if err != nil {
		t.Fatalf("Failed to read downloaded model.bin: %v", err)
	}
	if !bytes.Equal(content, binaryContent) {
		t.Fatalf("Downloaded LFS content mismatch: got %d bytes, want %d", len(content), len(binaryContent))
	}

	// Verify text file
	readmeContent, err := os.ReadFile(filepath.Join(downloadDir, "README.md"))
	if err != nil {
		t.Fatalf("Failed to read downloaded README.md: %v", err)
	}
	if string(readmeContent) != "# Xet LFS Test\n" {
		t.Errorf("Downloaded README mismatch: got %q", readmeContent)
	}
}
