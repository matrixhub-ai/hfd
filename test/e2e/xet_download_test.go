package e2e_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	backendxet "github.com/matrixhub-ai/hfd/pkg/backend/xet"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/storage"
	pkgxet "github.com/matrixhub-ai/hfd/pkg/xet"
)

// checkPythonHFXet skips the test if Python3, huggingface_hub, or hf_xet are not available.
func checkPythonHFXet(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available, skipping xet download test")
	}
	cmd := exec.CommandContext(t.Context(), "python3", "-c", "import huggingface_hub; import hf_xet")
	if err := cmd.Run(); err != nil {
		t.Skip("huggingface_hub or hf_xet not installed, skipping xet download test")
	}
}

// setupTestServerXet creates an httptest.Server with xet CAS backend enabled.
// The handler chain mirrors cmd/hfd/main.go: HTTP → Xet → LFS → HF, with
// anonymous auth middleware. Token-sign auth is intentionally omitted because
// the xet CAS access token is used across many different endpoints/methods.
func setupTestServerXet(t *testing.T) (*httptest.Server, string) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "hf-e2e-xet-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	store := storage.NewStorage(storage.WithRootDir(dataDir))
	lfsStorage := lfs.NewLocal(store.LFSDir())
	xetStorage := pkgxet.NewStorage(filepath.Join(dataDir, "xet"))

	// Build handler chain (same order as main.go)
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

	handler = authenticate.AnonymousAuthenticateHandler(handler)

	server := httptest.NewServer(handler)
	t.Cleanup(func() { server.Close() })

	return server, dataDir
}

// TestHFDownloadXetE2E verifies the full upload-then-download round-trip using
// the xet CAS storage path. The upload goes through huggingface_hub with hf_xet
// (which negotiates the "xet" LFS transfer adapter), and the download uses
// hf_hub_download which detects the X-Xet-Hash / X-Xet-Refresh-Route headers
// and fetches the file via xet CAS reconstruction.
func TestHFDownloadXetE2E(t *testing.T) {
	checkPythonHFXet(t)

	server, _ := setupTestServerXet(t)
	endpoint := server.URL

	// Upload a binary file large enough to trigger LFS (matched by default
	// .gitattributes patterns for *.bin). The hf_xet extension negotiates xet
	// transfer so the file content lands in the CAS backend.
	uploadScript := fmt.Sprintf(`
import os, sys
import huggingface_hub

api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="xet-user/xet-download-model", exist_ok=True)

# Create a temp file with deterministic binary content
content = (b"xet download test payload\n") * 200  # 5200 bytes
tmp = os.path.join(%q, "model.bin")
os.makedirs(os.path.dirname(tmp), exist_ok=True)
with open(tmp, "wb") as f:
    f.write(content)

api.upload_file(
    path_or_fileobj=tmp,
    path_in_repo="model.bin",
    repo_id="xet-user/xet-download-model",
    commit_message="Upload xet binary file",
)
print("upload ok", file=sys.stderr)
`, t.TempDir())

	runPythonScript(t, endpoint, uploadScript)

	// Download the file using hf_hub_download (with hf_xet available).
	cacheDir, err := os.MkdirTemp("", "hf-xet-cache")
	if err != nil {
		t.Fatalf("Failed to create cache dir: %v", err)
	}
	defer os.RemoveAll(cacheDir)

	downloadScript := fmt.Sprintf(`
import os, sys
import huggingface_hub

path = huggingface_hub.hf_hub_download(
    repo_id="xet-user/xet-download-model",
    filename="model.bin",
    cache_dir=%q,
    endpoint=os.environ["HF_ENDPOINT"],
    token=os.environ["HF_TOKEN"],
)
content = open(path, "rb").read()
expected = (b"xet download test payload\n") * 200
assert content == expected, (
    f"content mismatch: got {len(content)} bytes, want {len(expected)}"
)
print("download ok", file=sys.stderr)
`, cacheDir)

	runPythonScript(t, endpoint, downloadScript)
}

// TestHFDownloadXetMultipleFilesE2E uploads several files via xet and downloads
// them individually, verifying each round-trip.
func TestHFDownloadXetMultipleFilesE2E(t *testing.T) {
	checkPythonHFXet(t)

	server, _ := setupTestServerXet(t)
	endpoint := server.URL

	uploadDir := t.TempDir()

	// Prepare upload files
	files := map[string]string{
		"weights.bin":   "weights data repeated\n",
		"optimizer.bin": "optimizer state data\n",
	}
	for name, content := range files {
		// Write enough data to trigger LFS
		repeated := ""
		for range 200 {
			repeated += content
		}
		if err := os.WriteFile(filepath.Join(uploadDir, name), []byte(repeated), 0644); err != nil {
			t.Fatalf("Failed to write %s: %v", name, err)
		}
	}

	uploadScript := fmt.Sprintf(`
import os, sys
import huggingface_hub

api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="xet-user/xet-multi-model", exist_ok=True)
api.upload_folder(
    folder_path=%q,
    repo_id="xet-user/xet-multi-model",
    commit_message="Upload multiple xet files",
)
print("upload ok", file=sys.stderr)
`, uploadDir)

	runPythonScript(t, endpoint, uploadScript)

	// Download and verify each file
	for name, baseContent := range files {
		expected := ""
		for range 200 {
			expected += baseContent
		}

		cacheDir := t.TempDir()
		downloadScript := fmt.Sprintf(`
import os, sys
import huggingface_hub

path = huggingface_hub.hf_hub_download(
    repo_id="xet-user/xet-multi-model",
    filename=%q,
    cache_dir=%q,
    endpoint=os.environ["HF_ENDPOINT"],
    token=os.environ["HF_TOKEN"],
)
content = open(path, "rb").read()
expected = %q.encode()
assert content == expected, (
    f"content mismatch for %s: got {len(content)} bytes, want {len(expected)}"
)
print(f"download %s ok", file=sys.stderr)
`, name, cacheDir, expected, name, name)

		runPythonScript(t, endpoint, downloadScript)
	}
}
