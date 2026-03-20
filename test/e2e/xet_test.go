package e2e_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"

	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	backendxet "github.com/matrixhub-ai/hfd/pkg/backend/xet"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/storage"
	"github.com/matrixhub-ai/hfd/pkg/xet"
)

func checkHfXet(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available, skipping xet test")
	}
	cmd := exec.CommandContext(t.Context(), "python3", "-c", "import hf_xet")
	if err := cmd.Run(); err != nil {
		t.Skip("hf_xet not installed, skipping xet test")
	}
}

func setupXetE2EServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "xet-e2e-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	st := storage.NewStorage(storage.WithRootDir(dataDir))
	lfsStorage := lfs.NewLocal(st.LFSDir())
	xetStorage := xet.NewLocal(st.XetDir())

	var handler http.Handler

	handler = backendhf.NewHandler(
		backendhf.WithStorage(st),
		backendhf.WithLFSStorage(lfsStorage),
		backendhf.WithXetEnabled(true),
	)

	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(st),
		backendlfs.WithNext(handler),
		backendlfs.WithLFSStorage(lfsStorage),
		backendlfs.WithXetEnabled(true),
	)

	handler = backendxet.NewHandler(
		backendxet.WithNext(handler),
		backendxet.WithXetStorage(xetStorage),
	)

	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(st),
		backendhttp.WithNext(handler),
	)

	server := httptest.NewServer(handler)
	t.Cleanup(func() { server.Close() })

	return server, dataDir
}

func TestXetCASUploadAndDownload(t *testing.T) {
	checkHfXet(t)

	server, _ := setupXetE2EServer(t)
	endpoint := server.URL

	// Create a repo first (needed for xet token endpoint)
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"xet-e2e-model","organization":"xet-user"}`))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 creating repo, got %d", resp.StatusCode)
	}

	// Verify xet token endpoint returns the server's own URL
	resp, err = http.Get(endpoint + "/api/models/xet-user/xet-e2e-model/xet-read-token/main")
	if err != nil {
		t.Fatalf("Failed to get xet read token: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for xet token, got %d", resp.StatusCode)
	}
	xetEndpoint := resp.Header.Get("X-Xet-Endpoint")
	if xetEndpoint != endpoint {
		t.Fatalf("Expected X-Xet-Endpoint %q, got %q", endpoint, xetEndpoint)
	}

	// Verify health endpoint
	resp, err = http.Get(endpoint + "/health")
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for health check, got %d", resp.StatusCode)
	}

	// Use hf_xet Python library to upload data through the CAS.
	// This tests the full xet upload path: hf_xet chunks the data, compresses
	// into xorbs, uploads via POST /v1/xorbs, and uploads the shard via POST /shards.
	script := fmt.Sprintf(`
import hf_xet
import time
import sys

endpoint = %q

# Upload bytes to the CAS
data = b"Hello from xet e2e test! " * 100  # 2500 bytes
print(f"Uploading {len(data)} bytes to CAS at {endpoint}", flush=True)

result = hf_xet.upload_bytes(
    [data],
    endpoint=endpoint,
    token_info=("Bearer dummy-token", int(time.time()) + 3600),
    token_refresher=None,
    progress_updater=None,
    _repo_type=None,
    request_headers=None,
    sha256s=None,
    skip_sha256=True,
)

if len(result) != 1:
    print(f"FAIL: expected 1 upload result, got {len(result)}", flush=True)
    sys.exit(1)

xet_hash = result[0].hash
file_size = result[0].file_size
print(f"Upload OK: hash={xet_hash}, file_size={file_size}", flush=True)

if file_size != len(data):
    print(f"FAIL: expected file_size={len(data)}, got {file_size}", flush=True)
    sys.exit(1)

if len(xet_hash) != 64:
    print(f"FAIL: expected 64-char hash, got {len(xet_hash)}", flush=True)
    sys.exit(1)

print("PASS", flush=True)
`, endpoint)

	cmd := exec.CommandContext(t.Context(), "python3", "-c", script)
	cmd.Env = append(os.Environ(),
		"HF_HUB_DISABLE_TELEMETRY=1",
	)
	out, err := cmd.CombinedOutput()
	t.Logf("Python output:\n%s", out)
	if err != nil {
		t.Fatalf("Xet e2e test failed: %v\nOutput:\n%s", err, out)
	}
	if !strings.Contains(string(out), "PASS") {
		t.Fatalf("Expected PASS in output, got:\n%s", out)
	}
}

func TestXetCASHashFiles(t *testing.T) {
	checkHfXet(t)

	// Test that hf_xet.hash_files works locally (no server needed)
	tmpDir, err := os.MkdirTemp("", "xet-hash-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := tmpDir + "/test.bin"
	if err := os.WriteFile(testFile, []byte("test content for hashing"), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	script := fmt.Sprintf(`
import hf_xet
import sys

result = hf_xet.hash_files([%q])
if len(result) != 1:
    print(f"FAIL: expected 1 result, got {len(result)}")
    sys.exit(1)

h = result[0].hash
size = result[0].file_size
print(f"hash={h}, file_size={size}")

if size != 24:
    print(f"FAIL: expected file_size=24, got {size}")
    sys.exit(1)

if len(h) != 64:
    print(f"FAIL: expected 64-char hash, got {len(h)}")
    sys.exit(1)

print("PASS")
`, testFile)

	cmd := exec.CommandContext(t.Context(), "python3", "-c", script)
	out, err := cmd.CombinedOutput()
	t.Logf("Python output:\n%s", out)
	if err != nil {
		t.Fatalf("hash_files test failed: %v\nOutput:\n%s", err, out)
	}
	if !strings.Contains(string(out), "PASS") {
		t.Fatalf("Expected PASS in output, got:\n%s", out)
	}
}
