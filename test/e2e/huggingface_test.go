package e2e_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	backendhttp "github.com/wzshiming/gitd/pkg/backend/http"
	"github.com/wzshiming/gitd/pkg/backend/huggingface"
	backendlfs "github.com/wzshiming/gitd/pkg/backend/lfs"
	"github.com/wzshiming/gitd/pkg/storage"
)

func setupTestServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "hf-e2e-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	store := storage.NewStorage(storage.WithRootDir(dataDir))

	// Set up handler chain (same order as main.go)
	var handler http.Handler

	handler = huggingface.NewHandler(
		huggingface.WithStorage(store),
	)

	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(store),
		backendlfs.WithNext(handler),
	)

	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(store),
		backendhttp.WithNext(handler),
	)

	server := httptest.NewServer(handler)
	t.Cleanup(func() { server.Close() })

	return server, dataDir
}

// runHFCmd runs the hf CLI with the given endpoint and arguments.
func runHFCmd(t *testing.T, endpoint string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "hf", args...)
	cmd.Env = append(os.Environ(),
		"HF_ENDPOINT="+endpoint,
		"HF_HUB_DISABLE_TELEMETRY=1",
		"HF_TOKEN=dummy-token",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("HF command failed: hf %s\nError: %v\nOutput: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

func TestHuggingFaceUploadWithHFCLI(t *testing.T) {
	if _, err := exec.LookPath("hf"); err != nil {
		t.Skip("hf CLI not available, skipping HF CLI upload test")
	}

	server, _ := setupTestServer(t)
	endpoint := server.URL

	uploadDir, err := os.MkdirTemp("", "hf-upload-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(uploadDir)

	if err := os.WriteFile(filepath.Join(uploadDir, "test.txt"), []byte("Hello from HF CLI\n"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(uploadDir, "README.md"), []byte("# HF CLI Test\n"), 0644); err != nil {
		t.Fatalf("Failed to create README: %v", err)
	}

	// Run hf upload
	runHFCmd(t, endpoint, "upload", "test-user/hf-cli-model", uploadDir, ".", "--commit-message", "Upload via HF CLI")

	// Verify the uploaded files via HTTP
	for _, file := range []struct {
		path    string
		content string
	}{
		{"test.txt", "Hello from HF CLI\n"},
		{"README.md", "# HF CLI Test\n"},
	} {
		func() {
			resp, err := http.Get(endpoint + "/test-user/hf-cli-model/resolve/main/" + file.path)
			if err != nil {
				t.Fatalf("Failed to get %s: %v", file.path, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("Expected 200 for %s, got %d", file.path, resp.StatusCode)
			}

			content, _ := os.ReadFile(filepath.Join(uploadDir, file.path))
			if string(content) != file.content {
				t.Errorf("Unexpected content for %s: %q, want %q", file.path, content, file.content)
			}
		}()
	}
}

func TestHuggingFaceDownloadWithHFCLI(t *testing.T) {
	if _, err := exec.LookPath("hf"); err != nil {
		t.Skip("hf CLI not available, skipping HF CLI download test")
	}

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Upload files first
	uploadDir, err := os.MkdirTemp("", "hf-upload-for-download")
	if err != nil {
		t.Fatalf("Failed to create upload dir: %v", err)
	}
	defer os.RemoveAll(uploadDir)

	testFiles := []struct {
		path    string
		content string
	}{
		{"test.txt", "Hello from HF download test\n"},
		{"README.md", "# HF Download Test\n"},
		{"data/config.json", "{\"key\": \"value\"}\n"},
	}
	for _, file := range testFiles {
		filePath := filepath.Join(uploadDir, file.path)
		if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
			t.Fatalf("Failed to create dir for %s: %v", file.path, err)
		}
		if err := os.WriteFile(filePath, []byte(file.content), 0644); err != nil {
			t.Fatalf("Failed to create %s: %v", file.path, err)
		}
	}

	runHFCmd(t, endpoint, "upload", "test-user/dl-model", uploadDir, ".", "--commit-message", "Upload for download test")

	// Download files using hf download
	downloadDir, err := os.MkdirTemp("", "hf-download-test")
	if err != nil {
		t.Fatalf("Failed to create download dir: %v", err)
	}
	defer os.RemoveAll(downloadDir)

	runHFCmd(t, endpoint, "download", "test-user/dl-model", "--local-dir", downloadDir)

	// Verify downloaded files match uploaded files
	for _, file := range testFiles {
		content, err := os.ReadFile(filepath.Join(downloadDir, file.path))
		if err != nil {
			t.Fatalf("Failed to read downloaded %s: %v", file.path, err)
		}
		if string(content) != file.content {
			t.Errorf("Downloaded content mismatch for %s: got %q, want %q", file.path, content, file.content)
		}
	}
}

func TestHuggingFaceUploadAndDownloadRoundTrip(t *testing.T) {
	if _, err := exec.LookPath("hf"); err != nil {
		t.Skip("hf CLI not available, skipping HF CLI round-trip test")
	}

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// First upload
	uploadDir1, err := os.MkdirTemp("", "hf-roundtrip-upload1")
	if err != nil {
		t.Fatalf("Failed to create upload dir: %v", err)
	}
	defer os.RemoveAll(uploadDir1)

	if err := os.WriteFile(filepath.Join(uploadDir1, "file1.txt"), []byte("First upload\n"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(uploadDir1, "README.md"), []byte("# Round Trip v1\n"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	runHFCmd(t, endpoint, "upload", "test-user/rt-model", uploadDir1, ".", "--commit-message", "First commit")

	// Second upload (adds another file)
	uploadDir2, err := os.MkdirTemp("", "hf-roundtrip-upload2")
	if err != nil {
		t.Fatalf("Failed to create upload dir: %v", err)
	}
	defer os.RemoveAll(uploadDir2)

	if err := os.WriteFile(filepath.Join(uploadDir2, "file2.txt"), []byte("Second upload\n"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	runHFCmd(t, endpoint, "upload", "test-user/rt-model", uploadDir2, ".", "--commit-message", "Second commit")

	// Download and verify all files are present
	downloadDir, err := os.MkdirTemp("", "hf-roundtrip-download")
	if err != nil {
		t.Fatalf("Failed to create download dir: %v", err)
	}
	defer os.RemoveAll(downloadDir)

	runHFCmd(t, endpoint, "download", "test-user/rt-model", "--local-dir", downloadDir)

	// Verify all files from both uploads
	for _, file := range []struct {
		path    string
		content string
	}{
		{"file1.txt", "First upload\n"},
		{"README.md", "# Round Trip v1\n"},
		{"file2.txt", "Second upload\n"},
	} {
		content, err := os.ReadFile(filepath.Join(downloadDir, file.path))
		if err != nil {
			t.Fatalf("Failed to read downloaded %s: %v", file.path, err)
		}
		if string(content) != file.content {
			t.Errorf("Downloaded content mismatch for %s: got %q, want %q", file.path, content, file.content)
		}
	}
}
