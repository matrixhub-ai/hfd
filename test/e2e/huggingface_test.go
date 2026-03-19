package e2e_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func setupTestServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "hf-e2e-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	storage := storage.NewStorage(storage.WithRootDir(dataDir))
	lfsStorage := lfs.NewLocal(storage.LFSDir())

	// Set up handler chain (same order as main.go)
	var handler http.Handler

	handler = backendhf.NewHandler(
		backendhf.WithStorage(storage),
		backendhf.WithLFSStorage(lfsStorage),
	)

	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(storage),
		backendlfs.WithNext(handler),
		backendlfs.WithLFSStorage(lfsStorage),
	)

	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(storage),
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

func TestHuggingFaceUploadAndDownloadMatrix(t *testing.T) {
	if _, err := exec.LookPath("hf"); err != nil {
		t.Skip("hf CLI not available, skipping HF CLI matrix test")
	}

	server, _ := setupTestServer(t)
	endpoint := server.URL

	type file struct {
		path    string
		content string
	}

	testCases := []struct {
		name          string
		repoID        string
		resolvePrefix string
		uploadArgs    []string
		downloadArgs  []string
		files         []file
	}{
		{
			name:   "model",
			repoID: "matrix-user/hf-cli-model",
			files: []file{
				{"test.txt", "Hello from HF CLI\n"},
				{"README.md", "# HF CLI Test\n"},
				{"data/config.json", "{\"key\": \"value\"}\n"},
			},
			uploadArgs:   []string{"--commit-message", "Upload via HF CLI"},
			downloadArgs: nil,
		},
		{
			name:          "dataset",
			repoID:        "matrix-user/my-dataset",
			resolvePrefix: "/datasets",
			files: []file{
				{"README.md", "# Test Dataset\n"},
				{"data.csv", "col1,col2\na,b\n"},
			},
			uploadArgs:   []string{"--repo-type", "dataset", "--commit-message", "Upload dataset"},
			downloadArgs: []string{"--repo-type", "dataset"},
		},
		{
			name:          "space",
			repoID:        "matrix-user/my-space",
			resolvePrefix: "/spaces",
			files: []file{
				{"README.md", "# Test Space\n"},
				{"app.py", "import gradio as gr\n"},
			},
			uploadArgs:   []string{"--repo-type", "space", "--commit-message", "Upload space"},
			downloadArgs: []string{"--repo-type", "space"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			uploadDir, err := os.MkdirTemp("", "hf-matrix-upload-"+tc.name)
			if err != nil {
				t.Fatalf("Failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(uploadDir)

			for _, file := range tc.files {
				filePath := filepath.Join(uploadDir, file.path)
				if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
					t.Fatalf("Failed to create dir for %s: %v", file.path, err)
				}
				if err := os.WriteFile(filePath, []byte(file.content), 0644); err != nil {
					t.Fatalf("Failed to create %s: %v", file.path, err)
				}
			}

			args := []string{"upload", tc.repoID, uploadDir, "."}
			args = append(args, tc.uploadArgs...)
			runHFCmd(t, endpoint, args...)

			for _, file := range tc.files {
				resp, err := http.Get(endpoint + tc.resolvePrefix + "/" + tc.repoID + "/resolve/main/" + file.path)
				if err != nil {
					t.Fatalf("Failed to get %s: %v", file.path, err)
				}

				if resp.StatusCode != http.StatusOK {
					t.Fatalf("Expected 200 for %s, got %d", file.path, resp.StatusCode)
				}

				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					t.Fatalf("Failed to read %s: %v", file.path, err)
				}
				if string(body) != file.content {
					t.Errorf("Unexpected content for %s: got %q, want %q", file.path, body, file.content)
				}
			}

			downloadDir, err := os.MkdirTemp("", "hf-matrix-download-"+tc.name)
			if err != nil {
				t.Fatalf("Failed to create download dir: %v", err)
			}
			defer os.RemoveAll(downloadDir)

			downloadArgs := []string{"download", tc.repoID, "--local-dir", downloadDir}
			downloadArgs = append(downloadArgs, tc.downloadArgs...)
			runHFCmd(t, endpoint, downloadArgs...)

			for _, file := range tc.files {
				content, err := os.ReadFile(filepath.Join(downloadDir, file.path))
				if err != nil {
					t.Fatalf("Failed to read downloaded %s: %v", file.path, err)
				}
				if string(content) != file.content {
					t.Errorf("Downloaded content mismatch for %s: got %q, want %q", file.path, content, file.content)
				}
			}
		})
	}
}

func TestHuggingFaceRepoTypeIsolationE2E(t *testing.T) {
	if _, err := exec.LookPath("hf"); err != nil {
		t.Skip("hf CLI not available, skipping HF CLI repo type isolation test")
	}

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Upload different content to the same repo name but different types
	for _, tc := range []struct {
		repoType string
		content  string
	}{
		{"model", "model content\n"},
		{"dataset", "dataset content\n"},
		{"space", "space content\n"},
	} {
		uploadDir, err := os.MkdirTemp("", "hf-isolation-"+tc.repoType)
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(uploadDir)

		if err := os.WriteFile(filepath.Join(uploadDir, "data.txt"), []byte(tc.content), 0644); err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}

		args := []string{"upload", "test-user/shared-name", uploadDir, ".", "--repo-type", tc.repoType, "--commit-message", "Upload " + tc.repoType}
		runHFCmd(t, endpoint, args...)
	}

	// Download each type and verify isolation
	for _, tc := range []struct {
		repoType        string
		resolvePrefix   string
		expectedContent string
	}{
		{"model", "", "model content\n"},
		{"dataset", "/datasets", "dataset content\n"},
		{"space", "/spaces", "space content\n"},
	} {
		// Verify via HTTP resolve
		resp, err := http.Get(endpoint + tc.resolvePrefix + "/test-user/shared-name/resolve/main/data.txt")
		if err != nil {
			t.Fatalf("Failed to get file for %s: %v", tc.repoType, err)
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			t.Fatalf("Expected 200 for %s, got %d", tc.repoType, resp.StatusCode)
		}

		resp.Body.Close()

		// Verify via hf download
		downloadDir, err := os.MkdirTemp("", "hf-isolation-dl-"+tc.repoType)
		if err != nil {
			t.Fatalf("Failed to create download dir: %v", err)
		}
		defer os.RemoveAll(downloadDir)

		runHFCmd(t, endpoint, "download", "test-user/shared-name", "--repo-type", tc.repoType, "--local-dir", downloadDir)

		content, err := os.ReadFile(filepath.Join(downloadDir, "data.txt"))
		if err != nil {
			t.Fatalf("Failed to read downloaded data.txt for %s: %v", tc.repoType, err)
		}
		if string(content) != tc.expectedContent {
			t.Errorf("Content mismatch for %s: got %q, want %q", tc.repoType, content, tc.expectedContent)
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

func TestHuggingFaceRepoCreateAndDeleteE2E(t *testing.T) {
	if _, err := exec.LookPath("hf"); err != nil {
		t.Skip("hf CLI not available, skipping HF CLI repo create/delete test")
	}

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create a repo via hf CLI
	runHFCmd(t, endpoint, "repo", "create", "test-user/cli-model", "--exist-ok")

	// Upload a file to verify it works
	uploadDir, err := os.MkdirTemp("", "hf-repo-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(uploadDir)

	if err := os.WriteFile(filepath.Join(uploadDir, "README.md"), []byte("# CLI Model\n"), 0644); err != nil {
		t.Fatalf("Failed to create README: %v", err)
	}

	runHFCmd(t, endpoint, "upload", "test-user/cli-model", uploadDir, ".", "--commit-message", "Test upload")

	// Verify the file exists
	resp, err := http.Get(endpoint + "/test-user/cli-model/resolve/main/README.md")
	if err != nil {
		t.Fatalf("Failed to get file: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.StatusCode)
	}

	// Delete the repo via hf CLI
	runHFCmd(t, endpoint, "repo", "delete", "test-user/cli-model")

	// Verify the repo is gone
	resp, err = http.Get(endpoint + "/api/models/test-user/cli-model")
	if err != nil {
		t.Fatalf("Failed to check repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404 after delete, got %d", resp.StatusCode)
	}
}

func TestHuggingFaceRepoMoveE2E(t *testing.T) {
	if _, err := exec.LookPath("hf"); err != nil {
		t.Skip("hf CLI not available, skipping HF CLI repo move test")
	}

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create and populate a repo
	uploadDir, err := os.MkdirTemp("", "hf-move-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(uploadDir)

	if err := os.WriteFile(filepath.Join(uploadDir, "README.md"), []byte("# Move Test\n"), 0644); err != nil {
		t.Fatalf("Failed to create README: %v", err)
	}

	runHFCmd(t, endpoint, "upload", "old-user/move-model", uploadDir, ".", "--commit-message", "Before move")

	// Move the repo
	runHFCmd(t, endpoint, "repo", "move", "old-user/move-model", "new-user/move-model")

	// Verify old location is gone
	resp, err := http.Get(endpoint + "/api/models/old-user/move-model")
	if err != nil {
		t.Fatalf("Failed to check old repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404 for old location, got %d", resp.StatusCode)
	}

	// Verify file at new location
	resp, err = http.Get(endpoint + "/new-user/move-model/resolve/main/README.md")
	if err != nil {
		t.Fatalf("Failed to get file: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for new location, got %d", resp.StatusCode)
	}

	content, _ := io.ReadAll(resp.Body)
	if string(content) != "# Move Test\n" {
		t.Errorf("Unexpected content: %q", content)
	}
}

func TestHuggingFaceRepoSettingsE2E(t *testing.T) {
	if _, err := exec.LookPath("hf"); err != nil {
		t.Skip("hf CLI not available, skipping HF CLI repo settings test")
	}

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create a repo
	runHFCmd(t, endpoint, "repo", "create", "test-user/settings-model", "--exist-ok")

	// Update settings via CLI
	runHFCmd(t, endpoint, "repo", "settings", "test-user/settings-model", "--private")
	runHFCmd(t, endpoint, "repo", "settings", "test-user/settings-model", "--gated", "auto")
}

func TestHuggingFaceRepoBranchE2E(t *testing.T) {
	if _, err := exec.LookPath("hf"); err != nil {
		t.Skip("hf CLI not available, skipping HF CLI branch test")
	}

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create and populate a repo
	uploadDir, err := os.MkdirTemp("", "hf-branch-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(uploadDir)

	if err := os.WriteFile(filepath.Join(uploadDir, "README.md"), []byte("# Branch Test\n"), 0644); err != nil {
		t.Fatalf("Failed to create README: %v", err)
	}

	runHFCmd(t, endpoint, "upload", "test-user/branch-model", uploadDir, ".", "--commit-message", "Initial commit")

	// Create a branch
	runHFCmd(t, endpoint, "repo", "branch", "create", "test-user/branch-model", "dev")

	// Delete the branch
	runHFCmd(t, endpoint, "repo", "branch", "delete", "test-user/branch-model", "dev")
}

func TestHuggingFaceRepoTagE2E(t *testing.T) {
	if _, err := exec.LookPath("hf"); err != nil {
		t.Skip("hf CLI not available, skipping HF CLI tag test")
	}

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create and populate a repo
	uploadDir, err := os.MkdirTemp("", "hf-tag-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(uploadDir)

	if err := os.WriteFile(filepath.Join(uploadDir, "README.md"), []byte("# Tag Test\n"), 0644); err != nil {
		t.Fatalf("Failed to create README: %v", err)
	}

	runHFCmd(t, endpoint, "upload", "test-user/tag-model", uploadDir, ".", "--commit-message", "Initial commit")

	// Create a tag
	runHFCmd(t, endpoint, "repo", "tag", "create", "test-user/tag-model", "v1.0", "-m", "First release")

	// List tags
	output := runHFCmd(t, endpoint, "repo", "tag", "list", "test-user/tag-model")
	if !strings.Contains(output, "v1.0") {
		t.Errorf("Expected tag 'v1.0' in output, got: %s", output)
	}

	// Delete the tag
	runHFCmd(t, endpoint, "repo", "tag", "delete", "test-user/tag-model", "v1.0", "--yes")

	// Verify tag is gone
	output = runHFCmd(t, endpoint, "repo", "tag", "list", "test-user/tag-model")
	if strings.Contains(output, "v1.0") {
		t.Errorf("Tag 'v1.0' should have been deleted, but still found in output: %s", output)
	}
}

func TestHuggingFaceRepoDatasetBranchTagE2E(t *testing.T) {
	if _, err := exec.LookPath("hf"); err != nil {
		t.Skip("hf CLI not available, skipping HF CLI dataset branch/tag test")
	}

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create and populate a dataset repo
	uploadDir, err := os.MkdirTemp("", "hf-dataset-bt-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(uploadDir)

	if err := os.WriteFile(filepath.Join(uploadDir, "README.md"), []byte("# Dataset BT\n"), 0644); err != nil {
		t.Fatalf("Failed to create README: %v", err)
	}

	runHFCmd(t, endpoint, "upload", "test-user/bt-dataset", uploadDir, ".", "--repo-type", "dataset", "--commit-message", "Initial commit")

	// Create branch on dataset
	runHFCmd(t, endpoint, "repo", "branch", "create", "test-user/bt-dataset", "dev", "--repo-type", "dataset")

	// Create tag on dataset
	runHFCmd(t, endpoint, "repo", "tag", "create", "test-user/bt-dataset", "v1.0", "--repo-type", "dataset")

	// List tags on dataset
	output := runHFCmd(t, endpoint, "repo", "tag", "list", "test-user/bt-dataset", "--repo-type", "dataset")
	if !strings.Contains(output, "v1.0") {
		t.Errorf("Expected tag 'v1.0' in dataset tags, got: %s", output)
	}

	// Delete branch and tag
	runHFCmd(t, endpoint, "repo", "branch", "delete", "test-user/bt-dataset", "dev", "--repo-type", "dataset")
	runHFCmd(t, endpoint, "repo", "tag", "delete", "test-user/bt-dataset", "v1.0", "--repo-type", "dataset", "--yes")
}

func TestCreateRepoHasDefaultGitAttributes(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create a repo via the HF API
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"gitattrs-model","organization":"test-user"}`))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 creating repo, got %d", resp.StatusCode)
	}

	// Verify .gitattributes is accessible and contains LFS patterns
	gaResp, err := http.Get(endpoint + "/test-user/gitattrs-model/resolve/main/.gitattributes")
	if err != nil {
		t.Fatalf("Failed to get .gitattributes: %v", err)
	}
	defer gaResp.Body.Close()
	if gaResp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for .gitattributes, got %d", gaResp.StatusCode)
	}

	body, err := io.ReadAll(gaResp.Body)
	if err != nil {
		t.Fatalf("Failed to read .gitattributes: %v", err)
	}

	for _, pattern := range []string{"*.bin", "*.safetensors", "*.pt", "filter=lfs"} {
		if !strings.Contains(string(body), pattern) {
			t.Errorf("Expected .gitattributes to contain %q, got:\n%s", pattern, body)
		}
	}
}

func TestHuggingFaceUploadLFS(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Step 1: Create repo via HF API
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"hf-lfs-model","organization":"lfs-user"}`))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 creating repo, got %d", resp.StatusCode)
	}

	// Step 2: Verify preupload returns "lfs" for .bin files
	preuploadBody := `{"files":[{"path":"model.bin","size":1024,"sample":""}]}`
	resp, err = http.Post(endpoint+"/api/models/lfs-user/hf-lfs-model/preupload/main",
		"application/json", strings.NewReader(preuploadBody))
	if err != nil {
		t.Fatalf("Failed to call preupload: %v", err)
	}
	preuploadResp, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("Failed to read preupload response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for preupload, got %d: %s", resp.StatusCode, preuploadResp)
	}

	var preuploadResult struct {
		Files []struct {
			Path       string `json:"path"`
			UploadMode string `json:"uploadMode"`
		} `json:"files"`
	}
	if err := json.Unmarshal(preuploadResp, &preuploadResult); err != nil {
		t.Fatalf("Failed to parse preupload JSON: %v", err)
	}
	if len(preuploadResult.Files) != 1 || preuploadResult.Files[0].UploadMode != "lfs" {
		t.Fatalf("Expected lfs upload mode for model.bin, got: %s", preuploadResp)
	}

	// Step 3: Prepare binary content and compute SHA256
	binContent := make([]byte, 1024)
	for i := range binContent {
		binContent[i] = byte(i % 256)
	}

	oid := sha256Hex(binContent)

	// Step 4: Upload LFS content via batch API
	batchBody := fmt.Sprintf(`{"operation":"upload","objects":[{"oid":"%s","size":%d}]}`, oid, len(binContent))
	req, err := http.NewRequest("POST", endpoint+"/lfs-user/hf-lfs-model.git/info/lfs/objects/batch",
		strings.NewReader(batchBody))
	if err != nil {
		t.Fatalf("Failed to create batch request: %v", err)
	}
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to call LFS batch: %v", err)
	}
	batchResp, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("Failed to read batch response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for LFS batch, got %d: %s", resp.StatusCode, batchResp)
	}

	var batchResult struct {
		Objects []struct {
			OID     string `json:"oid"`
			Size    int64  `json:"size"`
			Actions map[string]struct {
				Href   string            `json:"href"`
				Header map[string]string `json:"header"`
			} `json:"actions"`
		} `json:"objects"`
	}
	if err := json.Unmarshal(batchResp, &batchResult); err != nil {
		t.Fatalf("Failed to parse batch JSON: %v", err)
	}
	if len(batchResult.Objects) != 1 {
		t.Fatalf("Expected 1 object in batch response, got %d", len(batchResult.Objects))
	}
	uploadAction, ok := batchResult.Objects[0].Actions["upload"]
	if !ok {
		t.Fatalf("Expected upload action in batch response, got: %s", batchResp)
	}

	// Step 5: PUT the actual file content to LFS storage
	putReq, err := http.NewRequest("PUT", uploadAction.Href, bytes.NewReader(binContent))
	if err != nil {
		t.Fatalf("Failed to create PUT request: %v", err)
	}
	putReq.Header.Set("Content-Type", "application/octet-stream")
	putReq.ContentLength = int64(len(binContent))
	for k, v := range uploadAction.Header {
		putReq.Header.Set(k, v)
	}

	resp, err = http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("Failed to PUT LFS content: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for LFS PUT, got %d", resp.StatusCode)
	}

	// Step 6: Commit with lfsFile operation via HF API
	ndjson := fmt.Sprintf("{\"key\":\"header\",\"value\":{\"summary\":\"Add LFS model file\"}}\n"+
		"{\"key\":\"lfsFile\",\"value\":{\"path\":\"model.bin\",\"algo\":\"sha256\",\"oid\":\"%s\",\"size\":%d}}\n"+
		"{\"key\":\"file\",\"value\":{\"content\":\"# LFS Upload Test\\n\",\"path\":\"README.md\",\"encoding\":\"utf-8\"}}\n",
		oid, len(binContent))

	resp, err = http.Post(endpoint+"/api/models/lfs-user/hf-lfs-model/commit/main",
		"application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	commitResp, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("Failed to read commit response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for commit, got %d: %s", resp.StatusCode, commitResp)
	}

	// Step 7: Verify resolve returns the actual LFS content (not the pointer)
	resp, err = http.Get(endpoint + "/lfs-user/hf-lfs-model/resolve/main/model.bin")
	if err != nil {
		t.Fatalf("Failed to resolve model.bin: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("Failed to read model.bin body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for model.bin resolve, got %d: %s", resp.StatusCode, body)
	}
	if len(body) != len(binContent) {
		t.Fatalf("model.bin size mismatch: got %d, want %d", len(body), len(binContent))
	}
	for i := range binContent {
		if body[i] != binContent[i] {
			t.Fatalf("model.bin content mismatch at byte %d: got %d, want %d", i, body[i], binContent[i])
		}
	}

	// Step 8: Verify regular file is also accessible
	resp, err = http.Get(endpoint + "/lfs-user/hf-lfs-model/resolve/main/README.md")
	if err != nil {
		t.Fatalf("Failed to resolve README.md: %v", err)
	}
	readmeBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("Failed to read README.md body: %v", err)
	}
	if string(readmeBody) != "# LFS Upload Test\n" {
		t.Fatalf("README.md content mismatch: got %q, want %q", readmeBody, "# LFS Upload Test\n")
	}

	// Step 9: Verify tree API marks model.bin as LFS with correct size
	resp, err = http.Get(endpoint + "/api/models/lfs-user/hf-lfs-model/tree/main")
	if err != nil {
		t.Fatalf("Failed to get tree: %v", err)
	}
	treeBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("Failed to read tree body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for tree, got %d: %s", resp.StatusCode, treeBody)
	}

	var treeEntries []struct {
		Path string `json:"path"`
		Size int64  `json:"size"`
		LFS  *struct {
			OID         string `json:"oid"`
			Size        int64  `json:"size"`
			PointerSize int64  `json:"pointerSize"`
		} `json:"lfs,omitempty"`
	}
	if err := json.Unmarshal(treeBody, &treeEntries); err != nil {
		t.Fatalf("Failed to parse tree JSON: %v", err)
	}

	found := false
	for _, entry := range treeEntries {
		if entry.Path == "model.bin" {
			found = true
			if entry.LFS == nil {
				t.Fatalf("Expected model.bin to have LFS metadata, but lfs field is nil")
			}
			if entry.LFS.OID != oid {
				t.Errorf("LFS OID mismatch: got %q, want %q", entry.LFS.OID, oid)
			}
			if entry.LFS.Size != int64(len(binContent)) {
				t.Errorf("LFS size mismatch: got %d, want %d", entry.LFS.Size, len(binContent))
			}
			if entry.Size != int64(len(binContent)) {
				t.Errorf("Tree entry size mismatch: got %d, want %d", entry.Size, len(binContent))
			}
		}
	}
	if !found {
		t.Fatalf("model.bin not found in tree entries: %s", treeBody)
	}
}
