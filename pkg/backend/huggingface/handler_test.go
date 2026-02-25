package huggingface_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	backendhttp "github.com/wzshiming/gitd/pkg/backend/http"
	"github.com/wzshiming/gitd/pkg/backend/huggingface"
	backendlfs "github.com/wzshiming/gitd/pkg/backend/lfs"
	"github.com/wzshiming/gitd/pkg/storage"
)

func setupTestServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "hf-test-data")
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

func TestHuggingFaceCreateRepo(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	body := `{"type":"model","name":"test-model","organization":"test-user"}`
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var result huggingface.HFCreateRepoResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if result.URL == "" {
		t.Error("Expected url in response")
	}

	// Creating the same repo again should succeed (exist_ok behavior)
	resp2, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to create repo again: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp2.Body)
		t.Fatalf("Expected 200 for existing repo, got %d: %s", resp2.StatusCode, respBody)
	}
}

func TestHuggingFacePreupload(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create repo first
	createBody := `{"type":"model","name":"test-model","organization":"test-user"}`
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(createBody))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	// Test preupload
	preuploadBody := `{"files":[{"path":"README.md","size":20,"sample":""},{"path":"large.bin","size":20000000,"sample":""}]}`
	resp, err = http.Post(endpoint+"/api/models/test-user/test-model/preupload/main", "application/json", strings.NewReader(preuploadBody))
	if err != nil {
		t.Fatalf("Failed to preupload: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var result huggingface.HFPreuploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(result.Files) != 2 {
		t.Fatalf("Expected 2 files, got %d", len(result.Files))
	}
	if result.Files[0].UploadMode != "regular" {
		t.Errorf("Expected regular mode for README.md, got %s", result.Files[0].UploadMode)
	}
	if result.Files[1].UploadMode != "lfs" {
		t.Errorf("Expected lfs mode for large.bin, got %s", result.Files[1].UploadMode)
	}
}

func TestHuggingFaceCommitAndResolve(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create repo first
	createBody := `{"type":"model","name":"test-model","organization":"test-user"}`
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(createBody))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	// Commit a regular file
	ndjson := "{\"key\":\"header\",\"value\":{\"summary\":\"Initial commit\"}}\n" +
		"{\"key\":\"file\",\"value\":{\"content\":\"# Test Model\\n\",\"path\":\"README.md\",\"encoding\":\"utf-8\"}}\n"

	resp, err = http.Post(endpoint+"/api/models/test-user/test-model/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var commitResult huggingface.HFCommitResponse
	if err := json.NewDecoder(resp.Body).Decode(&commitResult); err != nil {
		t.Fatalf("Failed to decode commit response: %v", err)
	}

	if commitResult.CommitOid == "" {
		t.Error("Expected commitOid in response")
	}
	if commitResult.CommitMessage != "Initial commit" {
		t.Errorf("Expected commit message 'Initial commit', got %q", commitResult.CommitMessage)
	}

	// Verify the file is accessible via resolve endpoint
	resp, err = http.Get(endpoint + "/test-user/test-model/resolve/main/README.md")
	if err != nil {
		t.Fatalf("Failed to get file: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 for resolve, got %d: %s", resp.StatusCode, respBody)
	}

	content, _ := io.ReadAll(resp.Body)
	if string(content) != "# Test Model\n" {
		t.Errorf("Unexpected content: %q", content)
	}

	// Verify the model info endpoint shows the file
	resp, err = http.Get(endpoint + "/api/models/test-user/test-model")
	if err != nil {
		t.Fatalf("Failed to get model info: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 for model info, got %d: %s", resp.StatusCode, respBody)
	}

	var modelInfo huggingface.HFModelInfo
	if err := json.NewDecoder(resp.Body).Decode(&modelInfo); err != nil {
		t.Fatalf("Failed to decode model info: %v", err)
	}

	if len(modelInfo.Siblings) != 1 {
		t.Fatalf("Expected 1 sibling, got %d", len(modelInfo.Siblings))
	}
	if modelInfo.Siblings[0].RFilename != "README.md" {
		t.Errorf("Expected sibling filename 'README.md', got %q", modelInfo.Siblings[0].RFilename)
	}
}

func TestHuggingFaceCommitMultipleFiles(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create repo first
	createBody := `{"type":"model","name":"multi-model","organization":"test-user"}`
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(createBody))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	// Commit multiple files
	ndjson := "{\"key\":\"header\",\"value\":{\"summary\":\"Add multiple files\"}}\n" +
		"{\"key\":\"file\",\"value\":{\"content\":\"# Multi Model\\n\",\"path\":\"README.md\",\"encoding\":\"utf-8\"}}\n" +
		"{\"key\":\"file\",\"value\":{\"content\":\"config data\\n\",\"path\":\"config.json\",\"encoding\":\"utf-8\"}}\n"

	resp, err = http.Post(endpoint+"/api/models/test-user/multi-model/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	resp.Body.Close()

	// Verify both files
	for _, file := range []struct {
		path    string
		content string
	}{
		{"README.md", "# Multi Model\n"},
		{"config.json", "config data\n"},
	} {
		func() {
			resp, err = http.Get(endpoint + "/test-user/multi-model/resolve/main/" + file.path)
			if err != nil {
				t.Fatalf("Failed to get %s: %v", file.path, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				respBody, _ := io.ReadAll(resp.Body)
				t.Fatalf("Expected 200 for %s, got %d: %s", file.path, resp.StatusCode, respBody)
			}

			content, _ := io.ReadAll(resp.Body)
			if string(content) != file.content {
				t.Errorf("Unexpected content for %s: %q", file.path, content)
			}
		}()
	}
}

func TestHuggingFaceCommitLFSFile(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create repo first
	createBody := `{"type":"model","name":"lfs-model","organization":"test-user"}`
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(createBody))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	// Commit an LFS file (pointer)
	ndjson := "{\"key\":\"header\",\"value\":{\"summary\":\"Add LFS file\"}}\n" +
		"{\"key\":\"lfsFile\",\"value\":{\"path\":\"model.bin\",\"algo\":\"sha256\",\"oid\":\"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\",\"size\":1024}}\n"

	resp, err = http.Post(endpoint+"/api/models/test-user/lfs-model/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var commitResult huggingface.HFCommitResponse
	if err := json.NewDecoder(resp.Body).Decode(&commitResult); err != nil {
		t.Fatalf("Failed to decode commit response: %v", err)
	}

	if commitResult.CommitOid == "" {
		t.Error("Expected commitOid in response")
	}
}

func TestHuggingFaceCommitDeleteFile(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create repo first
	createBody := `{"type":"model","name":"delete-model","organization":"test-user"}`
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(createBody))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	// First commit - add a file
	ndjson := "{\"key\":\"header\",\"value\":{\"summary\":\"Add file\"}}\n" +
		"{\"key\":\"file\",\"value\":{\"content\":\"to be deleted\\n\",\"path\":\"temp.txt\",\"encoding\":\"utf-8\"}}\n" +
		"{\"key\":\"file\",\"value\":{\"content\":\"# Keep me\\n\",\"path\":\"README.md\",\"encoding\":\"utf-8\"}}\n"

	resp, err = http.Post(endpoint+"/api/models/test-user/delete-model/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to first commit: %v", err)
	}
	resp.Body.Close()

	// Second commit - delete the file
	ndjson = "{\"key\":\"header\",\"value\":{\"summary\":\"Delete file\"}}\n" +
		"{\"key\":\"deletedFile\",\"value\":{\"path\":\"temp.txt\"}}\n"

	resp, err = http.Post(endpoint+"/api/models/test-user/delete-model/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to second commit: %v", err)
	}
	resp.Body.Close()

	// Verify temp.txt is deleted
	resp, err = http.Get(endpoint + "/test-user/delete-model/resolve/main/temp.txt")
	if err != nil {
		t.Fatalf("Failed to get temp.txt: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected 404 for deleted file, got %d", resp.StatusCode)
	}

	// Verify README.md still exists
	resp, err = http.Get(endpoint + "/test-user/delete-model/resolve/main/README.md")
	if err != nil {
		t.Fatalf("Failed to get README.md: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 for README.md, got %d: %s", resp.StatusCode, respBody)
	}

	content, _ := io.ReadAll(resp.Body)
	if string(content) != "# Keep me\n" {
		t.Errorf("Unexpected content for README.md: %q", content)
	}
}
