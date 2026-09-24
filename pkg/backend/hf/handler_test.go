package hf_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/go-git/go-billy/v6/util"

	"github.com/matrixhub-ai/hfd/internal/server"
	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// Response shapes decoded by the tests; the handler's own types are unexported.
type (
	createRepoResponse struct {
		URL string `json:"url"`
	}
	preuploadResponse struct {
		Files []struct {
			Path         string `json:"path"`
			UploadMode   string `json:"uploadMode"`
			ShouldIgnore bool   `json:"shouldIgnore"`
		} `json:"files"`
	}
	commitResponse struct {
		CommitURL     string `json:"commitUrl"`
		CommitOid     string `json:"commitOid"`
		CommitMessage string `json:"commitMessage"`
	}
	sibling struct {
		RFilename string `json:"rfilename"`
	}
	repoInfo struct {
		ID          string    `json:"id"`
		ModelID     string    `json:"modelId"`
		SHA         string    `json:"sha"`
		Siblings    []sibling `json:"siblings"`
		UsedStorage int64     `json:"usedStorage"`
	}
	treeSize struct {
		Path string `json:"path"`
		Size int64  `json:"size"`
	}
	gitRefInfo struct {
		Name         string `json:"name"`
		Ref          string `json:"ref"`
		TargetCommit string `json:"targetCommit"`
	}
	gitRefs struct {
		Branches []gitRefInfo `json:"branches"`
		Converts []gitRefInfo `json:"converts"`
		Tags     []gitRefInfo `json:"tags"`
	}
	commitInfo struct {
		ID      string `json:"id"`
		Title   string `json:"title"`
		Message string `json:"message"`
		Authors []struct {
			User string `json:"user"`
		} `json:"authors"`
		Date string `json:"date"`
	}
)

func newStorage(t *testing.T, dir string) *storage.Storage {
	t.Helper()
	st, err := storage.NewStorage(storage.WithRootDir(dir))
	if err != nil {
		t.Fatalf("new storage: %v", err)
	}
	return st
}

// catalogOptions enables the six catalog routes with the server defaults over st.
func catalogOptions(st *storage.Storage) []hf.Option {
	hooks := &server.Hooks{Storage: st}
	return []hf.Option{
		hf.WithCreateRepoFunc(hooks.CreateRepo),
		hf.WithDeleteRepoFunc(hooks.DeleteRepo),
		hf.WithMoveRepoFunc(hooks.MoveRepo),
		hf.WithUpdateRepoSettingsFunc(hooks.UpdateRepoSettings),
		hf.WithListReposFunc(hooks.ListRepos),
		hf.WithWhoamiFunc(hooks.Whoami),
	}
}

func setupTestServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "hf-test-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	storage := newStorage(t, dataDir)

	// Set up handler chain (same order as main.go)
	var handler http.Handler

	handler = hf.NewHandler(append(catalogOptions(storage),
		hf.WithStorage(storage),
	)...)

	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(storage),
		backendlfs.WithNext(handler),
	)

	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(storage),
		backendhttp.WithNext(handler),
	)

	server := httptest.NewServer(handler)
	t.Cleanup(func() { server.Close() })

	return server, dataDir
}

// TestHuggingFacePreOpenHook pins what the pre-open hook observes: the
// request's repository name and write flag, that it runs before the open so
// it can create the repository, that its errors map like open errors, and
// that direct operations never reach it.
func TestHuggingFacePreOpenHook(t *testing.T) {
	ctx := context.Background()
	st := newStorage(t, t.TempDir())
	repo, err := repository.Init(ctx, st.RepositoriesFS(), repository.ResolvePath("datasets/org/repo"), "main")
	if err != nil {
		t.Fatalf("init repo: %v", err)
	}
	if _, err := repo.CreateCommit(ctx, "main", "init", "Test", "test@test.com",
		[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: "README.md", Content: []byte("# Test\n")}}, ""); err != nil {
		t.Fatalf("create commit: %v", err)
	}

	type call struct {
		name  string
		write bool
	}
	var calls []call
	var hookErr error
	h := hf.NewHandler(append(catalogOptions(st), hf.WithStorage(st), hf.WithPreOpenHookFunc(func(ctx context.Context, repoName string, write bool) error {
		calls = append(calls, call{repoName, write})
		if hookErr != nil {
			return hookErr
		}
		if repoName == "org/late" {
			_, err := repository.Init(ctx, st.RepositoriesFS(), repository.ResolvePath(repoName), "main")
			return err
		}
		return nil
	}))...)
	do := func(t *testing.T, method, target, body string, want int) {
		t.Helper()
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(method, target, strings.NewReader(body)))
		if rec.Code != want {
			t.Fatalf("%s %s: status = %d, want %d: %s", method, target, rec.Code, want, rec.Body)
		}
	}

	t.Run("ForwardsNameAndWrite", func(t *testing.T) {
		calls = nil
		do(t, http.MethodGet, "/api/datasets/org/repo", "", http.StatusOK)
		do(t, http.MethodPost, "/api/datasets/org/repo/tag/main", `{"tag":"v1"}`, http.StatusOK)
		want := []call{{"datasets/org/repo", false}, {"datasets/org/repo", true}}
		if len(calls) != len(want) || calls[0] != want[0] || calls[1] != want[1] {
			t.Fatalf("hook calls = %v, want %v", calls, want)
		}
	})

	t.Run("RunsBeforeOpen", func(t *testing.T) {
		do(t, http.MethodGet, "/api/models/org/late", "", http.StatusOK)
	})

	t.Run("ErrorMapsLikeOpen", func(t *testing.T) {
		hookErr = repository.ErrRepositoryNotExists
		defer func() { hookErr = nil }()
		do(t, http.MethodGet, "/api/datasets/org/repo", "", http.StatusNotFound)
	})

	t.Run("DirectOperationsBypass", func(t *testing.T) {
		calls = nil
		do(t, http.MethodPost, "/api/datasets/org/repo/preupload/main", `{"files":[{"path":"a.txt","size":1}]}`, http.StatusOK)
		do(t, http.MethodPost, "/api/datasets/org/repo/branch/feature", `{"startingPoint":"main"}`, http.StatusOK)
		do(t, http.MethodPut, "/api/datasets/org/repo/settings", `{"private":true}`, http.StatusOK)
		do(t, http.MethodPost, "/api/datasets/org/repo/super-squash/main", `{"message":"squash"}`, http.StatusOK)
		do(t, http.MethodPost, "/api/repos/move", `{"fromRepo":"org/repo","toRepo":"org/moved","type":"dataset"}`, http.StatusOK)
		do(t, http.MethodDelete, "/api/repos/delete", `{"type":"dataset","name":"moved","organization":"org"}`, http.StatusOK)
		if len(calls) != 0 {
			t.Fatalf("hook calls = %v, want none", calls)
		}
	})
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

	var result createRepoResponse
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
	preuploadBody := `{"files":[{"path":"README.md","size":20,"sample":""},{"path":"large.bin","size":20000000,"sample":""},{"path":"notes.txt","size":10485761,"sample":""},{"path":"small.txt","size":10485760,"sample":""}]}`
	resp, err = http.Post(endpoint+"/api/models/test-user/test-model/preupload/main", "application/json", strings.NewReader(preuploadBody))
	if err != nil {
		t.Fatalf("Failed to preupload: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var result preuploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(result.Files) != 4 {
		t.Fatalf("Expected 4 files, got %d", len(result.Files))
	}
	if result.Files[0].UploadMode != "regular" {
		t.Errorf("Expected regular mode for README.md, got %s", result.Files[0].UploadMode)
	}
	if result.Files[1].UploadMode != "lfs" {
		t.Errorf("Expected lfs mode for large.bin, got %s", result.Files[1].UploadMode)
	}
	if result.Files[2].UploadMode != "lfs" {
		t.Errorf("Expected lfs mode for notes.txt above the 10 MiB regular limit, got %s", result.Files[2].UploadMode)
	}
	if result.Files[3].UploadMode != "regular" {
		t.Errorf("Expected regular mode for small.txt at exactly 10 MiB, got %s", result.Files[3].UploadMode)
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

	var commitResult commitResponse
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

	var repoInfo repoInfo
	if err := json.NewDecoder(resp.Body).Decode(&repoInfo); err != nil {
		t.Fatalf("Failed to decode model info: %v", err)
	}

	if len(repoInfo.Siblings) != 2 {
		t.Fatalf("Expected 2 siblings, got %d", len(repoInfo.Siblings))
	}
	foundGitAttrs := false
	foundReadme := false
	for _, s := range repoInfo.Siblings {
		switch s.RFilename {
		case ".gitattributes":
			foundGitAttrs = true
		case "README.md":
			foundReadme = true
		}
	}
	if !foundGitAttrs {
		t.Errorf("Expected a sibling with filename '.gitattributes', but none was found")
	}
	if !foundReadme {
		t.Errorf("Expected a sibling with filename 'README.md', but none was found")
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

	var commitResult commitResponse
	if err := json.NewDecoder(resp.Body).Decode(&commitResult); err != nil {
		t.Fatalf("Failed to decode commit response: %v", err)
	}

	if commitResult.CommitOid == "" {
		t.Error("Expected commitOid in response")
	}
}

func TestHuggingFaceDatasetCreateAndCommit(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create a dataset repo
	body := `{"type":"dataset","name":"test-dataset","organization":"test-user"}`
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to create dataset repo: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var result createRepoResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if !strings.Contains(result.URL, "/datasets/test-user/test-dataset") {
		t.Errorf("Expected URL to contain '/datasets/test-user/test-dataset', got %q", result.URL)
	}

	// Commit a file via datasets API
	ndjson := "{\"key\":\"header\",\"value\":{\"summary\":\"Add dataset readme\"}}\n" +
		"{\"key\":\"file\",\"value\":{\"content\":\"# Test Dataset\\n\",\"path\":\"README.md\",\"encoding\":\"utf-8\"}}\n"

	resp, err = http.Post(endpoint+"/api/datasets/test-user/test-dataset/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to commit to dataset: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	// Resolve file via datasets resolve endpoint
	resp, err = http.Get(endpoint + "/datasets/test-user/test-dataset/resolve/main/README.md")
	if err != nil {
		t.Fatalf("Failed to resolve dataset file: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 for dataset resolve, got %d: %s", resp.StatusCode, respBody)
	}

	content, _ := io.ReadAll(resp.Body)
	if string(content) != "# Test Dataset\n" {
		t.Errorf("Unexpected dataset content: %q", content)
	}

	// Verify dataset info endpoint works
	resp, err = http.Get(endpoint + "/api/datasets/test-user/test-dataset")
	if err != nil {
		t.Fatalf("Failed to get dataset info: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 for dataset info, got %d: %s", resp.StatusCode, respBody)
	}

	var repoInfo repoInfo
	if err := json.NewDecoder(resp.Body).Decode(&repoInfo); err != nil {
		t.Fatalf("Failed to decode dataset info: %v", err)
	}
	if len(repoInfo.Siblings) != 2 {
		t.Errorf("Expected 2 siblings, got %v", repoInfo.Siblings)
	}
	foundGitAttrs := false
	foundReadme := false
	for _, s := range repoInfo.Siblings {
		switch s.RFilename {
		case ".gitattributes":
			foundGitAttrs = true
		case "README.md":
			foundReadme = true
		}
	}
	if !foundGitAttrs {
		t.Errorf("Expected a sibling with filename '.gitattributes', but none was found")
	}
	if !foundReadme {
		t.Errorf("Expected a sibling with filename 'README.md', but none was found")
	}
}

func TestHuggingFaceSpaceCreateAndCommit(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create a space repo
	body := `{"type":"space","name":"test-space","organization":"test-user"}`
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to create space repo: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var result createRepoResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if !strings.Contains(result.URL, "/spaces/test-user/test-space") {
		t.Errorf("Expected URL to contain '/spaces/test-user/test-space', got %q", result.URL)
	}

	// Commit a file via spaces API
	ndjson := "{\"key\":\"header\",\"value\":{\"summary\":\"Add space app\"}}\n" +
		"{\"key\":\"file\",\"value\":{\"content\":\"# Test Space\\n\",\"path\":\"README.md\",\"encoding\":\"utf-8\"}}\n"

	resp, err = http.Post(endpoint+"/api/spaces/test-user/test-space/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to commit to space: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	// Resolve file via spaces resolve endpoint
	resp, err = http.Get(endpoint + "/spaces/test-user/test-space/resolve/main/README.md")
	if err != nil {
		t.Fatalf("Failed to resolve space file: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 for space resolve, got %d: %s", resp.StatusCode, respBody)
	}

	content, _ := io.ReadAll(resp.Body)
	if string(content) != "# Test Space\n" {
		t.Errorf("Unexpected space content: %q", content)
	}

	// Verify space info endpoint works
	resp, err = http.Get(endpoint + "/api/spaces/test-user/test-space")
	if err != nil {
		t.Fatalf("Failed to get space info: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 for space info, got %d: %s", resp.StatusCode, respBody)
	}

	var repoInfo repoInfo
	if err := json.NewDecoder(resp.Body).Decode(&repoInfo); err != nil {
		t.Fatalf("Failed to decode space info: %v", err)
	}
	if len(repoInfo.Siblings) != 2 {
		t.Errorf("Expected 2 siblings, got %v", repoInfo.Siblings)
	}
	foundGitAttrs := false
	foundReadme := false
	for _, s := range repoInfo.Siblings {
		switch s.RFilename {
		case ".gitattributes":
			foundGitAttrs = true
		case "README.md":
			foundReadme = true
		}
	}
	if !foundGitAttrs {
		t.Errorf("Expected a sibling with filename '.gitattributes', but none was found")
	}
	if !foundReadme {
		t.Errorf("Expected a sibling with filename 'README.md', but none was found")
	}
}

func TestHuggingFaceKernelCreateAndCommit(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(`{"type":"kernel","name":"test-kernel","organization":"test-user"}`))
	if err != nil {
		t.Fatalf("Failed to create kernel repo: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}
	var result createRepoResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if !strings.HasSuffix(result.URL, "/kernels/test-user/test-kernel") {
		t.Errorf("Expected URL ending in '/kernels/test-user/test-kernel', got %q", result.URL)
	}

	ndjson := "{\"key\":\"header\",\"value\":{\"summary\":\"Add kernel\"}}\n" +
		"{\"key\":\"file\",\"value\":{\"content\":\"# Test Kernel\\n\",\"path\":\"README.md\",\"encoding\":\"utf-8\"}}\n"
	resp, err = http.Post(endpoint+"/api/kernels/test-user/test-kernel/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to commit to kernel: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	resp, err = http.Get(endpoint + "/kernels/test-user/test-kernel/resolve/main/README.md")
	if err != nil {
		t.Fatalf("Failed to resolve kernel file: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 for kernel resolve, got %d: %s", resp.StatusCode, respBody)
	}
	content, _ := io.ReadAll(resp.Body)
	if string(content) != "# Test Kernel\n" {
		t.Errorf("Unexpected kernel content: %q", content)
	}

	for _, path := range []string{"/api/kernels/test-user/test-kernel", "/api/kernels/test-user/test-kernel/revision/main"} {
		resp, err = http.Get(endpoint + path)
		if err != nil {
			t.Fatalf("Failed to get %s: %v", path, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected 200 for %s, got %d: %s", path, resp.StatusCode, respBody)
		}
		var info repoInfo
		if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
			t.Fatalf("Failed to decode %s: %v", path, err)
		}
		if info.ID != "test-user/test-kernel" || info.ModelID != "" {
			t.Errorf("%s: id = %q, modelId = %q; want test-user/test-kernel and no modelId", path, info.ID, info.ModelID)
		}
		var names []string
		for _, s := range info.Siblings {
			names = append(names, s.RFilename)
		}
		if len(names) != 2 || !slices.Contains(names, ".gitattributes") || !slices.Contains(names, "README.md") {
			t.Errorf("%s: siblings = %v, want .gitattributes and README.md", path, names)
		}
	}
}

func TestHuggingFaceDatasetPreupload(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create dataset repo first
	createBody := `{"type":"dataset","name":"test-dataset","organization":"test-user"}`
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(createBody))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	// Test preupload via datasets API
	preuploadBody := `{"files":[{"path":"data.csv","size":100,"sample":""}]}`
	resp, err = http.Post(endpoint+"/api/datasets/test-user/test-dataset/preupload/main", "application/json", strings.NewReader(preuploadBody))
	if err != nil {
		t.Fatalf("Failed to preupload: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var result preuploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(result.Files) != 1 {
		t.Fatalf("Expected 1 file, got %d", len(result.Files))
	}
	if result.Files[0].UploadMode != "regular" {
		t.Errorf("Expected regular mode, got %s", result.Files[0].UploadMode)
	}
}

func TestHuggingFaceRepoTypeIsolation(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create repos with the same name but different types
	for _, repoType := range []string{"model", "dataset", "space", "kernel"} {
		body := `{"type":"` + repoType + `","name":"shared-name","organization":"test-user"}`
		resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create %s repo: %v", repoType, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200 creating %s repo, got %d", repoType, resp.StatusCode)
		}
	}

	// Commit different content to each
	for _, tc := range []struct {
		repoType  string
		apiPrefix string
		content   string
	}{
		{"model", "/api/models", "model content"},
		{"dataset", "/api/datasets", "dataset content"},
		{"space", "/api/spaces", "space content"},
		{"kernel", "/api/kernels", "kernel content"},
	} {
		ndjson := "{\"key\":\"header\",\"value\":{\"summary\":\"Add readme\"}}\n" +
			"{\"key\":\"file\",\"value\":{\"content\":\"" + tc.content + "\\n\",\"path\":\"README.md\",\"encoding\":\"utf-8\"}}\n"

		resp, err := http.Post(endpoint+tc.apiPrefix+"/test-user/shared-name/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
		if err != nil {
			t.Fatalf("Failed to commit to %s: %v", tc.repoType, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200 committing to %s, got %d", tc.repoType, resp.StatusCode)
		}
	}

	// Verify each type has its own content
	for _, tc := range []struct {
		resolvePrefix string
		expected      string
	}{
		{"", "model content\n"},
		{"/datasets", "dataset content\n"},
		{"/spaces", "space content\n"},
		{"/kernels", "kernel content\n"},
	} {
		resp, err := http.Get(endpoint + tc.resolvePrefix + "/test-user/shared-name/resolve/main/README.md")
		if err != nil {
			t.Fatalf("Failed to resolve: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected 200 for prefix %q, got %d: %s", tc.resolvePrefix, resp.StatusCode, respBody)
		}

		content, _ := io.ReadAll(resp.Body)
		if string(content) != tc.expected {
			t.Errorf("For prefix %q: expected %q, got %q", tc.resolvePrefix, tc.expected, content)
		}
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

func TestHuggingFacePreuploadWithGitAttributes(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create repo first
	createBody := `{"type":"model","name":"attrs-model","organization":"test-user"}`
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(createBody))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	// Commit a .gitattributes file that marks *.bin and *.safetensors as LFS
	ndjson := "{\"key\":\"header\",\"value\":{\"summary\":\"Add gitattributes\"}}\n" +
		"{\"key\":\"file\",\"value\":{\"content\":\"*.bin filter=lfs diff=lfs merge=lfs -text\\n*.safetensors filter=lfs diff=lfs merge=lfs -text\\n\",\"path\":\".gitattributes\",\"encoding\":\"utf-8\"}}\n"

	resp, err = http.Post(endpoint+"/api/models/test-user/attrs-model/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	resp.Body.Close()

	// Test preupload: small .bin file should be LFS (matches .gitattributes pattern),
	// small .txt file should be regular (doesn't match any LFS pattern)
	preuploadBody := `{"files":[{"path":"model.bin","size":100,"sample":""},{"path":"README.txt","size":100,"sample":""},{"path":"weights.safetensors","size":50,"sample":""}]}`
	resp, err = http.Post(endpoint+"/api/models/test-user/attrs-model/preupload/main", "application/json", strings.NewReader(preuploadBody))
	if err != nil {
		t.Fatalf("Failed to preupload: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var result preuploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(result.Files) != 3 {
		t.Fatalf("Expected 3 files, got %d", len(result.Files))
	}
	// model.bin matches *.bin pattern → lfs (even though size is small)
	if result.Files[0].UploadMode != "lfs" {
		t.Errorf("Expected lfs mode for model.bin (matches .gitattributes), got %s", result.Files[0].UploadMode)
	}
	// README.txt doesn't match any LFS pattern and is small → regular
	if result.Files[1].UploadMode != "regular" {
		t.Errorf("Expected regular mode for README.txt, got %s", result.Files[1].UploadMode)
	}
	// weights.safetensors matches *.safetensors pattern → lfs
	if result.Files[2].UploadMode != "lfs" {
		t.Errorf("Expected lfs mode for weights.safetensors (matches .gitattributes), got %s", result.Files[2].UploadMode)
	}
}

func TestHuggingFaceTreeSize(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create and populate a repo with two files
	createBody := `{"type":"model","name":"treesize-model","organization":"test-user"}`
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(createBody))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	ndjson := "{\"key\":\"header\",\"value\":{\"summary\":\"Add files\"}}\n" +
		"{\"key\":\"file\",\"value\":{\"content\":\"hello\\n\",\"path\":\"README.md\",\"encoding\":\"utf-8\"}}\n" +
		"{\"key\":\"file\",\"value\":{\"content\":\"world\\n\",\"path\":\"sub/data.txt\",\"encoding\":\"utf-8\"}}\n"

	resp, err = http.Post(endpoint+"/api/models/test-user/treesize-model/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	resp.Body.Close()

	// Get size of the root tree
	resp, err = http.Get(endpoint + "/api/models/test-user/treesize-model/treesize/main/")
	if err != nil {
		t.Fatalf("Failed to get treesize: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 for treesize, got %d: %s", resp.StatusCode, respBody)
	}

	var result treeSize
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode treesize response: %v", err)
	}

	// Compute the size of .gitattributes dynamically to avoid brittle, hard-coded values.
	gitattributesResp, err := http.Get(endpoint + "/test-user/treesize-model/resolve/main/.gitattributes")
	if err != nil {
		t.Fatalf("Failed to get .gitattributes: %v", err)
	}
	defer gitattributesResp.Body.Close()

	if gitattributesResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(gitattributesResp.Body)
		t.Fatalf("Expected 200 for .gitattributes, got %d: %s", gitattributesResp.StatusCode, respBody)
	}

	gitattributesBody, err := io.ReadAll(gitattributesResp.Body)
	if err != nil {
		t.Fatalf("Failed to read .gitattributes body: %v", err)
	}

	// "hello\n" = 6 bytes, "world\n" = 6 bytes, ".gitattributes" = len(gitattributesBody) bytes
	expectedRootSize := int64(6 + 6 + len(gitattributesBody)) // README.md + sub/data.txt + .gitattributes
	if result.Size != expectedRootSize {
		t.Errorf("Expected treesize %d, got %d", expectedRootSize, result.Size)
	}

	// Get size of the sub/ subdirectory only
	resp, err = http.Get(endpoint + "/api/models/test-user/treesize-model/treesize/main/sub")
	if err != nil {
		t.Fatalf("Failed to get treesize for sub: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 for treesize sub, got %d: %s", resp.StatusCode, respBody)
	}

	var subResult treeSize
	if err := json.NewDecoder(resp.Body).Decode(&subResult); err != nil {
		t.Fatalf("Failed to decode treesize sub response: %v", err)
	}

	// "world\n" = 6 bytes
	if subResult.Size != 6 {
		t.Errorf("Expected treesize 6 for sub/, got %d", subResult.Size)
	}
}

func TestHuggingFaceTreeSizeNotFound(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	resp, err := http.Get(endpoint + "/api/models/nonexistent/no-repo/treesize/main/")
	if err != nil {
		t.Fatalf("Failed to request treesize: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected 404 for nonexistent repo, got %d", resp.StatusCode)
	}

	// A missing path or revision inside an existing repo is a 404 as well, on tree and treesize alike.
	createRepoAndCommit(t, endpoint, "model", "test-user", "treesize-missing")
	for _, path := range []string{
		"/api/models/test-user/treesize-missing/treesize/main/missing",
		"/api/models/test-user/treesize-missing/treesize/main/missing/deeper",
		"/api/models/test-user/treesize-missing/treesize/no-such-rev/",
		"/api/models/test-user/treesize-missing/tree/main/missing",
		"/api/models/test-user/treesize-missing/tree/no-such-rev/",
	} {
		resp, err := http.Get(endpoint + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404: %s", path, resp.StatusCode, body)
		}
	}
}

func TestCommitAuthorFromIdentity(t *testing.T) {
	tests := []struct {
		name string
		id   authenticate.Identity
		want string
	}{
		{"anonymous", authenticate.Anonymous, "HuggingFace"},
		{"named", authenticate.NewIdentity("alice", "alice@example.com"), "alice"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newStorage(t, t.TempDir())
			handler := hf.NewHandler(append(catalogOptions(st), hf.WithStorage(st))...)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handler.ServeHTTP(w, r.WithContext(authenticate.WithIdentity(r.Context(), tt.id)))
			}))
			defer server.Close()

			body := `{"type":"model","name":"repo","organization":"authors"}`
			resp, err := http.Post(server.URL+"/api/repos/create", "application/json", strings.NewReader(body))
			if err != nil {
				t.Fatalf("Failed to create repo: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("Expected 200, got %d", resp.StatusCode)
			}

			resp, err = http.Get(server.URL + "/api/models/authors/repo/commits/main")
			if err != nil {
				t.Fatalf("Failed to list commits: %v", err)
			}
			defer resp.Body.Close()
			var commits []commitInfo
			if err := json.NewDecoder(resp.Body).Decode(&commits); err != nil {
				t.Fatalf("Failed to decode commits: %v", err)
			}
			if len(commits) != 1 || len(commits[0].Authors) != 1 || commits[0].Authors[0].User != tt.want {
				t.Fatalf("commits = %+v, want one commit by %q", commits, tt.want)
			}
		})
	}
}

// TestHuggingFaceCommitParentPrecondition pins the parentCommit contract of
// the commit route: a matching parent commits and reaches the hooks, a stale
// parent is 412 naming both commits with no ref, content, or post-receive
// change, other CreateCommit failures stay 500, and a body read error is 400
// before any commit.
func TestHuggingFaceCommitParentPrecondition(t *testing.T) {
	ctx := context.Background()
	st := newStorage(t, t.TempDir())
	repoPath := repository.ResolvePath("org/repo")
	repo, err := repository.Init(ctx, st.RepositoriesFS(), repoPath, "main")
	if err != nil {
		t.Fatalf("init repo: %v", err)
	}
	first, err := repo.CreateCommit(ctx, "main", "init", "Test", "test@test.com",
		[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: "file.txt", Content: []byte("v1\n")}}, "")
	if err != nil {
		t.Fatalf("create commit: %v", err)
	}

	var pres, posts []receive.RefUpdate
	h := hf.NewHandler(hf.WithStorage(st),
		hf.WithPreReceiveHookFunc(func(_ context.Context, _ string, updates []receive.RefUpdate) (bool, error) {
			pres = append(pres, updates...)
			return true, nil
		}),
		hf.WithPostReceiveHookFunc(func(_ context.Context, _ string, updates []receive.RefUpdate) error {
			posts = append(posts, updates...)
			return nil
		}))
	commit := func(t *testing.T, body io.Reader, want int) *httptest.ResponseRecorder {
		t.Helper()
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/models/org/repo/commit/main", body))
		if rec.Code != want {
			t.Fatalf("commit status = %d, want %d: %s", rec.Code, want, rec.Body)
		}
		return rec
	}
	ndjson := func(parent, content string) string {
		return `{"key":"header","value":{"summary":"update","parentCommit":` + strconv.Quote(parent) + "}}\n" +
			`{"key":"file","value":{"path":"file.txt","content":` + strconv.Quote(content) + "}}\n"
	}
	tip := func(t *testing.T) string {
		t.Helper()
		hash, err := repo.ResolveRevision("main")
		if err != nil {
			t.Fatalf("resolve main: %v", err)
		}
		return hash
	}
	content := func(t *testing.T) string {
		t.Helper()
		blob, err := repo.Blob("main", "file.txt")
		if err != nil {
			t.Fatalf("blob: %v", err)
		}
		r, err := blob.NewReader()
		if err != nil {
			t.Fatalf("blob reader: %v", err)
		}
		defer r.Close()
		data, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("read blob: %v", err)
		}
		return string(data)
	}

	var second string
	t.Run("MatchingParent", func(t *testing.T) {
		rec := commit(t, strings.NewReader(ndjson(first, "v2\n")), http.StatusOK)
		var resp commitResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("decode commit response: %v", err)
		}
		second = resp.CommitOid
		if second == "" || second == first || tip(t) != second {
			t.Fatalf("commitOid = %q, tip = %q, want a new tip after parent %s", second, tip(t), first)
		}
		if got := content(t); got != "v2\n" {
			t.Fatalf("file.txt = %q, want %q", got, "v2\n")
		}
		if len(pres) != 1 || pres[0].OldRev() != first || pres[0].RefName() != "refs/heads/main" {
			t.Fatalf("pre-receive updates = %v, want one refs/heads/main update from %s", pres, first)
		}
		if len(posts) != 1 || posts[0].OldRev() != first || posts[0].NewRev() != second {
			t.Fatalf("post-receive updates = %v, want one %s -> %s", posts, first, second)
		}
	})

	t.Run("StaleParent", func(t *testing.T) {
		rec := commit(t, strings.NewReader(ndjson(first, "v3\n")), http.StatusPreconditionFailed)
		if body := rec.Body.String(); !strings.Contains(body, first) || !strings.Contains(body, second) {
			t.Fatalf("412 body %s must name expected %s and tip %s", body, first, second)
		}
		if got := tip(t); got != second {
			t.Fatalf("tip = %s, want unchanged %s", got, second)
		}
		if got := content(t); got != "v2\n" {
			t.Fatalf("file.txt = %q, want unchanged %q", got, "v2\n")
		}
		if len(posts) != 1 {
			t.Fatalf("post-receive updates = %v, want none after the rejected commit", posts[1:])
		}
	})

	t.Run("BodyReadError", func(t *testing.T) {
		body := io.MultiReader(strings.NewReader(ndjson(second, "v4\n")), iotest.ErrReader(errors.New("connection reset")))
		commit(t, body, http.StatusBadRequest)
		if got := tip(t); got != second {
			t.Fatalf("tip = %s, want unchanged %s", got, second)
		}
		if len(posts) != 1 {
			t.Fatalf("post-receive updates = %v, want none after the failed read", posts[1:])
		}
	})

	t.Run("UnreadableRefStays500", func(t *testing.T) {
		fs := st.RepositoriesFS()
		loose := repoPath + "/refs/heads/main"
		if err := fs.Remove(loose); err != nil {
			t.Fatalf("remove loose ref: %v", err)
		}
		// A truncated packed-refs line fails the ref read; go-git only reads it once the loose ref is gone.
		if err := util.WriteFile(fs, repoPath+"/packed-refs", []byte(second+"\n"), 0o644); err != nil {
			t.Fatalf("write packed-refs: %v", err)
		}
		rec := commit(t, strings.NewReader(ndjson(second, "v5\n")), http.StatusInternalServerError)
		if body := rec.Body.String(); !strings.Contains(body, "packed-ref") {
			t.Fatalf("500 body %s must report the packed-refs read failure", body)
		}
		if _, err := fs.Stat(loose); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("refs/heads/main after failed commit: %v, want absent", err)
		}
		if len(posts) != 1 {
			t.Fatalf("post-receive updates = %v, want none after the failed commit", posts[1:])
		}
		if err := fs.Remove(repoPath + "/packed-refs"); err != nil {
			t.Fatalf("remove packed-refs: %v", err)
		}
		if err := util.WriteFile(fs, loose, []byte(second+"\n"), 0o644); err != nil {
			t.Fatalf("restore loose ref: %v", err)
		}
		if got := tip(t); got != second {
			t.Fatalf("tip = %s, want unchanged %s", got, second)
		}
		if got := content(t); got != "v2\n" {
			t.Fatalf("file.txt = %q, want unchanged %q", got, "v2\n")
		}
	})

	t.Run("UnreadableTipStays500", func(t *testing.T) {
		if err := util.WriteFile(st.RepositoriesFS(), repoPath+"/refs/heads/main", []byte(strings.Repeat("1", 40)+"\n"), 0o644); err != nil {
			t.Fatalf("write dangling ref: %v", err)
		}
		commit(t, strings.NewReader(ndjson("", "v5\n")), http.StatusInternalServerError)
		if len(posts) != 1 {
			t.Fatalf("post-receive updates = %v, want none after the failed commit", posts[1:])
		}
	})
}
