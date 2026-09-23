package hf

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/util"

	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// createRepoAndCommit creates a repo and commits a file, returning the commit SHA.
func createRepoAndCommit(t *testing.T, endpoint, repoType, org, name string) string {
	t.Helper()
	// Create repo
	body := `{"type":"` + repoType + `","name":"` + name + `","organization":"` + org + `"}`
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	// Determine API prefix
	apiPrefix := "/api/models"
	if repoType == "dataset" {
		apiPrefix = "/api/datasets"
	} else if repoType == "space" {
		apiPrefix = "/api/spaces"
	}

	// Commit a file
	ndjson := "{\"key\":\"header\",\"value\":{\"summary\":\"Initial commit\"}}\n" +
		"{\"key\":\"file\",\"value\":{\"content\":\"# Test\\n\",\"path\":\"README.md\",\"encoding\":\"utf-8\"}}\n"

	resp, err = http.Post(endpoint+apiPrefix+"/"+org+"/"+name+"/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	defer resp.Body.Close()

	var commitResult commitResponse
	if err := json.NewDecoder(resp.Body).Decode(&commitResult); err != nil {
		t.Fatalf("Failed to decode commit response: %v", err)
	}
	return commitResult.CommitOid
}

func TestHuggingFaceDeleteRepo(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create repo
	createBody := `{"type":"model","name":"delete-me","organization":"test-user"}`
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(createBody))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	// Verify repo exists
	resp, err = http.Get(endpoint + "/api/models/test-user/delete-me")
	if err != nil {
		t.Fatalf("Failed to get repo info: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected repo to exist, got %d", resp.StatusCode)
	}

	// Delete repo
	deleteBody := `{"type":"model","name":"delete-me","organization":"test-user"}`
	req, _ := http.NewRequest(http.MethodDelete, endpoint+"/api/repos/delete", strings.NewReader(deleteBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to delete repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.StatusCode)
	}

	// Verify repo no longer exists
	resp, err = http.Get(endpoint + "/api/models/test-user/delete-me")
	if err != nil {
		t.Fatalf("Failed to check repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404 after delete, got %d", resp.StatusCode)
	}
}

func TestHuggingFaceDeleteRepoNotFound(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	deleteBody := `{"type":"model","name":"nonexistent","organization":"test-user"}`
	req, _ := http.NewRequest(http.MethodDelete, endpoint+"/api/repos/delete", strings.NewReader(deleteBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to delete repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404, got %d", resp.StatusCode)
	}

	// The error names the repository as stored, type prefix included.
	deleteBody = `{"type":"dataset","name":"nonexistent","organization":"test-user"}`
	req, _ = http.NewRequest(http.MethodDelete, endpoint+"/api/repos/delete", strings.NewReader(deleteBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to delete repo: %v", err)
	}
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound || !strings.Contains(string(respBody), `datasets/test-user/nonexistent`) {
		t.Fatalf("Expected 404 naming datasets/test-user/nonexistent, got %d: %s", resp.StatusCode, respBody)
	}
}

func TestHuggingFaceDeleteRepoInvalidName(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	deleteBody := `{"type":"model","name":"../repo","organization":"x"}`
	req, _ := http.NewRequest(http.MethodDelete, endpoint+"/api/repos/delete", strings.NewReader(deleteBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to delete repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404, got %d", resp.StatusCode)
	}
}

func TestHuggingFaceDeleteDatasetRepo(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create dataset repo
	createBody := `{"type":"dataset","name":"delete-dataset","organization":"test-user"}`
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(createBody))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	// Delete dataset repo
	deleteBody := `{"type":"dataset","name":"delete-dataset","organization":"test-user"}`
	req, _ := http.NewRequest(http.MethodDelete, endpoint+"/api/repos/delete", strings.NewReader(deleteBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to delete repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.StatusCode)
	}

	// Verify repo no longer exists
	resp, err = http.Get(endpoint + "/api/datasets/test-user/delete-dataset")
	if err != nil {
		t.Fatalf("Failed to check repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404 after delete, got %d", resp.StatusCode)
	}
}

func TestHuggingFaceMoveRepo(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create and populate a repo
	createRepoAndCommit(t, endpoint, "model", "old-ns", "my-model")

	// Move repo
	moveBody := `{"fromRepo":"old-ns/my-model","toRepo":"new-ns/my-model","type":"model"}`
	resp, err := http.Post(endpoint+"/api/repos/move", "application/json", strings.NewReader(moveBody))
	if err != nil {
		t.Fatalf("Failed to move repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.StatusCode)
	}

	// Verify old location no longer exists
	resp, err = http.Get(endpoint + "/api/models/old-ns/my-model")
	if err != nil {
		t.Fatalf("Failed to check old repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404 for old location, got %d", resp.StatusCode)
	}

	// Verify new location exists and has the file
	resp, err = http.Get(endpoint + "/new-ns/my-model/resolve/main/README.md")
	if err != nil {
		t.Fatalf("Failed to get file at new location: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for new location, got %d", resp.StatusCode)
	}
	content, _ := io.ReadAll(resp.Body)
	if string(content) != "# Test\n" {
		t.Errorf("Unexpected content: %q", content)
	}
}

// An unresolvable source is a repository that does not exist, and it is
// judged before the destination.
func TestHuggingFaceMoveRepoInvalidSource(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	for _, moveBody := range []string{
		`{"fromRepo":"x/../repo","toRepo":"x/repo","type":"dataset"}`,
		`{"fromRepo":"x/../repo","toRepo":"y/../repo","type":"dataset"}`,
	} {
		resp, err := http.Post(endpoint+"/api/repos/move", "application/json", strings.NewReader(moveBody))
		if err != nil {
			t.Fatalf("Failed to move repo: %v", err)
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound || !strings.Contains(string(respBody), `datasets/x/../repo`) {
			t.Fatalf("%s: got %d %s, want 404 naming datasets/x/../repo", moveBody, resp.StatusCode, respBody)
		}
	}
}

func TestHuggingFaceRepoSettings(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create repo
	createBody := `{"type":"model","name":"settings-model","organization":"test-user"}`
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(createBody))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	// Update settings
	settingsBody := `{"private":true}`
	req, _ := http.NewRequest(http.MethodPut, endpoint+"/api/models/test-user/settings-model/settings", strings.NewReader(settingsBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to update settings: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.StatusCode)
	}

	// Update gated settings
	gatedBody := `{"gated":"auto"}`
	req, _ = http.NewRequest(http.MethodPut, endpoint+"/api/models/test-user/settings-model/settings", strings.NewReader(gatedBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to update gated setting: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.StatusCode)
	}

	// Settings of a missing repository
	req, _ = http.NewRequest(http.MethodPut, endpoint+"/api/models/test-user/missing/settings", strings.NewReader(settingsBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to update settings: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404, got %d", resp.StatusCode)
	}
}

func TestHuggingFaceCreateAndDeleteBranch(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create and populate a repo
	createRepoAndCommit(t, endpoint, "model", "test-user", "branch-model")

	// Create a branch
	req, _ := http.NewRequest(http.MethodPost, endpoint+"/api/models/test-user/branch-model/branch/dev", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to create branch: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("Expected 200 for create branch, got %d: %s", resp.StatusCode, respBody)
	}
	resp.Body.Close()

	// Verify the branch exists via refs endpoint
	resp, err = http.Get(endpoint + "/api/models/test-user/branch-model/refs")
	if err != nil {
		t.Fatalf("Failed to list refs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for refs, got %d", resp.StatusCode)
	}

	var refs gitRefs
	if err := json.NewDecoder(resp.Body).Decode(&refs); err != nil {
		t.Fatalf("Failed to decode refs: %v", err)
	}

	foundDev := false
	for _, b := range refs.Branches {
		if b.Name == "dev" {
			foundDev = true
			break
		}
	}
	if !foundDev {
		t.Errorf("Expected to find branch 'dev' in refs, got %+v", refs.Branches)
	}

	// Creating the same branch again should return conflict
	req, _ = http.NewRequest(http.MethodPost, endpoint+"/api/models/test-user/branch-model/branch/dev", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to create branch again: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("Expected 409 for duplicate branch, got %d", resp.StatusCode)
	}

	// Delete the branch
	req, _ = http.NewRequest(http.MethodDelete, endpoint+"/api/models/test-user/branch-model/branch/dev", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to delete branch: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for delete branch, got %d", resp.StatusCode)
	}

	// Verify branch is gone
	resp, err = http.Get(endpoint + "/api/models/test-user/branch-model/refs")
	if err != nil {
		t.Fatalf("Failed to list refs after delete: %v", err)
	}
	defer resp.Body.Close()

	var refs2 gitRefs
	if err := json.NewDecoder(resp.Body).Decode(&refs2); err != nil {
		t.Fatalf("Failed to decode refs: %v", err)
	}

	for _, b := range refs2.Branches {
		if b.Name == "dev" {
			t.Errorf("Branch 'dev' should have been deleted, but still found in refs")
		}
	}
}

func TestHuggingFaceCreateBranchFromRevision(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create and populate a repo
	commitSHA := createRepoAndCommit(t, endpoint, "model", "test-user", "branch-rev-model")

	// Create branch from specific revision
	body := `{"startingPoint":"` + commitSHA + `"}`
	req, _ := http.NewRequest(http.MethodPost, endpoint+"/api/models/test-user/branch-rev-model/branch/feature", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to create branch: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.StatusCode)
	}

	// Verify branch points to the correct commit
	resp, err = http.Get(endpoint + "/api/models/test-user/branch-rev-model/refs")
	if err != nil {
		t.Fatalf("Failed to list refs: %v", err)
	}
	defer resp.Body.Close()

	var refs gitRefs
	if err := json.NewDecoder(resp.Body).Decode(&refs); err != nil {
		t.Fatalf("Failed to decode refs: %v", err)
	}

	for _, b := range refs.Branches {
		if b.Name == "feature" {
			if b.TargetCommit != commitSHA {
				t.Errorf("Branch 'feature' points to %s, expected %s", b.TargetCommit, commitSHA)
			}
			return
		}
	}
	t.Error("Branch 'feature' not found in refs")
}

func TestHuggingFaceDeleteDefaultBranch(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	createRepoAndCommit(t, endpoint, "model", "test-user", "default-branch-model")

	// Try to delete default branch - should fail
	req, _ := http.NewRequest(http.MethodDelete, endpoint+"/api/models/test-user/default-branch-model/branch/main", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to delete branch: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("Expected 403 for deleting default branch, got %d", resp.StatusCode)
	}
}

func TestHuggingFaceCreateAndDeleteTag(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create and populate a repo
	createRepoAndCommit(t, endpoint, "model", "test-user", "tag-model")

	// Create a tag
	tagBody := `{"tag":"v1.0","message":"First release"}`
	resp, err := http.Post(endpoint+"/api/models/test-user/tag-model/tag/main", "application/json", strings.NewReader(tagBody))
	if err != nil {
		t.Fatalf("Failed to create tag: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("Expected 200 for create tag, got %d: %s", resp.StatusCode, respBody)
	}
	resp.Body.Close()

	// Verify tag exists via refs endpoint
	resp, err = http.Get(endpoint + "/api/models/test-user/tag-model/refs")
	if err != nil {
		t.Fatalf("Failed to list refs: %v", err)
	}
	defer resp.Body.Close()

	var refs gitRefs
	if err := json.NewDecoder(resp.Body).Decode(&refs); err != nil {
		t.Fatalf("Failed to decode refs: %v", err)
	}

	foundTag := false
	for _, tag := range refs.Tags {
		if tag.Name == "v1.0" {
			foundTag = true
			break
		}
	}
	if !foundTag {
		t.Errorf("Expected to find tag 'v1.0' in refs, got %+v", refs.Tags)
	}

	// Creating the same tag again should return conflict
	resp, err = http.Post(endpoint+"/api/models/test-user/tag-model/tag/main", "application/json", strings.NewReader(tagBody))
	if err != nil {
		t.Fatalf("Failed to create tag again: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("Expected 409 for duplicate tag, got %d", resp.StatusCode)
	}

	// Delete the tag
	req, _ := http.NewRequest(http.MethodDelete, endpoint+"/api/models/test-user/tag-model/tag/v1.0", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to delete tag: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for delete tag, got %d", resp.StatusCode)
	}

	// Verify tag is gone
	resp, err = http.Get(endpoint + "/api/models/test-user/tag-model/refs")
	if err != nil {
		t.Fatalf("Failed to list refs after tag delete: %v", err)
	}
	defer resp.Body.Close()

	var refs2 gitRefs
	if err := json.NewDecoder(resp.Body).Decode(&refs2); err != nil {
		t.Fatalf("Failed to decode refs: %v", err)
	}

	for _, tag := range refs2.Tags {
		if tag.Name == "v1.0" {
			t.Errorf("Tag 'v1.0' should have been deleted, but still found in refs")
		}
	}
}

func TestHuggingFaceListRefs(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create and populate a repo
	createRepoAndCommit(t, endpoint, "model", "test-user", "refs-model")

	// List refs
	resp, err := http.Get(endpoint + "/api/models/test-user/refs-model/refs")
	if err != nil {
		t.Fatalf("Failed to list refs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.StatusCode)
	}

	var refs gitRefs
	if err := json.NewDecoder(resp.Body).Decode(&refs); err != nil {
		t.Fatalf("Failed to decode refs: %v", err)
	}

	// Should have at least main branch
	if len(refs.Branches) == 0 {
		t.Error("Expected at least one branch")
	}

	foundMain := false
	for _, b := range refs.Branches {
		if b.Name == "main" {
			foundMain = true
			if b.Ref != "refs/heads/main" {
				t.Errorf("Expected ref 'refs/heads/main', got %q", b.Ref)
			}
			if b.TargetCommit == "" {
				t.Error("Expected non-empty target commit")
			}
		}
	}
	if !foundMain {
		t.Error("Expected to find branch 'main'")
	}

	// Converts should be empty
	if len(refs.Converts) != 0 {
		t.Errorf("Expected empty converts, got %+v", refs.Converts)
	}

	// Tags should be empty initially
	if len(refs.Tags) != 0 {
		t.Errorf("Expected empty tags, got %+v", refs.Tags)
	}
}

func TestHuggingFaceRepoInfoUsedStorage(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create a repo and commit a file
	createRepoAndCommit(t, endpoint, "model", "test-user", "storage-model")

	// Get repo info
	resp, err := http.Get(endpoint + "/api/models/test-user/storage-model")
	if err != nil {
		t.Fatalf("Failed to get repo info: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.StatusCode)
	}

	var info repoInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		t.Fatalf("Failed to decode repo info: %v", err)
	}

	if info.UsedStorage <= 0 {
		t.Errorf("Expected usedStorage > 0, got %d", info.UsedStorage)
	}
}

func TestHuggingFaceDatasetBranchAndTag(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create and populate a dataset repo
	createRepoAndCommit(t, endpoint, "dataset", "test-user", "branch-tag-dataset")

	// Create a branch on the dataset
	req, _ := http.NewRequest(http.MethodPost, endpoint+"/api/datasets/test-user/branch-tag-dataset/branch/dev", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to create branch: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for create branch on dataset, got %d", resp.StatusCode)
	}

	// Create a tag on the dataset
	tagBody := `{"tag":"v1.0"}`
	resp, err = http.Post(endpoint+"/api/datasets/test-user/branch-tag-dataset/tag/main", "application/json", strings.NewReader(tagBody))
	if err != nil {
		t.Fatalf("Failed to create tag: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for create tag on dataset, got %d", resp.StatusCode)
	}

	// List refs and verify both branch and tag exist
	resp, err = http.Get(endpoint + "/api/datasets/test-user/branch-tag-dataset/refs")
	if err != nil {
		t.Fatalf("Failed to list refs: %v", err)
	}
	defer resp.Body.Close()

	var refs gitRefs
	if err := json.NewDecoder(resp.Body).Decode(&refs); err != nil {
		t.Fatalf("Failed to decode refs: %v", err)
	}

	foundDev := false
	for _, b := range refs.Branches {
		if b.Name == "dev" {
			foundDev = true
		}
	}
	if !foundDev {
		t.Error("Expected to find branch 'dev' on dataset")
	}

	foundTag := false
	for _, tag := range refs.Tags {
		if tag.Name == "v1.0" {
			foundTag = true
		}
	}
	if !foundTag {
		t.Error("Expected to find tag 'v1.0' on dataset")
	}
}

func TestHuggingFaceSuperSquash(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create repo with an initial commit
	firstSHA := createRepoAndCommit(t, endpoint, "model", "test-user", "squash-model")

	// Make a second commit
	ndjson := "{\"key\":\"header\",\"value\":{\"summary\":\"Second commit\"}}\n" +
		"{\"key\":\"file\",\"value\":{\"content\":\"v2\\n\",\"path\":\"file.txt\",\"encoding\":\"utf-8\"}}\n"
	resp, err := http.Post(endpoint+"/api/models/test-user/squash-model/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to make second commit: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.StatusCode)
	}

	// Super-squash the branch
	squashBody := `{"message":"Squashed history"}`
	resp, err = http.Post(endpoint+"/api/models/test-user/squash-model/super-squash/main", "application/json", strings.NewReader(squashBody))
	if err != nil {
		t.Fatalf("Failed to super-squash: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.StatusCode)
	}

	// Verify we now have a single commit (the squashed one)
	resp, err = http.Get(endpoint + "/api/models/test-user/squash-model")
	if err != nil {
		t.Fatalf("Failed to get repo info: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.StatusCode)
	}

	var info repoInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		t.Fatalf("Failed to decode repo info: %v", err)
	}

	// The new SHA should differ from the original first commit SHA
	if info.SHA == "" {
		t.Error("Expected non-empty SHA after super-squash")
	}
	if info.SHA == firstSHA {
		t.Error("Expected SHA to differ from first commit after super-squash")
	}

	// Files should still be present
	resp2, err := http.Get(endpoint + "/test-user/squash-model/resolve/main/file.txt")
	if err != nil {
		t.Fatalf("Failed to resolve file: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("Expected file to exist after squash, got %d", resp2.StatusCode)
	}
}

func TestHuggingFaceSuperSquashDefaultMessage(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	createRepoAndCommit(t, endpoint, "model", "test-user", "squash-default-model")

	// Super-squash without a message (use default)
	resp, err := http.Post(endpoint+"/api/models/test-user/squash-default-model/super-squash/main", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("Failed to super-squash: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.StatusCode)
	}
}

func TestHuggingFaceSuperSquashNotFound(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	resp, err := http.Post(endpoint+"/api/models/test-user/nonexistent-model/super-squash/main", "application/json", strings.NewReader(`{"message":"test"}`))
	if err != nil {
		t.Fatalf("Failed to call super-squash: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404 for nonexistent repo, got %d", resp.StatusCode)
	}
}

func TestHuggingFaceCompare(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create a repo with an initial commit
	createRepoAndCommit(t, endpoint, "model", "test-user", "compare-model")

	// Make a second commit to have something to compare
	ndjson := "{\"key\":\"header\",\"value\":{\"summary\":\"Second commit\"}}\n" +
		"{\"key\":\"file\",\"value\":{\"content\":\"new content\\n\",\"path\":\"file2.txt\",\"encoding\":\"utf-8\"}}\n"
	resp, err := http.Post(endpoint+"/api/models/test-user/compare-model/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected commit status 200, got %d: %s", resp.StatusCode, respBody)
	}

	// Compare main..main (same revision, should return empty diff)
	resp, err = http.Get(endpoint + "/api/models/test-user/compare-model/compare/main..main")
	if err != nil {
		t.Fatalf("Failed to compare: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	body, _ := io.ReadAll(resp.Body)
	if len(body) != 0 {
		t.Errorf("Expected empty diff for same-rev compare, got %q", body)
	}
}

func TestHuggingFaceCompareWithChanges(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create a repo with an initial commit
	commit1SHA := createRepoAndCommit(t, endpoint, "model", "test-user", "compare-changes")

	// Create a branch from the initial commit
	req, _ := http.NewRequest(http.MethodPost, endpoint+"/api/models/test-user/compare-changes/branch/dev", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to create branch: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("Expected branch creation to return 200, got %d: %s", resp.StatusCode, respBody)
	}
	resp.Body.Close()

	// Add a file on the dev branch
	ndjson := "{\"key\":\"header\",\"value\":{\"summary\":\"Add file on dev\"}}\n" +
		"{\"key\":\"file\",\"value\":{\"content\":\"dev content\\n\",\"path\":\"dev-file.txt\",\"encoding\":\"utf-8\"}}\n"
	resp, err = http.Post(endpoint+"/api/models/test-user/compare-changes/commit/dev", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("Expected commit status 200, got %d: %s", resp.StatusCode, respBody)
	}
	resp.Body.Close()

	// Compare using commit SHA..dev (should show the new file as a unified diff)
	resp, err = http.Get(endpoint + "/api/models/test-user/compare-changes/compare/" + commit1SHA + "..dev")
	if err != nil {
		t.Fatalf("Failed to compare: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	body, _ := io.ReadAll(resp.Body)
	diff := string(body)

	// Verify the diff contains expected unified diff elements
	if !strings.Contains(diff, "diff --git") {
		t.Errorf("Expected diff to contain 'diff --git', got:\n%s", diff)
	}
	if !strings.Contains(diff, "dev-file.txt") {
		t.Errorf("Expected diff to mention 'dev-file.txt', got:\n%s", diff)
	}
	if !strings.Contains(diff, "+dev content") {
		t.Errorf("Expected diff to contain '+dev content', got:\n%s", diff)
	}
}

func TestHuggingFaceCompareInvalidFormat(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	createRepoAndCommit(t, endpoint, "model", "test-user", "compare-invalid")

	// Test invalid compare format (missing ..)
	resp, err := http.Get(endpoint + "/api/models/test-user/compare-invalid/compare/main")
	if err != nil {
		t.Fatalf("Failed to compare: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("Expected 400 for invalid compare format, got %d", resp.StatusCode)
	}

	// Test invalid compare format using "..." (three dots) instead of ".." (two dots)
	resp, err = http.Get(endpoint + "/api/models/test-user/compare-invalid/compare/main...dev")
	if err != nil {
		t.Fatalf("Failed to compare: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("Expected 400 for three-dot compare format, got %d", resp.StatusCode)
	}
}

func TestHuggingFaceCompareNotFound(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	resp, err := http.Get(endpoint + "/api/models/test-user/nonexistent-model/compare/main..dev")
	if err != nil {
		t.Fatalf("Failed to compare: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404 for nonexistent repo, got %d", resp.StatusCode)
	}
}

func TestHuggingFaceCompareDatasets(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create a dataset repo with an initial commit
	commit1SHA := createRepoAndCommit(t, endpoint, "dataset", "test-user", "compare-dataset")

	// Add another commit
	ndjson := "{\"key\":\"header\",\"value\":{\"summary\":\"Add data\"}}\n" +
		"{\"key\":\"file\",\"value\":{\"content\":\"data content\\n\",\"path\":\"data.csv\",\"encoding\":\"utf-8\"}}\n"
	resp, err := http.Post(endpoint+"/api/datasets/test-user/compare-dataset/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("Expected commit status 200, got %d: %s", resp.StatusCode, respBody)
	}
	resp.Body.Close()

	// Compare the two commits via datasets API
	resp, err = http.Get(endpoint + "/api/datasets/test-user/compare-dataset/compare/" + commit1SHA + "..main")
	if err != nil {
		t.Fatalf("Failed to compare: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	body, _ := io.ReadAll(resp.Body)
	diff := string(body)

	if !strings.Contains(diff, "diff --git") {
		t.Errorf("Expected diff output, got:\n%s", diff)
	}
	if !strings.Contains(diff, "data.csv") {
		t.Errorf("Expected diff to mention 'data.csv', got:\n%s", diff)
	}
}

func TestHuggingFaceListCommits(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// createRepoAndCommit makes 2 commits (.gitattributes + README.md).
	commit1SHA := createRepoAndCommit(t, endpoint, "model", "test-user", "commits-model")

	// Add a third commit.
	ndjson := "{\"key\":\"header\",\"value\":{\"summary\":\"Second commit\"}}\n" +
		"{\"key\":\"file\",\"value\":{\"content\":\"v2\\n\",\"path\":\"v2.txt\",\"encoding\":\"utf-8\"}}\n"
	resp, err := http.Post(endpoint+"/api/models/test-user/commits-model/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("Expected commit status 200, got %d: %s", resp.StatusCode, respBody)
	}
	resp.Body.Close()

	// List commits for the main branch.
	resp, err = http.Get(endpoint + "/api/models/test-user/commits-model/commits/main")
	if err != nil {
		t.Fatalf("Failed to list commits: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var commits []commitInfo
	if err := json.NewDecoder(resp.Body).Decode(&commits); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	// 3 commits total: .gitattributes, README.md, v2.txt
	if len(commits) != 3 {
		t.Fatalf("Expected 3 commits, got %d", len(commits))
	}
	if commits[0].Title != "Second commit" {
		t.Errorf("Expected first commit title 'Second commit', got %q", commits[0].Title)
	}
	// commit1SHA is the README.md commit (second oldest).
	if commits[1].ID != commit1SHA {
		t.Errorf("Expected second commit SHA %q, got %q", commit1SHA, commits[1].ID)
	}
	// No next page on a small repo.
	if link := resp.Header.Get("Link"); link != "" {
		t.Errorf("Expected no Link header, got %q", link)
	}
}

func TestHuggingFaceListCommitsPagination(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// createRepoAndCommit makes 2 commits; add 2 more → 4 total.
	createRepoAndCommit(t, endpoint, "model", "test-user", "commits-paged")

	for _, msg := range []string{"second", "third"} {
		ndjson := "{\"key\":\"header\",\"value\":{\"summary\":\"" + msg + "\"}}\n" +
			"{\"key\":\"file\",\"value\":{\"content\":\"" + msg + "\\n\",\"path\":\"" + msg + ".txt\",\"encoding\":\"utf-8\"}}\n"
		resp, err := http.Post(endpoint+"/api/models/test-user/commits-paged/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
		if err != nil {
			t.Fatalf("Failed to commit %q: %v", msg, err)
		}
		resp.Body.Close()
	}

	// First page: limit=2 → should return 2 commits and a Link header.
	resp, err := http.Get(endpoint + "/api/models/test-user/commits-paged/commits/main?limit=2")
	if err != nil {
		t.Fatalf("Failed to list commits page 1: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var page1 []commitInfo
	if err := json.NewDecoder(resp.Body).Decode(&page1); err != nil {
		t.Fatalf("Failed to decode page 1 response: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("Expected 2 commits on page 1, got %d", len(page1))
	}

	linkHeader := resp.Header.Get("Link")
	if linkHeader == "" {
		t.Fatalf("Expected Link header for page 1, got none")
	}

	// Extract the next-page URL from the Link header: <url>; rel="next"
	nextURL := strings.TrimSuffix(strings.TrimPrefix(strings.SplitN(linkHeader, ";", 2)[0], "<"), ">")

	// Second page via the Link URL.
	resp2, err := http.Get(nextURL)
	if err != nil {
		t.Fatalf("Failed to list commits page 2: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp2.Body)
		t.Fatalf("Expected 200 on page 2, got %d: %s", resp2.StatusCode, respBody)
	}

	var page2 []commitInfo
	if err := json.NewDecoder(resp2.Body).Decode(&page2); err != nil {
		t.Fatalf("Failed to decode page 2 response: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("Expected 2 commits on page 2, got %d", len(page2))
	}
	// Last page has no Link header.
	if link := resp2.Header.Get("Link"); link != "" {
		t.Errorf("Expected no Link header on last page, got %q", link)
	}

	// Ensure no overlap between pages.
	page1SHAs := map[string]bool{}
	for _, c := range page1 {
		page1SHAs[c.ID] = true
	}
	for _, c := range page2 {
		if page1SHAs[c.ID] {
			t.Errorf("Commit %q appears on both pages", c.ID)
		}
	}
}

func TestHuggingFaceListCommitsNotFound(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	resp, err := http.Get(endpoint + "/api/models/test-user/no-such-repo/commits/main")
	if err != nil {
		t.Fatalf("Failed to request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404 for nonexistent repo, got %d", resp.StatusCode)
	}
}

func TestHuggingFaceListCommitsDatasets(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// createRepoAndCommit makes 2 commits (.gitattributes + README.md).
	createRepoAndCommit(t, endpoint, "dataset", "test-user", "commits-dataset")

	resp, err := http.Get(endpoint + "/api/datasets/test-user/commits-dataset/commits/main")
	if err != nil {
		t.Fatalf("Failed to list commits: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var commits []commitInfo
	if err := json.NewDecoder(resp.Body).Decode(&commits); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(commits) != 2 {
		t.Fatalf("Expected 2 commits, got %d", len(commits))
	}
}

// seedCommit adds one file on branch of repoName, creating the repository when needed.
func seedCommit(t *testing.T, st *storage.Storage, repoName, branch, file string) string {
	t.Helper()
	ctx := context.Background()
	repoPath := repository.ResolvePath(repoName)
	repo, err := repository.Open(st.RepositoriesFS(), repoPath)
	if err != nil {
		if repo, err = repository.Init(ctx, st.RepositoriesFS(), repoPath, "main"); err != nil {
			t.Fatalf("init %s: %v", repoName, err)
		}
	}
	sha, err := repo.CreateCommit(ctx, branch, "Add "+file+"\n\nSeeded by the test.\n", "Alice", "alice@example.com",
		[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: file, Content: []byte(file)}}, "")
	if err != nil {
		t.Fatalf("commit %s: %v", repoName, err)
	}
	return sha
}

func getCommits(t *testing.T, h http.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

// decodeCommits asserts a 200 listing with the expected X-Total-Count and returns its titles.
func decodeCommits(t *testing.T, rec *httptest.ResponseRecorder, wantTotal string) ([]commitInfo, []string) {
	t.Helper()
	if rec.Code != http.StatusOK || rec.Header().Get("X-Total-Count") != wantTotal {
		t.Fatalf("status %d total %q, want 200 total %q: %s", rec.Code, rec.Header().Get("X-Total-Count"), wantTotal, rec.Body)
	}
	var items []commitInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &items); err != nil {
		t.Fatalf("decode %s: %v", rec.Body, err)
	}
	titles := make([]string, 0, len(items))
	for _, it := range items {
		titles = append(titles, it.Title)
	}
	return items, titles
}

// nextCommitsLink parses the rel="next" URL of the Link header.
func nextCommitsLink(t *testing.T, rec *httptest.ResponseRecorder) *url.URL {
	t.Helper()
	link := rec.Header().Get("Link")
	raw, _, ok := strings.Cut(strings.TrimPrefix(link, "<"), ">")
	if !ok || !strings.HasSuffix(link, `; rel="next"`) {
		t.Fatalf("Link %q", link)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("Link %q: %v", link, err)
	}
	return u
}

func TestHuggingFaceListCommitsTotalCountAndLink(t *testing.T) {
	st := storage.NewStorage(storage.WithRootDir(t.TempDir()))
	var checks, opened []string
	h := NewHandler(WithStorage(st),
		WithPermissionHookFunc(func(_ context.Context, op permission.Operation, name string, c permission.Context) (bool, error) {
			checks = append(checks, op.String()+" "+name+" "+c.Ref)
			return name != "alice/secret", nil
		}),
		WithPreOpenHookFunc(func(_ context.Context, name string, write bool) error {
			opened = append(opened, fmt.Sprintf("%s %v", name, write))
			return nil
		}))
	seedCommit(t, st, "alice/m1", "main", "README.md")
	seedCommit(t, st, "alice/m1", "main", "a.txt")
	sha3 := seedCommit(t, st, "alice/m1", "main", "b.txt")
	seedCommit(t, st, "alice/m1", "feature/x", "f1.txt")
	seedCommit(t, st, "alice/m1", "feature/x", "f2.txt")
	seedCommit(t, st, "alice/m1", "pct%x", "p1.txt")
	seedCommit(t, st, "alice/m1", "pct%x", "p2.txt")
	seedCommit(t, st, "datasets/alice/d1", "main", "README.md")
	seedCommit(t, st, "alice/secret", "main", "README.md")
	if _, err := repository.Init(context.Background(), st.RepositoriesFS(), repository.ResolvePath("alice/empty"), "main"); err != nil {
		t.Fatal(err)
	}

	rec := getCommits(t, h, "/api/models/alice/m1/commits/main")
	items, titles := decodeCommits(t, rec, "3")
	if want := []string{"Add b.txt", "Add a.txt", "Add README.md"}; !slices.Equal(titles, want) || rec.Header().Get("Link") != "" {
		t.Errorf("main: %v link %q", titles, rec.Header().Get("Link"))
	}
	first := items[0]
	if _, err := time.Parse(repository.TimeFormat, first.Date); err != nil {
		t.Errorf("date %q: %v", first.Date, err)
	}
	if first.ID != sha3 || first.Message != "Add b.txt\n\nSeeded by the test.\n" || len(first.Authors) != 1 || first.Authors[0].User != "Alice" {
		t.Errorf("commit %+v", first)
	}
	if !slices.Contains(checks, "read_repo alice/m1 main") || !slices.Contains(opened, "alice/m1 false") {
		t.Errorf("checks %v opened %v", checks, opened)
	}

	rec = getCommits(t, h, "/api/models/alice/m1/commits/main?limit=2&expand%5B%5D=formatted")
	if _, titles = decodeCommits(t, rec, "3"); len(titles) != 2 {
		t.Fatalf("page 0: %v", titles)
	}
	u := nextCommitsLink(t, rec)
	if u.Path != "/api/models/alice/m1/commits/main" || u.Query().Get("p") != "1" || u.Query().Get("limit") != "2" || u.Query().Get("expand[]") != "formatted" {
		t.Errorf("link %s", u)
	}
	rec = getCommits(t, h, u.RequestURI())
	if _, titles = decodeCommits(t, rec, "3"); !slices.Equal(titles, []string{"Add README.md"}) || rec.Header().Get("Link") != "" {
		t.Errorf("page 1: %v link %q", titles, rec.Header().Get("Link"))
	}
	rec = getCommits(t, h, "/api/models/alice/m1/commits/main?p=9223372036854775807&limit=50")
	if _, titles = decodeCommits(t, rec, "3"); len(titles) != 0 || rec.Header().Get("Link") != "" {
		t.Errorf("huge page: %v link %q", titles, rec.Header().Get("Link"))
	}
	rec = getCommits(t, h, "/api/models/alice/m1/commits/main?limit=9223372036854775807")
	if _, titles = decodeCommits(t, rec, "3"); len(titles) != 3 {
		t.Errorf("capped limit: %v", titles)
	}

	for _, tc := range []struct{ rev, second string }{{"feature%2Fx", "Add f1.txt"}, {"pct%25x", "Add p1.txt"}} {
		rec = getCommits(t, h, "/api/models/alice/m1/commits/"+tc.rev+"?limit=1")
		if _, titles = decodeCommits(t, rec, "2"); len(titles) != 1 {
			t.Fatalf("%s: %v", tc.rev, titles)
		}
		u = nextCommitsLink(t, rec)
		if !strings.HasSuffix(u.EscapedPath(), "/commits/"+tc.rev) {
			t.Errorf("%s link %s", tc.rev, u)
		}
		rec = getCommits(t, h, u.RequestURI())
		if _, titles = decodeCommits(t, rec, "2"); !slices.Equal(titles, []string{tc.second}) || rec.Header().Get("Link") != "" {
			t.Errorf("%s page 1: %v link %q", tc.rev, titles, rec.Header().Get("Link"))
		}
	}
	if !slices.Contains(checks, "read_repo alice/m1 feature/x") {
		t.Errorf("checks %v", checks)
	}

	rec = getCommits(t, h, "/api/datasets/alice/d1/commits/main")
	if _, titles = decodeCommits(t, rec, "1"); len(titles) != 1 || !slices.Contains(opened, "datasets/alice/d1 false") || !slices.Contains(checks, "read_repo datasets/alice/d1 main") {
		t.Errorf("dataset: %v opened %v checks %v", titles, opened, checks)
	}
	rec = getCommits(t, h, "/api/models/alice/empty/commits/main")
	if _, titles = decodeCommits(t, rec, "0"); len(titles) != 0 || rec.Header().Get("Link") != "" {
		t.Errorf("unborn: %v", titles)
	}

	for target, want := range map[string]int{
		"/api/models/alice/m1/commits/nope":                            http.StatusNotFound,
		"/api/models/alice/empty/commits/other":                        http.StatusNotFound,
		"/api/models/alice/missing/commits/main":                       http.StatusNotFound,
		"/api/datasets/alice/m1/commits/main":                          http.StatusNotFound,
		"/api/models/alice/secret/commits/main":                        http.StatusForbidden,
		"/api/models/alice/m1/commits/main?p=-1":                       http.StatusBadRequest,
		"/api/models/alice/m1/commits/main?p=x":                        http.StatusBadRequest,
		"/api/models/alice/m1/commits/main?p=9223372036854775808":      http.StatusBadRequest,
		"/api/models/alice/m1/commits/main?limit=0":                    http.StatusBadRequest,
		"/api/models/alice/m1/commits/main?limit=-1":                   http.StatusBadRequest,
		"/api/models/alice/m1/commits/main?limit=y":                    http.StatusBadRequest,
		"/api/models/alice/m1/commits/main?limit=99999999999999999999": http.StatusBadRequest,
	} {
		rec := getCommits(t, h, target)
		if rec.Code != want || !strings.Contains(rec.Body.String(), `"error"`) {
			t.Errorf("%s: %d %s", target, rec.Code, rec.Body)
		}
	}
	if slices.ContainsFunc(opened, func(s string) bool { return strings.HasPrefix(s, "alice/secret") }) {
		t.Errorf("denied repository was opened: %v", opened)
	}
}

func TestHuggingFaceListCommitsRevisionExpressions(t *testing.T) {
	root := t.TempDir()
	st := storage.NewStorage(storage.WithRootDir(root))
	h := NewHandler(WithStorage(st))
	sha1 := seedCommit(t, st, "alice/m1", "main", "README.md")
	seedCommit(t, st, "alice/m1", "main", "a.txt")
	if _, err := repository.Init(context.Background(), st.RepositoriesFS(), repository.ResolvePath("alice/empty"), "main"); err != nil {
		t.Fatal(err)
	}

	rec := getCommits(t, h, "/api/models/alice/m1/commits/"+url.PathEscape("HEAD^"))
	if items, _ := decodeCommits(t, rec, "1"); len(items) != 1 || items[0].ID != sha1 {
		t.Errorf("HEAD^: %+v", items)
	}
	for _, tc := range []struct{ repo, rev string }{
		{"m1", "^"}, {"m1", "~"}, {"m1", "main..main"}, {"m1", "main...main"}, {"m1", "main~999"}, {"m1", "main^2"}, {"m1", "nope"}, {"m1", "refs/heads/nope"},
		{"empty", "^"}, {"empty", "other"},
	} {
		target := "/api/models/alice/" + tc.repo + "/commits/" + url.PathEscape(tc.rev)
		if rec := getCommits(t, h, target); rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), `"error"`) {
			t.Errorf("%s: %d %s", target, rec.Code, rec.Body)
		}
	}

	// A parent whose object is gone is a storage failure, not an unknown revision.
	if err := st.RepositoriesFS().Remove(path.Join(repository.ResolvePath("alice/m1"), "objects", sha1[:2], sha1[2:])); err != nil {
		t.Fatal(err)
	}
	fresh := NewHandler(WithStorage(storage.NewStorage(storage.WithRootDir(root))))
	if rec := getCommits(t, fresh, "/api/models/alice/m1/commits/main~1"); rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), `"error"`) {
		t.Errorf("missing parent: %d %s", rec.Code, rec.Body)
	}
}

func TestHuggingFaceListCommitsMissingAncestor(t *testing.T) {
	root := t.TempDir()
	st := storage.NewStorage(storage.WithRootDir(root))
	sha1 := seedCommit(t, st, "alice/m1", "main", "README.md")
	seedCommit(t, st, "alice/m1", "main", "a.txt")
	seedCommit(t, st, "alice/m1", "main", "b.txt")
	if err := st.RepositoriesFS().Remove(path.Join(repository.ResolvePath("alice/m1"), "objects", sha1[:2], sha1[2:])); err != nil {
		t.Fatal(err)
	}
	h := NewHandler(WithStorage(storage.NewStorage(storage.WithRootDir(root))))
	rec := getCommits(t, h, "/api/models/alice/m1/commits/main?limit=1")
	if rec.Code != http.StatusInternalServerError || rec.Header().Get("X-Total-Count") != "" || !strings.Contains(rec.Body.String(), `"error"`) {
		t.Errorf("missing ancestor: %d total %q %s", rec.Code, rec.Header().Get("X-Total-Count"), rec.Body)
	}
}

// A reference whose target object cannot be read, alone or under a ~ or ^
// path, or a reference store that cannot be scanned, is a storage failure
// rather than an unknown revision.
func TestHuggingFaceListCommitsUnreadableRefTarget(t *testing.T) {
	root := t.TempDir()
	st := storage.NewStorage(storage.WithRootDir(root))
	first := seedCommit(t, st, "alice/m1", "main", "README.md")
	tip := seedCommit(t, st, "alice/m1", "main", "a.txt")
	repo, err := repository.Open(st.RepositoriesFS(), repository.ResolvePath("alice/m1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateBranch("feature/x", "main"); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateTag("v1", "main"); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateBranch("ok", first); err != nil {
		t.Fatal(err)
	}
	if err := st.RepositoriesFS().Remove(path.Join(repository.ResolvePath("alice/m1"), "objects", tip[:2], tip[2:])); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"alice/empty", "alice/broken"} {
		if _, err := repository.Init(context.Background(), st.RepositoriesFS(), repository.ResolvePath(name), "main"); err != nil {
			t.Fatal(err)
		}
	}
	if err := util.WriteFile(st.RepositoriesFS(), path.Join(repository.ResolvePath("alice/broken"), "packed-refs"), []byte("corrupt\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	h := NewHandler(WithStorage(storage.NewStorage(storage.WithRootDir(root))))
	for target, want := range map[string]int{
		"/api/models/alice/m1/commits/main":                       http.StatusInternalServerError,
		"/api/models/alice/m1/commits/HEAD":                       http.StatusInternalServerError,
		"/api/models/alice/m1/commits/@":                          http.StatusInternalServerError,
		"/api/models/alice/m1/commits/@~1":                        http.StatusInternalServerError,
		"/api/models/alice/m1/commits/refs%2Fheads%2Fmain":        http.StatusInternalServerError,
		"/api/models/alice/m1/commits/feature%2Fx":                http.StatusInternalServerError,
		"/api/models/alice/m1/commits/v1":                         http.StatusInternalServerError,
		"/api/models/alice/m1/commits/refs%2Ftags%2Fv1":           http.StatusInternalServerError,
		"/api/models/alice/m1/commits/main~1":                     http.StatusInternalServerError,
		"/api/models/alice/m1/commits/main~0":                     http.StatusInternalServerError,
		"/api/models/alice/m1/commits/HEAD^":                      http.StatusInternalServerError,
		"/api/models/alice/m1/commits/feature%2Fx~1":              http.StatusInternalServerError,
		"/api/models/alice/m1/commits/v1~1":                       http.StatusInternalServerError,
		"/api/models/alice/m1/commits/nope":                       http.StatusNotFound,
		"/api/models/alice/m1/commits/feature%2Fnope":             http.StatusNotFound,
		"/api/models/alice/m1/commits/refs%2Fheads%2Fnope":        http.StatusNotFound,
		"/api/models/alice/m1/commits/nope~1":                     http.StatusNotFound,
		"/api/models/alice/m1/commits/main^3":                     http.StatusNotFound,
		"/api/models/alice/m1/commits/ok^3":                       http.StatusNotFound,
		"/api/models/alice/m1/commits/ok~999":                     http.StatusNotFound,
		"/api/models/alice/m1/commits/" + strings.Repeat("a", 40): http.StatusNotFound,
		"/api/models/alice/broken/commits/main":                   http.StatusInternalServerError,
		"/api/models/alice/broken/commits/nope":                   http.StatusInternalServerError,
	} {
		rec := getCommits(t, h, target)
		if rec.Code != want || rec.Header().Get("X-Total-Count") != "" || !strings.Contains(rec.Body.String(), `"error"`) {
			t.Errorf("%s: %d total %q %s", target, rec.Code, rec.Header().Get("X-Total-Count"), rec.Body)
		}
	}
	if items, _ := decodeCommits(t, getCommits(t, h, "/api/models/alice/m1/commits/ok"), "1"); len(items) != 1 || items[0].ID != first {
		t.Errorf("ok: %+v", items)
	}
	if _, titles := decodeCommits(t, getCommits(t, h, "/api/models/alice/empty/commits/main"), "0"); len(titles) != 0 {
		t.Errorf("unborn: %v", titles)
	}
}
