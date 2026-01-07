package gitd_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/handlers"
	"github.com/wzshiming/gitd"
)

// TestImportRepository tests the repository import functionality.
func TestImportRepository(t *testing.T) {
	// Create a temporary directory for the destination repositories
	destRepoDir, err := os.MkdirTemp("", "gitd-test-dest-repos")
	if err != nil {
		t.Fatalf("Failed to create temp dest repo dir: %v", err)
	}
	defer os.RemoveAll(destRepoDir)

	// Create a temporary source repository with content to import
	sourceRepoDir, err := os.MkdirTemp("", "gitd-test-source-repos")
	if err != nil {
		t.Fatalf("Failed to create temp source repo dir: %v", err)
	}
	defer os.RemoveAll(sourceRepoDir)

	// Set up source repository server
	sourceHandler := handlers.LoggingHandler(os.Stderr, gitd.NewHandler(gitd.WithRootDir(sourceRepoDir)))
	sourceServer := httptest.NewServer(sourceHandler)
	defer sourceServer.Close()

	// Create source repository with content
	sourceRepoName := "source-repo.git"
	createSourceRepoWithContent(t, sourceRepoDir, sourceRepoName)

	// Set up destination server
	destHandler := handlers.LoggingHandler(os.Stderr, gitd.NewHandler(gitd.WithRootDir(destRepoDir)))
	destServer := httptest.NewServer(destHandler)
	defer destServer.Close()

	t.Run("ImportRepository", func(t *testing.T) {
		destRepoName := "imported-repo.git"
		sourceURL := sourceServer.URL + "/" + sourceRepoName

		// Start import
		importReq := map[string]string{"source_url": sourceURL}
		reqBody, _ := json.Marshal(importReq)

		req, _ := http.NewRequest(http.MethodPost, destServer.URL+"/api/repositories/"+destRepoName+"/import", bytes.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send import request: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusAccepted {
			t.Errorf("Expected status 202, got %d", resp.StatusCode)
		}

		// Wait for import to complete (poll status)
		completed := false
		for i := 0; i < 30; i++ {
			time.Sleep(500 * time.Millisecond)

			statusReq, _ := http.NewRequest(http.MethodGet, destServer.URL+"/api/repositories/"+destRepoName+"/import/status", nil)
			statusResp, err := http.DefaultClient.Do(statusReq)
			if err != nil {
				t.Fatalf("Failed to get import status: %v", err)
			}

			var status struct {
				Status string `json:"status"`
				Step   string `json:"step"`
				Error  string `json:"error"`
			}
			json.NewDecoder(statusResp.Body).Decode(&status)
			statusResp.Body.Close()

			if status.Status == "completed" {
				completed = true
				break
			}
			if status.Status == "failed" {
				t.Fatalf("Import failed: %s (step: %s)", status.Error, status.Step)
			}
		}

		if !completed {
			t.Fatal("Import did not complete within timeout")
		}

		// Verify imported repository exists and has content
		importedRepoPath := filepath.Join(destRepoDir, destRepoName)
		if _, err := os.Stat(importedRepoPath); os.IsNotExist(err) {
			t.Error("Imported repository does not exist")
		}

		// Verify the repository has branches
		cmd := exec.Command("git", "branch", "-a")
		cmd.Dir = importedRepoPath
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Failed to list branches: %v\nOutput: %s", err, output)
		}

		if len(output) == 0 {
			t.Error("Imported repository has no branches")
		}
	})

	t.Run("ImportMissingSourceURL", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodPost, destServer.URL+"/api/repositories/test.git/import", bytes.NewReader([]byte("{}")))
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", resp.StatusCode)
		}
	})

	t.Run("ImportInvalidRequestBody", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodPost, destServer.URL+"/api/repositories/test.git/import", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", resp.StatusCode)
		}
	})

	t.Run("ImportStatusNonexistent", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, destServer.URL+"/api/repositories/nonexistent.git/import/status", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status 404, got %d", resp.StatusCode)
		}
	})

	t.Run("MirrorInfoForImportedRepo", func(t *testing.T) {
		// The imported-repo.git from ImportRepository test should be marked as a mirror
		req, _ := http.NewRequest(http.MethodGet, destServer.URL+"/api/repositories/imported-repo.git/mirror", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}

		var mirrorInfo struct {
			IsMirror  bool   `json:"is_mirror"`
			SourceURL string `json:"source_url"`
		}
		json.NewDecoder(resp.Body).Decode(&mirrorInfo)

		if !mirrorInfo.IsMirror {
			t.Error("Expected repository to be marked as mirror")
		}
		if mirrorInfo.SourceURL == "" {
			t.Error("Expected source URL to be set")
		}
	})

	t.Run("SyncMirrorRepository", func(t *testing.T) {
		// Sync the imported mirror repository
		req, _ := http.NewRequest(http.MethodPost, destServer.URL+"/api/repositories/imported-repo.git/sync", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusAccepted {
			t.Errorf("Expected status 202, got %d", resp.StatusCode)
		}

		// Wait for sync to complete
		completed := false
		for i := 0; i < 10; i++ {
			time.Sleep(500 * time.Millisecond)

			statusReq, _ := http.NewRequest(http.MethodGet, destServer.URL+"/api/repositories/imported-repo.git/import/status", nil)
			statusResp, err := http.DefaultClient.Do(statusReq)
			if err != nil {
				continue
			}

			var status struct {
				Status string `json:"status"`
			}
			json.NewDecoder(statusResp.Body).Decode(&status)
			statusResp.Body.Close()

			if status.Status == "completed" {
				completed = true
				break
			}
		}

		if !completed {
			t.Error("Sync did not complete within timeout")
		}
	})

	t.Run("SyncNonMirrorRepository", func(t *testing.T) {
		// Create a regular (non-mirror) repository
		createReq, _ := http.NewRequest(http.MethodPost, destServer.URL+"/api/repositories/regular-repo.git", nil)
		createResp, _ := http.DefaultClient.Do(createReq)
		createResp.Body.Close()

		// Try to sync it - should fail
		req, _ := http.NewRequest(http.MethodPost, destServer.URL+"/api/repositories/regular-repo.git/sync", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", resp.StatusCode)
		}
	})
}

// createSourceRepoWithContent creates a source repository with some content for testing.
func createSourceRepoWithContent(t *testing.T, repoDir, repoName string) {
	t.Helper()

	repoPath := filepath.Join(repoDir, repoName)

	// Create temporary work directory for initial commit
	workDir, err := os.MkdirTemp("", "gitd-source-work")
	if err != nil {
		t.Fatalf("Failed to create work directory: %v", err)
	}
	defer os.RemoveAll(workDir)

	// Initialize a non-bare repository
	runCmd(t, workDir, "git", "init")
	runCmd(t, workDir, "git", "config", "user.email", "test@test.com")
	runCmd(t, workDir, "git", "config", "user.name", "Test User")

	// Create some files and commits
	os.WriteFile(filepath.Join(workDir, "README.md"), []byte("# Source Repository\n"), 0644)
	runCmd(t, workDir, "git", "add", "README.md")
	runCmd(t, workDir, "git", "commit", "-m", "Initial commit")

	os.WriteFile(filepath.Join(workDir, "file1.txt"), []byte("File 1 content\n"), 0644)
	runCmd(t, workDir, "git", "add", "file1.txt")
	runCmd(t, workDir, "git", "commit", "-m", "Add file1")

	os.WriteFile(filepath.Join(workDir, "file2.txt"), []byte("File 2 content\n"), 0644)
	runCmd(t, workDir, "git", "add", "file2.txt")
	runCmd(t, workDir, "git", "commit", "-m", "Add file2")

	// Create the bare repository
	runCmd(t, repoDir, "git", "clone", "--bare", workDir, repoPath)
}

// runCmd runs a command and fails the test if it errors.
func runCmd(t *testing.T, dir string, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Command failed: %s %v\nError: %v\nOutput: %s", name, args, err, output)
	}
}
