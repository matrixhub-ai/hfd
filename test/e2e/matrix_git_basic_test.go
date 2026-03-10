package e2e_test

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

// TestMatrixGitBasicOperations tests basic git operations across all backends
func TestMatrixGitBasicOperations(t *testing.T) {
	RunMatrixTests(t, []BackendType{BackendHTTP, BackendSSH}, nil, func(t *testing.T, backend *TestBackend) {
		clientDir, err := os.MkdirTemp("", "matrix-git-basic")
		if err != nil {
			t.Fatalf("Failed to create client dir: %v", err)
		}
		defer os.RemoveAll(clientDir)

		// Create repository
		backend.CreateRepo("test-org", "basic-repo")
		repoPath := "test-org/basic-repo"

		t.Run("CloneEmptyRepo", func(t *testing.T) {
			cloneDir := filepath.Join(clientDir, "clone-empty")
			backend.RunGitCmd("", "clone", backend.GitURL(repoPath), cloneDir)

			gitDir := filepath.Join(cloneDir, ".git")
			if _, err := os.Stat(gitDir); os.IsNotExist(err) {
				t.Errorf(".git directory not found")
			}
		})

		t.Run("PushInitialCommit", func(t *testing.T) {
			workDir := filepath.Join(clientDir, "clone-empty")
			backend.RunGitCmd(workDir, "config", "user.email", "test@test.com")
			backend.RunGitCmd(workDir, "config", "user.name", "Test User")

			// Create and push a file
			testFile := filepath.Join(workDir, "README.md")
			if err := os.WriteFile(testFile, []byte("# Matrix Test\n"), 0644); err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}

			backend.RunGitCmd(workDir, "add", "README.md")
			backend.RunGitCmd(workDir, "commit", "-m", "Initial commit")
			backend.RunGitCmd(workDir, "push", "origin", "main")
		})

		t.Run("CloneWithContent", func(t *testing.T) {
			cloneDir := filepath.Join(clientDir, "clone-with-content")
			backend.RunGitCmd("", "clone", backend.GitURL(repoPath), cloneDir)

			readmePath := filepath.Join(cloneDir, "README.md")
			content, err := os.ReadFile(readmePath)
			if err != nil {
				t.Fatalf("Failed to read README.md: %v", err)
			}
			if string(content) != "# Matrix Test\n" {
				t.Errorf("Unexpected content: %s", content)
			}
		})

		t.Run("FetchChanges", func(t *testing.T) {
			workDir := filepath.Join(clientDir, "clone-with-content")
			backend.RunGitCmd(workDir, "fetch", "origin")
		})

		t.Run("PushAdditionalCommit", func(t *testing.T) {
			workDir := filepath.Join(clientDir, "clone-with-content")
			backend.RunGitCmd(workDir, "config", "user.email", "test@test.com")
			backend.RunGitCmd(workDir, "config", "user.name", "Test User")

			testFile := filepath.Join(workDir, "file2.txt")
			if err := os.WriteFile(testFile, []byte("Second file\n"), 0644); err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}

			backend.RunGitCmd(workDir, "add", "file2.txt")
			backend.RunGitCmd(workDir, "commit", "-m", "Add second file")
			backend.RunGitCmd(workDir, "push")
		})

		t.Run("PullChanges", func(t *testing.T) {
			firstCloneDir := filepath.Join(clientDir, "clone-empty")
			backend.RunGitCmd(firstCloneDir, "config", "pull.rebase", "false")
			backend.RunGitCmd(firstCloneDir, "pull")

			file2Path := filepath.Join(firstCloneDir, "file2.txt")
			if _, err := os.Stat(file2Path); os.IsNotExist(err) {
				t.Errorf("file2.txt not found after pull")
			}
		})

		// Only verify HTTP content access for HTTP backend
		if backend.Type == BackendHTTP {
			t.Run("VerifyContentViaHTTP", func(t *testing.T) {
				resp, err := http.Get(backend.APIURL() + "/" + repoPath + "/resolve/main/README.md")
				if err != nil {
					t.Fatalf("Failed to get file: %v", err)
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("Expected 200, got %d", resp.StatusCode)
				}
				body, _ := io.ReadAll(resp.Body)
				if string(body) != "# Matrix Test\n" {
					t.Errorf("Unexpected content: %q", body)
				}
			})
		}
	})
}

// TestMatrixGitMultipleFiles tests pushing multiple files across backends
func TestMatrixGitMultipleFiles(t *testing.T) {
	RunMatrixTests(t, []BackendType{BackendHTTP, BackendSSH}, nil, func(t *testing.T, backend *TestBackend) {
		clientDir, err := os.MkdirTemp("", "matrix-git-multi")
		if err != nil {
			t.Fatalf("Failed to create client dir: %v", err)
		}
		defer os.RemoveAll(clientDir)

		backend.CreateRepo("multi-org", "multi-repo")
		repoPath := "multi-org/multi-repo"

		cloneDir := filepath.Join(clientDir, "clone")
		backend.InitWorkDir(repoPath, cloneDir)

		// Create multiple files
		files := map[string]string{
			"README.md":  "# Multi-File Test\n",
			"config.yml": "key: value\n",
			"data.json":  `{"name": "test"}` + "\n",
			"notes.txt":  "Some notes\n",
		}

		for name, content := range files {
			if err := os.WriteFile(filepath.Join(cloneDir, name), []byte(content), 0644); err != nil {
				t.Fatalf("Failed to create file %s: %v", name, err)
			}
		}

		backend.RunGitCmd(cloneDir, "add", ".")
		backend.RunGitCmd(cloneDir, "commit", "-m", "Add multiple files")
		backend.RunGitCmd(cloneDir, "push", "origin", "main")

		// Verify via new clone
		verifyDir := filepath.Join(clientDir, "verify")
		backend.RunGitCmd("", "clone", backend.GitURL(repoPath), verifyDir)

		for name, expectedContent := range files {
			content, err := os.ReadFile(filepath.Join(verifyDir, name))
			if err != nil {
				t.Errorf("Failed to read %s: %v", name, err)
				continue
			}
			if string(content) != expectedContent {
				t.Errorf("File %s: expected %q, got %q", name, expectedContent, content)
			}
		}

		// Verify via HTTP for HTTP backend
		if backend.Type == BackendHTTP {
			for name, expectedContent := range files {
				resp, err := http.Get(backend.APIURL() + "/" + repoPath + "/resolve/main/" + name)
				if err != nil {
					t.Errorf("Failed to get %s via HTTP: %v", name, err)
					continue
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Errorf("Expected 200 for %s, got %d", name, resp.StatusCode)
					continue
				}
				body, _ := io.ReadAll(resp.Body)
				if string(body) != expectedContent {
					t.Errorf("HTTP content for %s: expected %q, got %q", name, expectedContent, body)
				}
			}
		}
	})
}

// TestMatrixGitBranchOperations tests branch creation and deletion across backends
func TestMatrixGitBranchOperations(t *testing.T) {
	RunMatrixTests(t, []BackendType{BackendHTTP, BackendSSH}, nil, func(t *testing.T, backend *TestBackend) {
		clientDir, err := os.MkdirTemp("", "matrix-git-branch")
		if err != nil {
			t.Fatalf("Failed to create client dir: %v", err)
		}
		defer os.RemoveAll(clientDir)

		backend.CreateRepo("branch-org", "branch-repo")
		repoPath := "branch-org/branch-repo"

		workDir := filepath.Join(clientDir, "work")
		backend.InitWorkDir(repoPath, workDir)

		// Create initial commit
		testFile := filepath.Join(workDir, "README.md")
		if err := os.WriteFile(testFile, []byte("# Branch Test\n"), 0644); err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		backend.RunGitCmd(workDir, "add", "README.md")
		backend.RunGitCmd(workDir, "commit", "-m", "Initial commit")
		backend.RunGitCmd(workDir, "push", "origin", "main")

		t.Run("CreateBranch", func(t *testing.T) {
			backend.RunGitCmd(workDir, "checkout", "-b", "feature-branch")
			featureFile := filepath.Join(workDir, "feature.txt")
			if err := os.WriteFile(featureFile, []byte("feature\n"), 0644); err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}
			backend.RunGitCmd(workDir, "add", "feature.txt")
			backend.RunGitCmd(workDir, "commit", "-m", "Feature commit")
			backend.RunGitCmd(workDir, "push", "origin", "feature-branch")
		})

		t.Run("SwitchBranch", func(t *testing.T) {
			backend.RunGitCmd(workDir, "checkout", "main")
			// Verify feature.txt doesn't exist on main
			featureFile := filepath.Join(workDir, "feature.txt")
			if _, err := os.Stat(featureFile); !os.IsNotExist(err) {
				t.Errorf("feature.txt should not exist on main branch")
			}
		})

		t.Run("DeleteBranch", func(t *testing.T) {
			backend.RunGitCmd(workDir, "push", "origin", "--delete", "feature-branch")
		})
	})
}

// TestMatrixGitTagOperations tests tag creation and deletion across backends
func TestMatrixGitTagOperations(t *testing.T) {
	RunMatrixTests(t, []BackendType{BackendHTTP, BackendSSH}, nil, func(t *testing.T, backend *TestBackend) {
		clientDir, err := os.MkdirTemp("", "matrix-git-tag")
		if err != nil {
			t.Fatalf("Failed to create client dir: %v", err)
		}
		defer os.RemoveAll(clientDir)

		backend.CreateRepo("tag-org", "tag-repo")
		repoPath := "tag-org/tag-repo"

		workDir := filepath.Join(clientDir, "work")
		backend.InitWorkDir(repoPath, workDir)

		// Create initial commit
		testFile := filepath.Join(workDir, "README.md")
		if err := os.WriteFile(testFile, []byte("# Tag Test\n"), 0644); err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		backend.RunGitCmd(workDir, "add", "README.md")
		backend.RunGitCmd(workDir, "commit", "-m", "Initial commit")
		backend.RunGitCmd(workDir, "push", "origin", "main")

		t.Run("CreateTag", func(t *testing.T) {
			backend.RunGitCmd(workDir, "tag", "v1.0.0")
			backend.RunGitCmd(workDir, "push", "origin", "v1.0.0")
		})

		t.Run("CreateAnnotatedTag", func(t *testing.T) {
			backend.RunGitCmd(workDir, "tag", "-a", "v1.1.0", "-m", "Version 1.1.0")
			backend.RunGitCmd(workDir, "push", "origin", "v1.1.0")
		})

		t.Run("DeleteTag", func(t *testing.T) {
			backend.RunGitCmd(workDir, "push", "origin", "--delete", "v1.0.0")
		})
	})
}
