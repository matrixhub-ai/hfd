package e2e_test

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wzshiming/hfd/internal/utils"
)

func TestHTTPGitCloneAndPush(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	clientDir, err := os.MkdirTemp("", "http-git-e2e-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	// Create a repo via the HuggingFace HTTP API
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"http-git-model","organization":"http-user"}`))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 creating repo, got %d", resp.StatusCode)
	}

	httpGitURL := endpoint + "/http-user/http-git-model.git"
	env := []string{"GIT_TERMINAL_PROMPT=0"}

	t.Run("CloneEmptyRepo", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-empty")
		cmd := utils.Command(t.Context(), "git", "clone", httpGitURL, cloneDir)
		cmd.Env = append(os.Environ(), env...)
		if output, err := cmd.Output(); err != nil {
			t.Fatalf("HTTP clone failed: %v\n%s", err, output)
		}

		gitDir := filepath.Join(cloneDir, ".git")
		if _, err := os.Stat(gitDir); os.IsNotExist(err) {
			t.Errorf(".git directory not found in cloned repository")
		}
	})

	t.Run("PushToRepo", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-empty")

		for _, args := range [][]string{
			{"config", "user.email", "test@test.com"},
			{"config", "user.name", "Test User"},
		} {
			cmd := utils.Command(t.Context(), "git", args...)
			cmd.Dir = workDir
			cmd.Env = append(os.Environ(), env...)
			if output, err := cmd.Output(); err != nil {
				t.Fatalf("Git command failed: git %s\n%v\n%s", strings.Join(args, " "), err, output)
			}
		}

		testFile := filepath.Join(workDir, "README.md")
		if err := os.WriteFile(testFile, []byte("# HTTP Git Test\n"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		for _, args := range [][]string{
			{"add", "README.md"},
			{"commit", "-m", "Initial commit via HTTP git"},
			{"push", "origin", "main"},
		} {
			cmd := utils.Command(t.Context(), "git", args...)
			cmd.Dir = workDir
			cmd.Env = append(os.Environ(), env...)
			if output, err := cmd.Output(); err != nil {
				t.Fatalf("Git command failed: git %s\n%v\n%s", strings.Join(args, " "), err, output)
			}
		}
	})

	t.Run("CloneWithContent", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-content")
		cmd := utils.Command(t.Context(), "git", "clone", httpGitURL, cloneDir)
		cmd.Env = append(os.Environ(), env...)
		if output, err := cmd.Output(); err != nil {
			t.Fatalf("HTTP clone failed: %v\n%s", err, output)
		}

		readmePath := filepath.Join(cloneDir, "README.md")
		content, err := os.ReadFile(readmePath)
		if err != nil {
			t.Fatalf("Failed to read README.md: %v", err)
		}
		if string(content) != "# HTTP Git Test\n" {
			t.Errorf("Unexpected content: %s", content)
		}
	})

	t.Run("FetchFromRepo", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-content")
		cmd := utils.Command(t.Context(), "git", "fetch", "origin")
		cmd.Dir = workDir
		cmd.Env = append(os.Environ(), env...)
		if output, err := cmd.Output(); err != nil {
			t.Fatalf("Git fetch failed: %v\n%s", err, output)
		}
	})

	t.Run("PushMoreCommits", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-content")

		for _, args := range [][]string{
			{"config", "user.email", "test@test.com"},
			{"config", "user.name", "Test User"},
		} {
			cmd := utils.Command(t.Context(), "git", args...)
			cmd.Dir = workDir
			cmd.Env = append(os.Environ(), env...)
			if output, err := cmd.Output(); err != nil {
				t.Fatalf("Git command failed: git %s\n%v\n%s", strings.Join(args, " "), err, output)
			}
		}

		testFile := filepath.Join(workDir, "file2.txt")
		if err := os.WriteFile(testFile, []byte("Second file via HTTP git\n"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		for _, args := range [][]string{
			{"add", "file2.txt"},
			{"commit", "-m", "Add second file"},
			{"push"},
		} {
			cmd := utils.Command(t.Context(), "git", args...)
			cmd.Dir = workDir
			cmd.Env = append(os.Environ(), env...)
			if output, err := cmd.Output(); err != nil {
				t.Fatalf("Git command failed: git %s\n%v\n%s", strings.Join(args, " "), err, output)
			}
		}
	})

	t.Run("PullChanges", func(t *testing.T) {
		firstCloneDir := filepath.Join(clientDir, "clone-empty")
		cmd := utils.Command(t.Context(), "git", "config", "pull.rebase", "false")
		cmd.Dir = firstCloneDir
		cmd.Env = append(os.Environ(), env...)
		if output, err := cmd.Output(); err != nil {
			t.Fatalf("Git config failed: %v\n%s", err, output)
		}

		pullCmd := utils.Command(t.Context(), "git", "pull")
		pullCmd.Dir = firstCloneDir
		pullCmd.Env = append(os.Environ(), env...)
		if output, err := pullCmd.Output(); err != nil {
			t.Fatalf("Git pull failed: %v\n%s", err, output)
		}

		file2Path := filepath.Join(firstCloneDir, "file2.txt")
		if _, err := os.Stat(file2Path); os.IsNotExist(err) {
			t.Errorf("file2.txt not found after pull")
		}
	})

	t.Run("VerifyContentViaHTTP", func(t *testing.T) {
		resp, err := http.Get(endpoint + "/http-user/http-git-model/resolve/main/README.md")
		if err != nil {
			t.Fatalf("Failed to get file: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200, got %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "# HTTP Git Test\n" {
			t.Errorf("Unexpected content: %q", body)
		}
	})
}

func TestHTTPGitMultipleFiles(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	clientDir, err := os.MkdirTemp("", "http-git-multi-e2e-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	// Create a repo
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"multi-file-model","organization":"multi-user"}`))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 creating repo, got %d", resp.StatusCode)
	}

	httpGitURL := endpoint + "/multi-user/multi-file-model.git"
	env := []string{"GIT_TERMINAL_PROMPT=0"}

	cloneDir := filepath.Join(clientDir, "clone")
	cmd := utils.Command(t.Context(), "git", "clone", httpGitURL, cloneDir)
	cmd.Env = append(os.Environ(), env...)
	if output, err := cmd.Output(); err != nil {
		t.Fatalf("Clone failed: %v\n%s", err, output)
	}

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

	for _, args := range [][]string{
		{"config", "user.email", "test@test.com"},
		{"config", "user.name", "Test User"},
		{"add", "."},
		{"commit", "-m", "Add multiple files"},
		{"push", "origin", "main"},
	} {
		gitCmd := utils.Command(t.Context(), "git", args...)
		gitCmd.Dir = cloneDir
		gitCmd.Env = append(os.Environ(), env...)
		if output, err := gitCmd.Output(); err != nil {
			t.Fatalf("Git command failed: git %s\n%v\n%s", strings.Join(args, " "), err, output)
		}
	}

	// Verify all files via HTTP
	for name, expectedContent := range files {
		t.Run("VerifyFile_"+name, func(t *testing.T) {
			resp, err := http.Get(endpoint + "/multi-user/multi-file-model/resolve/main/" + name)
			if err != nil {
				t.Fatalf("Failed to get file %s: %v", name, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("Expected 200 for %s, got %d", name, resp.StatusCode)
			}
			body, _ := io.ReadAll(resp.Body)
			if string(body) != expectedContent {
				t.Errorf("Unexpected content for %s: %q", name, body)
			}
		})
	}

	// Verify via new clone
	t.Run("VerifyViaClone", func(t *testing.T) {
		verifyDir := filepath.Join(clientDir, "verify-clone")
		cmd := utils.Command(t.Context(), "git", "clone", httpGitURL, verifyDir)
		cmd.Env = append(os.Environ(), env...)
		if output, err := cmd.Output(); err != nil {
			t.Fatalf("Verify clone failed: %v\n%s", err, output)
		}

		for name, expectedContent := range files {
			content, err := os.ReadFile(filepath.Join(verifyDir, name))
			if err != nil {
				t.Errorf("Failed to read %s from clone: %v", name, err)
				continue
			}
			if string(content) != expectedContent {
				t.Errorf("Unexpected content for %s in clone: %q", name, content)
			}
		}
	})
}
