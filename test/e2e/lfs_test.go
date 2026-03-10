package e2e_test

import (
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wzshiming/hfd/internal/utils"
)

func TestLFSTrackPushPull(t *testing.T) {
	requireSuite(t, suiteGitLFS)

	if _, err := exec.LookPath("git-lfs"); err != nil {
		t.Skip("git-lfs not available, skipping LFS test")
	}

	server, _ := setupTestServer(t)
	endpoint := server.URL

	clientDir, err := os.MkdirTemp("", "lfs-e2e-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	// Create a repo via HuggingFace API
	resp, err := httpPost(endpoint+"/api/repos/create",
		`{"type":"model","name":"lfs-model","organization":"lfs-user"}`)
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Expected 200 creating repo, got %d", resp.StatusCode)
	}

	httpGitURL := endpoint + "/lfs-user/lfs-model.git"
	env := []string{"GIT_TERMINAL_PROMPT=0"}

	// Clone the repo
	cloneDir := filepath.Join(clientDir, "lfs-clone")
	cmd := utils.Command(t.Context(), "git", "clone", httpGitURL, cloneDir)
	cmd.Env = append(os.Environ(), env...)
	if output, err := cmd.Output(); err != nil {
		t.Fatalf("Clone failed: %v\n%s", err, output)
	}

	// Configure user
	for _, args := range [][]string{
		{"config", "user.email", "test@test.com"},
		{"config", "user.name", "Test User"},
	} {
		gitCmd := utils.Command(t.Context(), "git", args...)
		gitCmd.Dir = cloneDir
		gitCmd.Env = append(os.Environ(), env...)
		if output, err := gitCmd.Output(); err != nil {
			t.Fatalf("Git config failed: git %s\n%v\n%s", strings.Join(args, " "), err, output)
		}
	}

	// Track .bin files with LFS
	trackCmd := utils.Command(t.Context(), "git", "lfs", "track", "*.bin")
	trackCmd.Dir = cloneDir
	trackCmd.Env = append(os.Environ(), env...)
	if output, err := trackCmd.Output(); err != nil {
		t.Fatalf("Git LFS track failed: %v\n%s", err, output)
	}

	// Create a binary file that will be tracked by LFS
	binaryContent := make([]byte, 1024) // 1KB binary file
	for i := range binaryContent {
		binaryContent[i] = byte(i % 256)
	}
	binFile := filepath.Join(cloneDir, "model.bin")
	if err := os.WriteFile(binFile, binaryContent, 0644); err != nil {
		t.Fatalf("Failed to create binary file: %v", err)
	}

	// Also create a regular text file
	if err := os.WriteFile(filepath.Join(cloneDir, "README.md"), []byte("# LFS Test\n"), 0644); err != nil {
		t.Fatalf("Failed to create README: %v", err)
	}

	// Add and commit
	for _, args := range [][]string{
		{"add", ".gitattributes"},
		{"add", "model.bin"},
		{"add", "README.md"},
		{"commit", "-m", "Add LFS tracked file and README"},
		{"push", "origin", "main"},
	} {
		gitCmd := utils.Command(t.Context(), "git", args...)
		gitCmd.Dir = cloneDir
		gitCmd.Env = append(os.Environ(), env...)
		if output, err := gitCmd.Output(); err != nil {
			t.Fatalf("Git command failed: git %s\n%v\n%s", strings.Join(args, " "), err, output)
		}
	}

	// Clone into a new directory and verify LFS content
	verifyDir := filepath.Join(clientDir, "lfs-verify")
	verifyCmd := utils.Command(t.Context(), "git", "clone", httpGitURL, verifyDir)
	verifyCmd.Env = append(os.Environ(), env...)
	if output, err := verifyCmd.Output(); err != nil {
		t.Fatalf("Verify clone failed: %v\n%s", err, output)
	}

	// Pull LFS content
	lfsPullCmd := utils.Command(t.Context(), "git", "lfs", "pull")
	lfsPullCmd.Dir = verifyDir
	lfsPullCmd.Env = append(os.Environ(), env...)
	if output, err := lfsPullCmd.Output(); err != nil {
		t.Fatalf("Git LFS pull failed: %v\n%s", err, output)
	}

	// Verify the binary file content
	verifyContent, err := os.ReadFile(filepath.Join(verifyDir, "model.bin"))
	if err != nil {
		t.Fatalf("Failed to read model.bin from verify clone: %v", err)
	}
	if len(verifyContent) != len(binaryContent) {
		t.Errorf("Binary file size mismatch: got %d, want %d", len(verifyContent), len(binaryContent))
	} else {
		for i := range binaryContent {
			if verifyContent[i] != binaryContent[i] {
				t.Errorf("Binary file content mismatch at byte %d: got %d, want %d", i, verifyContent[i], binaryContent[i])
				break
			}
		}
	}

	// Verify the text file content
	readmeContent, err := os.ReadFile(filepath.Join(verifyDir, "README.md"))
	if err != nil {
		t.Fatalf("Failed to read README.md from verify clone: %v", err)
	}
	if string(readmeContent) != "# LFS Test\n" {
		t.Errorf("Unexpected README content: %q", readmeContent)
	}
}

func TestLFSMultipleFiles(t *testing.T) {
	requireSuite(t, suiteGitLFS)

	if _, err := exec.LookPath("git-lfs"); err != nil {
		t.Skip("git-lfs not available, skipping LFS test")
	}

	server, _ := setupTestServer(t)
	endpoint := server.URL

	clientDir, err := os.MkdirTemp("", "lfs-multi-e2e-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	// Create a repo
	resp, err := httpPost(endpoint+"/api/repos/create",
		`{"type":"model","name":"lfs-multi-model","organization":"lfs-multi-user"}`)
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	httpGitURL := endpoint + "/lfs-multi-user/lfs-multi-model.git"
	env := []string{"GIT_TERMINAL_PROMPT=0"}

	cloneDir := filepath.Join(clientDir, "clone")
	cmd := utils.Command(t.Context(), "git", "clone", httpGitURL, cloneDir)
	cmd.Env = append(os.Environ(), env...)
	if output, err := cmd.Output(); err != nil {
		t.Fatalf("Clone failed: %v\n%s", err, output)
	}

	for _, args := range [][]string{
		{"config", "user.email", "test@test.com"},
		{"config", "user.name", "Test User"},
	} {
		gitCmd := utils.Command(t.Context(), "git", args...)
		gitCmd.Dir = cloneDir
		gitCmd.Env = append(os.Environ(), env...)
		if output, err := gitCmd.Output(); err != nil {
			t.Fatalf("Git command failed: git %s\n%v\n%s", strings.Join(args, " "), err, output)
		}
	}

	// Track multiple file types
	for _, pattern := range []string{"*.bin", "*.weights", "*.safetensors"} {
		trackCmd := utils.Command(t.Context(), "git", "lfs", "track", pattern)
		trackCmd.Dir = cloneDir
		trackCmd.Env = append(os.Environ(), env...)
		if output, err := trackCmd.Output(); err != nil {
			t.Fatalf("Git LFS track %s failed: %v\n%s", pattern, err, output)
		}
	}

	// Create files of each type
	lfsFiles := map[string][]byte{
		"model.bin":         make([]byte, 512),
		"weights.weights":   make([]byte, 256),
		"model.safetensors": make([]byte, 128),
	}
	for name, content := range lfsFiles {
		for i := range content {
			content[i] = byte((i + len(name)) % 256)
		}
		if err := os.WriteFile(filepath.Join(cloneDir, name), content, 0644); err != nil {
			t.Fatalf("Failed to create file %s: %v", name, err)
		}
	}

	// Add regular file too
	if err := os.WriteFile(filepath.Join(cloneDir, "README.md"), []byte("# Multi-LFS Test\n"), 0644); err != nil {
		t.Fatalf("Failed to create README: %v", err)
	}

	for _, args := range [][]string{
		{"add", "."},
		{"commit", "-m", "Add multiple LFS files"},
		{"push", "origin", "main"},
	} {
		gitCmd := utils.Command(t.Context(), "git", args...)
		gitCmd.Dir = cloneDir
		gitCmd.Env = append(os.Environ(), env...)
		if output, err := gitCmd.Output(); err != nil {
			t.Fatalf("Git command failed: git %s\n%v\n%s", strings.Join(args, " "), err, output)
		}
	}

	// Clone and verify
	verifyDir := filepath.Join(clientDir, "verify")
	verifyCloneCmd := utils.Command(t.Context(), "git", "clone", httpGitURL, verifyDir)
	verifyCloneCmd.Env = append(os.Environ(), env...)
	if output, err := verifyCloneCmd.Output(); err != nil {
		t.Fatalf("Verify clone failed: %v\n%s", err, output)
	}

	lfsPullCmd := utils.Command(t.Context(), "git", "lfs", "pull")
	lfsPullCmd.Dir = verifyDir
	lfsPullCmd.Env = append(os.Environ(), env...)
	if output, err := lfsPullCmd.Output(); err != nil {
		t.Fatalf("Git LFS pull failed: %v\n%s", err, output)
	}

	// Verify all LFS files
	for name, expectedContent := range lfsFiles {
		t.Run("VerifyLFSFile_"+name, func(t *testing.T) {
			content, err := os.ReadFile(filepath.Join(verifyDir, name))
			if err != nil {
				t.Fatalf("Failed to read %s: %v", name, err)
			}
			if len(content) != len(expectedContent) {
				t.Errorf("Size mismatch for %s: got %d, want %d", name, len(content), len(expectedContent))
				return
			}
			for i := range expectedContent {
				if content[i] != expectedContent[i] {
					t.Errorf("Content mismatch for %s at byte %d", name, i)
					break
				}
			}
		})
	}
}

// httpPost is a simple helper for POST requests with JSON body.
func httpPost(url, body string) (*http.Response, error) {
	return http.Post(url, "application/json", strings.NewReader(body))
}
