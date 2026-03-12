package e2e_test

import (
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixhub-ai/hfd/internal/utils"
)

// TestLFSOperationsMatrix tests LFS operations across different scenarios
func TestLFSOperationsMatrix(t *testing.T) {
	if _, err := exec.LookPath("git-lfs"); err != nil {
		t.Skip("git-lfs not available, skipping LFS matrix test")
	}

	testCases := []struct {
		name         string
		filePatterns []string
		files        map[string][]byte
	}{
		{
			name:         "SingleBinaryFile",
			filePatterns: []string{"*.bin"},
			files: map[string][]byte{
				"model.bin": makeBinaryData(1024, 0),
			},
		},
		{
			name:         "MultipleFileTypes",
			filePatterns: []string{"*.bin", "*.weights", "*.safetensors"},
			files: map[string][]byte{
				"model.bin":         makeBinaryData(512, 1),
				"weights.weights":   makeBinaryData(256, 2),
				"model.safetensors": makeBinaryData(128, 3),
			},
		},
		{
			name:         "LargeFile",
			filePatterns: []string{"*.large"},
			files: map[string][]byte{
				"large.large": makeBinaryData(2048, 4),
			},
		},
		{
			name:         "MultiplePatterns",
			filePatterns: []string{"*.pt", "*.pth"},
			files: map[string][]byte{
				"model.pt":  makeBinaryData(512, 5),
				"state.pth": makeBinaryData(256, 6),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server, _ := setupTestServer(t)
			endpoint := server.URL

			clientDir, err := os.MkdirTemp("", "lfs-matrix-client")
			if err != nil {
				t.Fatalf("Failed to create temp client dir: %v", err)
			}
			defer os.RemoveAll(clientDir)

			// Create a repo
			resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
				strings.NewReader(`{"type":"model","name":"lfs-test","organization":"lfs-org"}`))
			if err != nil {
				t.Fatalf("Failed to create repo: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("Expected 200 creating repo, got %d", resp.StatusCode)
			}

			httpGitURL := endpoint + "/lfs-org/lfs-test.git"
			env := []string{"GIT_TERMINAL_PROMPT=0"}

			// Clone the repo
			cloneDir := filepath.Join(clientDir, "clone")
			runLFSGitCmd(t, "", env, "clone", httpGitURL, cloneDir)

			// Configure user
			runLFSGitCmd(t, cloneDir, env, "config", "user.email", "test@test.com")
			runLFSGitCmd(t, cloneDir, env, "config", "user.name", "Test User")

			// Track files with LFS
			for _, pattern := range tc.filePatterns {
				runLFSGitCmd(t, cloneDir, env, "lfs", "track", pattern)
			}

			// Create files
			for name, content := range tc.files {
				if err := os.WriteFile(filepath.Join(cloneDir, name), content, 0644); err != nil {
					t.Fatalf("Failed to create file %s: %v", name, err)
				}
			}

			// Also create a regular text file
			if err := os.WriteFile(filepath.Join(cloneDir, "README.md"), []byte("# LFS Test\n"), 0644); err != nil {
				t.Fatalf("Failed to create README: %v", err)
			}

			// Add and commit
			runLFSGitCmd(t, cloneDir, env, "add", ".")
			runLFSGitCmd(t, cloneDir, env, "commit", "-m", "Add LFS tracked files")
			runLFSGitCmd(t, cloneDir, env, "push", "origin", "main")

			// Clone into a new directory and verify LFS content
			verifyDir := filepath.Join(clientDir, "verify")
			runLFSGitCmd(t, "", env, "clone", httpGitURL, verifyDir)

			// Pull LFS content
			runLFSGitCmd(t, verifyDir, env, "lfs", "pull")

			// Verify all files
			for name, expectedContent := range tc.files {
				verifyContent, err := os.ReadFile(filepath.Join(verifyDir, name))
				if err != nil {
					t.Fatalf("Failed to read %s from verify clone: %v", name, err)
				}
				if len(verifyContent) != len(expectedContent) {
					t.Errorf("File %s size mismatch: got %d, want %d", name, len(verifyContent), len(expectedContent))
				} else {
					for i := range expectedContent {
						if verifyContent[i] != expectedContent[i] {
							t.Errorf("File %s content mismatch at byte %d", name, i)
							break
						}
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
		})
	}
}

// makeBinaryData creates binary data for testing
func makeBinaryData(size int, seed byte) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i + int(seed)) % 256)
	}
	return data
}

// runLFSGitCmd runs a git command for LFS tests
func runLFSGitCmd(t *testing.T, dir string, env []string, args ...string) {
	t.Helper()
	cmd := utils.Command(t.Context(), "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = append(os.Environ(), env...)
	if output, err := cmd.Output(); err != nil {
		t.Fatalf("Git command failed: git %s\nError: %v\nOutput: %s", strings.Join(args, " "), err, output)
	}
}
