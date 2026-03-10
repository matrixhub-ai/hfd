package e2e_test

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type hfCLIRepoType struct {
	name               string
	repoTypeArg        string
	resolvePrefix      string
	createArgs         []string
	uploadArgs         []string
	downloadArgs       []string
	repoArgs           []string
	supportsBranchTags bool
}

// TestHFCliOperationsMatrix exercises the hf CLI across repo types and common operations.
func TestHFCliOperationsMatrix(t *testing.T) {
	if _, err := exec.LookPath("hf"); err != nil {
		t.Skip("hf CLI not available, skipping HF CLI matrix test")
	}

	repoTypes := []hfCLIRepoType{
		{
			name:               "Model",
			repoTypeArg:        "model",
			resolvePrefix:      "",
			supportsBranchTags: true,
		},
		{
			name:               "Dataset",
			repoTypeArg:        "dataset",
			resolvePrefix:      "/datasets",
			createArgs:         []string{"--repo-type", "dataset"},
			uploadArgs:         []string{"--repo-type", "dataset"},
			downloadArgs:       []string{"--repo-type", "dataset"},
			repoArgs:           []string{"--repo-type", "dataset"},
			supportsBranchTags: true,
		},
		{
			name:          "Space",
			repoTypeArg:   "space",
			resolvePrefix: "/spaces",
			createArgs:    []string{"--repo-type", "space", "--space-sdk", "gradio"},
			uploadArgs:    []string{"--repo-type", "space"},
			downloadArgs:  []string{"--repo-type", "space"},
			repoArgs:      []string{"--repo-type", "space"},
		},
	}

	operations := []struct {
		name      string
		supported func(rt hfCLIRepoType) bool
		test      func(t *testing.T, endpoint string, rt hfCLIRepoType)
	}{
		{
			name:      "UploadAndDownload",
			supported: func(hfCLIRepoType) bool { return true },
			test:      testHFCliUploadDownload,
		},
		{
			name: "BranchAndTag",
			supported: func(rt hfCLIRepoType) bool {
				return rt.supportsBranchTags
			},
			test: testHFCliBranchAndTag,
		},
	}

	for _, rt := range repoTypes {
		rt := rt
		t.Run(rt.name, func(t *testing.T) {
			for _, op := range operations {
				op := op
				t.Run(op.name, func(t *testing.T) {
					if !op.supported(rt) {
						t.Skipf("%s not supported for %s", op.name, rt.name)
					}
					server, _ := setupTestServer(t)
					op.test(t, server.URL, rt)
				})
			}
		})
	}
}

func testHFCliUploadDownload(t *testing.T, endpoint string, rt hfCLIRepoType) {
	repoID := fmt.Sprintf("hf-cli/%s-upload", rt.repoTypeArg)

	createArgs := append([]string{"repo", "create", repoID, "--exist-ok"}, rt.createArgs...)
	runHFCmd(t, endpoint, createArgs...)

	uploadDir, err := os.MkdirTemp("", "hf-cli-upload-"+rt.repoTypeArg)
	if err != nil {
		t.Fatalf("Failed to create upload dir: %v", err)
	}
	defer os.RemoveAll(uploadDir)

	files := []struct {
		path    string
		content string
	}{
		{"README.md", "# HF CLI Matrix\n"},
		{"data/config.json", "{\"key\": \"value\"}\n"},
	}
	for _, file := range files {
		fp := filepath.Join(uploadDir, file.path)
		if err := os.MkdirAll(filepath.Dir(fp), 0755); err != nil {
			t.Fatalf("Failed to create dir for %s: %v", file.path, err)
		}
		if err := os.WriteFile(fp, []byte(file.content), 0644); err != nil {
			t.Fatalf("Failed to write %s: %v", file.path, err)
		}
	}

	uploadArgs := []string{"upload", repoID, uploadDir, ".", "--commit-message", "Upload via hf CLI"}
	uploadArgs = append(uploadArgs, rt.uploadArgs...)
	runHFCmd(t, endpoint, uploadArgs...)

	for _, file := range files {
		resp, err := http.Get(endpoint + rt.resolvePrefix + "/" + repoID + "/resolve/main/" + file.path)
		if err != nil {
			t.Fatalf("Failed to resolve %s: %v", file.path, err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
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

	downloadDir, err := os.MkdirTemp("", "hf-cli-download-"+rt.repoTypeArg)
	if err != nil {
		t.Fatalf("Failed to create download dir: %v", err)
	}
	defer os.RemoveAll(downloadDir)

	downloadArgs := []string{"download", repoID, "--local-dir", downloadDir}
	downloadArgs = append(downloadArgs, rt.downloadArgs...)
	runHFCmd(t, endpoint, downloadArgs...)

	for _, file := range files {
		content, err := os.ReadFile(filepath.Join(downloadDir, file.path))
		if err != nil {
			t.Fatalf("Failed to read downloaded %s: %v", file.path, err)
		}
		if string(content) != file.content {
			t.Errorf("Downloaded content mismatch for %s: got %q, want %q", file.path, content, file.content)
		}
	}
}

func testHFCliBranchAndTag(t *testing.T, endpoint string, rt hfCLIRepoType) {
	repoID := fmt.Sprintf("hf-cli/%s-branches", rt.repoTypeArg)

	createArgs := append([]string{"repo", "create", repoID, "--exist-ok"}, rt.createArgs...)
	runHFCmd(t, endpoint, createArgs...)

	uploadDir, err := os.MkdirTemp("", "hf-cli-branch-"+rt.repoTypeArg)
	if err != nil {
		t.Fatalf("Failed to create upload dir: %v", err)
	}
	defer os.RemoveAll(uploadDir)

	if err := os.WriteFile(filepath.Join(uploadDir, "README.md"), []byte("# Branch/Tag\n"), 0644); err != nil {
		t.Fatalf("Failed to write README: %v", err)
	}

	uploadArgs := []string{"upload", repoID, uploadDir, ".", "--commit-message", "Initial commit"}
	uploadArgs = append(uploadArgs, rt.uploadArgs...)
	runHFCmd(t, endpoint, uploadArgs...)

	runHFCmd(t, endpoint, append([]string{"repo", "branch", "create", repoID, "dev"}, rt.repoArgs...)...)
	runHFCmd(t, endpoint, append([]string{"repo", "branch", "delete", repoID, "dev"}, rt.repoArgs...)...)

	runHFCmd(t, endpoint, append([]string{"repo", "tag", "create", repoID, "v1.0", "-m", "First release"}, rt.repoArgs...)...)
	output := runHFCmd(t, endpoint, append([]string{"repo", "tag", "list", repoID}, rt.repoArgs...)...)
	if !strings.Contains(output, "v1.0") {
		t.Fatalf("Expected tag v1.0 in list output, got: %s", output)
	}
	runHFCmd(t, endpoint, append([]string{"repo", "tag", "delete", repoID, "v1.0", "--yes"}, rt.repoArgs...)...)

	output = runHFCmd(t, endpoint, append([]string{"repo", "tag", "list", repoID}, rt.repoArgs...)...)
	if strings.Contains(output, "v1.0") {
		t.Fatalf("Tag v1.0 should be removed, got list output: %s", output)
	}
}
