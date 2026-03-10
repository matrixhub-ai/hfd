package e2e_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// checkPythonHFHub skips the test if Python3 or huggingface_hub are not available.
func checkPythonHFHub(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available, skipping Python huggingface_hub test")
	}
	cmd := exec.CommandContext(t.Context(), "python3", "-c", "import huggingface_hub")
	if err := cmd.Run(); err != nil {
		t.Skip("huggingface_hub not installed, skipping Python huggingface_hub test")
	}
}

// runPythonScript runs a Python3 script with HF_ENDPOINT and HF_TOKEN set.
// It fails the test on non-zero exit, printing the combined output.
func runPythonScript(t *testing.T, endpoint, script string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "python3", "-c", script)
	cmd.Env = append(os.Environ(),
		"HF_ENDPOINT="+endpoint,
		"HF_HUB_DISABLE_TELEMETRY=1",
		"HF_TOKEN=dummy-token",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Python script failed:\n%s\nOutput:\n%s", script, out)
	}
	return string(out)
}

// runPythonScriptMayFail runs a Python3 script and returns output and error without failing.
func runPythonScriptMayFail(t *testing.T, endpoint, script string) (string, error) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "python3", "-c", script)
	cmd.Env = append(os.Environ(),
		"HF_ENDPOINT="+endpoint,
		"HF_HUB_DISABLE_TELEMETRY=1",
		"HF_TOKEN=dummy-token",
	)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestPythonHFUploadFile(t *testing.T) {
	requireSuite(t, suiteHFPython)

	checkPythonHFHub(t)

	server, _ := setupTestServer(t)
	endpoint := server.URL

	tmpFile, err := os.CreateTemp("", "hf-python-upload-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString("Hello from Python huggingface_hub\n"); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	tmpFile.Close()

	script := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="py-user/upload-file-model", exist_ok=True)
api.upload_file(path_or_fileobj=%q, path_in_repo="hello.txt", repo_id="py-user/upload-file-model", commit_message="Upload via Python")
`, tmpFile.Name())
	runPythonScript(t, endpoint, script)

	// Verify the uploaded file via HTTP
	resp, err := server.Client().Get(endpoint + "/py-user/upload-file-model/resolve/main/hello.txt")
	if err != nil {
		t.Fatalf("Failed to get uploaded file: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Expected 200 for uploaded file, got %d", resp.StatusCode)
	}
}

func TestPythonHFDownloadFile(t *testing.T) {
	requireSuite(t, suiteHFPython)

	checkPythonHFHub(t)

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// First upload a file using the Python API
	uploadScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="py-user/download-file-model", exist_ok=True)
api.upload_file(path_or_fileobj=b"download me\n", path_in_repo="data.txt", repo_id="py-user/download-file-model", commit_message="Upload for download test")
`
	runPythonScript(t, endpoint, uploadScript)

	// Now download the file using hf_hub_download
	cacheDir, err := os.MkdirTemp("", "hf-python-cache")
	if err != nil {
		t.Fatalf("Failed to create cache dir: %v", err)
	}
	defer os.RemoveAll(cacheDir)

	downloadScript := fmt.Sprintf(`
import os
import huggingface_hub
path = huggingface_hub.hf_hub_download(
    repo_id="py-user/download-file-model",
    filename="data.txt",
    cache_dir=%q,
    endpoint=os.environ["HF_ENDPOINT"],
    token=os.environ["HF_TOKEN"],
)
content = open(path).read()
assert content == "download me\n", f"unexpected content: {content!r}"
`, cacheDir)
	runPythonScript(t, endpoint, downloadScript)
}

func TestPythonHFUploadFolder(t *testing.T) {
	requireSuite(t, suiteHFPython)

	checkPythonHFHub(t)

	server, _ := setupTestServer(t)
	endpoint := server.URL

	folderDir, err := os.MkdirTemp("", "hf-python-folder")
	if err != nil {
		t.Fatalf("Failed to create temp folder: %v", err)
	}
	defer os.RemoveAll(folderDir)

	files := map[string]string{
		"README.md":   "# Python Folder Upload\n",
		"model.bin":   "fake model weights\n",
		"config.json": `{"model_type": "bert"}` + "\n",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(folderDir, name), []byte(content), 0644); err != nil {
			t.Fatalf("Failed to write %s: %v", name, err)
		}
	}

	script := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="py-user/upload-folder-model", exist_ok=True)
api.upload_folder(folder_path=%q, repo_id="py-user/upload-folder-model", commit_message="Upload folder via Python")
`, folderDir)
	runPythonScript(t, endpoint, script)

	// Verify all uploaded files
	for name := range files {
		resp, err := server.Client().Get(endpoint + "/py-user/upload-folder-model/resolve/main/" + name)
		if err != nil {
			t.Fatalf("Failed to get %s: %v", name, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("Expected 200 for %s, got %d", name, resp.StatusCode)
		}
	}
}

func TestPythonHFSnapshotDownload(t *testing.T) {
	requireSuite(t, suiteHFPython)

	checkPythonHFHub(t)

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Upload files first
	uploadScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="py-user/snapshot-model", exist_ok=True)
api.upload_file(path_or_fileobj=b"file one\n", path_in_repo="file1.txt", repo_id="py-user/snapshot-model")
api.upload_file(path_or_fileobj=b"file two\n", path_in_repo="file2.txt", repo_id="py-user/snapshot-model")
api.upload_file(path_or_fileobj=b"sub content\n", path_in_repo="sub/file3.txt", repo_id="py-user/snapshot-model")
`
	runPythonScript(t, endpoint, uploadScript)

	localDir, err := os.MkdirTemp("", "hf-python-snapshot")
	if err != nil {
		t.Fatalf("Failed to create local dir: %v", err)
	}
	defer os.RemoveAll(localDir)

	downloadScript := fmt.Sprintf(`
import os
import huggingface_hub
local_dir = huggingface_hub.snapshot_download(
    repo_id="py-user/snapshot-model",
    local_dir=%q,
    endpoint=os.environ["HF_ENDPOINT"],
    token=os.environ["HF_TOKEN"],
)
assert open(os.path.join(local_dir, "file1.txt")).read() == "file one\n"
assert open(os.path.join(local_dir, "file2.txt")).read() == "file two\n"
assert open(os.path.join(local_dir, "sub", "file3.txt")).read() == "sub content\n"
`, localDir)
	runPythonScript(t, endpoint, downloadScript)
}

func TestPythonHFCreateAndDeleteRepo(t *testing.T) {
	requireSuite(t, suiteHFPython)

	checkPythonHFHub(t)

	server, _ := setupTestServer(t)
	endpoint := server.URL

	createScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
url = api.create_repo(repo_id="py-user/create-delete-model", exist_ok=False)
assert url is not None
`
	runPythonScript(t, endpoint, createScript)

	// Verify the repo exists via API
	resp, err := server.Client().Get(endpoint + "/api/models/py-user/create-delete-model")
	if err != nil {
		t.Fatalf("Failed to check repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Expected 200 for created repo, got %d", resp.StatusCode)
	}

	// Delete the repo
	deleteScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.delete_repo(repo_id="py-user/create-delete-model")
`
	runPythonScript(t, endpoint, deleteScript)

	// Verify repo is gone
	resp, err = server.Client().Get(endpoint + "/api/models/py-user/create-delete-model")
	if err != nil {
		t.Fatalf("Failed to check repo after delete: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("Expected 404 after delete, got %d", resp.StatusCode)
	}
}

func TestPythonHFListRepoFiles(t *testing.T) {
	requireSuite(t, suiteHFPython)

	checkPythonHFHub(t)

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Upload a few files
	uploadScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="py-user/list-files-model", exist_ok=True)
api.upload_file(path_or_fileobj=b"a\n", path_in_repo="a.txt", repo_id="py-user/list-files-model")
api.upload_file(path_or_fileobj=b"b\n", path_in_repo="b.txt", repo_id="py-user/list-files-model")
api.upload_file(path_or_fileobj=b"c\n", path_in_repo="sub/c.txt", repo_id="py-user/list-files-model")
`
	runPythonScript(t, endpoint, uploadScript)

	// List files and verify
	listScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
files = sorted(api.list_repo_files(repo_id="py-user/list-files-model"))
assert "a.txt" in files, f"a.txt not in {files}"
assert "b.txt" in files, f"b.txt not in {files}"
assert "sub/c.txt" in files, f"sub/c.txt not in {files}"
`
	runPythonScript(t, endpoint, listScript)
}

func TestPythonHFDatasetUploadAndDownload(t *testing.T) {
	requireSuite(t, suiteHFPython)

	checkPythonHFHub(t)

	server, _ := setupTestServer(t)
	endpoint := server.URL

	uploadScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="py-user/my-dataset", repo_type="dataset", exist_ok=True)
api.upload_file(path_or_fileobj=b"col1,col2\na,b\n", path_in_repo="data.csv", repo_id="py-user/my-dataset", repo_type="dataset")
api.upload_file(path_or_fileobj=b"# Dataset\n", path_in_repo="README.md", repo_id="py-user/my-dataset", repo_type="dataset")
`
	runPythonScript(t, endpoint, uploadScript)

	// Verify via HTTP using datasets resolve endpoint
	resp, err := server.Client().Get(endpoint + "/datasets/py-user/my-dataset/resolve/main/data.csv")
	if err != nil {
		t.Fatalf("Failed to get dataset file: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Expected 200 for dataset file, got %d", resp.StatusCode)
	}

	localDir, err := os.MkdirTemp("", "hf-python-dataset-dl")
	if err != nil {
		t.Fatalf("Failed to create local dir: %v", err)
	}
	defer os.RemoveAll(localDir)

	downloadScript := fmt.Sprintf(`
import os
import huggingface_hub
local_dir = huggingface_hub.snapshot_download(
    repo_id="py-user/my-dataset",
    repo_type="dataset",
    local_dir=%q,
    endpoint=os.environ["HF_ENDPOINT"],
    token=os.environ["HF_TOKEN"],
)
content = open(os.path.join(local_dir, "data.csv")).read()
assert content == "col1,col2\na,b\n", f"unexpected: {content!r}"
`, localDir)
	runPythonScript(t, endpoint, downloadScript)
}

func TestPythonHFSpaceUploadAndDownload(t *testing.T) {
	requireSuite(t, suiteHFPython)

	checkPythonHFHub(t)

	server, _ := setupTestServer(t)
	endpoint := server.URL

	uploadScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="py-user/my-space", repo_type="space", space_sdk="gradio", exist_ok=True)
api.upload_file(path_or_fileobj=b"import gradio as gr\n", path_in_repo="app.py", repo_id="py-user/my-space", repo_type="space")
`
	runPythonScript(t, endpoint, uploadScript)

	// Verify via HTTP using spaces resolve endpoint
	resp, err := server.Client().Get(endpoint + "/spaces/py-user/my-space/resolve/main/app.py")
	if err != nil {
		t.Fatalf("Failed to get space file: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Expected 200 for space file, got %d", resp.StatusCode)
	}
}

func TestPythonHFBranchOperations(t *testing.T) {
	requireSuite(t, suiteHFPython)

	checkPythonHFHub(t)

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create repo and upload initial file
	setupScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="py-user/branch-model", exist_ok=True)
api.upload_file(path_or_fileobj=b"main content\n", path_in_repo="main.txt", repo_id="py-user/branch-model")
`
	runPythonScript(t, endpoint, setupScript)

	// Create a branch
	branchScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_branch(repo_id="py-user/branch-model", branch="dev")
`
	runPythonScript(t, endpoint, branchScript)

	// Verify file is accessible on the new branch
	resp, err := server.Client().Get(endpoint + "/py-user/branch-model/resolve/dev/main.txt")
	if err != nil {
		t.Fatalf("Failed to get file on dev branch: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Expected 200 for file on dev branch, got %d", resp.StatusCode)
	}

	// Delete the branch
	deleteBranchScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.delete_branch(repo_id="py-user/branch-model", branch="dev")
`
	runPythonScript(t, endpoint, deleteBranchScript)

	// Verify branch is gone
	resp, err = server.Client().Get(endpoint + "/py-user/branch-model/resolve/dev/main.txt")
	if err != nil {
		t.Fatalf("Failed to check deleted branch: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode == 200 {
		t.Fatalf("Expected non-200 after branch delete, got %d", resp.StatusCode)
	}
}

func TestPythonHFTagOperations(t *testing.T) {
	requireSuite(t, suiteHFPython)

	checkPythonHFHub(t)

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create repo and upload initial file
	setupScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="py-user/tag-model", exist_ok=True)
api.upload_file(path_or_fileobj=b"v1 content\n", path_in_repo="readme.txt", repo_id="py-user/tag-model")
`
	runPythonScript(t, endpoint, setupScript)

	// Create a tag
	createTagScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_tag(repo_id="py-user/tag-model", tag="v1.0", tag_message="First release")
`
	runPythonScript(t, endpoint, createTagScript)

	// Verify file is accessible via the tag
	resp, err := server.Client().Get(endpoint + "/py-user/tag-model/resolve/v1.0/readme.txt")
	if err != nil {
		t.Fatalf("Failed to get file at tag v1.0: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Expected 200 for file at tag v1.0, got %d", resp.StatusCode)
	}

	// Delete the tag
	deleteTagScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.delete_tag(repo_id="py-user/tag-model", tag="v1.0")
`
	runPythonScript(t, endpoint, deleteTagScript)

	// Verify tag is gone
	resp, err = server.Client().Get(endpoint + "/py-user/tag-model/resolve/v1.0/readme.txt")
	if err != nil {
		t.Fatalf("Failed to check deleted tag: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode == 200 {
		t.Fatalf("Expected non-200 after tag delete, got %d", resp.StatusCode)
	}
}

func TestPythonHFUploadAndDownloadRoundTrip(t *testing.T) {
	requireSuite(t, suiteHFPython)

	checkPythonHFHub(t)

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Upload multiple files in separate commits
	uploadScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="py-user/roundtrip-model", exist_ok=True)
api.upload_file(path_or_fileobj=b"first\n", path_in_repo="first.txt", repo_id="py-user/roundtrip-model", commit_message="First commit")
api.upload_file(path_or_fileobj=b"second\n", path_in_repo="second.txt", repo_id="py-user/roundtrip-model", commit_message="Second commit")
`
	runPythonScript(t, endpoint, uploadScript)

	localDir, err := os.MkdirTemp("", "hf-python-roundtrip")
	if err != nil {
		t.Fatalf("Failed to create local dir: %v", err)
	}
	defer os.RemoveAll(localDir)

	// Download all files and verify
	downloadScript := fmt.Sprintf(`
import os
import huggingface_hub
local_dir = huggingface_hub.snapshot_download(
    repo_id="py-user/roundtrip-model",
    local_dir=%q,
    endpoint=os.environ["HF_ENDPOINT"],
    token=os.environ["HF_TOKEN"],
)
assert open(os.path.join(local_dir, "first.txt")).read() == "first\n"
assert open(os.path.join(local_dir, "second.txt")).read() == "second\n"
`, localDir)
	runPythonScript(t, endpoint, downloadScript)
}

func TestPythonHFRepoTypeIsolation(t *testing.T) {
	requireSuite(t, suiteHFPython)

	checkPythonHFHub(t)

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Upload different content to the same repo name but different types
	uploadScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="py-user/shared-repo", repo_type="model", exist_ok=True)
api.upload_file(path_or_fileobj=b"model content\n", path_in_repo="data.txt", repo_id="py-user/shared-repo", repo_type="model")
api.create_repo(repo_id="py-user/shared-repo", repo_type="dataset", exist_ok=True)
api.upload_file(path_or_fileobj=b"dataset content\n", path_in_repo="data.txt", repo_id="py-user/shared-repo", repo_type="dataset")
api.create_repo(repo_id="py-user/shared-repo", repo_type="space", space_sdk="gradio", exist_ok=True)
api.upload_file(path_or_fileobj=b"space content\n", path_in_repo="data.txt", repo_id="py-user/shared-repo", repo_type="space")
`
	runPythonScript(t, endpoint, uploadScript)

	// Verify each type has its own content
	for _, tc := range []struct {
		resolvePrefix   string
		expectedContent string
	}{
		{"", "model content\n"},
		{"/datasets", "dataset content\n"},
		{"/spaces", "space content\n"},
	} {
		resp, err := server.Client().Get(endpoint + tc.resolvePrefix + "/py-user/shared-repo/resolve/main/data.txt")
		if err != nil {
			t.Fatalf("Failed to get %q: %v", tc.resolvePrefix, err)
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			t.Fatalf("Expected 200 for %q, got %d", tc.resolvePrefix, resp.StatusCode)
		}
		buf := make([]byte, 64)
		n, _ := resp.Body.Read(buf)
		resp.Body.Close()
		got := string(buf[:n])
		if got != tc.expectedContent {
			t.Errorf("Content mismatch for %q: got %q, want %q", tc.resolvePrefix, got, tc.expectedContent)
		}
	}
}

func TestPythonHFMixedCLIAndLibrary(t *testing.T) {
	requireSuite(t, suiteHFPython)

	checkPythonHFHub(t)
	if _, err := exec.LookPath("hf"); err != nil {
		t.Skip("hf CLI not available, skipping mixed CLI+Python test")
	}

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Upload using Python library
	uploadScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="mix-user/mixed-model", exist_ok=True)
api.upload_file(path_or_fileobj=b"from python\n", path_in_repo="python.txt", repo_id="mix-user/mixed-model")
`
	runPythonScript(t, endpoint, uploadScript)

	// Upload using HF CLI
	uploadDir, err := os.MkdirTemp("", "hf-mixed-upload")
	if err != nil {
		t.Fatalf("Failed to create upload dir: %v", err)
	}
	defer os.RemoveAll(uploadDir)
	if err := os.WriteFile(filepath.Join(uploadDir, "cli.txt"), []byte("from cli\n"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	runHFCmd(t, endpoint, "upload", "mix-user/mixed-model", uploadDir, ".", "--commit-message", "Upload via CLI")

	// Download using Python library and verify both files exist
	localDir, err := os.MkdirTemp("", "hf-mixed-download")
	if err != nil {
		t.Fatalf("Failed to create local dir: %v", err)
	}
	defer os.RemoveAll(localDir)

	downloadScript := fmt.Sprintf(`
import os
import huggingface_hub
local_dir = huggingface_hub.snapshot_download(
    repo_id="mix-user/mixed-model",
    local_dir=%q,
    endpoint=os.environ["HF_ENDPOINT"],
    token=os.environ["HF_TOKEN"],
)
assert open(os.path.join(local_dir, "python.txt")).read() == "from python\n"
assert open(os.path.join(local_dir, "cli.txt")).read() == "from cli\n"
`, localDir)
	runPythonScript(t, endpoint, downloadScript)

	// Download using HF CLI and verify both files exist
	cliDownloadDir, err := os.MkdirTemp("", "hf-mixed-cli-download")
	if err != nil {
		t.Fatalf("Failed to create cli download dir: %v", err)
	}
	defer os.RemoveAll(cliDownloadDir)

	runHFCmd(t, endpoint, "download", "mix-user/mixed-model", "--local-dir", cliDownloadDir)

	for _, file := range []struct {
		name    string
		content string
	}{
		{"python.txt", "from python\n"},
		{"cli.txt", "from cli\n"},
	} {
		content, err := os.ReadFile(filepath.Join(cliDownloadDir, file.name))
		if err != nil {
			t.Fatalf("Failed to read %s: %v", file.name, err)
		}
		if string(content) != file.content {
			t.Errorf("Content mismatch for %s: got %q, want %q", file.name, content, file.content)
		}
	}
}

func TestPythonHFRepoInfo(t *testing.T) {
	requireSuite(t, suiteHFPython)

	checkPythonHFHub(t)

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Upload files with a README to populate metadata
	uploadScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="py-user/info-model", exist_ok=True)
readme = b"""---
tags:
- text-classification
- pytorch
---
# Info Model
"""
api.upload_file(path_or_fileobj=readme, path_in_repo="README.md", repo_id="py-user/info-model")
api.upload_file(path_or_fileobj=b"weights\n", path_in_repo="model.bin", repo_id="py-user/info-model")
`
	runPythonScript(t, endpoint, uploadScript)

	// Query repo info using the Python library
	infoScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
info = api.model_info(repo_id="py-user/info-model")
assert info.id == "py-user/info-model", f"unexpected id: {info.id}"
siblings = [s.rfilename for s in info.siblings]
assert "README.md" in siblings, f"README.md not in {siblings}"
assert "model.bin" in siblings, f"model.bin not in {siblings}"
`
	runPythonScript(t, endpoint, infoScript)
}

func TestPythonHFUploadLargeFileBytes(t *testing.T) {
	requireSuite(t, suiteHFPython)

	checkPythonHFHub(t)

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Upload a file larger than the LFS threshold (10MB) using bytes in-memory
	// We'll just use a moderately large file to test the LFS code path
	uploadScript := `
import os
import huggingface_hub

# 11 MB file to trigger LFS upload path
large_content = b"x" * (11 * 1024 * 1024)
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="py-user/large-file-model", exist_ok=True)
api.upload_file(path_or_fileobj=large_content, path_in_repo="large.bin", repo_id="py-user/large-file-model")
`
	runPythonScript(t, endpoint, uploadScript)

	// Verify the file is accessible
	resp, err := server.Client().Get(endpoint + "/py-user/large-file-model/resolve/main/large.bin")
	if err != nil {
		t.Fatalf("Failed to get large file: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Expected 200 for large file, got %d", resp.StatusCode)
	}
}

func TestPythonHFDeleteFile(t *testing.T) {
	requireSuite(t, suiteHFPython)

	checkPythonHFHub(t)

	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create a repo and upload two files
	setupScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="py-user/delete-file-model", exist_ok=True)
api.upload_file(path_or_fileobj=b"keep me\n", path_in_repo="keep.txt", repo_id="py-user/delete-file-model")
api.upload_file(path_or_fileobj=b"delete me\n", path_in_repo="delete.txt", repo_id="py-user/delete-file-model")
`
	runPythonScript(t, endpoint, setupScript)

	// Delete one file
	deleteScript := `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.delete_file(path_in_repo="delete.txt", repo_id="py-user/delete-file-model")
`
	runPythonScript(t, endpoint, deleteScript)

	// Verify the deleted file returns 404
	resp, err := server.Client().Get(endpoint + "/py-user/delete-file-model/resolve/main/delete.txt")
	if err != nil {
		t.Fatalf("Failed to check deleted file: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("Expected 404 for deleted file, got %d", resp.StatusCode)
	}

	// Verify the kept file is still accessible
	resp, err = server.Client().Get(endpoint + "/py-user/delete-file-model/resolve/main/keep.txt")
	if err != nil {
		t.Fatalf("Failed to check kept file: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Expected 200 for kept file, got %d", resp.StatusCode)
	}
}
