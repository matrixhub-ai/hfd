package e2e_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestPythonHFLibraryOperationsMatrix tests Python huggingface_hub library operations across different scenarios
func TestPythonHFLibraryOperationsMatrix(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available, skipping Python HF library matrix test")
	}
	cmd := exec.CommandContext(t.Context(), "python3", "-c", "import huggingface_hub")
	if err := cmd.Run(); err != nil {
		t.Skip("huggingface_hub not installed, skipping Python HF library matrix test")
	}

	type repoType struct {
		name          string
		repoTypeArg   string
		resolvePrefix string
	}

	repoTypes := []repoType{
		{name: "Model", repoTypeArg: "model", resolvePrefix: ""},
		{name: "Dataset", repoTypeArg: "dataset", resolvePrefix: "/datasets"},
	}

	type operation struct {
		name string
		test func(t *testing.T, endpoint, repoTypeArg, resolvePrefix string)
	}

	operations := []operation{
		{name: "UploadAndDownloadFile", test: testPyUploadDownloadFile},
		{name: "UploadFolder", test: testPyUploadFolder},
		{name: "SnapshotDownload", test: testPySnapshotDownload},
		{name: "CreateAndDeleteRepo", test: testPyCreateDeleteRepo},
		{name: "ListRepoFiles", test: testPyListRepoFiles},
		{name: "BranchOperations", test: testPyBranchOperations},
		{name: "TagOperations", test: testPyTagOperations},
		{name: "DeleteFile", test: testPyDeleteFile},
		{name: "RepoInfo", test: testPyRepoInfo},
	}

	for _, rt := range repoTypes {
		t.Run(rt.name, func(t *testing.T) {
			for _, op := range operations {
				t.Run(op.name, func(t *testing.T) {
					server, _ := setupTestServer(t)
					op.test(t, server.URL, rt.repoTypeArg, rt.resolvePrefix)
				})
			}
		})
	}
}

func testPyUploadDownloadFile(t *testing.T, endpoint, repoTypeArg, resolvePrefix string) {
	repoID := fmt.Sprintf("py-user/upload-dl-%s", repoTypeArg)

	script := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id=%q, repo_type=%q, exist_ok=True)
api.upload_file(path_or_fileobj=b"test content\n", path_in_repo="test.txt", repo_id=%q, repo_type=%q)
`, repoID, repoTypeArg, repoID, repoTypeArg)
	runPyScript(t, endpoint, script)

	// Download and verify
	cacheDir, err := os.MkdirTemp("", "py-cache")
	if err != nil {
		t.Fatalf("Failed to create cache dir: %v", err)
	}
	defer os.RemoveAll(cacheDir)

	downloadScript := fmt.Sprintf(`
import os
import huggingface_hub
path = huggingface_hub.hf_hub_download(
    repo_id=%q,
    filename="test.txt",
    repo_type=%q,
    cache_dir=%q,
    endpoint=os.environ["HF_ENDPOINT"],
    token=os.environ["HF_TOKEN"],
)
content = open(path).read()
assert content == "test content\n", f"unexpected content: {content!r}"
`, repoID, repoTypeArg, cacheDir)
	runPyScript(t, endpoint, downloadScript)
}

func testPyUploadFolder(t *testing.T, endpoint, repoTypeArg, resolvePrefix string) {
	repoID := fmt.Sprintf("py-user/folder-%s", repoTypeArg)

	folderDir, err := os.MkdirTemp("", "py-folder")
	if err != nil {
		t.Fatalf("Failed to create temp folder: %v", err)
	}
	defer os.RemoveAll(folderDir)

	files := map[string]string{
		"README.md":   "# Test\n",
		"config.json": `{"key": "value"}` + "\n",
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
api.create_repo(repo_id=%q, repo_type=%q, exist_ok=True)
api.upload_folder(folder_path=%q, repo_id=%q, repo_type=%q)
`, repoID, repoTypeArg, folderDir, repoID, repoTypeArg)
	runPyScript(t, endpoint, script)
}

func testPySnapshotDownload(t *testing.T, endpoint, repoTypeArg, resolvePrefix string) {
	repoID := fmt.Sprintf("py-user/snapshot-%s", repoTypeArg)

	// Upload files first
	uploadScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id=%q, repo_type=%q, exist_ok=True)
api.upload_file(path_or_fileobj=b"file one\n", path_in_repo="file1.txt", repo_id=%q, repo_type=%q)
api.upload_file(path_or_fileobj=b"file two\n", path_in_repo="file2.txt", repo_id=%q, repo_type=%q)
`, repoID, repoTypeArg, repoID, repoTypeArg, repoID, repoTypeArg)
	runPyScript(t, endpoint, uploadScript)

	localDir, err := os.MkdirTemp("", "py-snapshot")
	if err != nil {
		t.Fatalf("Failed to create local dir: %v", err)
	}
	defer os.RemoveAll(localDir)

	downloadScript := fmt.Sprintf(`
import os
import huggingface_hub
local_dir = huggingface_hub.snapshot_download(
    repo_id=%q,
    repo_type=%q,
    local_dir=%q,
    endpoint=os.environ["HF_ENDPOINT"],
    token=os.environ["HF_TOKEN"],
)
assert open(os.path.join(local_dir, "file1.txt")).read() == "file one\n"
assert open(os.path.join(local_dir, "file2.txt")).read() == "file two\n"
`, repoID, repoTypeArg, localDir)
	runPyScript(t, endpoint, downloadScript)
}

func testPyCreateDeleteRepo(t *testing.T, endpoint, repoTypeArg, resolvePrefix string) {
	repoID := fmt.Sprintf("py-user/create-del-%s", repoTypeArg)

	createScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
url = api.create_repo(repo_id=%q, repo_type=%q, exist_ok=False)
assert url is not None
`, repoID, repoTypeArg)
	runPyScript(t, endpoint, createScript)

	deleteScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.delete_repo(repo_id=%q, repo_type=%q)
`, repoID, repoTypeArg)
	runPyScript(t, endpoint, deleteScript)
}

func testPyListRepoFiles(t *testing.T, endpoint, repoTypeArg, resolvePrefix string) {
	repoID := fmt.Sprintf("py-user/list-%s", repoTypeArg)

	uploadScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id=%q, repo_type=%q, exist_ok=True)
api.upload_file(path_or_fileobj=b"a\n", path_in_repo="a.txt", repo_id=%q, repo_type=%q)
api.upload_file(path_or_fileobj=b"b\n", path_in_repo="b.txt", repo_id=%q, repo_type=%q)
`, repoID, repoTypeArg, repoID, repoTypeArg, repoID, repoTypeArg)
	runPyScript(t, endpoint, uploadScript)

	listScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
files = sorted(api.list_repo_files(repo_id=%q, repo_type=%q))
assert "a.txt" in files, f"a.txt not in {files}"
assert "b.txt" in files, f"b.txt not in {files}"
`, repoID, repoTypeArg)
	runPyScript(t, endpoint, listScript)
}

func testPyBranchOperations(t *testing.T, endpoint, repoTypeArg, resolvePrefix string) {
	repoID := fmt.Sprintf("py-user/branch-%s", repoTypeArg)

	setupScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id=%q, repo_type=%q, exist_ok=True)
api.upload_file(path_or_fileobj=b"main content\n", path_in_repo="main.txt", repo_id=%q, repo_type=%q)
`, repoID, repoTypeArg, repoID, repoTypeArg)
	runPyScript(t, endpoint, setupScript)

	branchScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_branch(repo_id=%q, branch="dev", repo_type=%q)
`, repoID, repoTypeArg)
	runPyScript(t, endpoint, branchScript)

	deleteBranchScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.delete_branch(repo_id=%q, branch="dev", repo_type=%q)
`, repoID, repoTypeArg)
	runPyScript(t, endpoint, deleteBranchScript)
}

func testPyTagOperations(t *testing.T, endpoint, repoTypeArg, resolvePrefix string) {
	repoID := fmt.Sprintf("py-user/tag-%s", repoTypeArg)

	setupScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id=%q, repo_type=%q, exist_ok=True)
api.upload_file(path_or_fileobj=b"v1 content\n", path_in_repo="readme.txt", repo_id=%q, repo_type=%q)
`, repoID, repoTypeArg, repoID, repoTypeArg)
	runPyScript(t, endpoint, setupScript)

	createTagScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_tag(repo_id=%q, tag="v1.0", tag_message="First release", repo_type=%q)
`, repoID, repoTypeArg)
	runPyScript(t, endpoint, createTagScript)

	deleteTagScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.delete_tag(repo_id=%q, tag="v1.0", repo_type=%q)
`, repoID, repoTypeArg)
	runPyScript(t, endpoint, deleteTagScript)
}

func testPyDeleteFile(t *testing.T, endpoint, repoTypeArg, resolvePrefix string) {
	repoID := fmt.Sprintf("py-user/delfile-%s", repoTypeArg)

	setupScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id=%q, repo_type=%q, exist_ok=True)
api.upload_file(path_or_fileobj=b"keep me\n", path_in_repo="keep.txt", repo_id=%q, repo_type=%q)
api.upload_file(path_or_fileobj=b"delete me\n", path_in_repo="delete.txt", repo_id=%q, repo_type=%q)
`, repoID, repoTypeArg, repoID, repoTypeArg, repoID, repoTypeArg)
	runPyScript(t, endpoint, setupScript)

	deleteScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.delete_file(path_in_repo="delete.txt", repo_id=%q, repo_type=%q)
`, repoID, repoTypeArg)
	runPyScript(t, endpoint, deleteScript)
}

func testPyRepoInfo(t *testing.T, endpoint, repoTypeArg, resolvePrefix string) {
	repoID := fmt.Sprintf("py-user/info-%s", repoTypeArg)

	uploadScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id=%q, repo_type=%q, exist_ok=True)
readme = b"""# Info Test"""
api.upload_file(path_or_fileobj=readme, path_in_repo="README.md", repo_id=%q, repo_type=%q)
api.upload_file(path_or_fileobj=b"data\n", path_in_repo="data.txt", repo_id=%q, repo_type=%q)
`, repoID, repoTypeArg, repoID, repoTypeArg, repoID, repoTypeArg)
	runPyScript(t, endpoint, uploadScript)

	infoFuncName := "model_info"
	if repoTypeArg == "dataset" {
		infoFuncName = "dataset_info"
	} else if repoTypeArg == "space" {
		infoFuncName = "space_info"
	}

	infoScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
info = api.%s(repo_id=%q)
assert info.id == %q, f"unexpected id: {info.id}"
siblings = [s.rfilename for s in info.siblings]
assert "README.md" in siblings, f"README.md not in {siblings}"
assert "data.txt" in siblings, f"data.txt not in {siblings}"
`, infoFuncName, repoID, repoID)
	runPyScript(t, endpoint, infoScript)
}

// runPyScript runs a Python3 script with HF_ENDPOINT and HF_TOKEN set
func runPyScript(t *testing.T, endpoint, script string) {
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
}
