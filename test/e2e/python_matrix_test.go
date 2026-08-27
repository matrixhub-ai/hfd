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
		{name: "ListRepoCommits", test: testPyListRepoCommits},
		{name: "ListRepoRefs", test: testPyListRepoRefs},
		{name: "SuperSquashHistory", test: testPySuperSquashHistory},
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

func testPyListRepoCommits(t *testing.T, endpoint, repoTypeArg, resolvePrefix string) {
	repoID := fmt.Sprintf("py-user/commits-%s", repoTypeArg)

	// Upload and list run in one script so the CommitInfo.oid values returned
	// by upload_file anchor the listed commit_ids to the real commits.
	script := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id=%q, repo_type=%q, exist_ok=True)
first = api.upload_file(path_or_fileobj=b"first\n", path_in_repo="first.txt", repo_id=%q, repo_type=%q, commit_message="Add first file")
second = api.upload_file(path_or_fileobj=b"second\n", path_in_repo="second.txt", repo_id=%q, repo_type=%q, commit_message="Add second file")
commits = api.list_repo_commits(repo_id=%q, repo_type=%q)
assert len(commits) >= 2, f"expected at least 2 commits, got {len(commits)}"
for c in commits:
    assert len(c.commit_id) >= 40, f"unexpected commit_id: {c.commit_id!r}"
    assert c.created_at is not None, f"missing created_at for {c.commit_id}"
commit_ids = {c.commit_id for c in commits}
assert first.oid in commit_ids, f"first upload oid {first.oid} not in {commit_ids}"
assert second.oid in commit_ids, f"second upload oid {second.oid} not in {commit_ids}"
titles = [c.title for c in commits]
assert "Add first file" in titles, f"'Add first file' not in {titles}"
assert "Add second file" in titles, f"'Add second file' not in {titles}"
`, repoID, repoTypeArg, repoID, repoTypeArg, repoID, repoTypeArg, repoID, repoTypeArg)
	runPyScript(t, endpoint, script)
}

func testPyListRepoRefs(t *testing.T, endpoint, repoTypeArg, resolvePrefix string) {
	repoID := fmt.Sprintf("py-user/refs-%s", repoTypeArg)

	// Setup and list run in one script so the upload's CommitInfo.oid (the
	// main HEAD) anchors every target_commit. dev and v1.0 are created from
	// that exact revision; the server creates lightweight tags, so the tag
	// ref points at the commit itself.
	script := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id=%q, repo_type=%q, exist_ok=True)
head = api.upload_file(path_or_fileobj=b"refs content\n", path_in_repo="refs.txt", repo_id=%q, repo_type=%q).oid
api.create_branch(repo_id=%q, branch="dev", revision=head, repo_type=%q)
api.create_tag(repo_id=%q, tag="v1.0", tag_message="First release", revision=head, repo_type=%q)
refs = api.list_repo_refs(repo_id=%q, repo_type=%q)
branches = {b.name: b for b in refs.branches}
assert "main" in branches, f"main not in {sorted(branches)}"
assert "dev" in branches, f"dev not in {sorted(branches)}"
assert branches["dev"].ref == "refs/heads/dev", f"unexpected branch ref: {branches['dev'].ref}"
assert branches["main"].target_commit == head, f"main target {branches['main'].target_commit} != upload oid {head}"
assert branches["dev"].target_commit == head, f"dev target {branches['dev'].target_commit} != upload oid {head}"
tags = {t.name: t for t in refs.tags}
assert "v1.0" in tags, f"v1.0 not in {sorted(tags)}"
assert tags["v1.0"].ref == "refs/tags/v1.0", f"unexpected tag ref: {tags['v1.0'].ref}"
assert tags["v1.0"].target_commit == head, f"v1.0 target {tags['v1.0'].target_commit} != upload oid {head}"
`, repoID, repoTypeArg, repoID, repoTypeArg, repoID, repoTypeArg, repoID, repoTypeArg, repoID, repoTypeArg)
	runPyScript(t, endpoint, script)

	deleteScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.delete_branch(repo_id=%q, branch="dev", repo_type=%q)
api.delete_tag(repo_id=%q, tag="v1.0", repo_type=%q)
refs = api.list_repo_refs(repo_id=%q, repo_type=%q)
branch_names = [b.name for b in refs.branches]
assert "main" in branch_names, f"main not in {branch_names}"
assert "dev" not in branch_names, f"dev still in {branch_names}"
tag_names = [t.name for t in refs.tags]
assert "v1.0" not in tag_names, f"v1.0 still in {tag_names}"
`, repoID, repoTypeArg, repoID, repoTypeArg, repoID, repoTypeArg)
	runPyScript(t, endpoint, deleteScript)
}

func testPySuperSquashHistory(t *testing.T, endpoint, repoTypeArg, resolvePrefix string) {
	repoID := fmt.Sprintf("py-user/squash-%s", repoTypeArg)

	uploadScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id=%q, repo_type=%q, exist_ok=True)
api.upload_file(path_or_fileobj=b"content one\n", path_in_repo="one.txt", repo_id=%q, repo_type=%q, commit_message="Add one")
api.upload_file(path_or_fileobj=b"content two\n", path_in_repo="two.txt", repo_id=%q, repo_type=%q, commit_message="Add two")
api.upload_file(path_or_fileobj=b"content three\n", path_in_repo="three.txt", repo_id=%q, repo_type=%q, commit_message="Add three")
`, repoID, repoTypeArg, repoID, repoTypeArg, repoID, repoTypeArg, repoID, repoTypeArg)
	runPyScript(t, endpoint, uploadScript)

	squashScript := fmt.Sprintf(`
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
before = api.list_repo_commits(repo_id=%q, repo_type=%q)
assert len(before) >= 3, f"expected at least 3 commits before squash, got {len(before)}"
files_before = sorted(api.list_repo_files(repo_id=%q, repo_type=%q))
api.super_squash_history(repo_id=%q, branch="main", commit_message="squashed", repo_type=%q)
after = api.list_repo_commits(repo_id=%q, repo_type=%q)
assert len(after) == 1, f"expected exactly 1 commit after squash, got {len(after)}: {[c.title for c in after]}"
assert after[0].title == "squashed", f"unexpected title after squash: {after[0].title!r}"
assert after[0].commit_id, "empty commit_id after squash"
files_after = sorted(api.list_repo_files(repo_id=%q, repo_type=%q))
assert files_before == files_after, f"file set changed by squash: {files_before} -> {files_after}"
for name in ("one.txt", "two.txt", "three.txt"):
    assert name in files_after, f"{name} missing after squash: {files_after}"
`, repoID, repoTypeArg, repoID, repoTypeArg, repoID, repoTypeArg, repoID, repoTypeArg, repoID, repoTypeArg)
	runPyScript(t, endpoint, squashScript)

	cacheDir, err := os.MkdirTemp("", "py-squash-cache")
	if err != nil {
		t.Fatalf("Failed to create cache dir: %v", err)
	}
	defer os.RemoveAll(cacheDir)

	// Download every file through a fresh cache dir so all three contents
	// are proven intact from the rewritten history, not just one.
	downloadScript := fmt.Sprintf(`
import os
import huggingface_hub
expected = {"one.txt": "content one\n", "two.txt": "content two\n", "three.txt": "content three\n"}
for name, want in expected.items():
    path = huggingface_hub.hf_hub_download(
        repo_id=%q,
        filename=name,
        repo_type=%q,
        cache_dir=%q,
        endpoint=os.environ["HF_ENDPOINT"],
        token=os.environ["HF_TOKEN"],
    )
    content = open(path).read()
    assert content == want, f"unexpected {name} content after squash: {content!r}"
`, repoID, repoTypeArg, cacheDir)
	runPyScript(t, endpoint, downloadScript)
}

// runPyScript runs a Python3 script with HF_ENDPOINT and HF_TOKEN set
func runPyScript(t *testing.T, endpoint, script string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "python3", "-c", script)
	cmd.Env = append(testEnv(),
		"HF_ENDPOINT="+endpoint,
		"HF_HUB_DISABLE_TELEMETRY=1",
		"HF_TOKEN=dummy-token",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Python script failed:\n%s\nOutput:\n%s", script, out)
	}
}
