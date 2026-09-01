package e2e_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// hubClient is the client axis of the hub API matrix: the hf CLI or the
// huggingface_hub python library, both driven without xet.
type hubClient struct {
	name string
	py   bool
}

// require gates the cell on its client tool being usable.
func (c hubClient) require(t *testing.T) {
	t.Helper()
	if c.py {
		checkPythonHFHub(t)
		return
	}
	requireHFCli(t)
}

// requireHFCli skips locally when the hf CLI is missing; on CI it fails so
// the matrix stays enforced there, like requireUpDownMatrixTools.
func requireHFCli(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("hf"); err != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("hf CLI not found; pip install -U 'huggingface_hub[cli]'")
		}
		t.Skip("hf CLI not available, skipping")
	}
}

// hubRepoType is the repo type axis.
type hubRepoType struct {
	name          string
	arg           string // repo_type value clients pass
	apiPrefix     string // /api/<prefix>/... route segment
	resolvePrefix string // path prefix on resolve URLs
}

var hubRepoTypes = []hubRepoType{
	{name: "Model", arg: "model", apiPrefix: "models", resolvePrefix: ""},
	{name: "Dataset", arg: "dataset", apiPrefix: "datasets", resolvePrefix: "/datasets"},
	{name: "Space", arg: "space", apiPrefix: "spaces", resolvePrefix: "/spaces"},
}

// cliTypeArgs returns the --repo-type flag for non-default repo types.
func (rt hubRepoType) cliTypeArgs() []string {
	if rt.arg == "model" {
		return nil
	}
	return []string{"--repo-type", rt.arg}
}

// cliCreateArgs is cliTypeArgs plus the SDK spaces require at creation.
func (rt hubRepoType) cliCreateArgs() []string {
	args := rt.cliTypeArgs()
	if rt.arg == "space" {
		args = append(args, "--space-sdk", "gradio")
	}
	return args
}

// pyInfoFunc is the HfApi info accessor matching the repo type.
func (rt hubRepoType) pyInfoFunc() string {
	switch rt.arg {
	case "dataset":
		return "dataset_info"
	case "space":
		return "space_info"
	default:
		return "model_info"
	}
}

type hubFile struct {
	path    string
	content string
}

type hubOp struct {
	name      string
	supported func(c hubClient, rt hubRepoType) bool
	run       func(t *testing.T, s *e2eServer, c hubClient, rt hubRepoType)
}

func anyClientAnyType(hubClient, hubRepoType) bool { return true }

// pyOnly pins ops whose hf CLI rows this matrix does not implement; the
// python rows already cover the API semantics. hf CLI 1.28 does expose
// ls/info commands — CLI rows for those are an optional follow-up — while
// commits/refs/squash have no CLI equivalent.
func pyOnly(c hubClient, _ hubRepoType) bool { return c.py }

// modelCellOnly anchors ops that span all repo types internally to the Model
// cell, so they run once per client.
func modelCellOnly(_ hubClient, rt hubRepoType) bool { return rt.arg == "model" }

// cliModelOnly keeps the python row on every repo type but pins the hf CLI
// row of type-independent management ops (create/move/settings/delete-file)
// to the Model cell. The server handles these identically for every repo
// type — the type only selects the storage namespace, whose disjointness
// TypeIsolation proves — so CLI x {dataset,space} would re-test the same
// handler at ~2s interpreter start-up per call; the legacy suite covered
// these CLI ops on model only. The CLI x type surface itself stays covered
// by UploadAndDownload/Branch/Tag on all three types.
func cliModelOnly(c hubClient, rt hubRepoType) bool { return c.py || rt.arg == "model" }

// TestHubAPIOperationsMatrix exercises hub management operations across
// {hf CLI, python library} x {model, dataset, space}. Each client x type
// group shares one server (ops use disjoint op-named repoIDs) and the six
// groups run in parallel; with each python op merged into a single
// interpreter run, this keeps the double-storage-pass wall time well inside
// the go test timeout.
func TestHubAPIOperationsMatrix(t *testing.T) {
	clients := []hubClient{
		{name: "HFCli"},
		{name: "PyLib", py: true},
	}
	ops := []hubOp{
		{name: "CreateAndDelete", supported: cliModelOnly, run: runHubCreateAndDelete},
		{name: "UploadAndDownload", supported: anyClientAnyType, run: runHubUploadAndDownload},
		{name: "SnapshotDownload", supported: pyOnly, run: runHubSnapshotDownload},
		{name: "ListFiles", supported: pyOnly, run: runHubListFiles},
		{name: "Branch", supported: anyClientAnyType, run: runHubBranch},
		{name: "Tag", supported: anyClientAnyType, run: runHubTag},
		{name: "Move", supported: cliModelOnly, run: runHubMove},
		{name: "Settings", supported: cliModelOnly, run: runHubSettings},
		{name: "DeleteFile", supported: cliModelOnly, run: runHubDeleteFile},
		{name: "RepoInfo", supported: pyOnly, run: runHubRepoInfo},
		{name: "Commits", supported: pyOnly, run: runHubCommits},
		{name: "Refs", supported: pyOnly, run: runHubRefs},
		{name: "Squash", supported: pyOnly, run: runHubSquash},
		{name: "TypeIsolation", supported: modelCellOnly, run: runHubTypeIsolation},
	}

	for _, c := range clients {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			for _, rt := range hubRepoTypes {
				t.Run(rt.name, func(t *testing.T) {
					t.Parallel()
					c.require(t)
					// One server per client x type group; every op below works
					// on its own op-named repoID, so the cells stay isolated.
					s := newE2EServer(t)
					for _, op := range ops {
						t.Run(op.name, func(t *testing.T) {
							if !op.supported(c, rt) {
								t.Skipf("%s not supported for %s/%s", op.name, c.name, rt.name)
							}
							// Ops touch disjoint repos, so they parallelize on the
							// shared server; -parallel caps the subprocess burst.
							t.Parallel()
							op.run(t, s, c, rt)
						})
					}
				})
			}
		})
	}
}

// --- shared plumbing ---

// hubPyAPI is the prologue of every python script driving HfApi. Each op
// runs exactly one python script: every extra python3 invocation costs ~2s
// of interpreter start-up and huggingface_hub import, so setup, actions,
// and asserts are merged into a single interpreter run.
const hubPyAPI = `import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
`

// hubPyHTTPHelpers adds plain-HTTP helpers for asserts that must observe
// state between two API calls of the same merged script (mirroring the Go
// httpGetStatus/assertResolveContent asserts).
const hubPyHTTPHelpers = `import urllib.error, urllib.request
def http_status(url):
    try:
        with urllib.request.urlopen(url) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
def assert_content(url, want):
    with urllib.request.urlopen(url) as r:
        assert r.status == 200, f"GET {url} = {r.status}, want 200"
        body = r.read().decode()
    assert body == want, f"GET {url} content {body!r}, want {want!r}"
`

// hubPyCreateLine returns the api.create_repo call; spaces need an SDK at
// creation.
func hubPyCreateLine(repoID string, rt hubRepoType, existOK bool) string {
	ok := "False"
	if existOK {
		ok = "True"
	}
	sdk := ""
	if rt.arg == "space" {
		sdk = `, space_sdk="gradio"`
	}
	return fmt.Sprintf("api.create_repo(repo_id=%q, repo_type=%q, exist_ok=%s%s)\n", repoID, rt.arg, ok, sdk)
}

// hubPySetupLines returns create_repo plus upload_file lines for embedding
// at the top of an op's merged script (after the hubPyAPI prologue).
func hubPySetupLines(repoID string, rt hubRepoType, files []hubFile) string {
	var b strings.Builder
	b.WriteString(hubPyCreateLine(repoID, rt, true))
	for _, f := range files {
		fmt.Fprintf(&b, "api.upload_file(path_or_fileobj=b%q, path_in_repo=%q, repo_id=%q, repo_type=%q)\n",
			f.content, f.path, repoID, rt.arg)
	}
	return b.String()
}

func hubCliCreate(t *testing.T, s *e2eServer, rt hubRepoType, repoID string, existOK bool) {
	t.Helper()
	args := []string{"repos", "create", repoID}
	if existOK {
		args = append(args, "--exist-ok")
	}
	runHFCmd(t, s.httpURL, append(args, rt.cliCreateArgs()...)...)
}

func writeHubFiles(t *testing.T, dir string, files []hubFile) {
	t.Helper()
	for _, f := range files {
		fp := filepath.Join(dir, filepath.FromSlash(f.path))
		if err := os.MkdirAll(filepath.Dir(fp), 0755); err != nil {
			t.Fatalf("create dir for %s: %v", f.path, err)
		}
		if err := os.WriteFile(fp, []byte(f.content), 0644); err != nil {
			t.Fatalf("write %s: %v", f.path, err)
		}
	}
}

// hubCliUpload uploads files with `hf upload`, which itself creates a
// missing repo (create_repo with exist_ok=True, gradio SDK for spaces), so
// CLI setup call sites skip a redundant ~2s `repos create` invocation; the
// explicit-create path is covered by CreateAndDelete.
func hubCliUpload(t *testing.T, s *e2eServer, rt hubRepoType, repoID string, files []hubFile, msg string) {
	t.Helper()
	dir := t.TempDir()
	writeHubFiles(t, dir, files)
	args := append([]string{"upload", repoID, dir, ".", "--commit-message", msg}, rt.cliTypeArgs()...)
	runHFCmd(t, s.httpURL, args...)
}

func hubResolveURL(s *e2eServer, rt hubRepoType, repoID, rev, path string) string {
	return s.httpURL + rt.resolvePrefix + "/" + repoID + "/resolve/" + rev + "/" + path
}

func hubAPIRepoURL(s *e2eServer, rt hubRepoType, repoID string) string {
	return s.httpURL + "/api/" + rt.apiPrefix + "/" + repoID
}

func httpGetStatus(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func httpGetContent(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return resp.StatusCode, string(body)
}

func hubResolveStatus(t *testing.T, s *e2eServer, rt hubRepoType, repoID, rev, path string) int {
	t.Helper()
	return httpGetStatus(t, hubResolveURL(s, rt, repoID, rev, path))
}

// assertResolveContent fails unless rev/path resolves to 200 with content.
func assertResolveContent(t *testing.T, s *e2eServer, rt hubRepoType, repoID, rev, path, content string) {
	t.Helper()
	url := hubResolveURL(s, rt, repoID, rev, path)
	status, body := httpGetContent(t, url)
	if status != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200", url, status)
	}
	if body != content {
		t.Fatalf("GET %s content = %q, want %q", url, body, content)
	}
}

func hubCliDownloadAndAssert(t *testing.T, s *e2eServer, rt hubRepoType, repoID string, files []hubFile) {
	t.Helper()
	dir := t.TempDir()
	args := append([]string{"download", repoID, "--local-dir", dir}, rt.cliTypeArgs()...)
	runHFCmd(t, s.httpURL, args...)
	for _, f := range files {
		content, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(f.path)))
		if err != nil {
			t.Fatalf("read downloaded %s: %v", f.path, err)
		}
		if string(content) != f.content {
			t.Errorf("downloaded %s = %q, want %q", f.path, content, f.content)
		}
	}
}

// hubPyDownloadLines returns hf_hub_download lines fetching every file
// through cacheDir (so nothing is served from a warm cache) and asserting
// its exact bytes, for embedding in an op's merged script.
func hubPyDownloadLines(cacheDir string, rt hubRepoType, repoID string, files []hubFile) string {
	var b strings.Builder
	for _, f := range files {
		fmt.Fprintf(&b, `path = huggingface_hub.hf_hub_download(
    repo_id=%q,
    filename=%q,
    repo_type=%q,
    cache_dir=%q,
    endpoint=os.environ["HF_ENDPOINT"],
    token=os.environ["HF_TOKEN"],
)
content = open(path).read()
assert content == %q, f"unexpected %s content: {content!r}"
`, repoID, f.path, rt.arg, cacheDir, f.content, f.path)
	}
	return b.String()
}

// --- operations ---

// runHubCreateAndDelete: create (exist_ok=false path) -> API 200 ->
// delete -> API 404. The python row runs all steps in one script; the 200
// check rides inside it because it must observe the state between create
// and delete.
func runHubCreateAndDelete(t *testing.T, s *e2eServer, c hubClient, rt hubRepoType) {
	repoID := "hub-user/create-del-" + rt.arg

	if c.py {
		script := hubPyAPI + hubPyHTTPHelpers +
			"url = " + hubPyCreateLine(repoID, rt, false) +
			"assert url is not None\n" +
			fmt.Sprintf("status = http_status(%q)\n", hubAPIRepoURL(s, rt, repoID)) +
			"assert status == 200, f\"expected 200 for created repo, got {status}\"\n" +
			fmt.Sprintf("api.delete_repo(repo_id=%q, repo_type=%q)\n", repoID, rt.arg)
		runPyScript(t, s.httpURL, script)
	} else {
		// no --exist-ok: the repo name is used by no other op, so this is
		// the exist_ok=false path even on the group's shared server
		hubCliCreate(t, s, rt, repoID, false)
		if status := httpGetStatus(t, hubAPIRepoURL(s, rt, repoID)); status != http.StatusOK {
			t.Fatalf("expected 200 for created repo, got %d", status)
		}
		runHFCmd(t, s.httpURL, append([]string{"repos", "delete", repoID, "--yes"}, rt.cliTypeArgs()...)...)
	}

	if status := httpGetStatus(t, hubAPIRepoURL(s, rt, repoID)); status != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", status)
	}
}

// runHubUploadAndDownload: multi-file upload with a nested directory, then
// a client download with per-file content asserts and per-file resolve
// content asserts. The python row uploads via upload_folder and downloads
// in the same interpreter run; upload_file is covered by every other py
// op's setup.
func runHubUploadAndDownload(t *testing.T, s *e2eServer, c hubClient, rt hubRepoType) {
	repoID := "hub-user/updown-" + rt.arg
	files := []hubFile{
		{"README.md", "# Hub API Matrix\n"},
		{"data/config.json", "{\"key\": \"value\"}\n"},
	}

	if c.py {
		dir := t.TempDir()
		writeHubFiles(t, dir, files)
		script := hubPyAPI + hubPyCreateLine(repoID, rt, true) +
			fmt.Sprintf("api.upload_folder(folder_path=%q, repo_id=%q, repo_type=%q)\n", dir, repoID, rt.arg) +
			hubPyDownloadLines(t.TempDir(), rt, repoID, files)
		runPyScript(t, s.httpURL, script)
	} else {
		hubCliUpload(t, s, rt, repoID, files, "Upload via hf CLI")
		hubCliDownloadAndAssert(t, s, rt, repoID, files)
	}

	for _, f := range files {
		assertResolveContent(t, s, rt, repoID, "main", f.path, f.content)
	}
}

// runHubSnapshotDownload (py only): snapshot_download of a multi-file repo
// with a subdirectory, all contents asserted; setup, download, and asserts
// share one script.
func runHubSnapshotDownload(t *testing.T, s *e2eServer, c hubClient, rt hubRepoType) {
	repoID := "hub-user/snapshot-" + rt.arg
	files := []hubFile{
		{"file1.txt", "file one\n"},
		{"file2.txt", "file two\n"},
		{"sub/file3.txt", "sub content\n"},
	}

	localDir := t.TempDir()
	script := hubPyAPI + hubPySetupLines(repoID, rt, files) + fmt.Sprintf(`local_dir = huggingface_hub.snapshot_download(
    repo_id=%q,
    repo_type=%q,
    local_dir=%q,
    endpoint=os.environ["HF_ENDPOINT"],
    token=os.environ["HF_TOKEN"],
)
assert open(os.path.join(local_dir, "file1.txt")).read() == "file one\n"
assert open(os.path.join(local_dir, "file2.txt")).read() == "file two\n"
assert open(os.path.join(local_dir, "sub", "file3.txt")).read() == "sub content\n"
`, repoID, rt.arg, localDir)
	runPyScript(t, s.httpURL, script)
}

// runHubListFiles (py only): list_repo_files includes subdirectory files;
// setup and list share one script.
func runHubListFiles(t *testing.T, s *e2eServer, c hubClient, rt hubRepoType) {
	repoID := "hub-user/list-" + rt.arg
	files := []hubFile{
		{"a.txt", "a\n"},
		{"b.txt", "b\n"},
		{"sub/c.txt", "c\n"},
	}

	script := hubPyAPI + hubPySetupLines(repoID, rt, files) + fmt.Sprintf(`files = sorted(api.list_repo_files(repo_id=%q, repo_type=%q))
assert "a.txt" in files, f"a.txt not in {files}"
assert "b.txt" in files, f"b.txt not in {files}"
assert "sub/c.txt" in files, f"sub/c.txt not in {files}"
`, repoID, rt.arg)
	runPyScript(t, s.httpURL, script)
}

// runHubBranch: create -> resolve on the branch 200 -> delete -> resolve
// no longer 200. The python row runs setup, create, mid-state resolve
// assert, and delete in one script.
func runHubBranch(t *testing.T, s *e2eServer, c hubClient, rt hubRepoType) {
	repoID := "hub-user/branch-" + rt.arg
	file := hubFile{"main.txt", "main content\n"}

	if c.py {
		script := hubPyAPI + hubPyHTTPHelpers + hubPySetupLines(repoID, rt, []hubFile{file}) +
			fmt.Sprintf("api.create_branch(repo_id=%q, branch=\"dev\", repo_type=%q)\n", repoID, rt.arg) +
			fmt.Sprintf("assert_content(%q, %q)\n", hubResolveURL(s, rt, repoID, "dev", file.path), file.content) +
			fmt.Sprintf("api.delete_branch(repo_id=%q, branch=\"dev\", repo_type=%q)\n", repoID, rt.arg)
		runPyScript(t, s.httpURL, script)
	} else {
		hubCliUpload(t, s, rt, repoID, []hubFile{file}, "Initial commit")
		runHFCmd(t, s.httpURL, append([]string{"repos", "branch", "create", repoID, "dev"}, rt.cliTypeArgs()...)...)
		assertResolveContent(t, s, rt, repoID, "dev", file.path, file.content)
		runHFCmd(t, s.httpURL, append([]string{"repos", "branch", "delete", repoID, "dev"}, rt.cliTypeArgs()...)...)
	}

	if status := hubResolveStatus(t, s, rt, repoID, "dev", file.path); status == http.StatusOK {
		t.Fatalf("expected non-200 after branch delete, got %d", status)
	}
}

// runHubTag: create with message -> resolve at the tag 200 -> (CLI: list
// contains) -> delete -> resolve no longer 200 -> (CLI: list clean). The
// python row runs setup, create, mid-state resolve assert, and delete in
// one script.
func runHubTag(t *testing.T, s *e2eServer, c hubClient, rt hubRepoType) {
	repoID := "hub-user/tag-" + rt.arg
	file := hubFile{"readme.txt", "v1 content\n"}

	if c.py {
		script := hubPyAPI + hubPyHTTPHelpers + hubPySetupLines(repoID, rt, []hubFile{file}) +
			fmt.Sprintf("api.create_tag(repo_id=%q, tag=\"v1.0\", tag_message=\"First release\", repo_type=%q)\n", repoID, rt.arg) +
			fmt.Sprintf("assert_content(%q, %q)\n", hubResolveURL(s, rt, repoID, "v1.0", file.path), file.content) +
			fmt.Sprintf("api.delete_tag(repo_id=%q, tag=\"v1.0\", repo_type=%q)\n", repoID, rt.arg)
		runPyScript(t, s.httpURL, script)
	} else {
		hubCliUpload(t, s, rt, repoID, []hubFile{file}, "Initial commit")
		runHFCmd(t, s.httpURL, append([]string{"repos", "tag", "create", repoID, "v1.0", "-m", "First release"}, rt.cliTypeArgs()...)...)
		assertResolveContent(t, s, rt, repoID, "v1.0", file.path, file.content)
		output := runHFCmd(t, s.httpURL, append([]string{"repos", "tag", "list", repoID}, rt.cliTypeArgs()...)...)
		if !strings.Contains(output, "v1.0") {
			t.Fatalf("expected tag v1.0 in list output, got: %s", output)
		}
		runHFCmd(t, s.httpURL, append([]string{"repos", "tag", "delete", repoID, "v1.0", "--yes"}, rt.cliTypeArgs()...)...)
	}

	if status := hubResolveStatus(t, s, rt, repoID, "v1.0", file.path); status == http.StatusOK {
		t.Fatalf("expected non-200 after tag delete, got %d", status)
	}

	if !c.py {
		output := runHFCmd(t, s.httpURL, append([]string{"repos", "tag", "list", repoID}, rt.cliTypeArgs()...)...)
		if strings.Contains(output, "v1.0") {
			t.Fatalf("tag v1.0 should be removed, got list output: %s", output)
		}
	}
}

// runHubMove: upload -> move to a new namespace -> old API URL 404 -> new
// resolve serves the content. The python row runs setup and move in one
// script; both asserts check final state, so they stay in Go.
func runHubMove(t *testing.T, s *e2eServer, c hubClient, rt hubRepoType) {
	fromID := "old-user/move-" + rt.arg
	toID := "new-user/move-" + rt.arg
	file := hubFile{"README.md", "# Move Test\n"}

	if c.py {
		script := hubPyAPI + hubPySetupLines(fromID, rt, []hubFile{file}) +
			fmt.Sprintf("api.move_repo(from_id=%q, to_id=%q, repo_type=%q)\n", fromID, toID, rt.arg)
		runPyScript(t, s.httpURL, script)
	} else {
		hubCliUpload(t, s, rt, fromID, []hubFile{file}, "Initial commit")
		runHFCmd(t, s.httpURL, append([]string{"repos", "move", fromID, toID}, rt.cliTypeArgs()...)...)
	}

	if status := httpGetStatus(t, hubAPIRepoURL(s, rt, fromID)); status != http.StatusNotFound {
		t.Fatalf("expected 404 for old location, got %d", status)
	}
	assertResolveContent(t, s, rt, toID, "main", file.path, file.content)
}

// runHubSettings: --private and --gated auto both succeed. The server
// accepts the settings payload without persisting private/gated
// (handler_hf_repo_crud.go), so there is nothing to read back; success of
// both calls is the assertion. The python row runs create and both updates
// in one script.
func runHubSettings(t *testing.T, s *e2eServer, c hubClient, rt hubRepoType) {
	repoID := "hub-user/settings-" + rt.arg

	if c.py {
		script := hubPyAPI + hubPyCreateLine(repoID, rt, true) +
			fmt.Sprintf("api.update_repo_settings(repo_id=%q, private=True, repo_type=%q)\n", repoID, rt.arg) +
			fmt.Sprintf("api.update_repo_settings(repo_id=%q, gated=\"auto\", repo_type=%q)\n", repoID, rt.arg)
		runPyScript(t, s.httpURL, script)
	} else {
		hubCliCreate(t, s, rt, repoID, true)
		runHFCmd(t, s.httpURL, append([]string{"repos", "settings", repoID, "--private"}, rt.cliTypeArgs()...)...)
		runHFCmd(t, s.httpURL, append([]string{"repos", "settings", repoID, "--gated", "auto"}, rt.cliTypeArgs()...)...)
	}
}

// runHubDeleteFile: two files -> delete one -> deleted 404, kept 200. The
// python row runs setup and delete in one script; both asserts check final
// state, so they stay in Go.
func runHubDeleteFile(t *testing.T, s *e2eServer, c hubClient, rt hubRepoType) {
	repoID := "hub-user/delfile-" + rt.arg
	keep := hubFile{"keep.txt", "keep me\n"}
	del := hubFile{"delete.txt", "delete me\n"}

	if c.py {
		script := hubPyAPI + hubPySetupLines(repoID, rt, []hubFile{keep, del}) +
			fmt.Sprintf("api.delete_file(path_in_repo=%q, repo_id=%q, repo_type=%q)\n", del.path, repoID, rt.arg)
		runPyScript(t, s.httpURL, script)
	} else {
		hubCliUpload(t, s, rt, repoID, []hubFile{keep, del}, "Initial commit")
		runHFCmd(t, s.httpURL, append([]string{"repos", "delete-files", repoID, del.path}, rt.cliTypeArgs()...)...)
	}

	if status := hubResolveStatus(t, s, rt, repoID, "main", del.path); status != http.StatusNotFound {
		t.Fatalf("expected 404 for deleted file, got %d", status)
	}
	assertResolveContent(t, s, rt, repoID, "main", keep.path, keep.content)
}

// runHubRepoInfo (py only): the type's info accessor returns the id and the
// full sibling set; setup and info share one script. The README carries YAML
// front matter so the info path exercises the card-metadata parser.
func runHubRepoInfo(t *testing.T, s *e2eServer, c hubClient, rt hubRepoType) {
	repoID := "hub-user/info-" + rt.arg
	files := []hubFile{
		{"README.md", "---\ntags:\n- text-classification\n- pytorch\n---\n# Info Test\n"},
		{"data.txt", "data\n"},
	}

	script := hubPyAPI + hubPySetupLines(repoID, rt, files) + fmt.Sprintf(`info = api.%s(repo_id=%q)
assert info.id == %q, f"unexpected id: {info.id}"
siblings = [s.rfilename for s in info.siblings]
assert "README.md" in siblings, f"README.md not in {siblings}"
assert "data.txt" in siblings, f"data.txt not in {siblings}"
`, rt.pyInfoFunc(), repoID, repoID)
	runPyScript(t, s.httpURL, script)
}

// runHubCommits (py only): two uploads with messages, then
// list_repo_commits with the upload OIDs anchoring the listed commit_ids.
func runHubCommits(t *testing.T, s *e2eServer, c hubClient, rt hubRepoType) {
	repoID := "hub-user/commits-" + rt.arg

	// Upload and list run in one script so the CommitInfo.oid values returned
	// by upload_file anchor the listed commit_ids to the real commits.
	script := hubPyAPI + hubPyCreateLine(repoID, rt, true) + fmt.Sprintf(`first = api.upload_file(path_or_fileobj=b"first\n", path_in_repo="first.txt", repo_id=%q, repo_type=%q, commit_message="Add first file")
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
`, repoID, rt.arg, repoID, rt.arg, repoID, rt.arg)
	runPyScript(t, s.httpURL, script)
}

// runHubRefs (py only): branch+tag then list_repo_refs asserts
// name/ref/target_commit; after deletion the refs are gone.
func runHubRefs(t *testing.T, s *e2eServer, c hubClient, rt hubRepoType) {
	repoID := "hub-user/refs-" + rt.arg

	// Setup, both listings, and the deletions run in one script so the
	// upload's CommitInfo.oid (the main HEAD) anchors every target_commit.
	// dev and v1.0 are created from that exact revision; the server creates
	// lightweight tags, so the tag ref points at the commit itself.
	script := hubPyAPI + hubPyCreateLine(repoID, rt, true) + fmt.Sprintf(`head = api.upload_file(path_or_fileobj=b"refs content\n", path_in_repo="refs.txt", repo_id=%q, repo_type=%q).oid
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
api.delete_branch(repo_id=%q, branch="dev", repo_type=%q)
api.delete_tag(repo_id=%q, tag="v1.0", repo_type=%q)
refs = api.list_repo_refs(repo_id=%q, repo_type=%q)
branch_names = [b.name for b in refs.branches]
assert "main" in branch_names, f"main not in {branch_names}"
assert "dev" not in branch_names, f"dev still in {branch_names}"
tag_names = [t.name for t in refs.tags]
assert "v1.0" not in tag_names, f"v1.0 still in {tag_names}"
`, repoID, rt.arg, repoID, rt.arg, repoID, rt.arg, repoID, rt.arg,
		repoID, rt.arg, repoID, rt.arg, repoID, rt.arg)
	runPyScript(t, s.httpURL, script)
}

// runHubSquash (py only): three commits -> super_squash_history(main) ->
// exactly one commit with the squash title, the file set unchanged, and all
// pre-squash contents still downloadable.
func runHubSquash(t *testing.T, s *e2eServer, c hubClient, rt hubRepoType) {
	repoID := "hub-user/squash-" + rt.arg

	// Uploads, squash, listing asserts, and re-downloads run in one script.
	// The upload OIDs anchor the pre-squash listing to the real commits; the
	// downloads go through a fresh cache dir so all three contents are
	// proven intact from the rewritten history, not served from a warm cache.
	script := hubPyAPI + hubPyCreateLine(repoID, rt, true) + fmt.Sprintf(`one = api.upload_file(path_or_fileobj=b"content one\n", path_in_repo="one.txt", repo_id=%q, repo_type=%q, commit_message="Add one")
two = api.upload_file(path_or_fileobj=b"content two\n", path_in_repo="two.txt", repo_id=%q, repo_type=%q, commit_message="Add two")
three = api.upload_file(path_or_fileobj=b"content three\n", path_in_repo="three.txt", repo_id=%q, repo_type=%q, commit_message="Add three")
before = api.list_repo_commits(repo_id=%q, repo_type=%q)
assert len(before) >= 3, f"expected at least 3 commits before squash, got {len(before)}"
before_ids = {c.commit_id for c in before}
for up in (one, two, three):
    assert up.oid in before_ids, f"upload oid {up.oid} not in {before_ids}"
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
`, repoID, rt.arg, repoID, rt.arg, repoID, rt.arg, repoID, rt.arg, repoID, rt.arg,
		repoID, rt.arg, repoID, rt.arg, repoID, rt.arg) +
		hubPyDownloadLines(t.TempDir(), rt, repoID, []hubFile{
			{"one.txt", "content one\n"},
			{"two.txt", "content two\n"},
			{"three.txt", "content three\n"},
		})
	runPyScript(t, s.httpURL, script)
}

// runHubTypeIsolation uploads different content to the same repo name under
// all three repo types and proves each resolve prefix serves its own bytes.
// It spans all repo types internally, so it is anchored to the Model cell
// and runs once per client.
func runHubTypeIsolation(t *testing.T, s *e2eServer, c hubClient, _ hubRepoType) {
	repoID := "hub-user/shared-name"
	contents := map[string]string{
		"model":   "model content\n",
		"dataset": "dataset content\n",
		"space":   "space content\n",
	}

	if c.py {
		script := hubPyAPI + fmt.Sprintf(`api.create_repo(repo_id=%q, repo_type="model", exist_ok=True)
api.upload_file(path_or_fileobj=b"model content\n", path_in_repo="data.txt", repo_id=%q, repo_type="model")
api.create_repo(repo_id=%q, repo_type="dataset", exist_ok=True)
api.upload_file(path_or_fileobj=b"dataset content\n", path_in_repo="data.txt", repo_id=%q, repo_type="dataset")
api.create_repo(repo_id=%q, repo_type="space", space_sdk="gradio", exist_ok=True)
api.upload_file(path_or_fileobj=b"space content\n", path_in_repo="data.txt", repo_id=%q, repo_type="space")
`, repoID, repoID, repoID, repoID, repoID, repoID)
		runPyScript(t, s.httpURL, script)
	} else {
		for _, rt := range hubRepoTypes {
			dir := t.TempDir()
			writeHubFiles(t, dir, []hubFile{{"data.txt", contents[rt.arg]}})
			runHFCmd(t, s.httpURL, "upload", repoID, dir, ".", "--repo-type", rt.arg, "--commit-message", "Upload "+rt.arg)
		}
	}

	for _, rt := range hubRepoTypes {
		assertResolveContent(t, s, rt, repoID, "main", "data.txt", contents[rt.arg])
	}
	// No per-type client download here: hf download x every repo type is
	// already covered by the UploadAndDownload row; this op's claim is the
	// cross-type namespace split, which the resolve asserts above prove.
}

// --- standalone tests ---

// TestCreateRepoDefaultGitAttributes verifies an API-created repo starts
// with the default .gitattributes carrying the LFS patterns.
func TestCreateRepoDefaultGitAttributes(t *testing.T) {
	t.Parallel()
	s := newE2EServer(t)

	resp, err := http.Post(s.httpURL+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"gitattrs-model","organization":"test-user"}`))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 creating repo, got %d", resp.StatusCode)
	}

	status, body := httpGetContent(t, s.httpURL+"/test-user/gitattrs-model/resolve/main/.gitattributes")
	if status != http.StatusOK {
		t.Fatalf("Expected 200 for .gitattributes, got %d", status)
	}
	for _, pattern := range []string{"*.bin", "*.safetensors", "*.pt", "filter=lfs"} {
		if !strings.Contains(body, pattern) {
			t.Errorf("Expected .gitattributes to contain %q, got:\n%s", pattern, body)
		}
	}
}

// TestXETTokenRoutes pins that a plain server (no pull upstream) answers the
// xet token routes with the dual header+body contract; tokens are global.
func TestXETTokenRoutes(t *testing.T) {
	t.Parallel()
	s := newE2EServer(t)

	for _, url := range []string{
		s.httpURL + "/xet-token",
		s.httpURL + "/api/models/org/repo/xet-read-token/main",
	} {
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("GET %s: %v", url, err)
		}
		var tok struct {
			CasURL      string `json:"casUrl"`
			AccessToken string `json:"accessToken"`
			Exp         int64  `json:"exp"`
		}
		decodeErr := json.NewDecoder(resp.Body).Decode(&tok)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", url, resp.StatusCode)
		}
		if decodeErr != nil {
			t.Fatalf("decode %s: %v", url, decodeErr)
		}
		if tok.AccessToken == "" {
			t.Errorf("GET %s missing accessToken", url)
		}
		if tok.CasURL != s.httpURL {
			t.Errorf("GET %s casUrl = %q, want %q", url, tok.CasURL, s.httpURL)
		}
		if got := resp.Header.Get("X-Xet-Access-Token"); got != tok.AccessToken {
			t.Errorf("GET %s X-Xet-Access-Token = %q, want body accessToken %q", url, got, tok.AccessToken)
		}
		if got := resp.Header.Get("X-Xet-Cas-Url"); got != tok.CasURL {
			t.Errorf("GET %s X-Xet-Cas-Url = %q, want body casUrl %q", url, got, tok.CasURL)
		}
		if tok.Exp <= time.Now().Unix() {
			t.Errorf("GET %s exp = %d, want future unix seconds", url, tok.Exp)
		}
		if got := resp.Header.Get("X-Xet-Token-Expiration"); got != strconv.FormatInt(tok.Exp, 10) {
			t.Errorf("GET %s X-Xet-Token-Expiration = %q, want body exp %d", url, got, tok.Exp)
		}
		if got := resp.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("GET %s Content-Type = %q, want application/json", url, got)
		}
		// The cas backend marks minted credentials uncacheable.
		if got := resp.Header.Get("Cache-Control"); got != "no-store" {
			t.Errorf("GET %s Cache-Control = %q, want no-store", url, got)
		}
	}
}

// TestMixedCLIAndPythonLibrary interleaves the python library and the hf CLI
// on one model repo: each uploads a file, then both download and see both
// files.
func TestMixedCLIAndPythonLibrary(t *testing.T) {
	t.Parallel()
	checkPythonHFHub(t)
	requireHFCli(t)

	s := newE2EServer(t)
	endpoint := s.httpURL

	// Upload using the python library
	runPyScript(t, endpoint, `
import os
import huggingface_hub
api = huggingface_hub.HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
api.create_repo(repo_id="mix-user/mixed-model", exist_ok=True)
api.upload_file(path_or_fileobj=b"from python\n", path_in_repo="python.txt", repo_id="mix-user/mixed-model")
`)

	// Upload another file using the hf CLI
	uploadDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(uploadDir, "cli.txt"), []byte("from cli\n"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	runHFCmd(t, endpoint, "upload", "mix-user/mixed-model", uploadDir, ".", "--commit-message", "Upload via CLI")

	// Download using the python library and verify both files exist
	localDir := t.TempDir()
	runPyScript(t, endpoint, fmt.Sprintf(`
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
`, localDir))

	// Download using the hf CLI and verify both files exist
	cliDownloadDir := t.TempDir()
	runHFCmd(t, endpoint, "download", "mix-user/mixed-model", "--local-dir", cliDownloadDir)
	for _, file := range []hubFile{
		{"python.txt", "from python\n"},
		{"cli.txt", "from cli\n"},
	} {
		content, err := os.ReadFile(filepath.Join(cliDownloadDir, file.path))
		if err != nil {
			t.Fatalf("Failed to read %s: %v", file.path, err)
		}
		if string(content) != file.content {
			t.Errorf("Content mismatch for %s: got %q, want %q", file.path, content, file.content)
		}
	}
}
