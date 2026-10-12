package e2e_test

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	// gitMatrixRepoID is the repository every matrix cell operates on; the
	// protocol setup creates it before handing out the remote.
	gitMatrixRepoID = "test-org/test-repo"
	// gitMatrixReadme is what testPushCommit pushes as README.md and what the
	// read-back operations assert.
	gitMatrixReadme = "# Test\n"
)

// TestGitOperationsMatrix tests all git operations across HTTP and SSH
// protocols. Each cell gets a fresh server wired through the full production
// chain (http → lfs → hf → cas → data plane) with its own storage.
func TestGitOperationsMatrix(t *testing.T) {
	// Both rows start the SSH server so CrossProtocolReadBack can always
	// reach the other protocol's remote.
	protocols := []struct {
		name  string
		setup func(t *testing.T) (s *e2eServer, remote string, env []string)
	}{
		{
			name: "HTTP",
			setup: func(t *testing.T) (*e2eServer, string, []string) {
				s := newE2EServer(t, withSSH())
				s.createRepo(t, "test-org", "test-repo")
				remote, env := s.httpRemote(gitMatrixRepoID)
				return s, remote, env
			},
		},
		{
			name: "SSH",
			setup: func(t *testing.T) (*e2eServer, string, []string) {
				s := newE2EServer(t, withSSH())
				s.createRepo(t, "test-org", "test-repo")
				remote, env := s.sshRemote(gitMatrixRepoID)
				return s, remote, env
			},
		},
	}

	operations := []struct {
		name    string
		sshOnly bool
		test    func(t *testing.T, s *e2eServer, remote string, env []string)
	}{
		{name: "CloneEmptyRepo", test: testCloneEmptyRepo},
		{name: "PushCommit", test: testPushCommit},
		{name: "CloneWithContent", test: testCloneWithContent},
		{name: "FetchFromRepo", test: testFetchFromRepo},
		{name: "PushMoreCommits", test: testPushMoreCommits},
		{name: "PullChanges", test: testPullChanges},
		{name: "PushMultipleFiles", test: testPushMultipleFiles},
		{name: "CreateAndPushBranch", test: testCreateAndPushBranch},
		{name: "CreateAndPushTag", test: testCreateAndPushTag},
		{name: "DeleteBranch", test: testDeleteBranch},
		{name: "DeleteTag", test: testDeleteTag},
		{name: "ResolveAfterPush", test: testResolveAfterPush},
		{name: "CrossProtocolReadBack", test: testCrossProtocolReadBack},
		{name: "LargeRegularFileRefused", test: testLargeRegularFileRefused},
		{name: "BinaryFileRefused", test: testBinaryFileRefused},
		{name: "UnauthorizedKeyDenied", sshOnly: true, test: testUnauthorizedKeyDenied},
	}

	for _, protocol := range protocols {
		t.Run(protocol.name, func(t *testing.T) {
			for _, op := range operations {
				if op.sshOnly && protocol.name != "SSH" {
					continue
				}
				t.Run(op.name, func(t *testing.T) {
					s, remote, env := protocol.setup(t)
					op.test(t, s, remote, env)
				})
			}
		})
	}
}

func testCloneEmptyRepo(t *testing.T, s *e2eServer, remote string, env []string) {
	cloneDir := filepath.Join(t.TempDir(), "clone")
	runGit(t, "", env, "clone", remote, cloneDir)

	if _, err := os.Stat(filepath.Join(cloneDir, ".git")); os.IsNotExist(err) {
		t.Errorf(".git directory not found in cloned repository")
	}
}

func testPushCommit(t *testing.T, s *e2eServer, remote string, env []string) {
	cloneDir := filepath.Join(t.TempDir(), "clone")
	runGit(t, "", env, "clone", remote, cloneDir)
	runGit(t, cloneDir, env, "config", "user.email", "test@test.com")
	runGit(t, cloneDir, env, "config", "user.name", "Test User")

	if err := os.WriteFile(filepath.Join(cloneDir, "README.md"), []byte(gitMatrixReadme), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	runGit(t, cloneDir, env, "add", "README.md")
	runGit(t, cloneDir, env, "commit", "-m", "Initial commit")
	runGit(t, cloneDir, env, "push", "origin", "main")
}

func testCloneWithContent(t *testing.T, s *e2eServer, remote string, env []string) {
	// First push content
	testPushCommit(t, s, remote, env)

	// Then clone and verify
	cloneDir := filepath.Join(t.TempDir(), "clone-verify")
	runGit(t, "", env, "clone", remote, cloneDir)

	content, err := os.ReadFile(filepath.Join(cloneDir, "README.md"))
	if err != nil {
		t.Fatalf("Failed to read README.md: %v", err)
	}
	if string(content) != gitMatrixReadme {
		t.Errorf("Unexpected content: %s", content)
	}
}

func testFetchFromRepo(t *testing.T, s *e2eServer, remote string, env []string) {
	testPushCommit(t, s, remote, env)

	cloneDir := filepath.Join(t.TempDir(), "clone")
	runGit(t, "", env, "clone", remote, cloneDir)
	runGit(t, cloneDir, env, "fetch", "origin")
}

func testPushMoreCommits(t *testing.T, s *e2eServer, remote string, env []string) {
	testPushCommit(t, s, remote, env)

	cloneDir := filepath.Join(t.TempDir(), "clone")
	runGit(t, "", env, "clone", remote, cloneDir)
	runGit(t, cloneDir, env, "config", "user.email", "test@test.com")
	runGit(t, cloneDir, env, "config", "user.name", "Test User")

	if err := os.WriteFile(filepath.Join(cloneDir, "file2.txt"), []byte("Second file\n"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	runGit(t, cloneDir, env, "add", "file2.txt")
	runGit(t, cloneDir, env, "commit", "-m", "Add second file")
	runGit(t, cloneDir, env, "push")
}

func testPullChanges(t *testing.T, s *e2eServer, remote string, env []string) {
	testPushCommit(t, s, remote, env)

	clientDir := t.TempDir()

	// First clone
	clone1Dir := filepath.Join(clientDir, "clone1")
	runGit(t, "", env, "clone", remote, clone1Dir)

	// Second clone, push changes
	clone2Dir := filepath.Join(clientDir, "clone2")
	runGit(t, "", env, "clone", remote, clone2Dir)
	runGit(t, clone2Dir, env, "config", "user.email", "test@test.com")
	runGit(t, clone2Dir, env, "config", "user.name", "Test User")

	if err := os.WriteFile(filepath.Join(clone2Dir, "changes.txt"), []byte("Changes\n"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	runGit(t, clone2Dir, env, "add", "changes.txt")
	runGit(t, clone2Dir, env, "commit", "-m", "Add changes")
	runGit(t, clone2Dir, env, "push")

	// Pull changes in first clone
	runGit(t, clone1Dir, env, "config", "pull.rebase", "false")
	runGit(t, clone1Dir, env, "pull")

	if _, err := os.Stat(filepath.Join(clone1Dir, "changes.txt")); os.IsNotExist(err) {
		t.Errorf("changes.txt not found after pull")
	}
}

func testPushMultipleFiles(t *testing.T, s *e2eServer, remote string, env []string) {
	cloneDir := filepath.Join(t.TempDir(), "clone")
	runGit(t, "", env, "clone", remote, cloneDir)
	runGit(t, cloneDir, env, "config", "user.email", "test@test.com")
	runGit(t, cloneDir, env, "config", "user.name", "Test User")

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

	runGit(t, cloneDir, env, "add", ".")
	runGit(t, cloneDir, env, "commit", "-m", "Add multiple files")
	runGit(t, cloneDir, env, "push", "origin", "main")

	// Every pushed file must be readable through the hub resolve endpoint.
	for name, content := range files {
		resp, err := http.Get(s.httpURL + "/test-org/test-repo/resolve/main/" + name)
		if err != nil {
			t.Fatalf("Failed to resolve %s: %v", name, err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("Failed to read resolved %s: %v", name, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("resolve %s status = %d, want 200", name, resp.StatusCode)
		}
		if string(body) != content {
			t.Errorf("resolve %s content = %q, want %q", name, body, content)
		}
	}

	// A fresh clone must contain all files with identical bytes.
	verifyDir := filepath.Join(t.TempDir(), "verify")
	runGit(t, "", env, "clone", remote, verifyDir)
	for name, content := range files {
		got, err := os.ReadFile(filepath.Join(verifyDir, name))
		if err != nil {
			t.Fatalf("Failed to read %s from verify clone: %v", name, err)
		}
		if string(got) != content {
			t.Errorf("verify clone %s content = %q, want %q", name, got, content)
		}
	}
}

func testCreateAndPushBranch(t *testing.T, s *e2eServer, remote string, env []string) {
	testPushCommit(t, s, remote, env)

	cloneDir := filepath.Join(t.TempDir(), "clone")
	runGit(t, "", env, "clone", remote, cloneDir)
	runGit(t, cloneDir, env, "config", "user.email", "test@test.com")
	runGit(t, cloneDir, env, "config", "user.name", "Test User")

	runGit(t, cloneDir, env, "checkout", "-b", "feature")
	if err := os.WriteFile(filepath.Join(cloneDir, "feature.txt"), []byte("feature\n"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	runGit(t, cloneDir, env, "add", "feature.txt")
	runGit(t, cloneDir, env, "commit", "-m", "Feature commit")
	runGit(t, cloneDir, env, "push", "origin", "feature")
}

func testCreateAndPushTag(t *testing.T, s *e2eServer, remote string, env []string) {
	testPushCommit(t, s, remote, env)

	cloneDir := filepath.Join(t.TempDir(), "clone")
	runGit(t, "", env, "clone", remote, cloneDir)

	runGit(t, cloneDir, env, "tag", "v1.0")
	runGit(t, cloneDir, env, "push", "origin", "v1.0")
}

func testDeleteBranch(t *testing.T, s *e2eServer, remote string, env []string) {
	testCreateAndPushBranch(t, s, remote, env)

	cloneDir := filepath.Join(t.TempDir(), "clone")
	runGit(t, "", env, "clone", remote, cloneDir)

	runGit(t, cloneDir, env, "push", "origin", "--delete", "feature")
}

func testDeleteTag(t *testing.T, s *e2eServer, remote string, env []string) {
	testCreateAndPushTag(t, s, remote, env)

	cloneDir := filepath.Join(t.TempDir(), "clone")
	runGit(t, "", env, "clone", remote, cloneDir)

	runGit(t, cloneDir, env, "push", "origin", "--delete", "v1.0")
}

// testResolveAfterPush pushes README.md over the row's protocol and reads it
// back through the HF resolve API, byte for byte. On the SSH row this proves
// an SSH write is visible to HF API reads.
func testResolveAfterPush(t *testing.T, s *e2eServer, remote string, env []string) {
	testPushCommit(t, s, remote, env)

	resp, err := http.Get(s.httpURL + "/" + gitMatrixRepoID + "/resolve/main/README.md")
	if err != nil {
		t.Fatalf("Failed to get file: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read body: %v", err)
	}
	if !bytes.Equal(body, []byte(gitMatrixReadme)) {
		t.Errorf("Unexpected content: %q", body)
	}
}

// testCrossProtocolReadBack pushes over the row's protocol, then clones with
// the other protocol and verifies the content, proving both protocols serve
// the same repository.
func testCrossProtocolReadBack(t *testing.T, s *e2eServer, remote string, env []string) {
	testPushCommit(t, s, remote, env)

	otherRemote, otherEnv := s.sshRemote(gitMatrixRepoID)
	if strings.HasPrefix(remote, "ssh://") {
		otherRemote, otherEnv = s.httpRemote(gitMatrixRepoID)
	}

	cloneDir := filepath.Join(t.TempDir(), "cross-clone")
	runGit(t, "", otherEnv, "clone", otherRemote, cloneDir)

	content, err := os.ReadFile(filepath.Join(cloneDir, "README.md"))
	if err != nil {
		t.Fatalf("Failed to read README.md from cross-protocol clone: %v", err)
	}
	if string(content) != gitMatrixReadme {
		t.Errorf("Unexpected content from cross-protocol clone: %q", content)
	}
}

// testLargeRegularFileRefused pushes a regular blob one byte over 10 MiB, which
// huggingface.co's LFS policy refuses, then tracks the same file with git-lfs:
// the amended commit goes through and the file resolves byte for byte.
func testLargeRegularFileRefused(t *testing.T, s *e2eServer, remote string, env []string) {
	big := makeTextData(10<<20 + 1)
	cloneDir := filepath.Join(t.TempDir(), "clone")
	runGit(t, "", env, "clone", remote, cloneDir)
	runGit(t, cloneDir, env, "config", "user.email", "test@test.com")
	runGit(t, cloneDir, env, "config", "user.name", "Test User")
	if err := os.WriteFile(filepath.Join(cloneDir, "big.txt"), big, 0644); err != nil {
		t.Fatalf("Failed to create big.txt: %v", err)
	}
	runGit(t, cloneDir, env, "add", "big.txt")
	runGit(t, cloneDir, env, "commit", "-m", "Add big.txt")
	requirePushRefused(t, s, cloneDir, env, "big.txt",
		"Your push was rejected because it contains files larger than 10 MiB.",
		"- big.txt (ref: refs/heads/main)")

	runGit(t, cloneDir, env, "lfs", "install", "--local")
	// The SSH server has no LFS endpoint; point git-lfs at the HTTP server.
	runGit(t, cloneDir, env, "config", "lfs.url", s.httpURL+"/"+gitMatrixRepoID+".git/info/lfs")
	runGit(t, cloneDir, env, "lfs", "track", "big.txt")
	runGit(t, cloneDir, env, "rm", "--cached", "big.txt")
	runGit(t, cloneDir, env, "add", ".gitattributes", "big.txt")
	runGit(t, cloneDir, env, "commit", "--amend", "-m", "Add big.txt through LFS")
	runGit(t, cloneDir, env, "push", "origin", "main")
	if got := mustGet(t, s.httpURL+"/"+gitMatrixRepoID+"/resolve/main/big.txt"); !bytes.Equal(got, big) {
		t.Fatalf("resolve big.txt after the LFS push: got %d bytes, want %d", len(got), len(big))
	}
}

// testBinaryFileRefused pushes random bytes at the binary limit, which land as
// a regular blob, then one byte over it, which the LFS policy refuses.
func testBinaryFileRefused(t *testing.T, s *e2eServer, remote string, env []string) {
	small, blob := randomData(t, 100000, 71), randomData(t, 100001, 72)
	cloneDir := filepath.Join(t.TempDir(), "clone")
	runGit(t, "", env, "clone", remote, cloneDir)
	runGit(t, cloneDir, env, "config", "user.email", "test@test.com")
	runGit(t, cloneDir, env, "config", "user.name", "Test User")
	if err := os.WriteFile(filepath.Join(cloneDir, "small.dat"), small, 0644); err != nil {
		t.Fatalf("Failed to create small.dat: %v", err)
	}
	runGit(t, cloneDir, env, "add", "small.dat")
	runGit(t, cloneDir, env, "commit", "-m", "Add small.dat")
	runGit(t, cloneDir, env, "push", "origin", "main")
	if got := mustGet(t, s.httpURL+"/"+gitMatrixRepoID+"/resolve/main/small.dat"); !bytes.Equal(got, small) {
		t.Fatalf("resolve small.dat: got %d bytes, want %d", len(got), len(small))
	}

	if err := os.WriteFile(filepath.Join(cloneDir, "blob.dat"), blob, 0644); err != nil {
		t.Fatalf("Failed to create blob.dat: %v", err)
	}
	runGit(t, cloneDir, env, "add", "blob.dat")
	runGit(t, cloneDir, env, "commit", "-m", "Add blob.dat")
	requirePushRefused(t, s, cloneDir, env, "blob.dat",
		"Your push was rejected because it contains binary files.",
		"- blob.dat (ref: refs/heads/main)")
}

// requirePushRefused pushes main from dir and requires the LFS policy's
// refusal, with each of want in the client's output; the server's main must
// stay at origin/main and path must not resolve.
func requirePushRefused(t *testing.T, s *e2eServer, dir string, env []string, path string, want ...string) {
	t.Helper()
	before := strings.TrimSpace(runGit(t, dir, env, "rev-parse", "origin/main"))
	stdout, stderr, err := gitCmd(t, dir, env, "push", "origin", "main")
	output := stdout + stderr
	t.Logf("refused push output:\n%s", output)
	if err == nil {
		t.Fatalf("push succeeded, want the LFS policy to refuse it:\n%s", output)
	}
	for _, w := range append([]string{"Offending files:", "pre-receive hook declined"}, want...) {
		if !strings.Contains(output, w) {
			t.Errorf("push output lacks %q:\n%s", w, output)
		}
	}
	// stdout only: ssh warns about the throwaway known_hosts on stderr.
	stdout, stderr, err = gitCmd(t, dir, env, "ls-remote", "origin", "refs/heads/main")
	if after := strings.Fields(stdout); err != nil || len(after) == 0 || after[0] != before {
		t.Fatalf("server main = %q (%v) after the refused push, want %s\n%s", stdout, err, before, stderr)
	}
	if status := httpGetStatus(t, s.httpURL+"/"+gitMatrixRepoID+"/resolve/main/"+path); status != http.StatusNotFound {
		t.Fatalf("resolve %s after the refused push = %d, want 404", path, status)
	}
}

// testUnauthorizedKeyDenied clones with a fresh key that is not in the
// server's authorized list and requires the clone to fail.
func testUnauthorizedKeyDenied(t *testing.T, s *e2eServer, remote string, env []string) {
	badKeyFile := filepath.Join(t.TempDir(), "id_bad")
	generateTestKeyFile(t, badKeyFile)

	u, err := url.Parse(s.sshURL)
	if err != nil {
		t.Fatalf("Failed to parse SSH URL %q: %v", s.sshURL, err)
	}
	badEnv := sshGitEnv(badKeyFile, u.Port())

	cloneDir := filepath.Join(t.TempDir(), "clone-bad")
	stdout, stderr, err := gitCmd(t, "", badEnv, "clone", remote, cloneDir)
	if err == nil {
		t.Fatalf("Expected clone to fail with unauthorized key, but it succeeded:\nstdout: %s\nstderr: %s", stdout, stderr)
	}
}
