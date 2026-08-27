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
