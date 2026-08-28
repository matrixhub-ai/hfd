package e2e_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/lfs"
)

// TestLFSLockMatrix exercises the LFS file locking API end to end with the
// git-lfs CLI over both git protocols: lock, list, duplicate-lock conflict,
// the locks/verify ours/theirs split, and unlock by path and by id. TestMain
// runs the suite twice, covering local and S3 storage.
func TestLFSLockMatrix(t *testing.T) {
	if _, err := exec.LookPath("git-lfs"); err != nil {
		t.Skip("git-lfs not available, skipping LFS lock matrix test")
	}

	protocols := []struct {
		name   string
		repo   string
		remote func(s *e2eServer, repoID string) (string, []string)
	}{
		{name: "HTTP", repo: "matrix-org/lock-http", remote: (*e2eServer).httpRemote},
		{name: "SSH", repo: "matrix-org/lock-ssh", remote: (*e2eServer).sshRemote},
	}

	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			s := newE2EServer(t, withSSHLFSURL())
			org, name, _ := strings.Cut(p.repo, "/")
			s.createRepo(t, org, name)
			remote, env := p.remote(s, p.repo)

			// Step 1: repo with two small pushed LFS files as lock targets.
			dir := pushLockFixture(t, remote, env, "data.bin", "extra.bin")

			// Step 2: lock data.bin and see it listed.
			runGit(t, dir, env, "lfs", "lock", "data.bin")
			locks := listLFSLocks(t, dir, env)
			if findLFSLock(locks, "data.bin") == nil {
				t.Fatalf("git lfs locks after lock = %+v, want entry for data.bin", locks)
			}

			// Step 3: locking the same path again must fail; the server
			// replies 409 "lock already created". The message assertion stays
			// loose to not overfit any particular git-lfs version wording.
			stdout, stderr, err := gitCmd(t, dir, env, "lfs", "lock", "data.bin")
			if err == nil {
				t.Fatalf("duplicate git lfs lock succeeded, want failure\nstdout: %s\nstderr: %s", stdout, stderr)
			}
			if combined := strings.ToLower(stdout + stderr); !strings.Contains(combined, "lock") {
				t.Fatalf("duplicate git lfs lock error does not mention lock\nstdout: %s\nstderr: %s", stdout, stderr)
			}
			// The CLI failure alone cannot distinguish 409 from auth or
			// server errors, so re-post the lock directly and pin the status.
			// Both protocol units hit the same HTTP lock endpoint (the SSH
			// flow resolves to it via git-lfs-authenticate) and the e2e
			// server mounts no auth middleware, so no Authorization needed.
			if status := postLFSLockCreate(t, s.httpURL, p.repo, "data.bin"); status != http.StatusConflict {
				t.Fatalf("duplicate lock POST status = %d, want %d", status, http.StatusConflict)
			}

			// Step 4: the locks/verify endpoint splits locks by owner. Both
			// protocol units post it over HTTP directly: the SSH LFS flow
			// resolves to the same HTTP endpoint via git-lfs-authenticate.
			// The anonymous test identity owns every lock, so all land in ours.
			vl := postLFSLocksVerify(t, s.httpURL, p.repo)
			if len(vl.Ours) != 1 || vl.Ours[0].Path != "data.bin" {
				t.Fatalf("locks/verify ours = %+v, want single lock for data.bin", vl.Ours)
			}
			if len(vl.Theirs) != 0 {
				t.Fatalf("locks/verify theirs = %+v, want empty", vl.Theirs)
			}

			// Step 5: unlock by path.
			runGit(t, dir, env, "lfs", "unlock", "data.bin")
			if locks := listLFSLocks(t, dir, env); findLFSLock(locks, "data.bin") != nil {
				t.Fatalf("git lfs locks after unlock = %+v, want no entry for data.bin", locks)
			}

			// Step 6: unlock by id.
			runGit(t, dir, env, "lfs", "lock", "extra.bin")
			locks = listLFSLocks(t, dir, env)
			entry := findLFSLock(locks, "extra.bin")
			if entry == nil {
				t.Fatalf("git lfs locks after lock = %+v, want entry for extra.bin", locks)
			}
			runGit(t, dir, env, "lfs", "unlock", "--id="+entry.ID)
			if locks := listLFSLocks(t, dir, env); len(locks) != 0 {
				t.Fatalf("git lfs locks after unlock by id = %+v, want empty", locks)
			}
		})
	}
}

// pushLockFixture clones the repo, enables git-lfs locally, and pushes the
// given small files; the server-seeded .gitattributes already tracks *.bin,
// so they land as LFS objects. Locks attach to the origin remote, which
// requires at least one pushed commit.
func pushLockFixture(t *testing.T, remote string, env []string, files ...string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "clone")
	runGit(t, "", env, "clone", remote, dir)
	runGit(t, dir, env, "config", "user.email", "matrix@test.com")
	runGit(t, dir, env, "config", "user.name", "Matrix Test")
	runGit(t, dir, env, "lfs", "install", "--local")
	runGit(t, dir, env, "lfs", "track", "*.bin")
	for i, name := range files {
		if err := os.WriteFile(filepath.Join(dir, name), makeBinaryData(64, byte(i)), 0644); err != nil {
			t.Fatalf("write fixture file %s: %v", name, err)
		}
	}
	runGit(t, dir, env, "add", ".")
	runGit(t, dir, env, "commit", "-m", "add lock fixture files")
	runGit(t, dir, env, "push", "origin", "main")
	return dir
}

// lfsLockEntry is the subset of `git lfs locks --json` output the test needs.
type lfsLockEntry struct {
	ID   string `json:"id"`
	Path string `json:"path"`
}

// listLFSLocks returns the locks git-lfs sees on the origin remote. It goes
// through gitCmd because the JSON parse needs stdout unmixed with stderr.
func listLFSLocks(t *testing.T, dir string, env []string) []lfsLockEntry {
	t.Helper()
	out, stderr, err := gitCmd(t, dir, env, "lfs", "locks", "--json")
	if err != nil {
		t.Fatalf("git lfs locks --json failed: %v\nstdout: %s\nstderr: %s", err, out, stderr)
	}
	var locks []lfsLockEntry
	if err := json.Unmarshal([]byte(out), &locks); err != nil {
		t.Fatalf("parse git lfs locks --json: %v\noutput: %s", err, out)
	}
	return locks
}

func findLFSLock(locks []lfsLockEntry, path string) *lfsLockEntry {
	for i := range locks {
		if locks[i].Path == path {
			return &locks[i]
		}
	}
	return nil
}

// postLFSLockCreate posts a lock creation request the way an LFS client does
// and returns the HTTP status code.
func postLFSLockCreate(t *testing.T, httpURL, repoID, path string) int {
	t.Helper()
	url := httpURL + "/" + repoID + ".git/info/lfs/locks"
	body := fmt.Sprintf(`{"path":%q}`, path)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build lock create request: %v", err)
	}
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("lock create request: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// postLFSLocksVerify posts the locks/verify endpoint the way an LFS client
// does and returns the decoded ours/theirs split.
func postLFSLocksVerify(t *testing.T, httpURL, repoID string) *lfs.VerifiableLockList {
	t.Helper()
	url := httpURL + "/" + repoID + ".git/info/lfs/locks/verify"
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("build locks/verify request: %v", err)
	}
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("locks/verify request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("locks/verify status = %d, want 200; body: %s", resp.StatusCode, body)
	}
	var vl lfs.VerifiableLockList
	if err := json.NewDecoder(resp.Body).Decode(&vl); err != nil {
		t.Fatalf("decode locks/verify response: %v", err)
	}
	return &vl
}
