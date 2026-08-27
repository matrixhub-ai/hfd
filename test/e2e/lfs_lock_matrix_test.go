package e2e_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	backendssh "github.com/matrixhub-ai/hfd/pkg/backend/ssh"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
)

// lfsLockMatrixServer bundles an HTTP server (hf + LFS + git + xet data
// plane) and an SSH git server sharing the same storage and mirror. Unlike
// the transfer matrix server, the SSH server is configured with the HTTP base
// URL so git-lfs discovers the LFS (and lock) endpoint on a pure SSH remote
// through git-lfs-authenticate, with no lfs.url override on the client.
type lfsLockMatrixServer struct {
	httpURL string
	sshURL  string
	sshEnv  []string
}

func newLFSLockMatrixServer(t *testing.T) *lfsLockMatrixServer {
	t.Helper()

	dataDir := newDataDir(t, "lfs-lock-matrix-data")
	st := newTestStorage(t, dataDir)

	// The LFS-tracked fixture push lands through the basic transfer PUT
	// endpoint, which ingests into the xet data plane; locks themselves are
	// pure metadata and never touch it.
	upstream := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(upstream.Close)

	sharedMirror, dataPlane := newTestMirror(t, dataDir, upstream.URL, testS3Client != nil,
		mirror.WithRepositoriesFS(st.RepositoriesFS()),
	)

	var handler http.Handler
	handler = backendhf.NewHandler(
		backendhf.WithStorage(st),
		backendhf.WithMirror(sharedMirror),
		backendhf.WithNext(dataPlane),
	)
	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(st),
		backendlfs.WithNext(handler),
		backendlfs.WithMirror(sharedMirror),
	)
	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(st),
		backendhttp.WithNext(handler),
	)

	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)

	keyFile := filepath.Join(t.TempDir(), "id_lock_matrix")
	pubKey := generateTestKeyFile(t, keyFile)

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate host key: %v", err)
	}
	hostKey, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("create host key signer: %v", err)
	}
	sshServer := backendssh.NewServer(
		backendssh.WithHostKey(hostKey),
		backendssh.WithStorage(st),
		backendssh.WithPublicKeyCallback(backendssh.AuthorizedKeysCallback([]ssh.PublicKey{pubKey})),
		backendssh.WithLFSURL(httpServer.URL),
	)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for SSH: %v", err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		_ = sshServer.Serve(t.Context(), listener)
	}()

	addr := listener.Addr().(*net.TCPAddr)
	return &lfsLockMatrixServer{
		httpURL: httpServer.URL,
		sshURL:  "ssh://git@" + addr.String() + "/",
		sshEnv:  sshGitEnv(keyFile, strconv.Itoa(addr.Port)),
	}
}

func (s *lfsLockMatrixServer) createRepo(t *testing.T, org, name string) {
	t.Helper()
	body := fmt.Sprintf(`{"type":"model","name":%q,"organization":%q}`, name, org)
	resp, err := http.Post(s.httpURL+"/api/repos/create", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create repo status = %d, want 200", resp.StatusCode)
	}
}

func (s *lfsLockMatrixServer) httpRemote(repoID string) (string, []string) {
	return s.httpURL + "/" + repoID + ".git", []string{"GIT_TERMINAL_PROMPT=0"}
}

func (s *lfsLockMatrixServer) sshRemote(repoID string) (string, []string) {
	return s.sshURL + repoID + ".git", s.sshEnv
}

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
		remote func(s *lfsLockMatrixServer, repoID string) (string, []string)
	}{
		{name: "HTTP", repo: "matrix-org/lock-http", remote: (*lfsLockMatrixServer).httpRemote},
		{name: "SSH", repo: "matrix-org/lock-ssh", remote: (*lfsLockMatrixServer).sshRemote},
	}

	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			s := newLFSLockMatrixServer(t)
			org, name, _ := strings.Cut(p.repo, "/")
			s.createRepo(t, org, name)
			remote, env := p.remote(s, p.repo)

			// Step 1: repo with two small pushed LFS files as lock targets.
			dir := pushLockFixture(t, remote, env, "data.bin", "extra.bin")

			// Step 2: lock data.bin and see it listed.
			runLockGitCmd(t, dir, env, "lfs", "lock", "data.bin")
			locks := listLFSLocks(t, dir, env)
			if findLFSLock(locks, "data.bin") == nil {
				t.Fatalf("git lfs locks after lock = %+v, want entry for data.bin", locks)
			}

			// Step 3: locking the same path again must fail; the server
			// replies 409 "lock already created". The message assertion stays
			// loose to not overfit any particular git-lfs version wording.
			stdout, stderr, err := lockGitCmd(t, dir, env, "lfs", "lock", "data.bin")
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
			runLockGitCmd(t, dir, env, "lfs", "unlock", "data.bin")
			if locks := listLFSLocks(t, dir, env); findLFSLock(locks, "data.bin") != nil {
				t.Fatalf("git lfs locks after unlock = %+v, want no entry for data.bin", locks)
			}

			// Step 6: unlock by id.
			runLockGitCmd(t, dir, env, "lfs", "lock", "extra.bin")
			locks = listLFSLocks(t, dir, env)
			entry := findLFSLock(locks, "extra.bin")
			if entry == nil {
				t.Fatalf("git lfs locks after lock = %+v, want entry for extra.bin", locks)
			}
			runLockGitCmd(t, dir, env, "lfs", "unlock", "--id="+entry.ID)
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
	runLockGitCmd(t, "", env, "clone", remote, dir)
	runLockGitCmd(t, dir, env, "config", "user.email", "matrix@test.com")
	runLockGitCmd(t, dir, env, "config", "user.name", "Matrix Test")
	runLockGitCmd(t, dir, env, "lfs", "install", "--local")
	runLockGitCmd(t, dir, env, "lfs", "track", "*.bin")
	for i, name := range files {
		if err := os.WriteFile(filepath.Join(dir, name), makeBinaryData(64, byte(i)), 0644); err != nil {
			t.Fatalf("write fixture file %s: %v", name, err)
		}
	}
	runLockGitCmd(t, dir, env, "add", ".")
	runLockGitCmd(t, dir, env, "commit", "-m", "add lock fixture files")
	runLockGitCmd(t, dir, env, "push", "origin", "main")
	return dir
}

// lfsLockEntry is the subset of `git lfs locks --json` output the test needs.
type lfsLockEntry struct {
	ID   string `json:"id"`
	Path string `json:"path"`
}

// listLFSLocks returns the locks git-lfs sees on the origin remote.
func listLFSLocks(t *testing.T, dir string, env []string) []lfsLockEntry {
	t.Helper()
	out := runLockGitCmd(t, dir, env, "lfs", "locks", "--json")
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

// lockGitCmd runs a git command for the lock matrix, keeping stdout separate
// for JSON parsing. A watchdog kills wedged subprocesses so hangs fail fast.
func lockGitCmd(t *testing.T, dir string, env []string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = append(testEnv(), env...)
	cmd.WaitDelay = 10 * time.Second
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	err = cmd.Run()
	return outBuf.String(), errBuf.String(), err
}

// runLockGitCmd is lockGitCmd failing the test on error, returning stdout.
func runLockGitCmd(t *testing.T, dir string, env []string, args ...string) string {
	t.Helper()
	stdout, stderr, err := lockGitCmd(t, dir, env, args...)
	if err != nil {
		t.Fatalf("git %s failed: %v\nstdout: %s\nstderr: %s", strings.Join(args, " "), err, stdout, stderr)
	}
	return stdout
}
