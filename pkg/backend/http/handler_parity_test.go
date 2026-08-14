package backend_test

// Parity tests verifying that the go-git based smart-HTTP serving (info/refs
// advertisement, upload-pack and receive-pack) behaves identically to the
// canonical `git http-backend` CGI, as observed by a real git client over
// wire protocol v0 and v2. Identical seed repositories are served by both
// implementations and every operation is executed against both, comparing
// the resulting refs and repository state.

import (
	"bytes"
	"fmt"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// parityGit runs git with -c protocol.version=<ver> and returns stdout.
func parityGit(t *testing.T, dir string, protoVer int, args ...string) string {
	t.Helper()
	full := append([]string{"-c", fmt.Sprintf("protocol.version=%d", protoVer)}, args...)
	cmd := exec.CommandContext(t.Context(), "git", full...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(full, " "), err, stderr.String())
	}
	return string(out)
}

// lsRemoteLines returns the sorted pkt lines of `git ls-remote --symref <url>`,
// which covers ref names, hashes, and the HEAD symref advertisement. Lines are
// sorted because the wire protocol does not mandate an advertisement order.
func lsRemoteLines(t *testing.T, protoVer int, url string) []string {
	t.Helper()
	var lines []string
	for line := range strings.SplitSeq(parityGit(t, "", protoVer, "ls-remote", "--symref", url), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	sort.Strings(lines)
	return lines
}

// forEachRefLines returns the sorted `git for-each-ref` lines of a repository.
func forEachRefLines(t *testing.T, dir string) string {
	t.Helper()
	return runGitCmd(t, dir, "for-each-ref", "--format=%(objectname) %(objecttype) %(refname)")
}

// newHTTPBackendServer serves the bare repositories under root with the
// canonical git http-backend CGI.
func newHTTPBackendServer(t *testing.T, root string) string {
	t.Helper()
	execPath := strings.TrimSpace(runGitCmd(t, "", "--exec-path"))
	backendPath := filepath.Join(execPath, "git-http-backend")
	if _, err := os.Stat(backendPath); err != nil {
		t.Skipf("git-http-backend not available: %v", err)
	}
	srv := httptest.NewServer(&cgi.Handler{
		Path:       backendPath,
		InheritEnv: []string{"PATH"},
		Env: []string{
			"GIT_PROJECT_ROOT=" + root,
			"GIT_HTTP_EXPORT_ALL=1",
		},
	})
	t.Cleanup(srv.Close)
	return srv.URL
}

// serveParityFixture creates one seed repository and serves byte-identical
// copies with the production go-git handler and with git http-backend.
// It returns the two repository URLs and the seeding work directory.
type serveParityFixture struct {
	goGitURL string // production handler URL for the repository
	gitURL   string // git http-backend URL for the repository
	work     string // work repository pushing to both servers
}

func newServeParityFixture(t *testing.T) *serveParityFixture {
	t.Helper()
	root := t.TempDir()

	// Production go-git handler on its own storage root.
	goGitStorage := storage.NewStorage(storage.WithRootDir(filepath.Join(root, "gogit")))
	goGitRepo := filepath.Join(root, "gogit", "repositories", "repo.git")
	if err := os.MkdirAll(filepath.Dir(goGitRepo), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	runGitCmd(t, "", "init", "--bare", "--initial-branch=main", goGitRepo)
	goGitServer := httptest.NewServer(backendhttp.NewHandler(backendhttp.WithStorage(goGitStorage)))
	t.Cleanup(goGitServer.Close)

	// git http-backend on a parallel root with an identical repository.
	cgiRoot := filepath.Join(root, "cgi")
	gitRepo := filepath.Join(cgiRoot, "repo.git")
	if err := os.MkdirAll(cgiRoot, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	runGitCmd(t, "", "init", "--bare", "--initial-branch=main", gitRepo)
	runGitCmd(t, gitRepo, "config", "http.receivepack", "true")
	gitBase := newHTTPBackendServer(t, cgiRoot)

	f := &serveParityFixture{
		goGitURL: goGitServer.URL + "/repo.git",
		gitURL:   gitBase + "/repo.git",
		work:     filepath.Join(root, "work"),
	}

	// Seed both servers with the same commits, branches and tags. Pushing
	// identical objects yields identical hashes on both sides.
	runGitCmd(t, "", "init", "--initial-branch=main", f.work)
	runGitCmd(t, f.work, "config", "user.email", "test@example.com")
	runGitCmd(t, f.work, "config", "user.name", "Test User")
	f.commit(t, "file.txt", "one\n", "c1")
	f.commit(t, "file.txt", "two\n", "c2")
	runGitCmd(t, f.work, "tag", "v1", "HEAD~1")
	runGitCmd(t, f.work, "tag", "-a", "v2", "-m", "annotated v2")
	runGitCmd(t, f.work, "checkout", "-b", "topic/nested")
	f.commit(t, "file.txt", "topic\n", "topic change")
	runGitCmd(t, f.work, "checkout", "main")
	f.pushBoth(t, "refs/heads/main", "refs/heads/topic/nested", "refs/tags/v1", "refs/tags/v2")

	return f
}

func (f *serveParityFixture) commit(t *testing.T, name, content, msg string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(f.work, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	runGitCmd(t, f.work, "add", ".")
	runGitCmd(t, f.work, "commit", "-m", msg)
}

// pushBoth force-pushes the given refspecs to both servers so their state
// stays identical.
func (f *serveParityFixture) pushBoth(t *testing.T, refspecs ...string) {
	t.Helper()
	for _, url := range []string{f.goGitURL, f.gitURL} {
		runGitCmd(t, f.work, append([]string{"push", "--force", url}, refspecs...)...)
	}
}

func TestHTTPServeGitParity(t *testing.T) {
	for _, protoVer := range []int{0, 2} {
		t.Run(fmt.Sprintf("ProtocolV%d", protoVer), func(t *testing.T) {
			f := newServeParityFixture(t)

			t.Run("LsRemote", func(t *testing.T) {
				goGitLines := lsRemoteLines(t, protoVer, f.goGitURL)
				gitLines := lsRemoteLines(t, protoVer, f.gitURL)
				if len(goGitLines) != len(gitLines) {
					t.Fatalf("ls-remote line count mismatch:\ngo-git: %v\ngit:    %v", goGitLines, gitLines)
				}
				for i := range gitLines {
					if goGitLines[i] != gitLines[i] {
						t.Fatalf("ls-remote line %d mismatch:\ngo-git: %q\ngit:    %q", i, goGitLines[i], gitLines[i])
					}
				}
			})

			cloneDir := func(t *testing.T, url, name string) string {
				dir := filepath.Join(t.TempDir(), name)
				parityGit(t, "", protoVer, "clone", "--quiet", url, dir)
				return dir
			}

			t.Run("Clone", func(t *testing.T) {
				goGitClone := cloneDir(t, f.goGitURL, "gogit-clone")
				gitClone := cloneDir(t, f.gitURL, "git-clone")

				if got, want := forEachRefLines(t, goGitClone), forEachRefLines(t, gitClone); got != want {
					t.Fatalf("cloned refs mismatch:\ngo-git served:\n%s\ngit served:\n%s", got, want)
				}
				runGitCmd(t, goGitClone, "fsck", "--full", "--strict")

				goGitHead := runGitCmd(t, goGitClone, "symbolic-ref", "HEAD")
				gitHead := runGitCmd(t, gitClone, "symbolic-ref", "HEAD")
				if goGitHead != gitHead {
					t.Fatalf("cloned HEAD mismatch: go-git served %q, git served %q", goGitHead, gitHead)
				}
			})

			t.Run("IncrementalFetch", func(t *testing.T) {
				goGitClone := cloneDir(t, f.goGitURL, "gogit-clone")
				gitClone := cloneDir(t, f.gitURL, "git-clone")

				f.commit(t, "file.txt", "three\n", "c3")
				f.pushBoth(t, "refs/heads/main")

				parityGit(t, goGitClone, protoVer, "fetch", "--quiet", "origin")
				parityGit(t, gitClone, protoVer, "fetch", "--quiet", "origin")

				got := runGitCmd(t, goGitClone, "rev-parse", "refs/remotes/origin/main")
				want := runGitCmd(t, gitClone, "rev-parse", "refs/remotes/origin/main")
				if got != want {
					t.Fatalf("fetched main mismatch: go-git served %q, git served %q", got, want)
				}
			})

			t.Run("PushCreateUpdateDelete", func(t *testing.T) {
				// Create a branch and an annotated tag.
				runGitCmd(t, f.work, "checkout", "-b", "parity-branch")
				f.commit(t, "parity.txt", "parity\n", "parity commit")
				runGitCmd(t, f.work, "tag", "-a", "parity-tag", "-m", "parity tag")
				runGitCmd(t, f.work, "checkout", "main")
				for _, url := range []string{f.goGitURL, f.gitURL} {
					parityGit(t, f.work, protoVer, "push", url, "refs/heads/parity-branch", "refs/tags/parity-tag")
				}
				requireLsRemoteEqual(t, protoVer, f)

				// Non-fast-forward force update.
				runGitCmd(t, f.work, "checkout", "parity-branch")
				runGitCmd(t, f.work, "reset", "--hard", "HEAD~1")
				f.commit(t, "parity.txt", "rewritten\n", "rewritten")
				runGitCmd(t, f.work, "checkout", "main")
				for _, url := range []string{f.goGitURL, f.gitURL} {
					parityGit(t, f.work, protoVer, "push", "--force", url, "refs/heads/parity-branch")
				}
				requireLsRemoteEqual(t, protoVer, f)

				// Delete the branch and the tag.
				for _, url := range []string{f.goGitURL, f.gitURL} {
					parityGit(t, f.work, protoVer, "push", url, ":refs/heads/parity-branch", ":refs/tags/parity-tag")
				}
				requireLsRemoteEqual(t, protoVer, f)
			})

			t.Run("RejectedNonFastForward", func(t *testing.T) {
				// A non-fast-forward push without --force must be rejected by
				// both servers.
				runGitCmd(t, f.work, "checkout", "-b", "reject-branch")
				f.commit(t, "reject.txt", "a\n", "a")
				for _, url := range []string{f.goGitURL, f.gitURL} {
					parityGit(t, f.work, protoVer, "push", url, "refs/heads/reject-branch")
				}
				runGitCmd(t, f.work, "reset", "--hard", "HEAD~1")
				f.commit(t, "reject.txt", "b\n", "b")
				runGitCmd(t, f.work, "checkout", "main")

				goGitErr := tryGit(t, f.work, protoVer, "push", f.goGitURL, "refs/heads/reject-branch")
				gitErr := tryGit(t, f.work, protoVer, "push", f.gitURL, "refs/heads/reject-branch")
				if (goGitErr == nil) != (gitErr == nil) {
					t.Fatalf("non-fast-forward rejection diverged: go-git served err=%v, git served err=%v", goGitErr, gitErr)
				}
				requireLsRemoteEqual(t, protoVer, f)
			})

			t.Run("LargeNegotiationGzip", func(t *testing.T) {
				// A fetch negotiation body over 1KiB makes the git client send
				// the upload-pack request with Content-Encoding: gzip
				// (remote-curl.c post_rpc). git http-backend transparently
				// inflates it; the go-git served endpoint must do the same.
				goGitClone := cloneDir(t, f.goGitURL, "gogit-clone")
				gitClone := cloneDir(t, f.gitURL, "git-clone")

				// Give the client plenty of distinct local-only commits so the
				// negotiation sends large have batches unknown to the server.
				for _, clone := range []string{goGitClone, gitClone} {
					runGitCmd(t, clone, "config", "user.email", "test@example.com")
					runGitCmd(t, clone, "config", "user.name", "Test User")
					runGitCmd(t, clone, "checkout", "--quiet", "-b", "local-only")
					for i := range 96 {
						runGitCmd(t, clone, "commit", "--quiet", "--allow-empty", "-m", fmt.Sprintf("local %d", i))
					}
					runGitCmd(t, clone, "checkout", "--quiet", "main")
				}

				f.commit(t, "file.txt", "gzip trigger\n", "gzip trigger")
				f.pushBoth(t, "refs/heads/main")

				parityGit(t, goGitClone, protoVer, "fetch", "--quiet", "origin", "main")
				parityGit(t, gitClone, protoVer, "fetch", "--quiet", "origin", "main")

				got := runGitCmd(t, goGitClone, "rev-parse", "refs/remotes/origin/main")
				want := runGitCmd(t, gitClone, "rev-parse", "refs/remotes/origin/main")
				if got != want {
					t.Fatalf("gzip negotiation fetch mismatch: go-git served %q, git served %q", got, want)
				}
			})
		})
	}
}

// requireLsRemoteEqual asserts both servers advertise identical refs.
func requireLsRemoteEqual(t *testing.T, protoVer int, f *serveParityFixture) {
	t.Helper()
	goGitLines := strings.Join(lsRemoteLines(t, protoVer, f.goGitURL), "\n")
	gitLines := strings.Join(lsRemoteLines(t, protoVer, f.gitURL), "\n")
	if goGitLines != gitLines {
		t.Fatalf("advertised refs diverged:\ngo-git served:\n%s\ngit served:\n%s", goGitLines, gitLines)
	}
}

// tryGit runs git and returns the error instead of failing the test.
func tryGit(t *testing.T, dir string, protoVer int, args ...string) error {
	t.Helper()
	full := append([]string{"-c", fmt.Sprintf("protocol.version=%d", protoVer)}, args...)
	cmd := exec.CommandContext(t.Context(), "git", full...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git %s: %w\n%s", strings.Join(full, " "), err, out)
	}
	return nil
}
