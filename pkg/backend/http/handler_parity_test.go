package backend_test

// Parity tests verifying that the smart-HTTP serving (info/refs
// advertisement, upload-pack and receive-pack), whether by go-git or the
// native git binary, behaves identically to the canonical `git http-backend`
// CGI, as observed by a real git client over wire protocol v0 and v2.
// Identical seed repositories are served by both implementations and every
// operation is executed against both, comparing the resulting refs and
// repository state.

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"

	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
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

// goGitFS hides the OS filesystem type so repositories on it are served by go-git.
type goGitFS struct{ billy.Filesystem }

func (f goGitFS) Chroot(path string) (billy.Filesystem, error) {
	sub, err := f.Filesystem.Chroot(path)
	if err != nil {
		return nil, err
	}
	return goGitFS{sub}, nil
}

// newStorage returns storage rooted at root whose repositories are served natively or by go-git.
func newStorage(root string, native bool) *storage.Storage {
	if native {
		return storage.NewStorage(storage.WithRootDir(root))
	}
	return storage.NewStorage(storage.WithFilesystem(goGitFS{osfs.New(root)}))
}

// modeName labels the serving mode of a test.
func modeName(native bool) string {
	if native {
		return "native"
	}
	return "go-git"
}

// forEachMode runs fn with repositories served by go-git and again by the git binary.
func forEachMode(t *testing.T, fn func(t *testing.T, native bool)) {
	for _, native := range []bool{false, true} {
		t.Run(modeName(native), func(t *testing.T) { fn(t, native) })
	}
}

// wantAgent is the agent capability prefix advertised by the serving mode.
func wantAgent(native bool) string {
	if native {
		return "agent=git/"
	}
	return "agent=go-git/"
}

// requireAgent asserts the advertisement of url names the expected server implementation.
func requireAgent(t *testing.T, url string, native bool) {
	t.Helper()
	resp, err := http.Get(url + "/info/refs?service=git-upload-pack")
	if err != nil {
		t.Fatalf("info/refs: %v", err)
	}
	defer resp.Body.Close()
	adv, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read advertisement: %v", err)
	}
	if !bytes.Contains(adv, []byte(wantAgent(native))) {
		t.Fatalf("advertisement lacks %q: %q", wantAgent(native), adv)
	}
}

// serveParityFixture creates one seed repository and serves byte-identical
// copies with the production handler and with git http-backend, recording
// the production handler's receive hooks.
type serveParityFixture struct {
	hfdURL  string // production handler URL for the repository
	hfdRepo string // bare repository behind hfdURL
	storage *storage.Storage
	gitURL  string // git http-backend URL for the repository
	work    string // work repository pushing to both servers

	mu        sync.Mutex
	pre, post [][]receive.RefUpdate
	deny      error // returned by the pre-receive hook when set
	postErr   error // IsForce failures seen by the post-receive hook
}

func newServeParityFixture(t *testing.T, native bool) *serveParityFixture {
	t.Helper()
	root := t.TempDir()

	f := &serveParityFixture{
		hfdRepo: filepath.Join(root, "hfd", "repositories", "repo.git"),
		storage: newStorage(filepath.Join(root, "hfd"), native),
		work:    filepath.Join(root, "work"),
	}
	if err := os.MkdirAll(filepath.Dir(f.hfdRepo), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	runGitCmd(t, "", "init", "--bare", "--initial-branch=main", f.hfdRepo)
	hfdServer := httptest.NewServer(backendhttp.NewHandler(
		backendhttp.WithStorage(f.storage),
		backendhttp.WithPreReceiveHookFunc(func(_ context.Context, _ string, updates []receive.RefUpdate) (bool, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.pre = append(f.pre, updates)
			return f.deny == nil, f.deny
		}),
		backendhttp.WithPostReceiveHookFunc(func(ctx context.Context, _ string, updates []receive.RefUpdate) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.post = append(f.post, updates)
			for _, u := range updates {
				if _, err := u.IsForce(ctx); err != nil {
					f.postErr = errors.Join(f.postErr, err)
				}
			}
			return nil
		}),
	))
	t.Cleanup(hfdServer.Close)
	f.hfdURL = hfdServer.URL + "/repo.git"

	// git http-backend on a parallel root with an identical repository.
	cgiRoot := filepath.Join(root, "cgi")
	gitRepo := filepath.Join(cgiRoot, "repo.git")
	if err := os.MkdirAll(cgiRoot, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	runGitCmd(t, "", "init", "--bare", "--initial-branch=main", gitRepo)
	runGitCmd(t, gitRepo, "config", "http.receivepack", "true")
	f.gitURL = newHTTPBackendServer(t, cgiRoot) + "/repo.git"

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
	f.resetHooks(nil)

	return f
}

// resetHooks clears recorded hook calls and sets the pre-receive verdict.
func (f *serveParityFixture) resetHooks(deny error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pre, f.post, f.deny, f.postErr = nil, nil, deny, nil
}

// hookCalls returns the recorded pre- and post-receive updates as "old new ref" lines per call.
func (f *serveParityFixture) hookCalls() (pre, post [][]string, postErr error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, calls := range []struct {
		src []([]receive.RefUpdate)
		dst *[][]string
	}{{f.pre, &pre}, {f.post, &post}} {
		for _, updates := range calls.src {
			lines := make([]string, len(updates))
			for i, u := range updates {
				lines[i] = u.OldRev() + " " + u.NewRev() + " " + u.RefName()
			}
			*calls.dst = append(*calls.dst, lines)
		}
	}
	return pre, post, f.postErr
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
	for _, url := range []string{f.hfdURL, f.gitURL} {
		runGitCmd(t, f.work, append([]string{"push", "--force", url}, refspecs...)...)
	}
}

func TestHTTPServeGitParity(t *testing.T) {
	for _, mode := range []struct {
		native   bool
		protoVer int
	}{{false, 0}, {false, 1}, {false, 2}, {true, 0}, {true, 1}, {true, 2}} {
		native, protoVer := mode.native, mode.protoVer
		t.Run(fmt.Sprintf("%s/ProtocolV%d", modeName(native), protoVer), func(t *testing.T) {
			f := newServeParityFixture(t, native)
			requireAgent(t, f.hfdURL, native)

			t.Run("LsRemote", func(t *testing.T) {
				hfdLines := lsRemoteLines(t, protoVer, f.hfdURL)
				gitLines := lsRemoteLines(t, protoVer, f.gitURL)
				if len(hfdLines) != len(gitLines) {
					t.Fatalf("ls-remote line count mismatch:\nhfd: %v\ngit: %v", hfdLines, gitLines)
				}
				for i := range gitLines {
					if hfdLines[i] != gitLines[i] {
						t.Fatalf("ls-remote line %d mismatch:\nhfd: %q\ngit: %q", i, hfdLines[i], gitLines[i])
					}
				}
			})

			cloneDir := func(t *testing.T, url, name string) string {
				dir := filepath.Join(t.TempDir(), name)
				parityGit(t, "", protoVer, "clone", "--quiet", url, dir)
				return dir
			}

			t.Run("Clone", func(t *testing.T) {
				hfdClone := cloneDir(t, f.hfdURL, "hfd-clone")
				gitClone := cloneDir(t, f.gitURL, "git-clone")

				if got, want := forEachRefLines(t, hfdClone), forEachRefLines(t, gitClone); got != want {
					t.Fatalf("cloned refs mismatch:\nhfd served:\n%s\ngit served:\n%s", got, want)
				}
				runGitCmd(t, hfdClone, "fsck", "--full", "--strict")

				hfdHead := runGitCmd(t, hfdClone, "symbolic-ref", "HEAD")
				gitHead := runGitCmd(t, gitClone, "symbolic-ref", "HEAD")
				if hfdHead != gitHead {
					t.Fatalf("cloned HEAD mismatch: hfd served %q, git served %q", hfdHead, gitHead)
				}
			})

			t.Run("IncrementalFetch", func(t *testing.T) {
				hfdClone := cloneDir(t, f.hfdURL, "hfd-clone")
				gitClone := cloneDir(t, f.gitURL, "git-clone")

				f.commit(t, "file.txt", "three\n", "c3")
				f.pushBoth(t, "refs/heads/main")

				parityGit(t, hfdClone, protoVer, "fetch", "--quiet", "origin")
				parityGit(t, gitClone, protoVer, "fetch", "--quiet", "origin")

				got := runGitCmd(t, hfdClone, "rev-parse", "refs/remotes/origin/main")
				want := runGitCmd(t, gitClone, "rev-parse", "refs/remotes/origin/main")
				if got != want {
					t.Fatalf("fetched main mismatch: hfd served %q, git served %q", got, want)
				}
			})

			t.Run("PushCreateUpdateDelete", func(t *testing.T) {
				// Create a branch and an annotated tag.
				runGitCmd(t, f.work, "checkout", "-b", "parity-branch")
				f.commit(t, "parity.txt", "parity\n", "parity commit")
				runGitCmd(t, f.work, "tag", "-a", "parity-tag", "-m", "parity tag")
				runGitCmd(t, f.work, "checkout", "main")
				for _, url := range []string{f.hfdURL, f.gitURL} {
					parityGit(t, f.work, protoVer, "push", url, "refs/heads/parity-branch", "refs/tags/parity-tag")
				}
				requireLsRemoteEqual(t, protoVer, f)

				// Non-fast-forward force update.
				runGitCmd(t, f.work, "checkout", "parity-branch")
				runGitCmd(t, f.work, "reset", "--hard", "HEAD~1")
				f.commit(t, "parity.txt", "rewritten\n", "rewritten")
				runGitCmd(t, f.work, "checkout", "main")
				for _, url := range []string{f.hfdURL, f.gitURL} {
					parityGit(t, f.work, protoVer, "push", "--force", url, "refs/heads/parity-branch")
				}
				requireLsRemoteEqual(t, protoVer, f)

				// Delete the branch and the tag.
				for _, url := range []string{f.hfdURL, f.gitURL} {
					parityGit(t, f.work, protoVer, "push", url, ":refs/heads/parity-branch", ":refs/tags/parity-tag")
				}
				requireLsRemoteEqual(t, protoVer, f)
			})

			t.Run("RejectedNonFastForward", func(t *testing.T) {
				// A non-fast-forward push without --force must be rejected by
				// both servers.
				runGitCmd(t, f.work, "checkout", "-b", "reject-branch")
				f.commit(t, "reject.txt", "a\n", "a")
				for _, url := range []string{f.hfdURL, f.gitURL} {
					parityGit(t, f.work, protoVer, "push", url, "refs/heads/reject-branch")
				}
				runGitCmd(t, f.work, "reset", "--hard", "HEAD~1")
				f.commit(t, "reject.txt", "b\n", "b")
				runGitCmd(t, f.work, "checkout", "main")

				hfdErr := tryGit(t, f.work, protoVer, "push", f.hfdURL, "refs/heads/reject-branch")
				gitErr := tryGit(t, f.work, protoVer, "push", f.gitURL, "refs/heads/reject-branch")
				if (hfdErr == nil) != (gitErr == nil) {
					t.Fatalf("non-fast-forward rejection diverged: hfd served err=%v, git served err=%v", hfdErr, gitErr)
				}
				requireLsRemoteEqual(t, protoVer, f)
			})

			t.Run("LargeNegotiationGzip", func(t *testing.T) {
				// A fetch negotiation body over 1KiB makes the git client send
				// the upload-pack request with Content-Encoding: gzip
				// (remote-curl.c post_rpc). git http-backend transparently
				// inflates it; the hfd served endpoint must do the same.
				hfdClone := cloneDir(t, f.hfdURL, "hfd-clone")
				gitClone := cloneDir(t, f.gitURL, "git-clone")

				// Give the client plenty of distinct local-only commits so the
				// negotiation sends large have batches unknown to the server.
				for _, clone := range []string{hfdClone, gitClone} {
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

				parityGit(t, hfdClone, protoVer, "fetch", "--quiet", "origin", "main")
				parityGit(t, gitClone, protoVer, "fetch", "--quiet", "origin", "main")

				got := runGitCmd(t, hfdClone, "rev-parse", "refs/remotes/origin/main")
				want := runGitCmd(t, gitClone, "rev-parse", "refs/remotes/origin/main")
				if got != want {
					t.Fatalf("gzip negotiation fetch mismatch: hfd served %q, git served %q", got, want)
				}
			})

			t.Run("PreReceiveDenied", func(t *testing.T) {
				f.resetHooks(errors.New("branch is frozen"))
				defer f.resetHooks(nil)
				runGitCmd(t, f.work, "checkout", "-b", "denied-branch")
				// An incompressible blob keeps the client sending after the server refused.
				big := make([]byte, 5<<20)
				if _, err := rand.Read(big); err != nil {
					t.Fatal(err)
				}
				f.commit(t, "denied.bin", string(big), "denied")
				runGitCmd(t, f.work, "checkout", "main")
				err := tryGit(t, f.work, protoVer, "push", f.hfdURL, "refs/heads/denied-branch")
				if err == nil || !strings.Contains(err.Error(), "[remote rejected]") || !strings.Contains(err.Error(), "branch is frozen") {
					t.Fatalf("denied push = %v, want a remote rejection carrying the hook reason", err)
				}
				if pre, post, _ := f.hookCalls(); len(pre) != 1 || len(post) != 0 {
					t.Fatalf("pre calls = %v, post calls = %v; want one and none", pre, post)
				}
				// Neither server received the branch, so their refs still agree.
				requireLsRemoteEqual(t, protoVer, f)
			})

			t.Run("PackedPushRefreshesHandle", func(t *testing.T) {
				// Store the pushed objects in a pack the cached go-git handle has not indexed yet.
				runGitCmd(t, f.hfdRepo, "config", "receive.unpackLimit", "1")
				repo, err := repository.Open(f.storage.RepositoriesFS(), "/repo.git")
				if err != nil {
					t.Fatalf("open cached repository: %v", err)
				}
				if _, err := repo.Commits("main", nil); err != nil {
					t.Fatalf("warming the handle: %v", err)
				}
				oldMain := strings.TrimSpace(runGitCmd(t, f.work, "rev-parse", "main"))
				f.commit(t, "packed.txt", "packed\n", "packed")
				newMain := strings.TrimSpace(runGitCmd(t, f.work, "rev-parse", "main"))
				f.resetHooks(nil)
				parityGit(t, f.work, protoVer, "push", f.hfdURL, "refs/heads/main")
				parityGit(t, f.work, protoVer, "push", f.gitURL, "refs/heads/main")
				commits, err := repo.Commits("main", nil)
				if err != nil || len(commits) == 0 || commits[0].Hash().String() != newMain {
					t.Fatalf("cached handle after packed push: commits = %d, err = %v; want head %s", len(commits), err, newMain)
				}
				want := [][]string{{oldMain + " " + newMain + " refs/heads/main"}}
				if pre, post, postErr := f.hookCalls(); !reflect.DeepEqual(pre, want) || !reflect.DeepEqual(post, want) || postErr != nil {
					t.Fatalf("pre = %v, post = %v, post IsForce error = %v; want %v for both and no error", pre, post, postErr, want)
				}
			})

			t.Run("MixedRejection", func(t *testing.T) {
				if !native {
					t.Skip("TODO(go-git): honour receive.denyDeletes; transport.ReceivePack reads no receive.* config and applies the delete")
				}
				// receive.denyDeletes makes git refuse the delete while the create in the same push applies.
				runGitCmd(t, f.hfdRepo, "config", "receive.denyDeletes", "true")
				defer runGitCmd(t, f.hfdRepo, "config", "--unset", "receive.denyDeletes")
				f.resetHooks(nil)
				main := strings.TrimSpace(runGitCmd(t, f.work, "rev-parse", "main"))
				topic := strings.TrimSpace(runGitCmd(t, f.work, "rev-parse", "topic/nested"))
				err := tryGit(t, f.work, protoVer, "push", f.hfdURL, ":refs/heads/topic/nested", "refs/heads/main:refs/heads/mixed-branch")
				if err == nil || !strings.Contains(err.Error(), "deletion prohibited") {
					t.Fatalf("mixed push = %v, want the delete refused by git", err)
				}
				pre, post, _ := f.hookCalls()
				create := receive.ZeroHash + " " + main + " refs/heads/mixed-branch"
				del := topic + " " + receive.ZeroHash + " refs/heads/topic/nested"
				if len(pre) != 1 {
					t.Fatalf("pre calls = %v, want one", pre)
				}
				sort.Strings(pre[0])
				if got, want := pre[0], []string{create, del}; !reflect.DeepEqual(got, want) {
					t.Fatalf("pre-receive commands = %v, want both %v", got, want)
				}
				if want := [][]string{{create}}; !reflect.DeepEqual(post, want) {
					t.Fatalf("post-receive updates = %v, want only the applied create %v", post, want)
				}
				refs := strings.Join(lsRemoteLines(t, protoVer, f.hfdURL), "\n")
				if !strings.Contains(refs, "refs/heads/topic/nested") || !strings.Contains(refs, "refs/heads/mixed-branch") {
					t.Fatalf("refs after mixed push:\n%s", refs)
				}
			})
		})
	}
}

// requireLsRemoteEqual asserts both servers advertise identical refs.
func requireLsRemoteEqual(t *testing.T, protoVer int, f *serveParityFixture) {
	t.Helper()
	hfdLines := strings.Join(lsRemoteLines(t, protoVer, f.hfdURL), "\n")
	gitLines := strings.Join(lsRemoteLines(t, protoVer, f.gitURL), "\n")
	if hfdLines != gitLines {
		t.Fatalf("advertised refs diverged:\nhfd served:\n%s\ngit served:\n%s", hfdLines, gitLines)
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
