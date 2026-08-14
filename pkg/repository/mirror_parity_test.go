package repository

// Parity tests verifying that the go-git based mirror operations behave
// identically to the git subprocess commands they replaced:
//
//	GetRemoteDefaultBranch  <->  git ls-remote --symref <url> HEAD
//	GetRemoteRefs           <->  git ls-remote --refs <url>
//	PullMirrorRefs          <->  git fetch <url> --no-tags --progress +ref:ref... (+ prune)
//	PushMirrorRefs          <->  git push [--prune] --no-tags --progress <url> <refspecs>
//
// Every scenario runs over two transports: the file transport and real
// smart-HTTP served by `git http-backend` (the canonical server
// implementation). Repositories written by go-git are additionally
// cross-checked with the git binary (for-each-ref, fsck, cat-file) to prove
// on-disk compatibility in both directions.

import (
	"bytes"
	"fmt"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6/osfs"
)

// gitOut runs git and returns its stdout, failing the test on error.
func gitOut(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=safe.bareRepository",
		"GIT_CONFIG_VALUE_0=all",
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, stderr.String())
	}
	return string(out)
}

// gitTry runs git and returns an error instead of failing the test.
func gitTry(t *testing.T, dir string, args ...string) error {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git %s: %w\n%s", strings.Join(args, " "), err, out)
	}
	return nil
}

// gitLsRemoteRefs returns `git ls-remote --refs <url>` as a map of ref name to hash.
func gitLsRemoteRefs(t *testing.T, url string) map[string]string {
	t.Helper()
	refs := make(map[string]string)
	for line := range strings.SplitSeq(gitOut(t, "", "ls-remote", "--refs", url), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		hash, name, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("unexpected ls-remote line: %q", line)
		}
		refs[name] = hash
	}
	return refs
}

// gitSymrefHead returns the branch `git ls-remote --symref <url> HEAD` reports
// for HEAD, or "" if git does not report a symref.
func gitSymrefHead(t *testing.T, url string) string {
	t.Helper()
	for line := range strings.SplitSeq(gitOut(t, "", "ls-remote", "--symref", url, "HEAD"), "\n") {
		if ref, ok := strings.CutSuffix(line, "\tHEAD"); ok {
			if branch, ok := strings.CutPrefix(ref, "ref: refs/heads/"); ok {
				return branch
			}
		}
	}
	return ""
}

// gitCloneHeadBranch clones url with the git binary and returns the branch
// HEAD points at, i.e. the default branch a real git client ends up on.
func gitCloneHeadBranch(t *testing.T, url string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "head-clone")
	runGit(t, "", "clone", "--quiet", url, dir)
	head, err := os.ReadFile(filepath.Join(dir, ".git", "HEAD"))
	if err != nil {
		t.Fatalf("read cloned HEAD: %v", err)
	}
	return strings.TrimPrefix(strings.TrimSpace(string(head)), "ref: refs/heads/")
}

// gitLocalRefs returns all refs of a local repository as seen by the git
// binary via for-each-ref.
func gitLocalRefs(t *testing.T, repoPath string) map[string]string {
	t.Helper()
	refs := make(map[string]string)
	for line := range strings.SplitSeq(gitOut(t, repoPath, "for-each-ref", "--format=%(objectname) %(refname)"), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		hash, name, ok := strings.Cut(line, " ")
		if !ok {
			t.Fatalf("unexpected for-each-ref line: %q", line)
		}
		refs[name] = hash
	}
	return refs
}

// gitFsck verifies object connectivity and integrity with the git binary.
func gitFsck(t *testing.T, repoPath string) {
	t.Helper()
	runGit(t, repoPath, "fsck", "--full", "--strict")
}

// requireSameRefs asserts two ref maps are identical.
func requireSameRefs(t *testing.T, label string, got, want map[string]string) {
	t.Helper()
	for name, wantHash := range want {
		gotHash, ok := got[name]
		if !ok {
			t.Fatalf("%s: missing ref %s (want %s); got %v", label, name, wantHash, got)
		}
		if gotHash != wantHash {
			t.Fatalf("%s: ref %s = %s, want %s", label, name, gotHash, wantHash)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("%s: extra refs present: got %v, want %v", label, got, want)
	}
}

// legacyPullMirrorRefs reproduces the pre-go-git PullMirrorRefs implementation
// with git subprocesses: fetch with explicit forced refspecs, then prune
// local refs that are not in the desired set.
func legacyPullMirrorRefs(t *testing.T, repoPath, sourceURL string, refs []string) {
	t.Helper()
	args := []string{"fetch", sourceURL, "--no-tags", "--progress"}
	for _, ref := range refs {
		args = append(args, "+"+ref+":"+ref)
	}
	runGit(t, repoPath, args...)

	desired := make(map[string]bool, len(refs))
	for _, ref := range refs {
		desired[ref] = true
	}
	for name := range gitLocalRefs(t, repoPath) {
		if !desired[name] {
			runGit(t, repoPath, "update-ref", "-d", name)
		}
	}
}

// legacyPushMirrorRefs reproduces the pre-go-git PushMirrorRefs implementation
// with git subprocesses.
func legacyPushMirrorRefs(t *testing.T, repoPath, destURL string, refspecs []string, prune bool) {
	t.Helper()
	args := []string{"push"}
	if prune {
		args = append(args, "--prune")
	}
	args = append(args, "--no-tags", "--progress", destURL)
	args = append(args, refspecs...)
	runGit(t, repoPath, args...)
}

// newGitHTTPBackend serves every bare repository under root over smart HTTP
// using the canonical `git http-backend` CGI, and returns the base URL.
func newGitHTTPBackend(t *testing.T, root string) string {
	t.Helper()
	execPath := strings.TrimSpace(gitOut(t, "", "--exec-path"))
	backend := filepath.Join(execPath, "git-http-backend")
	if _, err := os.Stat(backend); err != nil {
		t.Skipf("git-http-backend not available: %v", err)
	}
	srv := httptest.NewServer(&cgi.Handler{
		Path:       backend,
		InheritEnv: []string{"PATH"},
		Env: []string{
			"GIT_PROJECT_ROOT=" + root,
			"GIT_HTTP_EXPORT_ALL=1",
		},
	})
	t.Cleanup(srv.Close)
	return srv.URL
}

// parityTransport maps a bare repository path under root to a URL for one transport.
type parityTransport struct {
	name string
	url  func(bareRepoPath string) string
}

// parityTransports returns the file and smart-HTTP transports for repositories under root.
// Bare repositories that should accept pushes over HTTP need http.receivepack=true.
func parityTransports(t *testing.T, root string) []parityTransport {
	t.Helper()
	base := newGitHTTPBackend(t, root)
	return []parityTransport{
		{name: "file", url: func(p string) string { return p }},
		{name: "http", url: func(p string) string {
			rel, err := filepath.Rel(root, p)
			if err != nil {
				t.Fatalf("rel %s: %v", p, err)
			}
			return base + "/" + filepath.ToSlash(rel)
		}},
	}
}

// initParityWork creates a non-bare work repository for building fixtures.
func initParityWork(t *testing.T, path string) {
	t.Helper()
	runGit(t, "", "init", "--initial-branch=main", path)
	runGit(t, path, "config", "user.email", "test@example.com")
	runGit(t, path, "config", "user.name", "Test User")
}

// commitFile writes content to name in work and commits it on the current branch.
func commitFile(t *testing.T, work, name, content, msg string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(work, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", msg)
}

// buildParityUpstream creates a bare upstream with branches main and
// topic/nested, a lightweight tag v1 and an annotated tag v2, plus the work
// clone used to evolve it.
func buildParityUpstream(t *testing.T, root string) (bare, work string) {
	t.Helper()
	bare = filepath.Join(root, "upstream.git")
	runGit(t, "", "init", "--bare", "--initial-branch=main", bare)

	work = filepath.Join(root, "work")
	initParityWork(t, work)
	commitFile(t, work, "file.txt", "one\n", "c1")
	commitFile(t, work, "file.txt", "two\n", "c2")
	runGit(t, work, "remote", "add", "origin", bare)
	runGit(t, work, "push", "-u", "origin", "main")

	runGit(t, work, "tag", "v1", "HEAD~1")
	runGit(t, work, "tag", "-a", "v2", "-m", "annotated v2")
	runGit(t, work, "push", "origin", "v1", "v2")

	runGit(t, work, "checkout", "-b", "topic/nested")
	commitFile(t, work, "file.txt", "topic\n", "topic change")
	runGit(t, work, "push", "-u", "origin", "topic/nested")
	runGit(t, work, "checkout", "main")

	return bare, work
}

func TestGitParityGetRemoteRefs(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	bare, _ := buildParityUpstream(t, root)

	empty := filepath.Join(root, "empty.git")
	runGit(t, "", "init", "--bare", "--initial-branch=main", empty)

	for _, tr := range parityTransports(t, root) {
		t.Run(tr.name, func(t *testing.T) {
			url := tr.url(bare)
			got, err := GetRemoteRefs(ctx, url)
			if err != nil {
				t.Fatalf("GetRemoteRefs: %v", err)
			}
			requireSameRefs(t, "GetRemoteRefs vs git ls-remote --refs", got, gitLsRemoteRefs(t, url))

			// The fixture must exercise annotated tags: ls-remote --refs
			// reports the tag object, not the peeled commit, and no ^{} entries.
			if typ := strings.TrimSpace(gitOut(t, bare, "cat-file", "-t", got["refs/tags/v2"])); typ != "tag" {
				t.Fatalf("refs/tags/v2 should resolve to a tag object, got %q", typ)
			}

			t.Run("EmptyRepository", func(t *testing.T) {
				url := tr.url(empty)
				got, err := GetRemoteRefs(ctx, url)
				if err != nil {
					t.Fatalf("GetRemoteRefs on empty repository: %v", err)
				}
				requireSameRefs(t, "empty repository refs", got, gitLsRemoteRefs(t, url))
			})

			t.Run("MissingRepository", func(t *testing.T) {
				url := tr.url(filepath.Join(root, "does-not-exist.git"))
				if _, err := GetRemoteRefs(ctx, url); err == nil {
					t.Fatalf("GetRemoteRefs should fail for missing repository, like git ls-remote")
				}
				if err := gitTry(t, "", "ls-remote", "--refs", url); err == nil {
					t.Fatalf("git ls-remote unexpectedly succeeded for missing repository")
				}
			})
		})
	}
}

func TestGitParityGetRemoteDefaultBranch(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()

	// A repository whose default branch differs from any client-side default.
	bare := filepath.Join(root, "custom.git")
	runGit(t, "", "init", "--bare", "--initial-branch=parity-branch", bare)
	work := filepath.Join(root, "work")
	initParityWork(t, work)
	runGit(t, work, "checkout", "-b", "parity-branch")
	commitFile(t, work, "file.txt", "content\n", "c1")
	// A second branch that sorts before the default one, to catch
	// implementations that guess instead of honoring the HEAD symref.
	runGit(t, work, "checkout", "-b", "aaa-first")
	commitFile(t, work, "file.txt", "other\n", "c2")
	runGit(t, work, "remote", "add", "origin", bare)
	runGit(t, work, "push", "origin", "parity-branch", "aaa-first")

	emptyBare := filepath.Join(root, "empty.git")
	runGit(t, "", "init", "--bare", "--initial-branch=unborn-branch", emptyBare)

	// A repository whose HEAD points at an unborn branch while other refs
	// exist, as seen on hub repositories before the default branch is born.
	unbornBare := filepath.Join(root, "unborn.git")
	runGit(t, "", "init", "--bare", "--initial-branch=unborn-branch", unbornBare)
	unbornWork := filepath.Join(root, "unborn-work")
	initParityWork(t, unbornWork)
	runGit(t, unbornWork, "checkout", "-b", "side")
	commitFile(t, unbornWork, "file.txt", "side\n", "side commit")
	runGit(t, unbornWork, "remote", "add", "origin", unbornBare)
	runGit(t, unbornWork, "push", "origin", "side")

	for _, tr := range parityTransports(t, root) {
		t.Run(tr.name, func(t *testing.T) {
			url := tr.url(bare)
			got, err := GetRemoteDefaultBranch(ctx, url)
			if err != nil {
				t.Fatalf("GetRemoteDefaultBranch: %v", err)
			}
			if want := gitSymrefHead(t, url); got != want {
				t.Fatalf("default branch = %q, git ls-remote --symref reports %q", got, want)
			}
			if want := gitCloneHeadBranch(t, url); got != want {
				t.Fatalf("default branch = %q, git clone checks out %q", got, want)
			}

			t.Run("EmptyRepository", func(t *testing.T) {
				url := tr.url(emptyBare)
				// Parity: `git ls-remote --symref` reports no symref for an
				// empty repository, so the legacy subprocess implementation
				// failed here too. The error must be preserved, not a bogus
				// branch invented.
				if want := gitSymrefHead(t, url); want != "" {
					t.Fatalf("expected git ls-remote --symref to report no symref, got %q", want)
				}
				if got, err := GetRemoteDefaultBranch(ctx, url); err == nil {
					// Resolving the unborn HEAD is an acceptable improvement
					// over the legacy behavior, but only if it matches what a
					// real git clone checks out.
					if want := gitCloneHeadBranch(t, url); got != want {
						t.Fatalf("default branch = %q, git clone checks out %q", got, want)
					}
				}
			})

			t.Run("UnbornHEADWithOtherRefs", func(t *testing.T) {
				url := tr.url(unbornBare)
				// Parity: `git ls-remote --symref` reports no symref when HEAD
				// is unborn, so the legacy subprocess implementation errored
				// here as well. Failing is acceptable; resolving a branch is
				// an improvement but must match what `git clone` checks out.
				if want := gitSymrefHead(t, url); want != "" {
					t.Fatalf("expected git ls-remote --symref to report no symref, got %q", want)
				}
				if got, err := GetRemoteDefaultBranch(ctx, url); err == nil {
					if want := gitCloneHeadBranch(t, url); got != want {
						t.Fatalf("default branch = %q, git clone checks out %q", got, want)
					}
				}
			})
		})
	}
}

func TestGitParityPullMirrorRefs(t *testing.T) {
	ctx := t.Context()

	type stage struct {
		name   string
		mutate func(t *testing.T, work string)
		refs   []string
	}
	allRefs := []string{"refs/heads/main", "refs/heads/topic/nested", "refs/tags/v1", "refs/tags/v2"}
	stages := []stage{
		{
			name:   "InitialSync",
			mutate: func(t *testing.T, work string) {},
			refs:   allRefs,
		},
		{
			name: "FastForward",
			mutate: func(t *testing.T, work string) {
				commitFile(t, work, "file.txt", "three\n", "c3")
				runGit(t, work, "push", "origin", "main")
			},
			refs: allRefs,
		},
		{
			name: "ForcedUpdate",
			mutate: func(t *testing.T, work string) {
				// Rewind main and move the annotated tag: both are
				// non-fast-forward updates requiring forced refspecs.
				runGit(t, work, "reset", "--hard", "HEAD~2")
				commitFile(t, work, "file.txt", "rewritten\n", "rewritten")
				runGit(t, work, "push", "--force", "origin", "main")
				runGit(t, work, "tag", "-f", "-a", "v2", "-m", "moved v2")
				runGit(t, work, "push", "--force", "origin", "v2")
			},
			refs: allRefs,
		},
		{
			name: "PruneUnlistedRefs",
			mutate: func(t *testing.T, work string) {
				runGit(t, work, "push", "origin", "--delete", "topic/nested")
			},
			// topic/nested is gone upstream and v1 dropped by the filter:
			// both must be pruned locally.
			refs: []string{"refs/heads/main", "refs/tags/v2"},
		},
	}

	for _, trName := range []string{"file", "http"} {
		t.Run(trName, func(t *testing.T) {
			root := t.TempDir()
			bare, work := buildParityUpstream(t, root)
			var tr parityTransport
			for _, cand := range parityTransports(t, root) {
				if cand.name == trName {
					tr = cand
				}
			}
			sourceURL := tr.url(bare)

			goGitMirror := filepath.Join(root, "gogit-mirror.git")
			repo, err := InitMirror(ctx, osfs.Default, goGitMirror, sourceURL)
			if err != nil {
				t.Fatalf("init go-git mirror: %v", err)
			}
			legacyMirror := filepath.Join(root, "legacy-mirror.git")
			runGit(t, "", "init", "--bare", "--initial-branch=main", legacyMirror)

			for _, st := range stages {
				t.Run(st.name, func(t *testing.T) {
					st.mutate(t, work)

					if err := repo.PullMirrorRefs(ctx, sourceURL, st.refs, nil); err != nil {
						t.Fatalf("PullMirrorRefs: %v", err)
					}
					legacyPullMirrorRefs(t, legacyMirror, sourceURL, st.refs)

					want := gitLocalRefs(t, legacyMirror)
					requireSameRefs(t, "go-git mirror vs git fetch mirror (git view)", gitLocalRefs(t, goGitMirror), want)

					goGitView, err := repo.Refs()
					if err != nil {
						t.Fatalf("Refs: %v", err)
					}
					requireSameRefs(t, "go-git Refs() vs git for-each-ref", goGitView, want)

					gitFsck(t, goGitMirror)
					for name, hash := range want {
						// Every synced object must be fully readable by the git binary.
						gitOut(t, goGitMirror, "rev-list", "--objects", hash)
						if strings.HasPrefix(name, "refs/tags/v2") {
							if typ := strings.TrimSpace(gitOut(t, goGitMirror, "cat-file", "-t", hash)); typ != "tag" {
								t.Fatalf("annotated tag %s fetched as %q, want tag object", name, typ)
							}
						}
					}
				})
			}

			t.Run("MissingRemoteRef", func(t *testing.T) {
				refs := []string{"refs/heads/main", "refs/heads/gone"}
				err := repo.PullMirrorRefs(ctx, sourceURL, refs, nil)
				legacyErr := func() error {
					args := []string{"fetch", sourceURL, "--no-tags", "--progress"}
					for _, ref := range refs {
						args = append(args, "+"+ref+":"+ref)
					}
					return gitTry(t, legacyMirror, args...)
				}()
				if (err == nil) != (legacyErr == nil) {
					t.Fatalf("missing ref behavior diverged: go-git err=%v, git err=%v", err, legacyErr)
				}
			})
		})
	}
}

func TestGitParityPushMirrorRefs(t *testing.T) {
	ctx := t.Context()

	// The production wildcard refspecs used by mirror push with prune.
	wildcard := []string{"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"}

	type stage struct {
		name     string
		mutate   func(t *testing.T, work, localBare string)
		refspecs []string
		prune    bool
	}
	stages := []stage{
		{
			name:     "InitialWildcardPush",
			mutate:   func(t *testing.T, work, localBare string) {},
			refspecs: wildcard,
			prune:    true,
		},
		{
			name: "FastForward",
			mutate: func(t *testing.T, work, localBare string) {
				commitFile(t, work, "file.txt", "three\n", "c3")
				runGit(t, work, "push", "origin", "main")
			},
			refspecs: wildcard,
			prune:    true,
		},
		{
			name: "ForcedUpdate",
			mutate: func(t *testing.T, work, localBare string) {
				runGit(t, work, "reset", "--hard", "HEAD~2")
				commitFile(t, work, "file.txt", "rewritten\n", "rewritten")
				runGit(t, work, "push", "--force", "origin", "main")
				runGit(t, work, "tag", "-f", "-a", "v2", "-m", "moved v2")
				runGit(t, work, "push", "--force", "origin", "v2")
			},
			refspecs: wildcard,
			prune:    true,
		},
		{
			name: "PruneDeletedBranch",
			mutate: func(t *testing.T, work, localBare string) {
				runGit(t, work, "push", "origin", "--delete", "topic/nested")
			},
			refspecs: wildcard,
			prune:    true,
		},
		{
			name:     "ExplicitDelete",
			mutate:   func(t *testing.T, work, localBare string) {},
			refspecs: []string{":refs/tags/v1"},
			prune:    false,
		},
		{
			name: "ExplicitRefspecs",
			mutate: func(t *testing.T, work, localBare string) {
				commitFile(t, work, "file.txt", "explicit\n", "explicit")
				runGit(t, work, "push", "origin", "main")
			},
			refspecs: []string{"+refs/heads/main:refs/heads/main", "+refs/tags/v2:refs/tags/v2"},
			prune:    false,
		},
	}

	for _, trName := range []string{"file", "http"} {
		t.Run(trName, func(t *testing.T) {
			root := t.TempDir()
			// The local repository whose refs are mirrored outward; the
			// upstream fixture doubles as that local repository here.
			localBare, work := buildParityUpstream(t, root)
			repo, err := Open(osfs.Default, localBare)
			if err != nil {
				t.Fatalf("open local repository: %v", err)
			}

			goGitDest := filepath.Join(root, "gogit-dest.git")
			legacyDest := filepath.Join(root, "legacy-dest.git")
			for _, dest := range []string{goGitDest, legacyDest} {
				runGit(t, "", "init", "--bare", "--initial-branch=main", dest)
				// Allow anonymous pushes through git http-backend.
				runGit(t, dest, "config", "http.receivepack", "true")
			}

			var tr parityTransport
			for _, cand := range parityTransports(t, root) {
				if cand.name == trName {
					tr = cand
				}
			}

			for _, st := range stages {
				t.Run(st.name, func(t *testing.T) {
					st.mutate(t, work, localBare)

					if err := repo.PushMirrorRefs(ctx, tr.url(goGitDest), st.refspecs, st.prune, nil); err != nil {
						t.Fatalf("PushMirrorRefs: %v", err)
					}
					legacyPushMirrorRefs(t, localBare, tr.url(legacyDest), st.refspecs, st.prune)

					want := gitLocalRefs(t, legacyDest)
					requireSameRefs(t, "go-git push dest vs git push dest", gitLocalRefs(t, goGitDest), want)

					gitFsck(t, goGitDest)
					if hash, ok := want["refs/tags/v2"]; ok {
						if typ := strings.TrimSpace(gitOut(t, goGitDest, "cat-file", "-t", hash)); typ != "tag" {
							t.Fatalf("annotated tag pushed as %q, want tag object", typ)
						}
					}
				})
			}
		})
	}
}
