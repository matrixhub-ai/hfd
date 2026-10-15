package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"

	"github.com/matrixhub-ai/hfd/pkg/receive"
)

// redirectTestObjectsTree is a bound repository's objects directory when native git wrote every object to the pool.
var redirectTestObjectsTree = []string{"info/", "info/alternates", "pack/"}

// redirectTestInit creates repoPath with dataFS's pool bound and native git serving it.
func redirectTestInit(t *testing.T, dataFS billy.Filesystem, repoPath string) *Repository {
	t.Helper()
	repo := sharedTestInit(t, sharedTestBind(dataFS), repoPath)
	requireGitMode(t, repo, true)
	return repo
}

// redirectTestObjects lists the objects revs reach in the repository at dir.
func redirectTestObjects(t *testing.T, dir string, revs ...string) []string {
	t.Helper()
	var hashes []string
	for line := range strings.Lines(gitOut(t, dir, append([]string{"rev-list", "--objects"}, revs...)...)) {
		hashes = append(hashes, strings.Fields(line)[0])
	}
	return hashes
}

// redirectTestPack packs the objects revs reach in work as a pushing client does, thin when asked.
func redirectTestPack(t *testing.T, work string, thin bool, revs ...string) []byte {
	t.Helper()
	args := []string{"-C", work, "pack-objects", "--stdout", "--revs", "-q"}
	if thin {
		args = append(args, "--thin")
	}
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Env = sharedTestGitEnv(t)
	cmd.Stdin = strings.NewReader(strings.Join(revs, "\n") + "\n")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	pack, err := cmd.Output()
	if err != nil {
		t.Fatalf("pack-objects: %v\n%s", err, stderr.String())
	}
	return pack
}

// redirectTestRefDeltaBases returns the base each REF_DELTA object in pack names.
func redirectTestRefDeltaBases(t *testing.T, pack []byte) []string {
	t.Helper()
	scanner := packfile.NewScanner(bytes.NewReader(pack))
	var bases []string
	for scanner.Scan() {
		data := scanner.Data()
		if data.Section != packfile.ObjectSection {
			continue
		}
		if oh := data.Value().(packfile.ObjectHeader); oh.Type == plumbing.REFDeltaObject {
			bases = append(bases, oh.Reference.String())
		}
	}
	if err := scanner.Error(); err != nil {
		t.Fatalf("scan pack: %v", err)
	}
	return bases
}

// redirectTestPush sends update and pack through receive-pack, stateless or stateful, and requires the update applied.
func redirectTestPush(t *testing.T, repo *Repository, stateless bool, hooks ReceivePackHooks, update string, pack []byte) {
	t.Helper()
	body := append(pktLines(t, update+"\x00report-status\n", ""), pack...)
	var out bytes.Buffer
	var err error
	if stateless {
		err = repo.Stateless(t.Context(), &out, bytes.NewReader(body), GitReceivePack, "", hooks)
	} else {
		err = repo.Serve(t.Context(), struct {
			io.Reader
			io.Writer
		}{bytes.NewReader(body), &out}, GitReceivePack, "", hooks)
	}
	if err != nil {
		t.Fatalf("receive-pack: %v", err)
	}
	if !stateless {
		// A stateful server advertises its refs before the report.
		for {
			l, _, err := pktline.ReadLine(&out)
			if err != nil {
				t.Fatalf("read advertisement: %v", err)
			}
			if l == pktline.Flush {
				break
			}
		}
	}
	if got, want := reportLines(t, &out, false), []string{"unpack ok", "ok " + strings.Fields(update)[2]}; !slices.Equal(got, want) {
		t.Fatalf("receive-pack report = %q, want %q", got, want)
	}
}

// redirectTestRequireInPool checks every hash is a loose object in dataFS's pool.
func redirectTestRequireInPool(t *testing.T, dataFS billy.Filesystem, hashes []string) {
	t.Helper()
	pool := sharedTestPoolObjects(t, dataFS)
	var missing []string
	for _, h := range hashes {
		if !slices.Contains(pool, h) {
			missing = append(missing, h)
		}
	}
	if len(missing) > 0 {
		t.Errorf("%d of %d objects native git wrote are not loose in the pool, %s among them", len(missing), len(hashes), missing[0])
	}
}

// redirectTestRequireOnlyAlternates checks native git left repoPath on dataFS without objects of its own.
func redirectTestRequireOnlyAlternates(t *testing.T, dataFS billy.Filesystem, repoPath string) {
	t.Helper()
	if got := sharedTestTree(t, dataFS, path.Join("/repositories", repoPath, "objects")); !slices.Equal(got, redirectTestObjectsTree) {
		t.Errorf("%s objects = %q, want %q: native git stored objects in the repository instead of the pool", repoPath, got, redirectTestObjectsTree)
	}
}

// redirectTestRecorder returns a git wrapper that saves each invocation's arguments, then its environment, to a new file in dir before running git.
func redirectTestRecorder(t *testing.T) (bin, dir string) {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	dir = t.TempDir()
	bin = filepath.Join(t.TempDir(), "git")
	script := fmt.Sprintf("#!/bin/sh\nf=$(mktemp '%s/call.XXXXXX') || exit 1\n{ printf '%%s\\n' \"$*\"; env; } > \"$f\"\nexec '%s' \"$@\"\n", dir, gitPath)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, dir
}

// redirectTestCall is one recorded git invocation: its arguments and its GIT_* environment.
type redirectTestCall struct {
	args string
	env  map[string]string
}

// redirectTestCalls returns the invocations recorded in dir whose arguments include sub, failing when there is none.
func redirectTestCalls(t *testing.T, dir, sub string) []redirectTestCall {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var calls []redirectTestCall
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		args, env, _ := strings.Cut(string(data), "\n")
		if !slices.Contains(strings.Fields(args), sub) {
			continue
		}
		call := redirectTestCall{args: args, env: map[string]string{}}
		for line := range strings.Lines(env) {
			if k, v, ok := strings.Cut(strings.TrimSuffix(line, "\n"), "="); ok && strings.HasPrefix(k, "GIT_") {
				call.env[k] = v
			}
		}
		calls = append(calls, call)
	}
	if len(calls) == 0 {
		t.Fatalf("hfd ran no git %s", sub)
	}
	return calls
}

// redirectTestUnpackLimits are the keys that decide whether git unpacks a pack loose; a repository's receive. or fetch.unpackLimit outranks transfer.unpackLimit.
var redirectTestUnpackLimits = []string{"transfer.unpackLimit", "receive.unpackLimit", "fetch.unpackLimit"}

// config returns the last value of key among the call's GIT_CONFIG_* entries.
func (c redirectTestCall) config(key string) (value string, ok bool) {
	n, _ := strconv.Atoi(c.env["GIT_CONFIG_COUNT"])
	for i := range n {
		if strings.EqualFold(c.env["GIT_CONFIG_KEY_"+strconv.Itoa(i)], key) {
			value, ok = c.env["GIT_CONFIG_VALUE_"+strconv.Itoa(i)], true
		}
	}
	return value, ok
}

// redirectTestRequireRedirected checks each call writes objects to pool, borrows the repository's own objects at own and unpacks every pack loose.
func redirectTestRequireRedirected(t *testing.T, calls []redirectTestCall, pool, own string) {
	t.Helper()
	for _, c := range calls {
		if got := c.env["GIT_OBJECT_DIRECTORY"]; got != pool {
			t.Errorf("git %s: GIT_OBJECT_DIRECTORY = %q, want the pool %s", c.args, got, pool)
		}
		if got := c.env["GIT_ALTERNATE_OBJECT_DIRECTORIES"]; got != own {
			t.Errorf("git %s: GIT_ALTERNATE_OBJECT_DIRECTORIES = %q, want the repository's objects %s", c.args, got, own)
		}
		for _, key := range redirectTestUnpackLimits {
			if got, ok := c.config(key); got != "2147483647" {
				t.Errorf("git %s: %s = %q (set %t), want 2147483647 so every pack unpacks loose", c.args, key, got, ok)
			}
		}
	}
}

// redirectTestRequireUnset checks no call carries the GIT_* variables named.
func redirectTestRequireUnset(t *testing.T, calls []redirectTestCall, names ...string) {
	t.Helper()
	for _, c := range calls {
		for _, name := range names {
			if got, ok := c.env[name]; ok {
				t.Errorf("git %s: %s = %q, want it unset", c.args, name, got)
			}
		}
	}
}

// redirectTestRequireNotRedirected checks no call moves git's object directory, adds alternates or pins an unpack limit.
func redirectTestRequireNotRedirected(t *testing.T, calls []redirectTestCall) {
	t.Helper()
	redirectTestRequireUnset(t, calls, "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES")
	for _, c := range calls {
		for _, key := range redirectTestUnpackLimits {
			if got, ok := c.config(key); ok {
				t.Errorf("git %s: %s = %q, want it unset", c.args, key, got)
			}
		}
	}
}

// redirectTestLegacy commits content as a.txt to repoPath on dataFS before binding it, so commit old keeps its objects in the repository, then opens it bound and clones it from host.
func redirectTestLegacy(t *testing.T, dataFS billy.Filesystem, host, repoPath, content string) (repo *Repository, old, clone string) {
	t.Helper()
	old = mustCommit(t, sharedTestInit(t, dataFS, path.Join("/repositories", repoPath)), "main", "", CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: []byte(content)})
	repo, err := Open(sharedTestBind(dataFS), repoPath)
	if err != nil {
		t.Fatalf("open %s bound: %v", repoPath, err)
	}
	requireGitMode(t, repo, true)
	clone = filepath.Join(t.TempDir(), "clone")
	sharedTestGit(t, sharedTestGitEnv(t), "", "clone", "--quiet", "--no-local", host, clone)
	return repo, old, clone
}

// redirectTestCommit commits content as name in the work tree at work and returns the new commit.
func redirectTestCommit(t *testing.T, work, name, content string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(work, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	env := sharedTestGitEnv(t)
	sharedTestGit(t, env, work, "add", name)
	sharedTestGit(t, env, work, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--quiet", "-m", name)
	return strings.TrimSpace(sharedTestGit(t, env, work, "rev-parse", "HEAD"))
}

func TestSharedObjectsNativeWrites(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not in PATH")
	}
	setGitBinary(t, gitPath)
	const content = "pushed natively\n"
	work := filepath.Join(t.TempDir(), "work")
	initParityWork(t, work)
	commitFile(t, work, "a.txt", content, "native push")
	commit := strings.TrimSpace(gitOut(t, work, "rev-parse", "HEAD"))
	objects := redirectTestObjects(t, work, commit)
	pack := redirectTestPack(t, work, false, commit)
	create := receive.ZeroHash + " " + commit + " refs/heads/main"

	t.Run("ReceivePackLandsInPool", func(t *testing.T) {
		for _, mode := range []struct {
			name      string
			stateless bool
		}{{"Stateless", true}, {"Stateful", false}} {
			t.Run(mode.name, func(t *testing.T) {
				root := t.TempDir()
				dataFS := osfs.New(root)
				const repoPath = "/org/repo.git"
				host := filepath.Join(root, "repositories", "org", "repo.git")
				repo := redirectTestInit(t, dataFS, repoPath)
				before := len(sharedTestPoolObjects(t, dataFS))
				var pre, post [][]string
				hooks := ReceivePackHooks{
					PreReceive: func(_ context.Context, updates []receive.RefUpdate) error {
						pre = append(pre, updateStrings(updates))
						return nil
					},
					PostReceive: func(_ context.Context, updates []receive.RefUpdate) {
						post = append(post, updateStrings(updates))
					},
				}
				redirectTestPush(t, repo, mode.stateless, hooks, create, pack)

				t.Run("PoolGainsObjects", func(t *testing.T) {
					redirectTestRequireInPool(t, dataFS, objects)
					if gained := len(sharedTestPoolObjects(t, dataFS)) - before; gained != len(objects) {
						t.Errorf("pool gained %d loose objects, want the %d pushed", gained, len(objects))
					}
				})

				t.Run("RepositoryKeepsOnlyAlternates", func(t *testing.T) {
					redirectTestRequireOnlyAlternates(t, dataFS, repoPath)
				})

				t.Run("Fsck", func(t *testing.T) {
					sharedTestGit(t, sharedTestGitEnv(t), host, "fsck", "--full", "--strict")
				})

				t.Run("CloneNoLocal", func(t *testing.T) {
					clone := filepath.Join(t.TempDir(), "clone")
					sharedTestGit(t, sharedTestGitEnv(t), "", "clone", "--quiet", "--no-local", host, clone)
					if got, err := os.ReadFile(filepath.Join(clone, "a.txt")); err != nil || string(got) != content {
						t.Errorf("cloned a.txt = %q, %v; want %q", got, err, content)
					}
				})

				t.Run("HooksStillRun", func(t *testing.T) {
					want := []string{create}
					if len(pre) != 1 || !slices.Equal(pre[0], want) {
						t.Errorf("pre-receive calls = %q, want one with %q", pre, want)
					}
					if len(post) != 1 || !slices.Equal(post[0], want) {
						t.Errorf("post-receive calls = %q, want one with %q", post, want)
					}
					requireSameRefs(t, "refs after push", gitLocalRefs(t, host), map[string]string{"refs/heads/main": commit})
				})
			})
		}
	})

	t.Run("DedupAgainstPool", func(t *testing.T) {
		dataFS := osfs.New(t.TempDir())
		redirectTestPush(t, redirectTestInit(t, dataFS, "/org/first.git"), true, ReceivePackHooks{}, create, pack)
		before := sharedTestFiles(t, dataFS, sharedTestPool)
		const second = "/org/second.git"
		redirectTestPush(t, redirectTestInit(t, dataFS, second), true, ReceivePackHooks{}, create, pack)
		if after := sharedTestFiles(t, dataFS, sharedTestPool); !slices.Equal(after, before) {
			t.Errorf("pushing pooled objects again changed the pool files from %q to %q", before, after)
		}
		redirectTestRequireOnlyAlternates(t, dataFS, second)
	})

	t.Run("ThinPack", func(t *testing.T) {
		root := t.TempDir()
		dataFS := osfs.New(root)
		const repoPath = "/org/thin.git"
		host := filepath.Join(root, "repositories", "org", "thin.git")
		repo := redirectTestInit(t, dataFS, repoPath)
		base := strings.Repeat("a line the pushed version keeps, so its blob deltifies against this one\n", 64)
		old := mustCommit(t, repo, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "data.txt", Content: []byte(base)})
		baseBlob := sharedTestBlobHash([]byte(base))
		if got := sharedTestLocalFiles(t, dataFS, repoPath); !slices.Equal(got, sharedTestOnlyAlternates) {
			t.Fatalf("delta base is not pool-only before the push: repository objects %q", got)
		}
		env := sharedTestGitEnv(t)
		clone := filepath.Join(t.TempDir(), "clone")
		sharedTestGit(t, env, "", "clone", "--quiet", "--no-local", host, clone)
		if err := os.WriteFile(filepath.Join(clone, "data.txt"), []byte(base+"a line only the pushed version has\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		sharedTestGit(t, env, clone, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--quiet", "-am", "thin")
		head := strings.TrimSpace(sharedTestGit(t, env, clone, "rev-parse", "HEAD"))
		thin := redirectTestPack(t, clone, true, head, "^"+old)
		if !slices.Contains(redirectTestRefDeltaBases(t, thin), baseBlob) {
			t.Fatalf("fixture pack is not thin: no delta against the pooled blob %s", baseBlob)
		}
		redirectTestPush(t, repo, true, ReceivePackHooks{}, old+" "+head+" refs/heads/main", thin)

		t.Run("ObjectsLandInPool", func(t *testing.T) {
			redirectTestRequireInPool(t, dataFS, redirectTestObjects(t, clone, head, "^"+old))
		})

		t.Run("RepositoryKeepsOnlyAlternates", func(t *testing.T) {
			redirectTestRequireOnlyAlternates(t, dataFS, repoPath)
		})
	})

	t.Run("FetchLandsInPool", func(t *testing.T) {
		upstream, upstreamWork := buildParityUpstream(t, t.TempDir())
		// More objects than git's default unpack limit of 100, so a fetch left alone keeps them as a pack.
		for i := range 120 {
			if err := os.WriteFile(filepath.Join(upstreamWork, fmt.Sprintf("many-%03d.txt", i)), fmt.Appendf(nil, "file %d\n", i), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		commitFile(t, upstreamWork, "file.txt", "many\n", "many files")
		runGit(t, upstreamWork, "push", "origin", "main")
		want := gitLocalRefs(t, upstream)
		root := t.TempDir()
		dataFS := osfs.New(root)
		const repoPath = "/org/mirror.git"
		host := filepath.Join(root, "repositories", "org", "mirror.git")
		repo := redirectTestInit(t, dataFS, repoPath)
		if err := repo.PullMirrorRefs(t.Context(), upstream, slices.Sorted(maps.Keys(want)), nil); err != nil {
			t.Fatalf("PullMirrorRefs: %v", err)
		}
		requireSameRefs(t, "mirrored refs", gitLocalRefs(t, host), want)

		t.Run("PoolGainsObjects", func(t *testing.T) {
			redirectTestRequireInPool(t, dataFS, redirectTestObjects(t, upstream, "--all"))
		})

		t.Run("RepositoryKeepsOnlyAlternates", func(t *testing.T) {
			redirectTestRequireOnlyAlternates(t, dataFS, repoPath)
		})

		t.Run("Fsck", func(t *testing.T) {
			sharedTestGit(t, sharedTestGitEnv(t), host, "fsck", "--full", "--strict")
		})
	})

	t.Run("EnvironmentRecorded", func(t *testing.T) {
		root := t.TempDir()
		repo := redirectTestInit(t, osfs.New(root), "/org/repo.git")
		mustCommit(t, repo, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: []byte("recorded\n")})
		pool := filepath.Join(root, "git", "sha1", "objects")
		host := filepath.Join(root, "repositories", "org", "repo.git")
		own := filepath.Join(host, "objects")
		record := func(t *testing.T, sub string, op func(ctx context.Context) error) []redirectTestCall {
			t.Helper()
			bin, dir := redirectTestRecorder(t)
			setGitBinary(t, bin)
			if err := op(t.Context()); err != nil {
				t.Fatalf("hfd running git %s: %v", sub, err)
			}
			return redirectTestCalls(t, dir, sub)
		}

		// hfd unpacks a push into the pool itself, so git receive-pack only moves refs and reads the pool through the repository's alternates.
		for _, mode := range []struct {
			name      string
			stateless bool
			ref       string
		}{{"ReceivePackStateless", true, "refs/heads/stateless"}, {"ReceivePackStateful", false, "refs/heads/stateful"}} {
			t.Run(mode.name, func(t *testing.T) {
				update := receive.ZeroHash + " " + commit + " " + mode.ref
				var post [][]string
				hooks := ReceivePackHooks{PostReceive: func(_ context.Context, updates []receive.RefUpdate) {
					post = append(post, updateStrings(updates))
				}}
				redirectTestRequireNotRedirected(t, record(t, "receive-pack", func(context.Context) error {
					redirectTestPush(t, repo, mode.stateless, hooks, update, pack)
					return nil
				}))
				if len(post) != 1 || !slices.Equal(post[0], []string{update}) {
					t.Errorf("post-receive calls = %q, want one with %q", post, update)
				}
				sharedTestGit(t, sharedTestGitEnv(t), host, "fsck", "--full", "--strict")
			})
		}

		t.Run("Fetch", func(t *testing.T) {
			redirectTestRequireRedirected(t, record(t, "fetch", func(ctx context.Context) error {
				return repo.PullMirrorRefs(ctx, work, []string{"refs/heads/main"}, nil)
			}), pool, own)
		})

		t.Run("UploadPack", func(t *testing.T) {
			redirectTestRequireUnset(t, record(t, "upload-pack", func(ctx context.Context) error {
				return errors.Join(
					repo.Stateless(ctx, io.Discard, strings.NewReader("0000"), GitUploadPack, "", ReceivePackHooks{}),
					repo.Serve(ctx, flushOnlyRW{strings.NewReader("0000"), io.Discard}, GitUploadPack, "", ReceivePackHooks{}))
			}), "GIT_OBJECT_DIRECTORY")
		})

		t.Run("AdvertiseRefs", func(t *testing.T) {
			redirectTestRequireUnset(t, record(t, "--advertise-refs", func(ctx context.Context) error {
				return errors.Join(
					repo.AdvertiseRefs(ctx, io.Discard, GitReceivePack, ""),
					repo.AdvertiseRefs(ctx, io.Discard, GitUploadPack, ""))
			}), "GIT_OBJECT_DIRECTORY")
		})

		t.Run("GC", func(t *testing.T) {
			redirectTestRequireUnset(t, record(t, "gc", func(ctx context.Context) error {
				_, err := repo.GC(ctx, time.Time{}, false)
				return err
			}), "GIT_OBJECT_DIRECTORY")
		})

		t.Run("Push", func(t *testing.T) {
			dest := filepath.Join(t.TempDir(), "dest.git")
			runGit(t, "", "init", "--bare", "--initial-branch=main", dest)
			redirectTestRequireUnset(t, record(t, "push", func(ctx context.Context) error {
				return repo.PushMirrorRefs(ctx, dest, []string{"+refs/heads/main:refs/heads/main"}, false, nil)
			}), "GIT_OBJECT_DIRECTORY")
		})
	})

	t.Run("UnboundUnchanged", func(t *testing.T) {
		plain := osfs.New(t.TempDir())
		const repoPath = "/org/repo.git"
		repo := sharedTestInit(t, plain, repoPath)
		requireGitMode(t, repo, true)
		bin, dir := redirectTestRecorder(t)
		setGitBinary(t, bin)
		redirectTestPush(t, repo, true, ReceivePackHooks{}, create, pack)
		if err := repo.PullMirrorRefs(t.Context(), work, []string{"refs/heads/main"}, nil); err != nil {
			t.Fatalf("PullMirrorRefs: %v", err)
		}

		t.Run("ObjectsStayLocal", func(t *testing.T) {
			// Loose or packed: without alternates, every object the repository finds is its own.
			st := sharedTestReopen(t, plain, repoPath).repo.Storer
			for _, h := range objects {
				if err := st.HasEncodedObject(plumbing.NewHash(h)); err != nil {
					t.Errorf("pushed object %s not stored in the repository: %v", h, err)
				}
			}
			if _, err := plain.Stat("/git"); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("stat /git: %v, want no pool on an unbound filesystem", err)
			}
		})

		for _, tc := range []struct{ name, sub string }{
			{"ReceivePackEnvironmentClean", "receive-pack"},
			{"FetchEnvironmentClean", "fetch"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				calls := redirectTestCalls(t, dir, tc.sub)
				redirectTestRequireUnset(t, calls, "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES")
				for _, c := range calls {
					for _, key := range redirectTestUnpackLimits {
						if got, ok := c.config(key); ok {
							t.Errorf("git %s: %s = %q on an unbound repository, want it unset", c.args, key, got)
						}
					}
				}
			})
		}
	})
}

func TestSharedObjectsNativeWritesQuotedPath(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not in PATH")
	}
	for _, tc := range []struct {
		name, dir, escaped string
		optional           bool // some filesystems refuse dir
	}{
		{"Colon", "a:b", "a:b", false},
		{"ColonAndQuote", `a:b"c`, `a:b\"c`, true},
		{"ColonAndBackslash", `a:b\c`, `a:b\\c`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setGitBinary(t, gitPath)
			tmp := t.TempDir()
			root := filepath.Join(tmp, tc.dir)
			if err := os.Mkdir(root, 0o755); err != nil {
				if tc.optional {
					t.Skipf("cannot create data root %q: %v", tc.dir, err)
				}
				t.Fatal(err)
			}
			dataFS := osfs.New(root)
			const repoPath = "/org/repo.git"
			host := filepath.Join(root, "repositories", "org", "repo.git")
			repo, old, clone := redirectTestLegacy(t, dataFS, host, repoPath, "local history\n")
			head := redirectTestCommit(t, clone, "b.txt", "pushed onto local history\n")
			pack := redirectTestPack(t, clone, false, head, "^"+old)
			bin, recorded := redirectTestRecorder(t)
			setGitBinary(t, bin)
			pushed := t.Run("PushOntoLocalHistory", func(t *testing.T) {
				redirectTestPush(t, repo, true, ReceivePackHooks{}, old+" "+head+" refs/heads/main", pack)
			})
			if !pushed {
				return
			}

			t.Run("RefUpdated", func(t *testing.T) {
				requireSameRefs(t, "refs after push", gitLocalRefs(t, host), map[string]string{"refs/heads/main": head})
			})

			t.Run("Fsck", func(t *testing.T) {
				sharedTestGit(t, sharedTestGitEnv(t), host, "fsck", "--full", "--strict")
			})

			t.Run("ObjectsLandInPool", func(t *testing.T) {
				redirectTestRequireInPool(t, dataFS, redirectTestObjects(t, clone, head, "^"+old))
			})

			// Only a fetch still names the repository's objects in git's environment; git reads an entry opening with a double quote as one C-quoted path, so its colons do not split it.
			t.Run("FetchAlternateQuoted", func(t *testing.T) {
				next := redirectTestCommit(t, clone, "c.txt", "fetched onto local history\n")
				err := repo.PullMirrorRefs(t.Context(), clone, []string{"refs/heads/main"}, nil)
				want := `"` + filepath.Join(tmp, tc.escaped, "repositories", "org", "repo.git", "objects") + `"`
				for _, c := range redirectTestCalls(t, recorded, "fetch") {
					if got := c.env["GIT_ALTERNATE_OBJECT_DIRECTORIES"]; got != want {
						t.Errorf("git %s: GIT_ALTERNATE_OBJECT_DIRECTORIES = %s, want %s", c.args, got, want)
					}
				}
				if err != nil {
					t.Fatalf("PullMirrorRefs: %v", err)
				}
				requireSameRefs(t, "refs after fetch", gitLocalRefs(t, host), map[string]string{"refs/heads/main": next})
			})
		})
	}
}

func TestSharedObjectsNativeWritesLegacyLocalHistory(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not in PATH")
	}
	setGitBinary(t, gitPath)
	root := t.TempDir()
	dataFS := osfs.New(root)
	const repoPath = "/org/repo.git"
	host := filepath.Join(root, "repositories", "org", "repo.git")
	base := strings.Repeat("a line the fetched version keeps, so its blob deltifies against this one\n", 64)
	repo, old, clone := redirectTestLegacy(t, dataFS, host, repoPath, base)
	head := redirectTestCommit(t, clone, "b.txt", "pushed onto local history\n")
	redirectTestPush(t, repo, true, ReceivePackHooks{}, old+" "+head+" refs/heads/main", redirectTestPack(t, clone, false, head, "^"+old))

	t.Run("PushLandsInPool", func(t *testing.T) {
		redirectTestRequireInPool(t, dataFS, redirectTestObjects(t, clone, head, "^"+old))
	})

	t.Run("LocalHistoryStaysLocal", func(t *testing.T) {
		for _, h := range redirectTestObjects(t, clone, old) {
			if _, err := dataFS.Stat(path.Join("/repositories", repoPath, "objects", h[:2], h[2:])); err != nil {
				t.Errorf("local history object %s no longer loose in the repository: %v", h, err)
			}
		}
	})

	t.Run("PushFsck", func(t *testing.T) {
		sharedTestGit(t, sharedTestGitEnv(t), host, "fsck", "--full", "--strict")
	})

	t.Run("FetchLandsInPool", func(t *testing.T) {
		next := redirectTestCommit(t, clone, "a.txt", base+"a line only the fetched version has\n")
		// The fetch gets a thin pack deltifying a.txt against the blob only the repository's own objects hold.
		if !slices.Contains(redirectTestRefDeltaBases(t, redirectTestPack(t, clone, true, next, "^"+head)), sharedTestBlobHash([]byte(base))) {
			t.Fatalf("fixture: a thin pack of %s does not deltify against the repository's own a.txt", next)
		}
		if err := repo.PullMirrorRefs(t.Context(), clone, []string{"refs/heads/main"}, nil); err != nil {
			t.Fatalf("PullMirrorRefs: %v", err)
		}
		requireSameRefs(t, "refs after fetch", gitLocalRefs(t, host), map[string]string{"refs/heads/main": next})
		redirectTestRequireInPool(t, dataFS, redirectTestObjects(t, clone, next, "^"+head))
		sharedTestGit(t, sharedTestGitEnv(t), host, "fsck", "--full", "--strict")
	})
}
