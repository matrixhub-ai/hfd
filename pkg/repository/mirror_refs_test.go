package repository

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6/osfs"
)

func TestPullMirrorRefs(t *testing.T) {
	forEachGitMode(t, testPullMirrorRefs)
}

func testPullMirrorRefs(t *testing.T, native bool) {
	ctx := context.Background()
	root := t.TempDir()

	upstream := setupMirrorSyncUpstream(t, root)

	mirrorPath := filepath.Join(root, "mirror.git")
	repo, err := InitMirror(ctx, osfs.Default, mirrorPath, upstream)
	if err != nil {
		t.Fatalf("init mirror: %v", err)
	}
	requireGitMode(t, repo, native)

	remoteRefs, err := GetRemoteRefs(ctx, upstream)
	if err != nil {
		t.Fatalf("remote refs: %v", err)
	}

	refsToSync := []string{"refs/heads/main", "refs/heads/feature", "refs/tags/v1"}
	if err := repo.PullMirrorRefs(ctx, upstream, refsToSync, nil); err != nil {
		t.Fatalf("sync mirror refs: %v", err)
	}

	localRefs, err := repo.Refs()
	if err != nil {
		t.Fatalf("local refs: %v", err)
	}

	expected := make(map[string]string, len(refsToSync))
	for _, ref := range refsToSync {
		expected[ref] = remoteRefs[ref]
	}

	for ref, want := range expected {
		if got, ok := localRefs[ref]; !ok {
			t.Fatalf("expected %s to be fetched", ref)
		} else if got != want {
			t.Fatalf("ref %s mismatch: got %s, want %s", ref, got, want)
		}
	}
	if len(localRefs) != len(expected) {
		t.Fatalf("unexpected refs present after sync: got %d, want %d (%v)", len(localRefs), len(expected), localRefs)
	}

	mainHash := localRefs["refs/heads/main"]
	runGit(t, mirrorPath, "update-ref", "refs/heads/stale", mainHash)

	if err := repo.PullMirrorRefs(ctx, upstream, []string{"refs/heads/main", "refs/tags/v1"}, nil); err != nil {
		t.Fatalf("resync mirror refs: %v", err)
	}

	prunedRefs, err := repo.Refs()
	if err != nil {
		t.Fatalf("pruned refs: %v", err)
	}

	expectedAfterPrune := map[string]string{
		"refs/heads/main": remoteRefs["refs/heads/main"],
		"refs/tags/v1":    remoteRefs["refs/tags/v1"],
	}
	for ref, want := range expectedAfterPrune {
		if got, ok := prunedRefs[ref]; !ok {
			t.Fatalf("expected %s to remain after prune", ref)
		} else if got != want {
			t.Fatalf("ref %s mismatch after prune: got %s, want %s", ref, got, want)
		}
	}
	if len(prunedRefs) != len(expectedAfterPrune) {
		t.Fatalf("unexpected refs present after prune: got %d, want %d (%v)", len(prunedRefs), len(expectedAfterPrune), prunedRefs)
	}
}

func TestPullMirrorRefsInvalidRef(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		ctx := t.Context()
		root := t.TempDir()
		bare, _ := buildParityUpstream(t, root)
		upstream := gitLocalRefs(t, bare)
		newMirror := func(t *testing.T) (*Repository, string) {
			t.Helper()
			mirror := filepath.Join(t.TempDir(), "mirror.git")
			repo, err := InitMirror(ctx, osfs.Default, mirror, bare)
			if err != nil {
				t.Fatalf("init mirror: %v", err)
			}
			requireGitMode(t, repo, native)
			return repo, mirror
		}

		for _, tc := range []struct {
			name, ref string
		}{
			{"Newline", "refs/tags/v1\nrefs/heads/topic/nested"},
			{"NUL", "refs/heads/main\x00refs/heads/topic/nested"},
			{"Colon", "refs/tags/v1:refs/heads/main"},
			{"Control", "refs/heads/bad\x01"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				repo, mirror := newMirror(t)
				retained := strings.TrimSpace(gitOut(t, mirror, "hash-object", "-w", os.DevNull))
				runGit(t, mirror, "update-ref", "refs/tags/retained", retained)
				before := gitLocalRefs(t, mirror)
				for name, hash := range upstream {
					if gitTry(t, mirror, "cat-file", "-e", hash) == nil {
						t.Fatalf("fixture already contains %s object %s", name, hash)
					}
				}
				if err := repo.PullMirrorRefs(ctx, bare, []string{"refs/heads/main", tc.ref}, nil); err == nil {
					t.Error("PullMirrorRefs accepted a malformed ref")
				}
				requireSameRefs(t, "mirror refs after rejected pull", gitLocalRefs(t, mirror), before)
				for name, hash := range upstream {
					if gitTry(t, mirror, "cat-file", "-e", hash) == nil {
						t.Errorf("%s object %s was fetched", name, hash)
					}
				}
			})
		}

		// Control: the same call with well-formed nested and annotated-tag refs.
		repo, mirror := newMirror(t)
		refs := []string{"refs/heads/main", "refs/heads/topic/nested", "refs/tags/v2"}
		if err := repo.PullMirrorRefs(ctx, bare, refs, nil); err != nil {
			t.Fatalf("PullMirrorRefs: %v", err)
		}
		want := make(map[string]string, len(refs))
		for _, ref := range refs {
			want[ref] = upstream[ref]
		}
		requireSameRefs(t, "mirror refs after valid pull", gitLocalRefs(t, mirror), want)
	})
}

func setupMirrorSyncUpstream(t *testing.T, root string) string {
	t.Helper()

	upstream := filepath.Join(root, "upstream.git")
	runGit(t, "", "init", "--bare", "--initial-branch=main", upstream)

	work := filepath.Join(root, "work")
	runGit(t, "", "init", "--initial-branch=main", work)
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test User")

	if err := os.WriteFile(filepath.Join(work, "file.txt"), []byte("main\n"), 0o644); err != nil {
		t.Fatalf("write main file: %v", err)
	}

	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", "initial")
	runGit(t, work, "remote", "add", "origin", upstream)
	runGit(t, work, "push", "-u", "origin", "main")
	runGit(t, work, "tag", "v1")
	runGit(t, work, "push", "origin", "v1")

	runGit(t, work, "checkout", "-b", "feature")
	if err := os.WriteFile(filepath.Join(work, "file.txt"), []byte("feature\n"), 0o644); err != nil {
		t.Fatalf("write feature file: %v", err)
	}
	runGit(t, work, "commit", "-am", "feature change")
	runGit(t, work, "push", "-u", "origin", "feature")

	runGit(t, work, "checkout", "main")
	runGit(t, work, "checkout", "-b", "other")
	if err := os.WriteFile(filepath.Join(work, "file.txt"), []byte("other\n"), 0o644); err != nil {
		t.Fatalf("write other file: %v", err)
	}
	runGit(t, work, "commit", "-am", "other change")
	runGit(t, work, "push", "-u", "origin", "other")

	runGit(t, work, "checkout", "main")
	runGit(t, work, "checkout", "-b", "noise/deep")
	if err := os.WriteFile(filepath.Join(work, "file.txt"), []byte("noise\n"), 0o644); err != nil {
		t.Fatalf("write noise file: %v", err)
	}
	runGit(t, work, "commit", "-am", "noise change")
	runGit(t, work, "push", "-u", "origin", "noise/deep")
	runGit(t, work, "tag", "v2")
	runGit(t, work, "push", "origin", "v2")

	return upstream
}

func TestPushMirrorRefs(t *testing.T) {
	forEachGitMode(t, testPushMirrorRefs)
}

func testPushMirrorRefs(t *testing.T, native bool) {
	ctx := context.Background()
	root := t.TempDir()

	// Set up the remote destination (bare repo)
	remote := filepath.Join(root, "remote.git")
	runGit(t, "", "init", "--bare", "--initial-branch=main", remote)

	// Set up the local repo with some commits
	work := filepath.Join(root, "work")
	runGit(t, "", "init", "--initial-branch=main", work)
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test User")

	if err := os.WriteFile(filepath.Join(work, "file.txt"), []byte("content\n"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", "initial")

	// Create a bare local repo and push the commit there
	local := filepath.Join(root, "local.git")
	runGit(t, "", "init", "--bare", "--initial-branch=main", local)
	runGit(t, work, "remote", "add", "local", local)
	runGit(t, work, "push", "-u", "local", "main")

	repo, err := Open(osfs.Default, local)
	if err != nil {
		t.Fatalf("open local repo: %v", err)
	}
	requireGitMode(t, repo, native)

	localRefs, err := repo.Refs()
	if err != nil {
		t.Fatalf("get local refs: %v", err)
	}

	// Push main to the remote destination
	if err := repo.PushMirrorRefs(ctx, remote, []string{"+refs/heads/main:refs/heads/main"}, false, nil); err != nil {
		t.Fatalf("push mirror refs: %v", err)
	}

	// Verify the ref was pushed to the remote
	remoteRefs, err := GetRemoteRefs(ctx, remote)
	if err != nil {
		t.Fatalf("get remote refs: %v", err)
	}

	if got, ok := remoteRefs["refs/heads/main"]; !ok {
		t.Fatalf("expected refs/heads/main to be present in remote")
	} else if got != localRefs["refs/heads/main"] {
		t.Fatalf("refs/heads/main hash mismatch: got %s, want %s", got, localRefs["refs/heads/main"])
	}

	// Push a tag
	runGit(t, work, "tag", "v1")
	runGit(t, work, "push", "local", "v1")

	if err := repo.PushMirrorRefs(ctx, remote, []string{"+refs/tags/v1:refs/tags/v1"}, false, nil); err != nil {
		t.Fatalf("push tag: %v", err)
	}

	remoteRefs, err = GetRemoteRefs(ctx, remote)
	if err != nil {
		t.Fatalf("get remote refs after tag push: %v", err)
	}
	if _, ok := remoteRefs["refs/tags/v1"]; !ok {
		t.Fatalf("expected refs/tags/v1 to be present in remote after push")
	}

	// Delete the tag from remote using empty refspec
	if err := repo.PushMirrorRefs(ctx, remote, []string{":refs/tags/v1"}, false, nil); err != nil {
		t.Fatalf("delete tag from remote: %v", err)
	}

	remoteRefs, err = GetRemoteRefs(ctx, remote)
	if err != nil {
		t.Fatalf("get remote refs after tag delete: %v", err)
	}
	if _, ok := remoteRefs["refs/tags/v1"]; ok {
		t.Fatalf("expected refs/tags/v1 to be absent from remote after delete")
	}
}

func TestPushMirrorRefsPrune(t *testing.T) {
	forEachGitMode(t, testPushMirrorRefsPrune)
}

func testPushMirrorRefsPrune(t *testing.T, native bool) {
	ctx := t.Context()
	root := t.TempDir()

	local, work := buildParityUpstream(t, root)
	repo, err := Open(osfs.Default, local)
	if err != nil {
		t.Fatalf("open local repository: %v", err)
	}
	requireGitMode(t, repo, native)

	wildcard := []string{"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"}

	destA := filepath.Join(root, "dest-a.git")
	runGit(t, "", "init", "--bare", "--initial-branch=main", destA)

	// pushPrune pushes with prune=true and asserts local refs are never
	// touched: go-git's own prune implementation deleted local refs that
	// were missing on the remote.
	pushPrune := func(t *testing.T, dest string, refspecs []string) {
		t.Helper()
		before := gitLocalRefs(t, local)
		if err := repo.PushMirrorRefs(ctx, dest, refspecs, true, nil); err != nil {
			t.Fatalf("PushMirrorRefs with prune: %v", err)
		}
		requireSameRefs(t, "local refs must survive a prune push", gitLocalRefs(t, local), before)
	}

	t.Run("InitialPushToEmptyDestination", func(t *testing.T) {
		// destA is empty here: prune must tolerate an empty remote.
		pushPrune(t, destA, wildcard)
		requireSameRefs(t, "destination after initial push", gitLocalRefs(t, destA), gitLocalRefs(t, local))
	})

	t.Run("RepushKeepsExistingRefs", func(t *testing.T) {
		// Regression: a second pruning push used to delete every remote ref
		// that still existed locally.
		commitFile(t, work, "file.txt", "advance\n", "advance")
		runGit(t, work, "push", "origin", "main")

		pushPrune(t, destA, wildcard)
		requireSameRefs(t, "destination after repush", gitLocalRefs(t, destA), gitLocalRefs(t, local))
	})

	t.Run("MappedWildcardRefspec", func(t *testing.T) {
		destB := filepath.Join(root, "dest-b.git")
		runGit(t, "", "init", "--bare", "--initial-branch=main", destB)

		mapped := []string{"+refs/heads/*:refs/mirror/*"}
		pushPrune(t, destB, mapped)

		localRefs := gitLocalRefs(t, local)
		want := map[string]string{
			"refs/mirror/main":         localRefs["refs/heads/main"],
			"refs/mirror/topic/nested": localRefs["refs/heads/topic/nested"],
		}
		requireSameRefs(t, "mapped destination refs", gitLocalRefs(t, destB), want)

		// A destination ref outside the refspec patterns must survive prune.
		runGit(t, destB, "update-ref", "refs/heads/standalone", localRefs["refs/heads/main"])

		runGit(t, local, "update-ref", "-d", "refs/heads/topic/nested")
		pushPrune(t, destB, mapped)

		want = map[string]string{
			"refs/mirror/main":      localRefs["refs/heads/main"],
			"refs/heads/standalone": localRefs["refs/heads/main"],
		}
		requireSameRefs(t, "mapped destination refs after prune", gitLocalRefs(t, destB), want)
	})

	t.Run("PrunesRefsDeletedLocally", func(t *testing.T) {
		// topic/nested was deleted above; also drop a tag. Both must be
		// pruned from destA while an unrelated destination ref survives.
		runGit(t, local, "update-ref", "-d", "refs/tags/v1")
		keepHash := gitLocalRefs(t, local)["refs/heads/main"]
		runGit(t, destA, "update-ref", "refs/keep/x", keepHash)

		pushPrune(t, destA, wildcard)

		want := gitLocalRefs(t, local)
		want["refs/keep/x"] = keepHash
		requireSameRefs(t, "destination after pruning deleted refs", gitLocalRefs(t, destA), want)
	})
}

// TestPushMirrorRefsNonFastForward pushes an unforced refspec whose
// destination moved ahead; both modes must report the rejection.
func TestPushMirrorRefsNonFastForward(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		ctx := t.Context()
		root := t.TempDir()
		local, work := buildParityUpstream(t, root)
		repo, err := Open(osfs.Default, local)
		if err != nil {
			t.Fatalf("open local repository: %v", err)
		}
		requireGitMode(t, repo, native)
		dest := filepath.Join(root, "dest.git")
		runGit(t, "", "init", "--bare", "--initial-branch=main", dest)
		if err := repo.PushMirrorRefs(ctx, dest, []string{"+refs/heads/main:refs/heads/main"}, false, nil); err != nil {
			t.Fatalf("initial push: %v", err)
		}
		runGit(t, work, "reset", "--hard", "HEAD~1")
		runGit(t, work, "push", "--force", "origin", "main")

		before := gitLocalRefs(t, dest)
		err = repo.PushMirrorRefs(ctx, dest, []string{"refs/heads/main:refs/heads/main"}, false, nil)
		if err == nil || !strings.Contains(err.Error(), "non-fast-forward") {
			t.Fatalf("PushMirrorRefs = %v, want non-fast-forward rejection", err)
		}
		requireSameRefs(t, "destination after rejected push", gitLocalRefs(t, dest), before)
	})
}

// TestPushMirrorRefsEmptySource pushes wildcard refspecs that select no local
// ref: an empty destination is a successful no-op in both modes, a populated
// destination is still pruned, and remote or refspec errors still surface.
func TestPushMirrorRefsEmptySource(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		ctx := t.Context()
		root := t.TempDir()
		local := filepath.Join(root, "local.git")
		runGit(t, "", "init", "--bare", "--initial-branch=main", local)
		repo, err := Open(osfs.Default, local)
		if err != nil {
			t.Fatalf("open local repository: %v", err)
		}
		requireGitMode(t, repo, native)
		populated, _ := buildParityUpstream(t, root)
		wildcard := []string{"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"}
		newDest := func(t *testing.T, name string) string {
			t.Helper()
			dest := filepath.Join(root, name)
			runGit(t, "", "init", "--bare", "--initial-branch=main", dest)
			runGit(t, dest, "config", "http.receivepack", "true")
			return dest
		}
		const user, password = "alice", "s3cret"
		backend := gitHTTPBackend(t, root)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if u, p, ok := r.BasicAuth(); !ok || u != user || p != password {
				w.Header().Set("WWW-Authenticate", `Basic realm="mirror"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			backend.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)
		httpURL := func(dest, userinfo string) string {
			return strings.Replace(srv.URL, "http://", "http://"+userinfo, 1) + "/" + filepath.Base(dest)
		}
		transports := []struct {
			name string
			url  func(dest string) string
		}{
			{"file", func(dest string) string { return dest }},
			{"http", func(dest string) string { return httpURL(dest, user+":"+password+"@") }},
		}

		t.Run("EmptyDestination", func(t *testing.T) {
			for _, tr := range transports {
				for _, prune := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/prune=%t", tr.name, prune), func(t *testing.T) {
						dest := newDest(t, fmt.Sprintf("empty-%s-%t.git", tr.name, prune))
						if err := repo.PushMirrorRefs(ctx, tr.url(dest), wildcard, prune, nil); err != nil {
							t.Fatalf("PushMirrorRefs: %v", err)
						}
						if got := gitLocalRefs(t, dest); len(got) != 0 {
							t.Fatalf("destination refs = %v, want none", got)
						}
					})
				}
			}
		})

		t.Run("UnmatchedWildcard", func(t *testing.T) {
			// A populated source whose refs miss the pattern is the same commandless push.
			src, err := Open(osfs.Default, populated)
			if err != nil {
				t.Fatalf("open populated repository: %v", err)
			}
			dest := newDest(t, "unmatched.git")
			if err := src.PushMirrorRefs(ctx, dest, []string{"+refs/notes/*:refs/notes/*"}, false, nil); err != nil {
				t.Fatalf("PushMirrorRefs: %v", err)
			}
			if got := gitLocalRefs(t, dest); len(got) != 0 {
				t.Fatalf("destination refs = %v, want none", got)
			}
		})

		t.Run("PopulatedDestination", func(t *testing.T) {
			// The destination HEAD stays on unborn main, so deleting these refs is permitted.
			dest := newDest(t, "populated.git")
			runGit(t, populated, "push", dest, "refs/heads/topic/nested:refs/heads/side", "refs/tags/v1:refs/tags/v1")
			before := gitLocalRefs(t, dest)
			if len(before) != 2 {
				t.Fatalf("fixture refs = %v, want side and v1", before)
			}
			if err := repo.PushMirrorRefs(ctx, dest, wildcard, false, nil); err != nil {
				t.Fatalf("PushMirrorRefs without prune: %v", err)
			}
			requireSameRefs(t, "destination without prune", gitLocalRefs(t, dest), before)
			if err := repo.PushMirrorRefs(ctx, dest, []string{":refs/tags/v1"}, false, nil); err != nil {
				t.Fatalf("PushMirrorRefs explicit delete: %v", err)
			}
			requireSameRefs(t, "destination after explicit delete", gitLocalRefs(t, dest), map[string]string{"refs/heads/side": before["refs/heads/side"]})
			if err := repo.PushMirrorRefs(ctx, dest, wildcard, true, nil); err != nil {
				t.Fatalf("PushMirrorRefs with prune: %v", err)
			}
			if got := gitLocalRefs(t, dest); len(got) != 0 {
				t.Fatalf("destination refs after prune = %v, want none", got)
			}
		})

		t.Run("OptionLikeRefspec", func(t *testing.T) {
			src, err := Open(osfs.Default, populated)
			if err != nil {
				t.Fatalf("open populated repository: %v", err)
			}
			dest := newDest(t, "option-like.git")
			runGit(t, populated, "push", dest, "refs/tags/v1:refs/tags/v1")
			before := gitLocalRefs(t, dest)
			unpushed := gitLocalRefs(t, populated)["refs/tags/v2"]
			for _, tc := range []struct {
				name  string
				src   *Repository
				specs []string
			}{
				{"Delete", repo, []string{"--delete", "refs/tags/v1"}},
				{"MixedWithValid", src, []string{"+refs/tags/v2:refs/tags/v2", "--force"}},
			} {
				for _, prune := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/prune=%t", tc.name, prune), func(t *testing.T) {
						if gitTry(t, dest, "cat-file", "-e", unpushed) == nil {
							t.Fatal("fixture already contains the unpushed tag object")
						}
						if err := tc.src.PushMirrorRefs(ctx, dest, tc.specs, prune, nil); err == nil {
							t.Error("option-like refspecs succeeded")
						}
						requireSameRefs(t, "destination after invalid refspecs", gitLocalRefs(t, dest), before)
						if gitTry(t, dest, "cat-file", "-e", unpushed) == nil {
							t.Error("rejected push transferred the unpushed tag object")
						}
					})
				}
			}
		})

		// git refuses these before contacting the remote, whether or not a local ref matches.
		t.Run("InvalidRefspec", func(t *testing.T) {
			src, err := Open(osfs.Default, populated)
			if err != nil {
				t.Fatalf("open populated repository: %v", err)
			}
			// v1 is main's parent, so main's commit is only transferred by a push that got through.
			unpushed := gitLocalRefs(t, populated)["refs/heads/main"]
			sources := []struct {
				name string
				src  *Repository
			}{{"EmptySource", repo}, {"PopulatedSource", src}}
			for i, spec := range []string{
				"+refs/heads/main:refs/heads/bad..name",
				"+refs/heads/main:refs/heads/topic.lock",
				"+refs/heads/main:refs/heads/has space",
				"+refs/heads/main:refs/heads/ctl\x01",
				"+refs/heads/main:refs/heads/main.",
				"+refs/heads/main:refs/heads/.hidden",
				"+refs/heads/bad..name:refs/heads/main",
				"+refs/heads/*:refs/heads/bad..name/*",
				"+refs/notes/bad..name/*:refs/heads/*",
				"+refs/tags/bad..name/*:refs/tags/*",
				"+refs/heads/*:refs/heads/main",
				"+refs/heads/*:refs/heads/*/*",
				":refs/heads/bad..name",
			} {
				for _, tc := range sources {
					t.Run(fmt.Sprintf("%s/%q", tc.name, spec), func(t *testing.T) {
						// side and v1 sit under the destination patterns: a prune computed before validation would delete them.
						dest := newDest(t, fmt.Sprintf("invalid-%d-%s.git", i, tc.name))
						runGit(t, populated, "push", dest, "refs/tags/v1:refs/heads/side", "refs/tags/v1:refs/tags/v1")
						before := gitLocalRefs(t, dest)
						if gitTry(t, dest, "cat-file", "-e", unpushed) == nil {
							t.Fatal("fixture already contains main's commit")
						}
						for _, prune := range []bool{false, true} {
							if err := tc.src.PushMirrorRefs(ctx, dest, []string{spec}, prune, nil); err == nil {
								t.Errorf("prune=%t: invalid refspec succeeded", prune)
							}
						}
						requireSameRefs(t, "destination after invalid refspec", gitLocalRefs(t, dest), before)
						if gitTry(t, dest, "cat-file", "-e", unpushed) == nil {
							t.Error("rejected push transferred main's commit")
						}
					})
				}
			}
		})
		t.Run("ExplicitMissingSource", func(t *testing.T) {
			src, err := Open(osfs.Default, populated)
			if err != nil {
				t.Fatalf("open populated repository: %v", err)
			}
			main := gitLocalRefs(t, populated)["refs/heads/main"]
			for _, tc := range []struct {
				name  string
				src   *Repository
				specs []string
			}{
				{"EmptySource", repo, []string{"+refs/heads/main:refs/heads/main"}},
				// git resolves every explicit source before pushing anything.
				{"BesideValidRef", src, []string{"+refs/heads/main:refs/heads/main", "+refs/heads/nope:refs/heads/nope"}},
			} {
				for _, prune := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/prune=%t", tc.name, prune), func(t *testing.T) {
						dest := newDest(t, fmt.Sprintf("missing-%s-%t.git", tc.name, prune))
						err := tc.src.PushMirrorRefs(ctx, dest, tc.specs, prune, nil)
						if err == nil || !strings.Contains(err.Error(), "does not match any") {
							t.Fatalf("PushMirrorRefs = %v, want git's missing source error", err)
						}
						if got := gitLocalRefs(t, dest); len(got) != 0 {
							t.Errorf("destination refs = %v, want none", got)
						}
						if gitTry(t, dest, "cat-file", "-e", main) == nil {
							t.Error("rejected push transferred the valid source")
						}
					})
				}
			}
		})

		t.Run("RemoteErrors", func(t *testing.T) {
			unauthorized := httpURL(newDest(t, "unauthorized.git"), "")
			for _, prune := range []bool{false, true} {
				for name, url := range map[string]string{"missing": filepath.Join(root, "missing.git"), "unauthorized": unauthorized} {
					if err := repo.PushMirrorRefs(ctx, url, wildcard, prune, nil); err == nil {
						t.Errorf("%s destination, prune=%t: push succeeded", name, prune)
					}
				}
			}
		})

		t.Run("ReadOnlyDestination", func(t *testing.T) {
			dest := newDest(t, "readonly.git")
			readOnly := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.URL.Query().Get("service") == GitReceivePack || strings.HasSuffix(request.URL.Path, "/"+GitReceivePack) {
					http.Error(w, "push denied", http.StatusForbidden)
					return
				}
				backend.ServeHTTP(w, request)
			}))
			t.Cleanup(readOnly.Close)
			destination := readOnly.URL + "/" + filepath.Base(dest)
			if _, err := GetRemoteRefs(ctx, destination); err != nil {
				t.Fatalf("read-only destination must allow fetch: %v", err)
			}
			for _, prune := range []bool{false, true} {
				if err := repo.PushMirrorRefs(ctx, destination, wildcard, prune, nil); err == nil {
					t.Errorf("prune=%t: push to read-only destination succeeded", prune)
				}
			}
		})
	})
}

func TestPushMirrorRefsValidNames(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		ctx := t.Context()
		root := t.TempDir()
		local, _ := buildParityUpstream(t, root)
		repo, err := Open(osfs.Default, local)
		if err != nil {
			t.Fatalf("open local repository: %v", err)
		}
		requireGitMode(t, repo, native)
		main := gitLocalRefs(t, local)["refs/heads/main"]
		for _, name := range []string{"refs/heads/@", "refs/heads/-dash"} {
			runGit(t, local, "update-ref", name, main)
		}
		dest := filepath.Join(root, "dest.git")
		runGit(t, "", "init", "--bare", "--initial-branch=main", dest)

		want := map[string]string{
			"refs/heads/@":            main,
			"refs/heads/-dash":        main,
			"refs/heads/topic/nested": gitLocalRefs(t, local)["refs/heads/topic/nested"],
		}
		specs := []string{":refs/heads/never"}
		for name := range want {
			specs = append(specs, "+"+name+":"+name)
		}
		if err := repo.PushMirrorRefs(ctx, dest, specs, false, nil); err != nil {
			t.Fatalf("PushMirrorRefs: %v", err)
		}
		requireSameRefs(t, "destination after valid names", gitLocalRefs(t, dest), want)

		if err := repo.PushMirrorRefs(ctx, dest, []string{":refs/heads/@", ":refs/heads/-dash"}, false, nil); err != nil {
			t.Fatalf("PushMirrorRefs delete: %v", err)
		}
		delete(want, "refs/heads/@")
		delete(want, "refs/heads/-dash")
		requireSameRefs(t, "destination after deleting valid names", gitLocalRefs(t, dest), want)
	})
}

func TestPushMirrorRefsSourceForms(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		ctx := t.Context()
		local, _ := buildParityUpstream(t, t.TempDir())
		repo, err := Open(osfs.Default, local)
		if err != nil {
			t.Fatal(err)
		}
		requireGitMode(t, repo, native)
		refs := gitLocalRefs(t, local)
		main, v1, v2 := refs["refs/heads/main"], refs["refs/tags/v1"], refs["refs/tags/v2"]
		newPair := func(t *testing.T) (oracle, dest string) {
			t.Helper()
			oracle, dest = filepath.Join(t.TempDir(), "oracle.git"), filepath.Join(t.TempDir(), "dest.git")
			for _, directory := range []string{oracle, dest} {
				runGit(t, "", "init", "--bare", "--initial-branch=main", directory)
				runGit(t, local, "push", directory, "refs/tags/v1:refs/heads/seeded")
			}
			return oracle, dest
		}
		for _, form := range []struct {
			name, spec, destination, hash string
		}{
			{"HEAD", "+HEAD:refs/heads/copied", "refs/heads/copied", main},
			{"ColonlessHEAD", "HEAD", "refs/heads/main", main},
			{"ShortBranch", "+main:refs/heads/copied", "refs/heads/copied", main},
			{"ShortTag", "+v2:refs/tags/copied", "refs/tags/copied", v2},
			{"ObjectID", "+" + main + ":refs/heads/copied", "refs/heads/copied", main},
			{"Revision", "main~1:refs/heads/copied", "refs/heads/copied", v1},
			{"SameName", "refs/heads/main", "refs/heads/main", main},
			{"GuessedBranch", "+main:copied", "refs/heads/copied", main},
			{"GuessedTag", "+v2:copied", "refs/tags/copied", v2},
			{"ForcedDelete", "+:refs/heads/seeded", "refs/heads/seeded", ""},
		} {
			for _, prune := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/prune=%t", form.name, prune), func(t *testing.T) {
					oracle, dest := newPair(t)
					args := []string{"push"}
					if prune {
						args = append(args, "--prune")
					}
					runGit(t, local, append(args, oracle, form.spec)...)
					want := map[string]string{"refs/heads/seeded": v1}
					if form.hash == "" {
						delete(want, form.destination)
					} else {
						want[form.destination] = form.hash
					}
					requireSameRefs(t, "native oracle", gitLocalRefs(t, oracle), want)
					for range 2 {
						if err := repo.PushMirrorRefs(ctx, dest, []string{form.spec}, prune, nil); err != nil {
							t.Fatalf("PushMirrorRefs(%q): %v", form.spec, err)
						}
						requireSameRefs(t, "source form destination", gitLocalRefs(t, dest), want)
					}
				})
			}
		}

		// git rejects these before contacting the remote.
		runGit(t, local, "update-ref", "refs/tags/main", v1)
		for _, tc := range []struct{ name, spec, want string }{
			{"AmbiguousShortName", "+main:refs/heads/copied", "matches more than one"},
			{"ColonlessObjectID", main, "cannot be resolved to branch"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				oracle, dest := newPair(t)
				if gitTry(t, local, "push", oracle, tc.spec) == nil {
					t.Fatalf("git push accepted %q", tc.spec)
				}
				err := repo.PushMirrorRefs(ctx, dest, []string{tc.spec}, false, nil)
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("PushMirrorRefs(%q) = %v, want %q", tc.spec, err, tc.want)
				}
				requireSameRefs(t, "destination after rejected push", gitLocalRefs(t, dest), gitLocalRefs(t, oracle))
				if gitTry(t, dest, "cat-file", "-e", main) == nil {
					t.Error("rejected push transferred main's commit")
				}
			})
		}
	})
}

func TestPushMirrorRefsRefspecShapes(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		ctx := t.Context()
		root := t.TempDir()
		local, _ := buildParityUpstream(t, root)
		repo, err := Open(osfs.Default, local)
		if err != nil {
			t.Fatal(err)
		}
		requireGitMode(t, repo, native)
		v1, v2 := gitLocalRefs(t, local)["refs/tags/v1"], gitLocalRefs(t, local)["refs/tags/v2"]
		// A remote-tracking ref is only a weak match for "main"; the branch must win.
		runGit(t, local, "update-ref", "refs/remotes/main", v1)
		transports := parityTransports(t, root)
		objects := func(dir string) string {
			return gitOut(t, dir, "cat-file", "--batch-all-objects", "--batch-check=%(objectname)")
		}
		for _, shape := range []struct {
			name  string
			specs []string
			prune bool
			seed  []string // refspecs pushed from local into both destinations first
			setup []string // git commands run in local first; later shapes inherit them
			want  string   // substring of the rejection when git refuses the push
			http  bool     // also over smart HTTP
		}{
			{name: "MixedPrune", specs: []string{"+refs/heads/main:refs/heads/alias", "+refs/heads/*:refs/heads/*"}, prune: true, seed: []string{"refs/tags/v1:refs/heads/alias"}},
			{name: "ExplicitOverWildcard", specs: []string{"+refs/tags/v1:refs/heads/main", "+refs/heads/*:refs/heads/*"}, prune: true},
			{name: "SameDestinationTwice", specs: []string{"+refs/heads/main:refs/heads/main", "+refs/heads/*:refs/heads/*"}, prune: true},
			{name: "BareDestinationUniqueTag", specs: []string{"+main:release"}, seed: []string{"refs/tags/v1:refs/tags/release"}, http: true},
			{name: "BareDestinationAmbiguous", specs: []string{"+refs/heads/main:release"}, seed: []string{"refs/tags/v1:refs/tags/release", "refs/tags/v1:refs/heads/release"}, want: "matches more than one"},
			{name: "BareDelete", specs: []string{":release"}, seed: []string{"refs/tags/v1:refs/tags/release"}, want: "remote ref does not exist"},
			{name: "StrongOverWeakSource", specs: []string{"main:refs/heads/copied"}},
			{name: "Matching", specs: []string{":"}, seed: []string{"refs/tags/v1:refs/heads/main", "refs/tags/v1:refs/heads/other"}, http: true},
			{name: "MatchingPrune", specs: []string{":"}, prune: true, seed: []string{"refs/tags/v1:refs/heads/main", "refs/tags/v1:refs/heads/other"}},
			{name: "MatchingNonFastForward", specs: []string{":"}, seed: []string{"refs/heads/topic/nested:refs/heads/main"}, want: "non-fast-forward"},
			{name: "ForcedMatching", specs: []string{"+:"}, seed: []string{"refs/heads/topic/nested:refs/heads/main"}},
			{name: "OneLevelWildcard", specs: []string{"+*:refs/copy/*"}, prune: true},
			{name: "ColonlessWildcard", specs: []string{"+*"}},
			{name: "TagObjectID", specs: []string{v2 + ":refs/tags/copied"}},
			{name: "SymrefChain", specs: []string{"HEAD:refs/heads/copied"}, setup: []string{"symbolic-ref refs/heads/alias refs/heads/main", "symbolic-ref HEAD refs/heads/alias"}},
			{name: "SymrefWildcard", specs: []string{"+refs/heads/*:refs/heads/*"}, prune: true},
			{name: "BrokenSymrefWildcard", specs: []string{"+refs/heads/*:refs/heads/*"}, prune: true, setup: []string{"symbolic-ref refs/heads/alias refs/heads/missing"}, seed: []string{"refs/tags/v1:refs/heads/alias"}},
			{name: "DetachedHEAD", specs: []string{"HEAD:refs/heads/copied"}, setup: []string{"update-ref --no-deref HEAD " + v1}},
			{name: "SymrefCycle", specs: []string{"HEAD:refs/heads/copied"}, setup: []string{"symbolic-ref refs/heads/loop refs/heads/loop", "symbolic-ref HEAD refs/heads/loop"}, want: "does not match any"},
			{name: "SymrefCycleWildcard", specs: []string{"+refs/heads/*:refs/heads/*"}, prune: true, seed: []string{"refs/tags/v1:refs/heads/loop"}},
		} {
			for _, cmd := range shape.setup {
				runGit(t, local, strings.Fields(cmd)...)
			}
			trs := transports[:1]
			if shape.http {
				trs = transports
			}
			for _, tr := range trs {
				t.Run(shape.name+"/"+tr.name, func(t *testing.T) {
					dir := filepath.Join(root, shape.name+"-"+tr.name)
					oracle, dest := filepath.Join(dir, "oracle.git"), filepath.Join(dir, "dest.git")
					for _, directory := range []string{oracle, dest} {
						runGit(t, "", "init", "--bare", "--initial-branch=unused", directory)
						if tr.name == "http" {
							runGit(t, directory, "config", "http.receivepack", "true")
						}
						if len(shape.seed) > 0 {
							runGit(t, local, append([]string{"push", directory}, shape.seed...)...)
						}
					}
					push := []string{"push"}
					if shape.prune {
						push = append(push, "--prune")
					}
					for round := 1; round <= 2; round++ {
						var before string
						if shape.want != "" {
							before = objects(dest)
						}
						oracleErr := gitTry(t, local, slices.Concat(push, []string{oracle}, shape.specs)...)
						err := repo.PushMirrorRefs(ctx, tr.url(dest), shape.specs, shape.prune, nil)
						if (err == nil) != (oracleErr == nil) {
							t.Fatalf("round %d: PushMirrorRefs = %v, git push = %v", round, err, oracleErr)
						}
						if err != nil && (shape.want == "" || !strings.Contains(err.Error(), shape.want)) {
							t.Fatalf("round %d: PushMirrorRefs = %v, want rejection %q", round, err, shape.want)
						}
						requireSameRefs(t, fmt.Sprintf("round %d destination", round), gitLocalRefs(t, dest), gitLocalRefs(t, oracle))
						if err != nil && objects(dest) != before {
							t.Errorf("round %d: rejected push transferred objects", round)
						}
					}
				})
			}
		}
	})
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=safe.bareRepository",
		"GIT_CONFIG_VALUE_0=all",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
}
