package repository

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
)

// resolveTestFixture is a, whose main runs c1 → c2 beside refs of every kind, b holding cb alone, and d holding cd behind a detached HEAD only.
type resolveTestFixture struct {
	a, b, d        *reachTestRepo
	c1, c2, cb, cd plumbing.Hash
	hexBranch      string // names a branch on c1 and prefixes c2
	ambiguous      string // the four hex digits two commits a reaches share
}

func newResolveTestFixture(t *testing.T, fs billy.Filesystem) *resolveTestFixture {
	t.Helper()
	f := &resolveTestFixture{
		a: newReachTestRepo(t, fs, "/org/a.git"),
		b: newReachTestRepo(t, fs, "/org/b.git"),
		d: newReachTestRepo(t, fs, "/org/d.git"),
	}
	a := f.a
	f.c1 = a.commit(resolveTestReadme(a, "one\n"))
	tree := resolveTestReadme(a, "two\n")
	f.c2 = a.commit(tree, f.c1)
	a.ref(reachMain, f.c2)
	a.ref("refs/heads/side", f.c1)
	// A symbolic branch whose target runs through main's ref file.
	if err := a.repo.repo.Storer.SetReference(plumbing.NewSymbolicReference("refs/heads/through-main", "refs/heads/main/x")); err != nil {
		t.Fatalf("set refs/heads/through-main: %v", err)
	}
	f.hexBranch = f.c2.String()[:6]
	a.ref(plumbing.NewBranchReferenceName(f.hexBranch), f.c1)
	// The branch loses to the tag of the same name, as in git rev-parse.
	a.ref("refs/tags/lw", f.c2)
	a.ref("refs/heads/lw", f.c1)
	v1 := a.tag(f.c1, plumbing.CommitObject)
	v2 := a.tag(v1, plumbing.TagObject)
	tt := a.tag(tree, plumbing.TreeObject)
	tb := a.tag(a.blob("tagged blob\n"), plumbing.BlobObject)
	a.ref("refs/tags/v1", v1)
	a.ref("refs/tags/v2", v2)
	a.ref("refs/tags/tt", tt)
	a.ref("refs/tags/tb", tb)
	x, y := resolveTestAmbiguous(a, tree, f.c1, f.c2, v1, v2, tt, tb)
	a.ref("refs/heads/ambiguous-x", x)
	a.ref("refs/heads/ambiguous-y", y)
	f.ambiguous = x.String()[:4]

	f.cb = f.b.commit(resolveTestReadme(f.b, "b's secret\n"))
	f.b.ref(reachMain, f.cb)
	f.cd = f.d.commit(resolveTestReadme(f.d, "d's detached head\n"))
	f.d.ref(plumbing.HEAD, f.cd)
	return f
}

// resolveTestReadme stores a tree holding README.md with content.
func resolveTestReadme(f *reachTestRepo, content string) plumbing.Hash {
	f.t.Helper()
	return f.tree(reachEntry("README.md", filemode.Regular, f.blob(content)))
}

// resolveTestAmbiguous stores two root commits on tree whose hashes share four leading hex digits that no hash in taken starts with.
func resolveTestAmbiguous(f *reachTestRepo, tree plumbing.Hash, taken ...plumbing.Hash) (plumbing.Hash, plumbing.Hash) {
	f.t.Helper()
	skip := map[string]bool{}
	for _, h := range taken {
		skip[h.String()[:4]] = true
	}
	seen := map[string]*object.Commit{}
	// Hashed in memory, a birthday collision turns up within a few hundred candidates.
	for i := 0; ; i++ {
		c := &object.Commit{Author: reachSig, Committer: reachSig, Message: fmt.Sprintf("ambiguous %d\n", i), TreeHash: tree}
		o := f.repo.repo.Storer.NewEncodedObject()
		if err := c.Encode(o); err != nil {
			f.t.Fatalf("encode: %v", err)
		}
		prefix := o.Hash().String()[:4]
		if skip[prefix] {
			continue
		}
		if other, ok := seen[prefix]; ok {
			return f.store(other), f.store(c)
		}
		seen[prefix] = c
	}
}

// A revision resolves through the refs in git rev-parse order, then as a unique hex prefix of what they reach, and otherwise not at all.
func TestResolveRevision(t *testing.T) {
	for _, mode := range []struct {
		name string
		fs   func(t *testing.T) billy.Filesystem
	}{
		{"Bound", func(t *testing.T) billy.Filesystem { return sharedTestBind(osfs.New(t.TempDir())) }},
		{"Unbound", func(t *testing.T) billy.Filesystem { return osfs.New(t.TempDir()) }},
	} {
		t.Run(mode.name, func(t *testing.T) {
			fs := mode.fs(t)
			f := newResolveTestFixture(t, fs)
			a := f.a.repo
			c1, c2, cb := f.c1.String(), f.c2.String(), f.cb.String()
			resolves := func(t *testing.T, repo *Repository, rev, want string) {
				t.Helper()
				if got, err := repo.ResolveRevision(rev); err != nil || got != want {
					t.Errorf("ResolveRevision(%q) = %q, %v; want %s", rev, got, err, want)
				}
			}
			notFound := func(t *testing.T, repo *Repository, rev string) {
				t.Helper()
				if got, err := repo.ResolveRevision(rev); !errors.Is(err, ErrRevisionNotFound) {
					t.Errorf("ResolveRevision(%q) = %q, %v; want ErrRevisionNotFound", rev, got, err)
				}
			}

			for _, tc := range []struct {
				name, rev string
				want      string // empty when rev must not resolve
			}{
				{"Branch", "main", c2},
				{"FullRefName", "refs/heads/main", c2},
				{"OtherBranch", "side", c1},
				{"HexNamedBranchBeforePrefix", f.hexBranch, c1},
				{"TagBeforeBranch", "lw", c2},
				{"AnnotatedTag", "v1", c1},
				{"NestedTag", "v2", c1},
				{"TagOfTree", "tt", ""},
				{"TagOfBlob", "tb", ""},
				{"HEAD", "HEAD", c2},
				{"Empty", "", c2},
				{"FullHash", c2, c2},
				{"ShortHash", c2[:7], c2},
				{"FourHexDigits", c2[:4], c2},
				{"UpperCaseHex", strings.ToUpper(c2[:7]), c2},
				{"ThreeHexDigits", c2[:3], ""},
				{"AmbiguousPrefix", f.ambiguous, ""},
				{"Tilde", "main~1", ""},
				{"Caret", "HEAD^", ""},
				{"Reflog", "main@{0}", ""},
				{"MessageSearch", ":/commit", ""},
				{"CaretMessageSearch", "main^{/commit}", ""},
				{"PeelSuffix", "main^{commit}", ""},
				{"OtherRepositoryFullHash", cb, ""},
				{"OtherRepositoryShortHash", cb[:7], ""},
				{"PathThroughBranchFile", "main/x", ""},
				{"SymbolicRefThroughBranchFile", "through-main", ""},
				{"TrailingSlash", "foo/", ""},
				{"DoubleSlash", "a//b", ""},
				{"Backslash", "a\\b", ""},
				{"EscapingRefName", "refs/heads/../config", ""},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if tc.want == "" {
						notFound(t, a, tc.rev)
						return
					}
					resolves(t, a, tc.rev, tc.want)
				})
			}

			t.Run("DetachedHEAD", func(t *testing.T) {
				resolves(t, f.d.repo, "HEAD", f.cd.String())
			})

			t.Run("CreateBranchRefusesOtherRepositoryCommit", func(t *testing.T) {
				if err := a.CreateBranch("x", cb); !errors.Is(err, ErrRevisionNotFound) {
					t.Errorf("CreateBranch(x, %s) = %v, want ErrRevisionNotFound", cb, err)
				}
				if ok, err := a.BranchExists("x"); err != nil || ok {
					t.Errorf("BranchExists(x) = %t, %v; want no branch on another repository's commit", ok, err)
				}
			})
			t.Run("CreateTagRefusesOtherRepositoryCommit", func(t *testing.T) {
				if err := a.CreateTag("y", cb); !errors.Is(err, ErrRevisionNotFound) {
					t.Errorf("CreateTag(y, %s) = %v, want ErrRevisionNotFound", cb, err)
				}
				if ok, err := a.TagExists("y"); err != nil || ok {
					t.Errorf("TagExists(y) = %t, %v; want no tag on another repository's commit", ok, err)
				}
			})
			t.Run("CreateBranchFromShortHash", func(t *testing.T) {
				if err := a.CreateBranch("ok", c2[:7]); err != nil {
					t.Errorf("CreateBranch(ok, %s) = %v, want nil", c2[:7], err)
				}
			})

			ctx := t.Context()
			for _, r := range []struct {
				name string
				read func(rev string) error
			}{
				{"CompareBase", func(rev string) error { _, err := a.Compare(ctx, rev, "main"); return err }},
				{"CompareHead", func(rev string) error { _, err := a.Compare(ctx, "main", rev); return err }},
				{"Commits", func(rev string) error { _, err := a.Commits(rev, nil); return err }},
				{"Tree", func(rev string) error { _, err := a.Tree(rev, "", nil); return err }},
				{"TreeSize", func(rev string) error { _, err := a.TreeSize(rev, ""); return err }},
				{"Blob", func(rev string) error { _, err := a.Blob(rev, "README.md"); return err }},
				{"ListLFSPointers", func(rev string) error { _, err := a.ListLFSPointers(rev); return err }},
			} {
				t.Run(r.name+"RefusesOtherRepositoryCommit", func(t *testing.T) {
					if err := r.read(cb); err == nil {
						t.Errorf("%s(%s) succeeded on another repository's commit, want an error", r.name, cb)
					}
				})
				t.Run(r.name+"ResolvesShortHash", func(t *testing.T) {
					if err := r.read(c1[:7]); err != nil {
						t.Errorf("%s(%s) = %v, want c1 resolved by its short hash", r.name, c1[:7], err)
					}
				})
			}

			f.a.ref("refs/heads/side", f.c2)
			t.Run("AncestorOfMovedBranchResolves", func(t *testing.T) {
				for _, rev := range []string{c1, c1[:7]} {
					resolves(t, a, rev, c1)
				}
			})
			f.a.ref(reachMain, f.a.commit(resolveTestReadme(f.a, "unrelated\n")))
			for _, name := range []string{"side", f.hexBranch, "lw", "ok"} {
				if err := a.DeleteBranch(name); err != nil {
					t.Fatalf("delete branch %s: %v", name, err)
				}
			}
			for _, name := range []string{"lw", "v1", "v2"} {
				if err := a.DeleteTag(name); err != nil {
					t.Fatalf("delete tag %s: %v", name, err)
				}
			}
			t.Run("UnreachableCommitDoesNotResolve", func(t *testing.T) {
				for _, rev := range []string{c1, c1[:7]} {
					notFound(t, a, rev)
				}
			})

			t.Run("SuperSquashBranch", func(t *testing.T) {
				if _, err := a.SuperSquash(t.Context(), "main", "squash", "Test", "test@test.com"); err != nil {
					t.Errorf("SuperSquash(main) = %v, want nil", err)
				}
			})

			f.a.ref("refs/heads/dangling", plumbing.NewHash(strings.Repeat("ab", 20)))
			t.Run("DanglingRefFailsResolution", func(t *testing.T) {
				if got, err := a.ResolveRevision("main"); err == nil {
					t.Errorf("ResolveRevision(main) = %s beside a ref naming a missing object, want an error", got)
				}
			})
		})
	}
}
