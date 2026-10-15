package repository

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
)

// reachTestRepo is a bare repository a test writes through the package's API or straight through its storer.
type reachTestRepo struct {
	t    *testing.T
	fs   billy.Filesystem
	path string
	host string // set only by newReachTestLocal
	repo *Repository
	seq  int // keeps every commit and tag stored here distinct
}

func newReachTestRepo(t *testing.T, fs billy.Filesystem, repoPath string) *reachTestRepo {
	t.Helper()
	return &reachTestRepo{t: t, fs: fs, path: repoPath, repo: sharedTestInit(t, fs, repoPath)}
}

// newReachTestLocal creates an unbound repository in a fresh host directory.
func newReachTestLocal(t *testing.T) *reachTestRepo {
	t.Helper()
	root := t.TempDir()
	f := newReachTestRepo(t, osfs.New(root), "/repo.git")
	f.host = filepath.Join(root, "repo.git")
	return f
}

// commitFiles commits files on branch with CreateCommit, which starts a missing branch from no parent.
func (f *reachTestRepo) commitFiles(branch string, files map[string]string) plumbing.Hash {
	f.t.Helper()
	var ops []CommitOperation
	for name, content := range files {
		ops = append(ops, CommitOperation{Type: CommitOperationAdd, Path: name, Content: []byte(content)})
	}
	return plumbing.NewHash(mustCommit(f.t, f.repo, branch, "", ops...))
}

func (f *reachTestRepo) branch(name string) {
	f.t.Helper()
	if err := f.repo.CreateBranch(name, "main"); err != nil {
		f.t.Fatalf("create %s: %v", name, err)
	}
}

func (f *reachTestRepo) store(obj interface {
	Encode(plumbing.EncodedObject) error
}) plumbing.Hash {
	f.t.Helper()
	o := f.repo.repo.Storer.NewEncodedObject()
	if err := obj.Encode(o); err != nil {
		f.t.Fatalf("encode: %v", err)
	}
	h, err := f.repo.repo.Storer.SetEncodedObject(o)
	if err != nil {
		f.t.Fatalf("store: %v", err)
	}
	return h
}

func (f *reachTestRepo) blob(content string) plumbing.Hash {
	f.t.Helper()
	return storeBlob(f.t, f.repo.repo.Storer, content)
}

func (f *reachTestRepo) tree(entries ...object.TreeEntry) plumbing.Hash {
	f.t.Helper()
	sort.Sort(object.TreeEntrySorter(entries))
	return f.store(&object.Tree{Entries: entries})
}

func (f *reachTestRepo) commit(tree plumbing.Hash, parents ...plumbing.Hash) plumbing.Hash {
	f.t.Helper()
	f.seq++
	return f.store(&object.Commit{Author: reachSig, Committer: reachSig, Message: fmt.Sprintf("commit %d\n", f.seq), TreeHash: tree, ParentHashes: parents})
}

// tag returns an annotated tag object on target, which no ref names yet.
func (f *reachTestRepo) tag(target plumbing.Hash, kind plumbing.ObjectType) plumbing.Hash {
	f.t.Helper()
	f.seq++
	return f.store(&object.Tag{Name: fmt.Sprintf("v%d", f.seq), Tagger: reachSig, Message: "tag\n", TargetType: kind, Target: target})
}

func (f *reachTestRepo) ref(name plumbing.ReferenceName, h plumbing.Hash) {
	f.t.Helper()
	if err := f.repo.repo.Storer.SetReference(plumbing.NewHashReference(name, h)); err != nil {
		f.t.Fatalf("set %s: %v", name, err)
	}
}

// write puts content at name below the repository, as a writer other than go-git would.
func (f *reachTestRepo) write(name, content string) {
	f.t.Helper()
	if err := util.WriteFile(f.fs, path.Join(f.path, name), []byte(content), 0o644); err != nil {
		f.t.Fatalf("write %s: %v", name, err)
	}
}

func (f *reachTestRepo) treeOf(commit plumbing.Hash) *object.Tree {
	f.t.Helper()
	c, err := f.repo.repo.CommitObject(commit)
	if err != nil {
		f.t.Fatalf("read commit %s: %v", commit, err)
	}
	tree, err := c.Tree()
	if err != nil {
		f.t.Fatalf("read tree of %s: %v", commit, err)
	}
	return tree
}

// entry returns the entry called name in the root tree of commit.
func (f *reachTestRepo) entry(commit plumbing.Hash, name string) object.TreeEntry {
	f.t.Helper()
	e, err := f.treeOf(commit).FindEntry(name)
	if err != nil {
		f.t.Fatalf("entry %s of %s: %v", name, commit, err)
	}
	return *e
}

func (f *reachTestRepo) set() *reachSet {
	f.t.Helper()
	s, err := f.repo.reachable(f.t.Context())
	if err != nil {
		f.t.Fatalf("reachable: %v", err)
	}
	return s
}

// reachTestWant is what the index must say once a change to the refs is in place.
type reachTestWant struct {
	in, out []plumbing.Hash
	// peel maps a hash to the commit it peels to; the zero hash means it must not peel.
	peel map[plumbing.Hash]plumbing.Hash
	err  bool // reachable must fail
}

// reachRaceFS stands in for writers racing a listing of the refs: a torn file reads empty while its count lasts, a gone path vanishes once listed.
type reachRaceFS struct {
	billy.Filesystem
	torn map[string]int
	gone map[string]bool
}

func (fs *reachRaceFS) Open(name string) (billy.File, error) {
	key := filepath.ToSlash(name)
	if fs.gone[key] {
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
	}
	if fs.torn[key] > 0 {
		fs.torn[key]--
		return memfs.New().Create("torn")
	}
	return fs.Filesystem.Open(name)
}

func (fs *reachRaceFS) ReadDir(name string) ([]os.DirEntry, error) {
	if fs.gone[filepath.ToSlash(name)] {
		return nil, &os.PathError{Op: "readdir", Path: name, Err: os.ErrNotExist}
	}
	return fs.Filesystem.ReadDir(name)
}

// The index holds what the current refs reach, whether first built after a change (Cold) or refreshed across it (Warm).
func TestReachable(t *testing.T) {
	absent := plumbing.NewHash(strings.Repeat("ab", 20))
	for _, tc := range []struct {
		name string
		// change edits the refs of f, whose main holds base, calling prime once the state a warm index starts from is in place.
		change func(f *reachTestRepo, base plumbing.Hash, prime func()) reachTestWant
	}{
		{"NewCommit", func(f *reachTestRepo, base plumbing.Hash, prime func()) reachTestWant {
			prime()
			return reachTestWant{in: []plumbing.Hash{base, f.commitFiles("main", map[string]string{"new.txt": "new\n"})}}
		}},
		{"DeletedBranch", func(f *reachTestRepo, base plumbing.Hash, prime func()) reachTestWant {
			f.branch("dev")
			dev := f.commitFiles("dev", map[string]string{"dev.txt": "dev\n"})
			prime()
			if err := f.repo.DeleteBranch("dev"); err != nil {
				f.t.Fatalf("delete dev: %v", err)
			}
			return reachTestWant{in: []plumbing.Hash{base}, out: []plumbing.Hash{dev}}
		}},
		{"BranchMovedToUnrelatedRoot", func(f *reachTestRepo, base plumbing.Hash, prime func()) reachTestWant {
			f.branch("dev")
			first := f.commitFiles("dev", map[string]string{"dev.txt": "one\n"})
			second := f.commitFiles("dev", map[string]string{"dev.txt": "two\n"})
			root := f.commit(f.tree(reachEntry("other.txt", filemode.Regular, f.blob("unrelated\n"))))
			prime()
			f.ref("refs/heads/dev", root)
			return reachTestWant{in: []plumbing.Hash{base, root}, out: []plumbing.Hash{first, second}}
		}},
		// The merged branch is gone, so only the merge's second parent leads to side.
		{"MergeSecondParent", func(f *reachTestRepo, base plumbing.Hash, prime func()) reachTestWant {
			f.branch("side")
			side := f.commitFiles("side", map[string]string{"side.txt": "side\n"})
			prime()
			merge := f.commit(f.treeOf(base).Hash, base, side)
			f.ref(reachMain, merge)
			if err := f.repo.DeleteBranch("side"); err != nil {
				f.t.Fatalf("delete side: %v", err)
			}
			return reachTestWant{in: []plumbing.Hash{merge, base, side}}
		}},
		{"DetachedHEAD", func(f *reachTestRepo, base plumbing.Hash, prime func()) reachTestWant {
			detached := f.commit(f.tree(reachEntry("detached.txt", filemode.Regular, f.blob("detached\n"))), base)
			prime()
			f.ref(plumbing.HEAD, detached)
			return reachTestWant{in: []plumbing.Hash{detached, base}}
		}},
		{"TagChain", func(f *reachTestRepo, base plumbing.Hash, prime func()) reachTestWant {
			target := f.commit(f.tree(reachEntry("tagged.txt", filemode.Regular, f.blob("tagged\n"))), base)
			inner := f.tag(target, plumbing.CommitObject)
			outer := f.tag(inner, plumbing.TagObject)
			treeTag := f.tag(f.treeOf(base).Hash, plumbing.TreeObject)
			blobTag := f.tag(f.blob("tagged blob\n"), plumbing.BlobObject)
			// Stored and aimed at a reachable commit, yet named by no ref.
			stray := f.tag(base, plumbing.CommitObject)
			prime()
			f.ref("refs/tags/outer", outer)
			f.ref("refs/tags/tree", treeTag)
			f.ref("refs/tags/blob", blobTag)
			return reachTestWant{
				in:   []plumbing.Hash{target, inner, outer, treeTag, blobTag},
				out:  []plumbing.Hash{stray, absent},
				peel: map[plumbing.Hash]plumbing.Hash{outer: target, inner: target, target: target, base: base, treeTag: {}, blobTag: {}, stray: {}, absent: {}},
			}
		}},
		{"ExternalUpdateRef", func(f *reachTestRepo, base plumbing.Hash, prime func()) reachTestWant {
			if _, err := exec.LookPath("git"); err != nil {
				f.t.Skip("git not installed")
			}
			ext := f.commit(f.tree(reachEntry("ext.txt", filemode.Regular, f.blob("ext\n"))), base)
			prime()
			sharedTestGit(f.t, sharedTestGitEnv(f.t), f.host, "update-ref", "refs/heads/ext", ext.String())
			return reachTestWant{in: []plumbing.Hash{ext}}
		}},
		{"DanglingRef", func(f *reachTestRepo, _ plumbing.Hash, prime func()) reachTestWant {
			prime()
			f.ref("refs/heads/dangling", absent)
			return reachTestWant{err: true}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, warm := range []bool{false, true} {
				mode := "Cold"
				if warm {
					mode = "Warm"
				}
				t.Run(mode, func(t *testing.T) {
					f := newReachTestLocal(t)
					base := f.commitFiles("main", map[string]string{"README.md": "base\n"})
					prime := func() {}
					if warm {
						prime = func() { f.set() }
					}
					want := tc.change(f, base, prime)
					s, err := f.repo.reachable(t.Context())
					if want.err {
						if err == nil {
							t.Fatal("reachable succeeded, want an error for a ref naming a missing object")
						}
						return
					}
					if err != nil {
						t.Fatalf("reachable: %v", err)
					}
					for _, h := range want.in {
						if !s.contains(h) {
							t.Errorf("contains(%s) = false, want true", h)
						}
					}
					for _, h := range want.out {
						if s.contains(h) {
							t.Errorf("contains(%s) = true, want false", h)
						}
					}
					for h, commit := range want.peel {
						if got, ok := s.peel(h); ok != !commit.IsZero() || ok && got != commit {
							t.Errorf("peel(%s) = %s, %t; want %s, %t", h, got, ok, commit, !commit.IsZero())
						}
					}
				})
			}
		})
	}

	t.Run("Snapshot", func(t *testing.T) {
		f := newReachTestLocal(t)
		f.commitFiles("main", map[string]string{"README.md": "base\n"})
		before := f.set()
		c := f.commitFiles("main", map[string]string{"new.txt": "new\n"})
		if !f.set().contains(c) {
			t.Errorf("contains(%s) = false right after the commit, want true", c)
		}
		if before.contains(c) {
			t.Errorf("a set taken before commit %s now contains it, want an immutable snapshot", c)
		}
	})

	t.Run("ByPrefix", func(t *testing.T) {
		f := newReachTestLocal(t)
		first := f.commitFiles("main", map[string]string{"README.md": "base\n"})
		second := f.commitFiles("main", map[string]string{"new.txt": "new\n"})
		tag := f.tag(second, plumbing.CommitObject)
		f.ref("refs/tags/v1", tag)
		stray := f.commit(f.treeOf(second).Hash, second)
		index := []plumbing.Hash{first, second, tag}
		s := f.set()
		hex := second.String()
		for _, prefix := range []string{hex[:1], hex[:2], hex[:7], hex, tag.String(), stray.String()} {
			var want []plumbing.Hash
			for _, h := range index {
				if strings.HasPrefix(h.String(), prefix) {
					want = append(want, h)
				}
			}
			plumbing.HashesSort(want)
			if got := s.byPrefix(prefix); !slices.Equal(got, want) {
				t.Errorf("byPrefix(%q) = %v, want %v", prefix, got, want)
			}
		}
	})

	// The boundary is in the index; its parent, never fetched, is not.
	t.Run("Shallow", func(t *testing.T) {
		f := newReachTestLocal(t)
		first := f.commitFiles("main", map[string]string{"README.md": "base\n"})
		tip := f.commitFiles("main", map[string]string{"new.txt": "new\n"})
		f.repo = reachTestShallow(t, f.fs, f.path, tip.String(), first.String())
		s := f.set()
		if !s.contains(tip) {
			t.Errorf("contains(boundary %s) = false, want true", tip)
		}
		if s.contains(first) {
			t.Errorf("contains(%s beyond the boundary) = true, want false", first)
		}
	})

	// A shallow list written while the index is warm ends history at its boundary, as it does for a cold one.
	t.Run("ShallowChangeRebuilds", func(t *testing.T) {
		f := newReachTestLocal(t)
		first := f.commitFiles("main", map[string]string{"README.md": "base\n"})
		tip := f.commitFiles("main", map[string]string{"new.txt": "new\n"})
		if !f.set().contains(first) {
			t.Fatalf("contains(%s) = false before the shallow change, want true", first)
		}
		if err := os.WriteFile(filepath.Join(f.host, "shallow"), []byte(tip.String()+"\n"), 0o644); err != nil {
			t.Fatalf("write shallow: %v", err)
		}
		s := f.set()
		if s.contains(first) {
			t.Errorf("contains(%s beyond the new boundary) = true, want the shallow change to rebuild the index", first)
		}
		if !s.contains(tip) {
			t.Errorf("contains(boundary %s) = false, want true", tip)
		}
	})

	t.Run("OtherRepository", func(t *testing.T) {
		bound := sharedTestBind(osfs.New(t.TempDir()))
		x := newReachTestRepo(t, bound, "/a/x.git")
		y := newReachTestRepo(t, bound, "/b/y.git")
		x.commitFiles("main", map[string]string{"x.txt": "x\n"})
		secret := y.commitFiles("main", map[string]string{"y.txt": "y\n"})
		s := x.set()
		if s.contains(secret) {
			t.Errorf("x contains %s, a commit of y in the shared pool", secret)
		}
		if got := s.byPrefix(secret.String()[:2]); slices.Contains(got, secret) {
			t.Errorf("x byPrefix(%q) = %v, which names y's commit %s", secret.String()[:2], got, secret)
		}
		if _, ok := s.peel(secret); ok {
			t.Errorf("x peels %s, a commit of y in the shared pool", secret)
		}
		if !y.set().contains(secret) {
			t.Errorf("y does not contain its own commit %s", secret)
		}
	})

	// Refs are listed as git's files backend lists them, whether the repository shares the pool (Bound) or not (Unbound).
	t.Run("RefEnumeration", func(t *testing.T) {
		modes := []struct {
			name string
			new  func(t *testing.T) *reachTestRepo
		}{
			{"Unbound", newReachTestLocal},
			{"Bound", func(t *testing.T) *reachTestRepo {
				return newReachTestRepo(t, sharedTestBind(osfs.New(t.TempDir())), "/repo.git")
			}},
		}
		for _, tc := range []struct {
			name string
			// files writes ref files into f, whose main holds base.
			files func(f *reachTestRepo, base plumbing.Hash) reachTestWant
		}{
			// What a reader catches mid-rewrite: go-git truncates a loose ref before writing it.
			{"TornRefSkipped", func(f *reachTestRepo, base plumbing.Hash) reachTestWant {
				f.write("refs/heads/torn", "")
				return reachTestWant{in: []plumbing.Hash{base}}
			}},
			{"TornHEADSkipped", func(f *reachTestRepo, base plumbing.Hash) reachTestWant {
				f.write("HEAD", "")
				return reachTestWant{in: []plumbing.Hash{base}}
			}},
			// What a reader catches of a write cut short.
			{"BrokenRefSkipped", func(f *reachTestRepo, base plumbing.Hash) reachTestWant {
				f.write("refs/heads/broken", base.String()[:20])
				return reachTestWant{in: []plumbing.Hash{base}}
			}},
			{"LockFileIgnored", func(f *reachTestRepo, base plumbing.Hash) reachTestWant {
				pending := f.commit(f.treeOf(base).Hash, base)
				f.write("refs/heads/main.lock", pending.String()+"\n")
				return reachTestWant{in: []plumbing.Hash{base}, out: []plumbing.Hash{pending}}
			}},
			// receive-pack locks a ref it deletes with an empty lock file.
			{"EmptyLockFileIgnored", func(f *reachTestRepo, base plumbing.Hash) reachTestWant {
				f.write("refs/heads/main.lock", "")
				return reachTestWant{in: []plumbing.Hash{base}}
			}},
			// A peel line caches what the tag before it ends at; aimed elsewhere, it still names nothing.
			{"PackedRefsIndexed", func(f *reachTestRepo, base plumbing.Hash) reachTestWant {
				packed := f.commit(f.treeOf(base).Hash, base)
				tag := f.tag(packed, plumbing.CommitObject)
				peeled := f.commit(f.treeOf(base).Hash, base)
				f.write("packed-refs", "# pack-refs with: peeled fully-peeled sorted\n"+
					packed.String()+" refs/heads/packed\n"+
					tag.String()+" refs/tags/packed\n"+
					"^"+peeled.String()+"\n")
				return reachTestWant{in: []plumbing.Hash{base, packed, tag}, out: []plumbing.Hash{peeled}}
			}},
			{"LooseWinsOverPacked", func(f *reachTestRepo, base plumbing.Hash) reachTestWant {
				packed, loose := f.commit(f.treeOf(base).Hash, base), f.commit(f.treeOf(base).Hash, base)
				f.write("packed-refs", packed.String()+" refs/heads/dev\n")
				f.write("refs/heads/dev", loose.String()+"\n")
				return reachTestWant{in: []plumbing.Hash{loose}, out: []plumbing.Hash{packed}}
			}},
			{"SymbolicRefSkipped", func(f *reachTestRepo, base plumbing.Hash) reachTestWant {
				f.write("refs/heads/alias", "ref: refs/heads/main\n")
				f.write("refs/heads/dangling-sym", "ref: refs/heads/nope\n")
				return reachTestWant{in: []plumbing.Hash{base}}
			}},
			{"DanglingHashRefErrors", func(f *reachTestRepo, _ plumbing.Hash) reachTestWant {
				f.write("refs/heads/dangling", absent.String()+"\n")
				return reachTestWant{err: true}
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				for _, mode := range modes {
					t.Run(mode.name, func(t *testing.T) {
						f := mode.new(t)
						want := tc.files(f, f.commitFiles("main", map[string]string{"README.md": "base\n"}))
						s, err := f.repo.reachable(t.Context())
						if want.err {
							if err == nil {
								t.Fatal("reachable succeeded, want an error for a ref naming a missing object")
							}
							return
						}
						if err != nil {
							t.Fatalf("reachable: %v", err)
						}
						for _, h := range want.in {
							if !s.contains(h) {
								t.Errorf("contains(%s) = false, want true", h)
							}
						}
						for _, h := range want.out {
							if s.contains(h) {
								t.Errorf("contains(%s) = true, want false", h)
							}
						}
					})
				}
			})
		}
	})

	// A ref file caught empty mid-rewrite is read once more; one still empty then is skipped as broken.
	t.Run("RetriesEmptyRefFile", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			torn  int  // reads in a row that find refs/heads/flaky empty
			found bool // whether the commit only flaky names is indexed
		}{{"IndexedOnReread", 1, true}, {"SkippedWhileEmpty", math.MaxInt, false}} {
			t.Run(tc.name, func(t *testing.T) {
				f := newReachTestLocal(t)
				base := f.commitFiles("main", map[string]string{"README.md": "base\n"})
				flaky := f.commit(f.treeOf(base).Hash, base)
				f.ref("refs/heads/flaky", flaky)
				name := path.Join(f.path, "refs/heads/flaky")
				fs := &reachRaceFS{Filesystem: f.repo.fs, torn: map[string]int{name: tc.torn}}
				f.repo.fs = fs
				s := f.set()
				if fs.torn[name] == tc.torn {
					t.Fatalf("%s never read through the repository's filesystem", name)
				}
				if got := s.contains(flaky); got != tc.found {
					t.Errorf("contains(%s) = %t, want %t", flaky, got, tc.found)
				}
				if !s.contains(base) {
					t.Errorf("contains(%s) = false, want true", base)
				}
			})
		}
	})

	// A ref deleted once the listing named it is skipped, as is a directory removed with its last ref.
	t.Run("VanishedRefSkipped", func(t *testing.T) {
		f := newReachTestLocal(t)
		base := f.commitFiles("main", map[string]string{"README.md": "base\n"})
		gone := f.commit(f.treeOf(base).Hash, base)
		f.ref("refs/heads/gone", gone)
		f.ref("refs/heads/dir/gone", gone)
		f.repo.fs = &reachRaceFS{Filesystem: f.repo.fs, gone: map[string]bool{
			path.Join(f.path, "refs/heads/gone"): true,
			path.Join(f.path, "refs/heads/dir"):  true,
		}}
		s := f.set()
		if s.contains(gone) {
			t.Errorf("contains(%s) = true, want false once the refs naming it are gone", gone)
		}
		if !s.contains(base) {
			t.Errorf("contains(%s) = false, want true", base)
		}
	})
}

// Readers refreshing the index while CreateCommit moves main never fail and never lose what the refs held before.
func TestReachableConcurrent(t *testing.T) {
	f := newReachTestLocal(t)
	base := f.commitFiles("main", map[string]string{"README.md": "base\n"})
	// No writer touches this ref, so base stays reachable whatever a reader catches of main.
	f.ref("refs/tags/base", base)
	done := make(chan struct{})
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for {
				s, err := f.repo.reachable(t.Context())
				switch {
				case err != nil:
					errs <- err
					return
				case !s.contains(base):
					errs <- fmt.Errorf("index lost %s", base)
					return
				}
				select {
				case <-done:
					return
				default:
				}
			}
		})
	}
	var commits []plumbing.Hash
	for i := range 5 {
		commits = append(commits, f.commitFiles("main", map[string]string{fmt.Sprintf("%d.txt", i): fmt.Sprintf("%d\n", i)}))
	}
	close(done)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent reachable: %v", err)
	}
	s := f.set()
	for _, c := range commits {
		if !s.contains(c) {
			t.Errorf("contains(%s) = false once the writer finished, want true", c)
		}
	}
}

// reachTestConn is what a push to a meets: main at tip (README.md, sub/), a side branch alone holding sideBlob, and b holding foreign.
type reachTestConn struct {
	a, b                   *reachTestRepo
	strict                 bool // a and b share a pool, and a check names what the push delivered
	tip, sideBlob, foreign plumbing.Hash
}

func newReachTestConn(t *testing.T, strict bool) *reachTestConn {
	t.Helper()
	fs := osfs.New(t.TempDir())
	if strict {
		fs = sharedTestBind(fs)
	}
	f := &reachTestConn{a: newReachTestRepo(t, fs, "/org/a.git"), b: newReachTestRepo(t, fs, "/org/b.git"), strict: strict}
	f.tip = f.a.commitFiles("main", map[string]string{"README.md": "a\n", "sub/kept.txt": "kept\n"})
	f.sideBlob = f.a.entry(f.a.commitFiles("side", map[string]string{"side.txt": "only on side\n"}), "side.txt").Hash
	f.foreign = f.b.commitFiles("main", map[string]string{"secret.txt": "b's secret\n"})
	return f
}

// push stores a commit on parents of a tree of entries; the push delivers the commit and the tree alone.
func (f *reachTestConn) push(parents []plumbing.Hash, entries ...object.TreeEntry) (tips, delivered []plumbing.Hash) {
	tree := f.a.tree(entries...)
	commit := f.a.commit(tree, parents...)
	return []plumbing.Hash{commit}, []plumbing.Hash{commit, tree}
}

// pushFile is push with a new file beside entries, its blob delivered too.
func (f *reachTestConn) pushFile(parents []plumbing.Hash, entries ...object.TreeEntry) (tips, delivered []plumbing.Hash) {
	blob := f.a.blob("new\n")
	tips, delivered = f.push(parents, append(entries, reachEntry("new.txt", filemode.Regular, blob))...)
	return tips, append(delivered, blob)
}

// check runs checkConnectivity on a, naming the delivered objects only in strict mode.
func (f *reachTestConn) check(ctx context.Context, tips, delivered []plumbing.Hash) error {
	var isDelivered func(plumbing.Hash) bool
	if f.strict {
		isDelivered = func(h plumbing.Hash) bool { return slices.Contains(delivered, h) }
	}
	return f.a.repo.checkConnectivity(ctx, tips, isDelivered)
}

// A push may name without delivering only what a's refs reach in strict mode, and anything a stores in classic mode.
func TestCheckConnectivity(t *testing.T) {
	absent := func(digit string) plumbing.Hash { return plumbing.NewHash(strings.Repeat(digit, 40)) }
	modes := []struct {
		name   string
		strict bool
	}{{"Strict", true}, {"Classic", false}}
	for _, tc := range []struct {
		name string
		push func(f *reachTestConn) (tips, delivered []plumbing.Hash)
		// Whether the push is connected in each mode; if not, the check fails with errMissingObject.
		strict, classic bool
	}{
		{"NewCommitOnTip", func(f *reachTestConn) ([]plumbing.Hash, []plumbing.Hash) {
			return f.pushFile([]plumbing.Hash{f.tip}, f.a.entry(f.tip, "README.md"), f.a.entry(f.tip, "sub"))
		}, true, true},
		{"ExistingTip", func(f *reachTestConn) ([]plumbing.Hash, []plumbing.Hash) {
			return []plumbing.Hash{f.tip}, nil
		}, true, true},
		{"BlobOnOtherBranch", func(f *reachTestConn) ([]plumbing.Hash, []plumbing.Hash) {
			return f.push([]plumbing.Hash{f.tip}, reachEntry("copy.txt", filemode.Regular, f.sideBlob))
		}, true, true},
		// The gitlink names a commit of another repository, absent from every store.
		{"Gitlink", func(f *reachTestConn) ([]plumbing.Hash, []plumbing.Hash) {
			return f.pushFile([]plumbing.Hash{f.tip}, reachEntry("module", filemode.Submodule, absent("a")))
		}, true, true},
		{"TagOnDeliveredCommit", func(f *reachTestConn) ([]plumbing.Hash, []plumbing.Hash) {
			tips, delivered := f.pushFile([]plumbing.Hash{f.tip})
			tag := f.a.tag(tips[0], plumbing.CommitObject)
			return []plumbing.Hash{tag}, append(delivered, tag)
		}, true, true},
		{"ShallowTip", func(f *reachTestConn) ([]plumbing.Hash, []plumbing.Hash) {
			tip := f.a.commitFiles("main", map[string]string{"more.txt": "more\n"})
			f.a.repo = reachTestShallow(f.a.t, f.a.fs, f.a.path, tip.String(), f.tip.String())
			return f.pushFile([]plumbing.Hash{tip}, f.a.entry(tip, "README.md"))
		}, true, true},
		{"StoredUnreachableParent", func(f *reachTestConn) ([]plumbing.Hash, []plumbing.Hash) {
			parked := f.a.commit(f.a.tree(reachEntry("parked.txt", filemode.Regular, f.a.blob("parked\n"))), f.tip)
			return f.pushFile([]plumbing.Hash{parked})
		}, false, true},
		{"StoredUnreachableBlob", func(f *reachTestConn) ([]plumbing.Hash, []plumbing.Hash) {
			return f.push([]plumbing.Hash{f.tip}, reachEntry("stray.txt", filemode.Regular, f.a.blob("stray\n")))
		}, false, true},
		{"ParentOfOtherRepository", func(f *reachTestConn) ([]plumbing.Hash, []plumbing.Hash) {
			return f.pushFile([]plumbing.Hash{f.foreign})
		}, false, false},
		{"TreeOfOtherRepository", func(f *reachTestConn) ([]plumbing.Hash, []plumbing.Hash) {
			commit := f.a.commit(f.b.treeOf(f.foreign).Hash, f.tip)
			return []plumbing.Hash{commit}, []plumbing.Hash{commit}
		}, false, false},
		{"BlobOfOtherRepository", func(f *reachTestConn) ([]plumbing.Hash, []plumbing.Hash) {
			return f.push([]plumbing.Hash{f.tip}, f.b.entry(f.foreign, "secret.txt"))
		}, false, false},
		{"TipOfOtherRepository", func(f *reachTestConn) ([]plumbing.Hash, []plumbing.Hash) {
			return []plumbing.Hash{f.foreign}, nil
		}, false, false},
		{"TagOnOtherRepository", func(f *reachTestConn) ([]plumbing.Hash, []plumbing.Hash) {
			tag := f.a.tag(f.foreign, plumbing.CommitObject)
			return []plumbing.Hash{tag}, []plumbing.Hash{tag}
		}, false, false},
		{"ParentNowhere", func(f *reachTestConn) ([]plumbing.Hash, []plumbing.Hash) {
			return f.pushFile([]plumbing.Hash{absent("e")})
		}, false, false},
		{"BlobNowhere", func(f *reachTestConn) ([]plumbing.Hash, []plumbing.Hash) {
			return f.push([]plumbing.Hash{f.tip}, reachEntry("ghost.bin", filemode.Regular, absent("d")))
		}, false, false},
		{"DeliveredTreeAbsent", func(f *reachTestConn) ([]plumbing.Hash, []plumbing.Hash) {
			commit := f.a.commit(absent("c"), f.tip)
			return []plumbing.Hash{commit}, []plumbing.Hash{commit, absent("c")}
		}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range modes {
				t.Run(mode.name, func(t *testing.T) {
					f := newReachTestConn(t, mode.strict)
					tips, delivered := tc.push(f)
					err := f.check(t.Context(), tips, delivered)
					connected := tc.classic
					if mode.strict {
						connected = tc.strict
					}
					if connected && err != nil {
						t.Fatalf("checkConnectivity = %v, want nil", err)
					}
					if !connected && !errors.Is(err, errMissingObject) {
						t.Fatalf("checkConnectivity = %v, want errMissingObject", err)
					}
				})
			}
		})
	}

	t.Run("Cancelled", func(t *testing.T) {
		for _, mode := range modes {
			t.Run(mode.name, func(t *testing.T) {
				f := newReachTestConn(t, mode.strict)
				tips, delivered := f.pushFile([]plumbing.Hash{f.tip})
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				if err := f.check(ctx, tips, delivered); !errors.Is(err, context.Canceled) {
					t.Fatalf("checkConnectivity with a cancelled context = %v, want context.Canceled", err)
				}
			})
		}
	})
}
