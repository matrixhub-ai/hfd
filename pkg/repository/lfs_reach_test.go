package repository

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage/memory"

	"github.com/matrixhub-ai/hfd/pkg/lfs"
)

const reachMain plumbing.ReferenceName = "refs/heads/main"

var reachSig = object.Signature{Name: "Test", Email: "test@test.com", When: time.Unix(1700000000, 0).UTC()}

// reachStorage counts EncodedObject and EncodedObjectSize calls by hash and fails EncodedObject for the hashes in fail.
type reachStorage struct {
	*memory.Storage
	mu    sync.Mutex
	reads map[plumbing.Hash]int
	sizes map[plumbing.Hash]int
	fail  map[plumbing.Hash]error
}

func (s *reachStorage) EncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	s.mu.Lock()
	s.reads[h]++
	err := s.fail[h]
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s.Storage.EncodedObject(t, h)
}

func (s *reachStorage) EncodedObjectSize(h plumbing.Hash) (int64, error) {
	s.mu.Lock()
	s.sizes[h]++
	s.mu.Unlock()
	return s.Storage.EncodedObjectSize(h)
}

// calls reports how often h was read and sized.
func (s *reachStorage) calls(h plumbing.Hash) (reads, sizes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads[h], s.sizes[h]
}

// remove drops h from every object map, as if it had never been stored.
func (s *reachStorage) remove(h plumbing.Hash) {
	for _, objects := range []map[plumbing.Hash]plumbing.EncodedObject{s.Objects, s.Commits, s.Trees, s.Blobs, s.Tags} {
		delete(objects, h)
	}
}

// reachRepo is an unbound in-memory repository whose objects and refs a test writes directly.
type reachRepo struct {
	t  *testing.T
	st *reachStorage
	r  *Repository
}

func newReachRepo(t *testing.T) *reachRepo {
	t.Helper()
	st := &reachStorage{
		Storage: memory.NewStorage(),
		reads:   map[plumbing.Hash]int{},
		sizes:   map[plumbing.Hash]int{},
		fail:    map[plumbing.Hash]error{},
	}
	r, err := git.Init(st, git.WithDefaultBranch(reachMain))
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	return &reachRepo{t: t, st: st, r: &Repository{repo: r}}
}

func reachEntry(name string, mode filemode.FileMode, h plumbing.Hash) object.TreeEntry {
	return object.TreeEntry{Name: name, Mode: mode, Hash: h}
}

func (f *reachRepo) blob(content string) plumbing.Hash {
	f.t.Helper()
	return storeBlob(f.t, f.st, content)
}

func (f *reachRepo) store(obj interface {
	Encode(plumbing.EncodedObject) error
}) plumbing.Hash {
	f.t.Helper()
	o := f.st.NewEncodedObject()
	if err := obj.Encode(o); err != nil {
		f.t.Fatalf("encode: %v", err)
	}
	h, err := f.st.SetEncodedObject(o)
	if err != nil {
		f.t.Fatalf("store: %v", err)
	}
	return h
}

func (f *reachRepo) tree(entries ...object.TreeEntry) plumbing.Hash {
	f.t.Helper()
	sort.Sort(object.TreeEntrySorter(entries))
	return f.store(&object.Tree{Entries: entries})
}

// nested returns a tree holding blob at nested/model.bin.
func (f *reachRepo) nested(blob plumbing.Hash) plumbing.Hash {
	f.t.Helper()
	return f.tree(reachEntry("nested", filemode.Dir, f.tree(reachEntry("model.bin", filemode.Regular, blob))))
}

func (f *reachRepo) commit(tree plumbing.Hash, parents ...plumbing.Hash) plumbing.Hash {
	f.t.Helper()
	return f.store(&object.Commit{Author: reachSig, Committer: reachSig, Message: "test\n", TreeHash: tree, ParentHashes: parents})
}

// tag returns an annotated tag object on target, which no ref names yet.
func (f *reachRepo) tag(target plumbing.Hash, kind plumbing.ObjectType) plumbing.Hash {
	f.t.Helper()
	return f.store(&object.Tag{Name: "v1", Tagger: reachSig, Message: "tag\n", TargetType: kind, Target: target})
}

func (f *reachRepo) ref(name plumbing.ReferenceName, h plumbing.Hash) {
	f.t.Helper()
	if err := f.st.SetReference(plumbing.NewHashReference(name, h)); err != nil {
		f.t.Fatalf("set %s: %v", name, err)
	}
}

// requirePointers scans, skipping except, and requires exactly the OIDs of want's pointer blobs, in blob hash order.
func (f *reachRepo) requirePointers(want map[plumbing.Hash]string, except ...plumbing.Hash) {
	f.t.Helper()
	ptrs, err := f.r.ScanLFSPointersExcept(f.t.Context(), except)
	if err != nil {
		f.t.Fatalf("scan: %v", err)
	}
	got := []string{}
	for _, p := range ptrs {
		got = append(got, p.OID())
	}
	blobs := slices.Collect(maps.Keys(want))
	plumbing.HashesSort(blobs)
	wantOIDs := []string{}
	for _, h := range blobs {
		wantOIDs = append(wantOIDs, want[h])
	}
	if !slices.Equal(got, wantOIDs) {
		f.t.Fatalf("scan = %v, want %v", got, wantOIDs)
	}
}

// ScanLFSPointers* list the pointer blobs the refs reach, and nothing else that is stored.
func TestScanLFSPointersReachability(t *testing.T) {
	oid := func(n int) string { return fmt.Sprintf("%064x", n) }
	t.Run("Found", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			// wire makes ptr reachable through one kind of root; base is main's tip, which names no pointer.
			wire func(f *reachRepo, base, ptr plumbing.Hash)
		}{
			{"MainHistory", func(f *reachRepo, base, ptr plumbing.Hash) {
				f.ref(reachMain, f.commit(f.tree(reachEntry("README.md", filemode.Regular, f.blob("moved on\n"))), f.commit(f.nested(ptr), base)))
			}},
			{"SecondBranch", func(f *reachRepo, base, ptr plumbing.Hash) {
				f.ref("refs/heads/dev", f.commit(f.nested(ptr), base))
			}},
			{"AnnotatedTag", func(f *reachRepo, base, ptr plumbing.Hash) {
				f.ref("refs/tags/v1", f.tag(f.commit(f.nested(ptr), base), plumbing.CommitObject))
			}},
			{"TagOfTag", func(f *reachRepo, base, ptr plumbing.Hash) {
				f.ref("refs/tags/v1", f.tag(f.tag(f.commit(f.nested(ptr), base), plumbing.CommitObject), plumbing.TagObject))
			}},
			{"AnnotatedTagToTree", func(f *reachRepo, _, ptr plumbing.Hash) {
				f.ref("refs/tags/v1", f.tag(f.nested(ptr), plumbing.TreeObject))
			}},
			{"AnnotatedTagToBlob", func(f *reachRepo, _, ptr plumbing.Hash) {
				f.ref("refs/tags/v1", f.tag(ptr, plumbing.BlobObject))
			}},
			{"RefToTree", func(f *reachRepo, _, ptr plumbing.Hash) {
				f.ref("refs/tags/v1", f.nested(ptr))
			}},
			{"RefToBlob", func(f *reachRepo, _, ptr plumbing.Hash) {
				f.ref("refs/tags/v1", ptr)
			}},
			{"DetachedHEAD", func(f *reachRepo, base, ptr plumbing.Hash) {
				f.ref(plumbing.HEAD, f.commit(f.nested(ptr), base))
			}},
			{"ExecutableEntry", func(f *reachRepo, base, ptr plumbing.Hash) {
				f.ref("refs/heads/dev", f.commit(f.tree(reachEntry("run.sh", filemode.Executable, ptr)), base))
			}},
			{"SymlinkEntry", func(f *reachRepo, base, ptr plumbing.Hash) {
				f.ref("refs/heads/dev", f.commit(f.tree(reachEntry("link", filemode.Symlink, ptr)), base))
			}},
			// The gitlink names a commit of another repository, absent from this one.
			{"BesideGitlink", func(f *reachRepo, base, ptr plumbing.Hash) {
				gitlink := plumbing.NewHash(strings.Repeat("e", 40))
				f.ref("refs/heads/dev", f.commit(f.tree(reachEntry("sub", filemode.Submodule, gitlink), reachEntry("model.bin", filemode.Regular, ptr)), base))
			}},
			// The merged branch is gone, so only the merge's second parent leads to ptr.
			{"MergeSecondParent", func(f *reachRepo, base, ptr plumbing.Hash) {
				side := f.commit(f.nested(ptr), base)
				f.ref("refs/heads/side", side)
				baseCommit, err := object.GetCommit(f.st, base)
				if err != nil {
					f.t.Fatalf("read base: %v", err)
				}
				f.ref(reachMain, f.commit(baseCommit.TreeHash, base, side))
				if err := f.r.DeleteBranch("side"); err != nil {
					f.t.Fatalf("delete side: %v", err)
				}
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f := newReachRepo(t)
				ptr := f.blob(pointerText(oid(1), 1))
				// Stored but never referenced, so only a scan that enumerates objects would list it.
				f.blob(pointerText(oid(2), 2))
				base := f.commit(f.tree(reachEntry("README.md", filemode.Regular, f.blob("hello\n"))))
				f.ref(reachMain, base)
				tc.wire(f, base, ptr)
				f.requirePointers(map[plumbing.Hash]string{ptr: oid(1)})
			})
		}
	})
	t.Run("SortedByBlobHash", func(t *testing.T) {
		f := newReachRepo(t)
		want := map[plumbing.Hash]string{}
		for i := range 8 {
			want[f.blob(pointerText(oid(i+1), i+1))] = oid(i + 1)
		}
		blobs := slices.Collect(maps.Keys(want))
		plumbing.HashesSort(blobs)
		var entries []object.TreeEntry
		for i, h := range blobs {
			// Names run against hash order, so the walk order is never the wanted one.
			entries = append(entries, reachEntry(fmt.Sprintf("f%d", len(blobs)-1-i), filemode.Regular, h))
		}
		f.ref(reachMain, f.commit(f.tree(entries...)))
		f.requirePointers(want)
	})
	t.Run("Unreachable", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			drop func(f *reachRepo, base plumbing.Hash)
		}{
			{"DeletedBranch", func(f *reachRepo, _ plumbing.Hash) {
				if err := f.r.DeleteBranch("dev"); err != nil {
					f.t.Fatalf("delete dev: %v", err)
				}
			}},
			{"RewoundBranch", func(f *reachRepo, base plumbing.Hash) {
				f.ref("refs/heads/dev", base)
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f := newReachRepo(t)
				live := f.blob(pointerText(oid(1), 1))
				base := f.commit(f.nested(live))
				f.ref(reachMain, base)
				f.ref("refs/heads/dev", f.commit(f.tree(reachEntry("dead.bin", filemode.Regular, f.blob(pointerText(oid(2), 2)))), base))
				tc.drop(f, base)
				f.requirePointers(map[plumbing.Hash]string{live: oid(1)})
			})
		}
	})
	t.Run("Except", func(t *testing.T) {
		f := newReachRepo(t)
		kept, skipped := f.blob(pointerText(oid(1), 1)), f.blob(pointerText(oid(2), 2))
		f.ref(reachMain, f.commit(f.tree(reachEntry("a.bin", filemode.Regular, kept), reachEntry("b.bin", filemode.Regular, skipped))))
		f.requirePointers(map[plumbing.Hash]string{kept: oid(1)}, skipped)
		if reads, sizes := f.st.calls(skipped); reads != 0 || sizes != 0 {
			t.Fatalf("excepted blob read %d and sized %d times; want neither", reads, sizes)
		}
	})
	t.Run("Missing", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			build func(f *reachRepo, ptr plumbing.Hash)
		}{
			{"Subtree", func(f *reachRepo, ptr plumbing.Hash) {
				sub := f.tree(reachEntry("model.bin", filemode.Regular, ptr))
				f.ref(reachMain, f.commit(f.tree(reachEntry("nested", filemode.Dir, sub))))
				f.st.remove(sub)
			}},
			{"Blob", func(f *reachRepo, ptr plumbing.Hash) {
				f.ref(reachMain, f.commit(f.nested(ptr)))
				f.st.remove(ptr)
			}},
			{"Parent", func(f *reachRepo, ptr plumbing.Hash) {
				parent := f.commit(f.nested(ptr))
				f.ref(reachMain, f.commit(f.tree(reachEntry("README.md", filemode.Regular, f.blob("hello\n"))), parent))
				f.st.remove(parent)
			}},
			{"RefTarget", func(f *reachRepo, ptr plumbing.Hash) {
				f.ref(reachMain, f.commit(f.nested(ptr)))
				f.ref("refs/heads/x", plumbing.NewHash(strings.Repeat("1", 40)))
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f := newReachRepo(t)
				tc.build(f, f.blob(pointerText(oid(1), 1)))
				if _, err := f.r.ScanLFSPointers(t.Context()); !errors.Is(err, plumbing.ErrObjectNotFound) {
					t.Fatalf("scan = %v, want an error wrapping plumbing.ErrObjectNotFound", err)
				}
			})
		}
	})
	t.Run("ReadError", func(t *testing.T) {
		f := newReachRepo(t)
		ptr := f.blob(pointerText(oid(1), 1))
		f.ref(reachMain, f.commit(f.nested(ptr)))
		injected := errors.New("injected blob read failure")
		f.st.fail[ptr] = injected
		if _, err := f.r.ScanLFSPointers(t.Context()); !errors.Is(err, injected) {
			t.Fatalf("scan = %v, want the blob read error surfaced", err)
		}
	})
	t.Run("OversizedNotRead", func(t *testing.T) {
		f := newReachRepo(t)
		ptr, big := f.blob(pointerText(oid(1), 1)), f.blob(strings.Repeat("x", lfs.MaxLFSPointerSize))
		f.ref(reachMain, f.commit(f.tree(reachEntry("big.bin", filemode.Regular, big), reachEntry("model.bin", filemode.Regular, ptr))))
		f.requirePointers(map[plumbing.Hash]string{ptr: oid(1)})
		if reads, _ := f.st.calls(big); reads != 0 {
			t.Fatalf("blob of %d bytes read %d times; want its content never read", lfs.MaxLFSPointerSize, reads)
		}
	})
	t.Run("DedupBlob", func(t *testing.T) {
		f := newReachRepo(t)
		ptr := f.blob(pointerText(oid(1), 1))
		first := f.commit(f.tree(reachEntry("a.bin", filemode.Regular, ptr), reachEntry("b.bin", filemode.Regular, ptr)))
		f.ref(reachMain, f.commit(f.tree(reachEntry("a.bin", filemode.Regular, ptr), reachEntry("b.bin", filemode.Regular, ptr), reachEntry("c.bin", filemode.Regular, ptr)), first))
		f.requirePointers(map[plumbing.Hash]string{ptr: oid(1)})
		if reads, _ := f.st.calls(ptr); reads != 1 {
			t.Fatalf("pointer blob named by three files in two commits read %d times; want once", reads)
		}
	})
	t.Run("NonPointers", func(t *testing.T) {
		f := newReachRepo(t)
		var entries []object.TreeEntry
		for name, content := range map[string]string{
			"empty":      "",
			"text.txt":   "hello\n",
			"binary.bin": "\x00\xff\xfe\x01",
			"truncated":  "version https://git-lfs.github.com/spec/v1\n",
		} {
			entries = append(entries, reachEntry(name, filemode.Regular, f.blob(content)))
		}
		f.ref(reachMain, f.commit(f.tree(entries...)))
		f.requirePointers(nil)
	})
	t.Run("EmptyRepository", func(t *testing.T) {
		ptrs, err := initTestRepo(t).ScanLFSPointers(t.Context())
		if err != nil || ptrs == nil || len(ptrs) != 0 {
			t.Fatalf("scan of a repository without commits = %v, %v; want an empty list", ptrs, err)
		}
	})
	t.Run("Cancelled", func(t *testing.T) {
		f := newReachRepo(t)
		f.ref(reachMain, f.commit(f.nested(f.blob(pointerText(oid(1), 1)))))
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := f.r.ScanLFSPointers(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("scan with a cancelled context = %v, want context.Canceled", err)
		}
	})
	// The boundary's parent was never fetched: the scan reads the boundary's own tree and stops there.
	t.Run("Shallow", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			bind bool
		}{{"Unbound", false}, {"Bound", true}} {
			t.Run(tc.name, func(t *testing.T) {
				fs := osfs.New(t.TempDir())
				if tc.bind {
					fs = sharedTestBind(fs)
				}
				repo := sharedTestInit(t, fs, "/repo.git")
				first := mustCommit(t, repo, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "README.md", Content: []byte("hello\n")})
				tip := mustCommit(t, repo, "main", first, CommitOperation{Type: CommitOperationAdd, Path: "model.bin", Content: []byte(pointerText(oid(1), 1))})
				repo = reachTestShallow(t, fs, "/repo.git", tip, first)
				ptrs, err := repo.ScanLFSPointers(t.Context())
				var got []string
				for _, p := range ptrs {
					got = append(got, p.OID())
				}
				if err != nil || !slices.Equal(got, []string{oid(1)}) {
					t.Fatalf("scan = %v, %v; want [%s] from the boundary's tree", got, err, oid(1))
				}
				if usage, err := repo.DiskUsage(t.Context()); err != nil || usage <= 0 {
					t.Fatalf("DiskUsage = %d, %v; want a positive size", usage, err)
				}
			})
		}
	})
}

// reachTestShallow cuts history below tip as a depth-1 fetch would, then reopens repoPath so no cache still holds parent.
func reachTestShallow(t *testing.T, fs billy.Filesystem, repoPath, tip, parent string) *Repository {
	t.Helper()
	if err := util.WriteFile(fs, path.Join(repoPath, "shallow"), []byte(tip+"\n"), 0o644); err != nil {
		t.Fatalf("write shallow: %v", err)
	}
	store, objects := fs, path.Join(repoPath, "objects")
	if bound, ok := fs.(*sharedObjectsFS); ok {
		store, objects = bound.dataFS, path.Join(bound.objectsDir, "objects")
	}
	if err := store.Remove(path.Join(objects, parent[:2], parent[2:])); err != nil {
		t.Fatalf("remove commit %s: %v", parent, err)
	}
	return sharedTestReopen(t, fs, repoPath)
}
