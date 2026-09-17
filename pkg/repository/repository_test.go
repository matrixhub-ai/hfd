package repository

import (
	"context"
	"errors"
	iofs "io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/memory"

	"github.com/matrixhub-ai/hfd/internal/lru"
)

func TestDiskUsage(t *testing.T) {
	dir, err := os.MkdirTemp("", "repo-diskusage-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	repo, err := Init(context.Background(), osfs.Default, dir, "main")
	if err != nil {
		t.Fatalf("Failed to init repo: %v", err)
	}

	usage, err := repo.DiskUsage(t.Context())
	if err != nil {
		t.Fatalf("DiskUsage returned error: %v", err)
	}
	if usage <= 0 {
		t.Errorf("Expected DiskUsage > 0 for non-empty repo dir, got %d", usage)
	}

	// Add a file directly to the repo directory and verify usage increases.
	testFile := filepath.Join(dir, "testfile")
	content := make([]byte, 1024)
	if err := os.WriteFile(testFile, content, 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	usage2, err := repo.DiskUsage(t.Context())
	if err != nil {
		t.Fatalf("DiskUsage returned error after adding file: %v", err)
	}
	if usage2 <= usage {
		t.Errorf("Expected DiskUsage to increase after adding a file: before=%d, after=%d", usage, usage2)
	}
}

func TestDiskUsageIncludesLFSSize(t *testing.T) {
	dir, err := os.MkdirTemp("", "repo-diskusage-lfs-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	repo, err := Init(context.Background(), osfs.Default, dir, "main")
	if err != nil {
		t.Fatalf("Failed to init repo: %v", err)
	}

	// Commit a regular file first to get a baseline
	if _, err := repo.CreateCommit(context.Background(), "main", "init", "Test", "test@test.com",
		[]CommitOperation{{Type: CommitOperationAdd, Path: "README.md", Content: []byte("# Test\n")}}, ""); err != nil {
		t.Fatalf("Failed to create initial commit: %v", err)
	}

	usageBefore, err := repo.DiskUsage(t.Context())
	if err != nil {
		t.Fatalf("DiskUsage returned error: %v", err)
	}

	// Commit an LFS pointer file with a declared size of 10 MB
	const lfsSize = 10 * 1024 * 1024
	lfsPointer := "version https://git-lfs.github.com/spec/v1\n" +
		"oid sha256:4d7a214614ab2935c943f9e0ff69d22eadbb8f32b1258daaa5e2ca24d17e2393\n" +
		"size 10485760\n"

	if _, err := repo.CreateCommit(context.Background(), "main", "add lfs file", "Test", "test@test.com",
		[]CommitOperation{{Type: CommitOperationAdd, Path: "model.bin", Content: []byte(lfsPointer)}}, ""); err != nil {
		t.Fatalf("Failed to commit LFS pointer: %v", err)
	}

	usageAfter, err := repo.DiskUsage(t.Context())
	if err != nil {
		t.Fatalf("DiskUsage returned error after LFS commit: %v", err)
	}

	// The declared LFS size is 10 MB; usage must increase by at least that amount.
	if usageAfter-usageBefore < lfsSize {
		t.Errorf("Expected DiskUsage to include LFS size (%d): before=%d, after=%d, delta=%d",
			lfsSize, usageBefore, usageAfter, usageAfter-usageBefore)
	}
}

func TestWalk(t *testing.T) {
	ctx := context.Background()
	mustInit := func(t *testing.T, fs billy.Filesystem, paths ...string) {
		t.Helper()
		for _, p := range paths {
			if _, err := Init(ctx, fs, p, "main"); err != nil {
				t.Fatalf("Init(%s): %v", p, err)
			}
		}
	}

	t.Run("Layouts", func(t *testing.T) {
		fs := osfs.New(t.TempDir())
		mustInit(t, fs, "/repo.git", "/org/repo.git", "/datasets/org/ds.git", "/org/a/b/c.git", "/ns.git/repo.git")
		var got []string
		err := Walk(ctx, fs, "/", func(path string) error {
			got = append(got, path)
			return nil
		})
		if err != nil {
			t.Fatalf("Walk: %v", err)
		}
		want := []string{"/datasets/org/ds.git", "/ns.git/repo.git", "/org/a/b/c.git", "/org/repo.git", "/repo.git"}
		if !slices.Equal(got, want) {
			t.Fatalf("Walk yielded %v, want %v", got, want)
		}
	})

	t.Run("Noise", func(t *testing.T) {
		dir := t.TempDir()
		fs := osfs.New(dir)
		if err := os.WriteFile(filepath.Join(dir, "README"), []byte("hi"), 0o644); err != nil {
			t.Fatal(err)
		}
		for _, d := range []string{"/empty", "/empty.git"} {
			if err := fs.MkdirAll(d, 0o755); err != nil {
				t.Fatal(err)
			}
		}
		mustInit(t, fs, "/org/repo.git")
		var got []string
		err := Walk(ctx, fs, "/", func(path string) error {
			got = append(got, path)
			return nil
		})
		if err != nil {
			t.Fatalf("Walk: %v", err)
		}
		if want := []string{"/org/repo.git"}; !slices.Equal(got, want) {
			t.Fatalf("Walk yielded %v, want %v", got, want)
		}
	})

	t.Run("MissingRoot", func(t *testing.T) {
		fs := osfs.New(t.TempDir())
		err := Walk(ctx, fs, "/missing", func(path string) error {
			t.Errorf("fn called with %s", path)
			return nil
		})
		if !errors.Is(err, iofs.ErrNotExist) {
			t.Fatalf("Walk = %v, want ErrNotExist", err)
		}
	})

	t.Run("Abort", func(t *testing.T) {
		fs := osfs.New(t.TempDir())
		mustInit(t, fs, "/a/x.git", "/b/y.git")
		errStop := errors.New("stop")
		calls := 0
		err := Walk(ctx, fs, "/", func(string) error {
			calls++
			return errStop
		})
		if err != errStop {
			t.Fatalf("Walk = %v, want errStop unwrapped", err)
		}
		if calls != 1 {
			t.Fatalf("fn called %d times, want 1", calls)
		}
	})

	t.Run("Canceled", func(t *testing.T) {
		fs := osfs.New(t.TempDir())
		mustInit(t, fs, "/org/repo.git")
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		err := Walk(cctx, fs, "/", func(path string) error {
			t.Errorf("fn called with %s after cancel", path)
			return nil
		})
		if err != context.Canceled {
			t.Fatalf("Walk = %v, want context.Canceled", err)
		}
	})

	t.Run("RootIsRepo", func(t *testing.T) {
		fs := osfs.New(t.TempDir())
		mustInit(t, fs, "/org/repo.git")
		var got []string
		err := Walk(ctx, fs, "/org/repo.git", func(path string) error {
			got = append(got, path)
			return nil
		})
		if err != nil {
			t.Fatalf("Walk: %v", err)
		}
		if want := []string{"/org/repo.git"}; !slices.Equal(got, want) {
			t.Fatalf("Walk yielded %v, want %v", got, want)
		}
	})

	t.Run("StopsAtRepository", func(t *testing.T) {
		fs := osfs.New(t.TempDir())
		mustInit(t, fs, "/org/a.git", "/org/a.git/nested.git", "/org/b.git/nested.git", "/org/b.git")
		var got []string
		err := Walk(ctx, fs, "/", func(path string) error {
			got = append(got, path)
			return nil
		})
		if err != nil {
			t.Fatalf("Walk: %v", err)
		}
		want := []string{"/org/a.git", "/org/b.git"}
		if !slices.Equal(got, want) {
			t.Fatalf("Walk yielded %v, want %v", got, want)
		}
		got = nil
		err = Walk(ctx, fs, "/org/a.git", func(path string) error {
			got = append(got, path)
			return nil
		})
		if err != nil {
			t.Fatalf("Walk: %v", err)
		}
		if want := want[:1]; !slices.Equal(got, want) {
			t.Fatalf("Walk from a repository yielded %v, want %v", got, want)
		}
	})

	t.Run("HEADStatError", func(t *testing.T) {
		fs := &headFaultFS{Filesystem: osfs.New(t.TempDir()), err: errors.New("unreadable")}
		mustInit(t, fs, "/a/x.git", "/b/y.git")
		fs.head = "/b/y.git/HEAD"
		var got []string
		err := Walk(ctx, fs, "/", func(path string) error {
			got = append(got, path)
			return nil
		})
		if !errors.Is(err, fs.err) {
			t.Fatalf("Walk = %v, want the HEAD stat error", err)
		}
		if want := []string{"/a/x.git"}; !slices.Equal(got, want) {
			t.Fatalf("Walk yielded %v, want %v", got, want)
		}
	})

	t.Run("LeavesLRUOrder", func(t *testing.T) {
		fs := osfs.New(t.TempDir())
		saved := lruCache
		lruCache = lru.New[cacheKey, *Repository](128)
		t.Cleanup(func() { lruCache = saved })
		// Init opens x then y, so x is the eviction candidate unless the walk (y first) touches the cache.
		mustInit(t, fs, "/b/x.git", "/a/y.git")
		if err := Walk(ctx, fs, "/", func(string) error { return nil }); err != nil {
			t.Fatalf("Walk: %v", err)
		}
		lruCache.RemoveOldest()
		if _, ok := lruCache.Get(cacheKey{fs, "/b/x.git"}); ok {
			t.Fatal("Walk promoted /b/x.git in the LRU: /a/y.git was evicted instead")
		}
	})
}

// headFaultFS fails Stat on one HEAD with a non-NotExist error.
type headFaultFS struct {
	billy.Filesystem
	head string
	err  error
}

func (h *headFaultFS) Stat(name string) (os.FileInfo, error) {
	if name == h.head {
		return nil, &os.PathError{Op: "stat", Path: name, Err: h.err}
	}
	return h.Filesystem.Stat(name)
}

// recordingStorage counts the objects read through EncodedObject.
type recordingStorage struct {
	*memory.Storage
	reads map[plumbing.Hash]int
}

func (s *recordingStorage) EncodedObject(kind plumbing.ObjectType, hash plumbing.Hash) (plumbing.EncodedObject, error) {
	s.reads[hash]++
	return s.Storage.EncodedObject(kind, hash)
}

// orderedRefs iterates references by name, reversed when desc is set.
type orderedRefs struct {
	*memory.Storage
	desc bool
}

func (s *orderedRefs) IterReferences() (storer.ReferenceIter, error) {
	iter, err := s.Storage.IterReferences()
	if err != nil {
		return nil, err
	}
	var refs []*plumbing.Reference
	if err := iter.ForEach(func(ref *plumbing.Reference) error {
		refs = append(refs, ref)
		return nil
	}); err != nil {
		return nil, err
	}
	slices.SortFunc(refs, func(a, b *plumbing.Reference) int { return strings.Compare(string(a.Name()), string(b.Name())) })
	if s.desc {
		slices.Reverse(refs)
	}
	return storer.NewReferenceSliceIter(refs), nil
}

func TestWalkObjects(t *testing.T) {
	add := func(path, content string) CommitOperation {
		return CommitOperation{Type: CommitOperationAdd, Path: path, Content: []byte(content)}
	}
	collect := func(t *testing.T, repo *Repository) map[plumbing.Hash]plumbing.ObjectType {
		t.Helper()
		got := map[plumbing.Hash]plumbing.ObjectType{}
		calls := 0
		err := repo.WalkObjects(t.Context(), func(hash plumbing.Hash, typ plumbing.ObjectType) error {
			calls++
			got[hash] = typ
			return nil
		})
		if err != nil {
			t.Fatalf("WalkObjects: %v", err)
		}
		if calls != len(got) {
			t.Errorf("fn called %d times for %d distinct objects", calls, len(got))
		}
		return got
	}
	encode := func(t *testing.T, st storer.EncodedObjectStorer, obj object.Object) plumbing.Hash {
		t.Helper()
		encoded := st.NewEncodedObject()
		if err := obj.Encode(encoded); err != nil {
			t.Fatal(err)
		}
		hash, err := st.SetEncodedObject(encoded)
		if err != nil {
			t.Fatal(err)
		}
		return hash
	}

	t.Run("Reachable", func(t *testing.T) {
		repo := initTestRepo(t)
		st := repo.repo.Storer
		mustCommit(t, repo, "main", "", add("a", "a1"), add("dir/b", "b1"))
		main := mustCommit(t, repo, "main", "", add("a", "a2"))
		branch := func(name string) plumbing.Hash {
			t.Helper()
			if err := repo.CreateBranch(name, main); err != nil {
				t.Fatal(err)
			}
			return plumbing.NewHash(mustCommit(t, repo, name, "", add(name, name)))
		}
		branch("dev")
		gone := branch("gone")
		tagged := branch("tagged")
		detached := branch("detached")
		for _, name := range []string{"gone", "tagged", "detached"} {
			if err := repo.DeleteBranch(name); err != nil {
				t.Fatal(err)
			}
		}
		// tagged stays reachable only through v2 -> v1 -> commit once refs/tags/v1 is gone.
		tagger := &object.Signature{Name: "t", Email: "t@t", When: time.Now()}
		v1, err := repo.repo.CreateTag("v1", tagged, &git.CreateTagOptions{Message: "v1", Tagger: tagger})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := repo.repo.CreateTag("v2", v1.Hash(), &git.CreateTagOptions{Message: "v2", Tagger: tagger}); err != nil {
			t.Fatal(err)
		}
		if err := repo.DeleteTag("v1"); err != nil {
			t.Fatal(err)
		}
		if err := st.SetReference(plumbing.NewHashReference(plumbing.HEAD, detached)); err != nil {
			t.Fatal(err)
		}
		blob := func(content string) plumbing.Hash {
			t.Helper()
			hash, err := repo.storeBlob([]byte(content))
			if err != nil {
				t.Fatal(err)
			}
			return hash
		}
		// The gitlink hash exists nowhere: loading or yielding it fails the walk.
		modes := &object.Tree{Entries: []object.TreeEntry{
			{Name: "exec", Mode: filemode.Executable, Hash: blob("exec")},
			{Name: "link", Mode: filemode.Symlink, Hash: blob("target")},
			{Name: "sub", Mode: filemode.Submodule, Hash: plumbing.NewHash(strings.Repeat("1", 40))},
		}}
		encoded := st.NewEncodedObject()
		if err := modes.Encode(encoded); err != nil {
			t.Fatal(err)
		}
		modesHash, err := st.SetEncodedObject(encoded)
		if err != nil {
			t.Fatal(err)
		}
		for name, target := range map[string]plumbing.Hash{"modes": modesHash, "blob": blob("lonely")} {
			if err := st.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName(name), target)); err != nil {
				t.Fatal(err)
			}
		}
		goneCommit, err := repo.repo.CommitObject(gone)
		if err != nil {
			t.Fatal(err)
		}
		unreachable := map[plumbing.Hash]bool{gone: true, goneCommit.TreeHash: true, plumbing.NewHash(sharedBlobHash([]byte("gone"))): true}
		want := map[plumbing.Hash]plumbing.ObjectType{}
		all, err := st.IterEncodedObjects(plumbing.AnyObject)
		if err != nil {
			t.Fatal(err)
		}
		err = all.ForEach(func(obj plumbing.EncodedObject) error {
			if !unreachable[obj.Hash()] {
				want[obj.Hash()] = obj.Type()
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, hash := range []plumbing.Hash{v1.Hash(), tagged, detached, modesHash} {
			if _, ok := want[hash]; !ok {
				t.Fatalf("fixture lost %s", hash)
			}
		}
		got := collect(t, repo)
		for hash, typ := range want {
			if got[hash] != typ {
				t.Errorf("%s %s yielded as %q", typ, hash, got[hash])
			}
		}
		for hash, typ := range got {
			if _, ok := want[hash]; !ok {
				t.Errorf("unreachable %s %s yielded", typ, hash)
			}
		}
	})

	t.Run("Abort", func(t *testing.T) {
		repo := initTestRepo(t)
		mustCommit(t, repo, "main", "", add("a", "a"))
		for _, errStop := range []error{errors.New("stop"), storer.ErrStop, errors.Join(errors.New("wrapped"), storer.ErrStop)} {
			calls := 0
			err := repo.WalkObjects(t.Context(), func(plumbing.Hash, plumbing.ObjectType) error {
				calls++
				return errStop
			})
			if err != errStop {
				t.Errorf("WalkObjects = %v, want %v unwrapped", err, errStop)
			}
			if calls != 1 {
				t.Fatalf("fn called %d times, want 1", calls)
			}
		}
	})

	t.Run("CanceledEmpty", func(t *testing.T) {
		repo := initTestRepo(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := repo.WalkObjects(ctx, func(plumbing.Hash, plumbing.ObjectType) error {
			t.Fatal("callback after cancellation")
			return nil
		})
		if err != context.Canceled {
			t.Fatalf("WalkObjects = %v, want context.Canceled", err)
		}
	})

	t.Run("CancelFromCallback", func(t *testing.T) {
		repo := initTestRepo(t)
		mustCommit(t, repo, "main", "", add("a", "a"))
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		err := repo.WalkObjects(ctx, func(_ plumbing.Hash, typ plumbing.ObjectType) error {
			if typ == plumbing.BlobObject {
				cancel()
			}
			return nil
		})
		if err != context.Canceled {
			t.Fatalf("WalkObjects = %v, want context.Canceled", err)
		}
	})

	t.Run("Canceled", func(t *testing.T) {
		repo := initTestRepo(t)
		mustCommit(t, repo, "main", "", add("a", "a"))
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := repo.WalkObjects(ctx, func(hash plumbing.Hash, typ plumbing.ObjectType) error {
			t.Errorf("fn called with %s %s after cancel", typ, hash)
			return nil
		})
		if err != context.Canceled {
			t.Fatalf("WalkObjects = %v, want context.Canceled", err)
		}
	})

	t.Run("MissingSubtree", func(t *testing.T) {
		store := &missingObjectStorage{Storage: memory.NewStorage()}
		gitRepo, err := git.Init(store, git.WithDefaultBranch(plumbing.NewBranchReferenceName("main")))
		if err != nil {
			t.Fatal(err)
		}
		repo := &Repository{repo: gitRepo}
		hash := mustCommit(t, repo, "main", "", add("dir/b", "b"), add("top", "t"))
		commit, err := repo.repo.CommitObject(plumbing.NewHash(hash))
		if err != nil {
			t.Fatal(err)
		}
		tree, err := commit.Tree()
		if err != nil {
			t.Fatal(err)
		}
		subtree, err := tree.Tree("dir")
		if err != nil {
			t.Fatal(err)
		}
		store.missing = subtree.Hash
		err = repo.WalkObjects(t.Context(), func(plumbing.Hash, plumbing.ObjectType) error { return nil })
		if !errors.Is(err, plumbing.ErrObjectNotFound) {
			t.Fatalf("WalkObjects = %v, want missing tree error", err)
		}
		for _, context := range []string{"refs/heads/main", subtree.Hash.String()} {
			if !strings.Contains(err.Error(), context) {
				t.Errorf("error %q lacks %s", err, context)
			}
		}
	})

	t.Run("BlobsUnread", func(t *testing.T) {
		store := &recordingStorage{Storage: memory.NewStorage(), reads: map[plumbing.Hash]int{}}
		gitRepo, err := git.Init(store, git.WithDefaultBranch(plumbing.NewBranchReferenceName("main")))
		if err != nil {
			t.Fatal(err)
		}
		repo := &Repository{repo: gitRepo}
		mustCommit(t, repo, "main", "", add("a", "a"), add("dir/b", "b"))
		clear(store.reads)
		blobs := 0
		for hash, typ := range collect(t, repo) {
			want := 1
			if typ == plumbing.BlobObject {
				blobs++
				want = 0
			}
			if store.reads[hash] != want {
				t.Errorf("%s %s read %d times, want %d", typ, hash, store.reads[hash], want)
			}
		}
		if blobs != 2 {
			t.Fatalf("yielded %d blobs, want 2", blobs)
		}
	})

	t.Run("EntryTypeConflict", func(t *testing.T) {
		// A tree naming sub as both blob and tree must fail in either entry order, not drop sub's leaf.
		for _, modes := range [][2]filemode.FileMode{{filemode.Regular, filemode.Dir}, {filemode.Dir, filemode.Regular}} {
			repo := initTestRepo(t)
			st := repo.repo.Storer
			leaf, err := repo.storeBlob([]byte("leaf"))
			if err != nil {
				t.Fatal(err)
			}
			sub := encode(t, st, &object.Tree{Entries: []object.TreeEntry{{Name: "b", Mode: filemode.Regular, Hash: leaf}}})
			root := encode(t, st, &object.Tree{Entries: []object.TreeEntry{
				{Name: "a", Mode: modes[0], Hash: sub},
				{Name: "z", Mode: modes[1], Hash: sub},
			}})
			if err := st.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("root"), root)); err != nil {
				t.Fatal(err)
			}
			yielded := map[plumbing.Hash]plumbing.ObjectType{}
			err = repo.WalkObjects(t.Context(), func(hash plumbing.Hash, typ plumbing.ObjectType) error {
				yielded[hash] = typ
				return nil
			})
			if err == nil {
				_, leafYielded := yielded[leaf]
				t.Fatalf("WalkObjects(%v) = nil, want type conflict; leaf yielded: %v", modes, leafYielded)
			}
			for _, context := range []string{"refs/tags/root", sub.String()} {
				if !strings.Contains(err.Error(), context) {
					t.Errorf("error %q lacks %s", err, context)
				}
			}
		}
	})

	t.Run("RefTypeConflict", func(t *testing.T) {
		// refs/heads/aaa's tree labels as a blob a hash another ref reaches as a commit parent, a tag root or a real blob.
		for _, tc := range []struct {
			name    string
			wantErr bool
		}{{"parent", true}, {"root", true}, {"blob", false}} {
			for _, aaaFirst := range []bool{true, false} {
				store := &orderedRefs{Storage: memory.NewStorage(), desc: !aaaFirst}
				gitRepo, err := git.Init(store, git.WithDefaultBranch(plumbing.NewBranchReferenceName("main")))
				if err != nil {
					t.Fatal(err)
				}
				repo := &Repository{repo: gitRepo}
				target := plumbing.NewHash(mustCommit(t, repo, "main", "", add("a", "a1")))
				mustCommit(t, repo, "main", "", add("a", "a2"))
				legit := plumbing.ReferenceName("refs/heads/main")
				if tc.name != "parent" {
					if target, err = repo.storeBlob([]byte("leaf")); err != nil {
						t.Fatal(err)
					}
					if tc.name == "root" {
						target = encode(t, store, &object.Tree{Entries: []object.TreeEntry{{Name: "b", Mode: filemode.Regular, Hash: target}}})
					}
					legit = plumbing.NewTagReferenceName(tc.name)
					if err := store.SetReference(plumbing.NewHashReference(legit, target)); err != nil {
						t.Fatal(err)
					}
				}
				labelled := encode(t, store, &object.Tree{Entries: []object.TreeEntry{{Name: "x", Mode: filemode.Regular, Hash: target}}})
				sig := object.Signature{Name: "t", Email: "t@t", When: time.Now()}
				commit := encode(t, store, &object.Commit{TreeHash: labelled, Author: sig, Committer: sig, Message: "x"})
				if err := store.SetReference(plumbing.NewHashReference("refs/heads/aaa", commit)); err != nil {
					t.Fatal(err)
				}
				yields := 0
				err = repo.WalkObjects(t.Context(), func(hash plumbing.Hash, _ plumbing.ObjectType) error {
					if hash == target {
						yields++
					}
					return nil
				})
				switch {
				case !tc.wantErr:
					if err != nil || yields != 1 {
						t.Errorf("%s aaaFirst=%v: WalkObjects = %v, target yielded %d times; want nil, once", tc.name, aaaFirst, err, yields)
					}
				case err == nil:
					t.Errorf("%s aaaFirst=%v: WalkObjects = nil, want type conflict", tc.name, aaaFirst)
				case !strings.Contains(err.Error(), target.String()) || !(strings.Contains(err.Error(), "refs/heads/aaa") || strings.Contains(err.Error(), string(legit))):
					t.Errorf("%s aaaFirst=%v: error %q lacks target hash or ref", tc.name, aaaFirst, err)
				}
			}
		}
	})

	t.Run("LinkTypeConflict", func(t *testing.T) {
		// refs/heads/zzz's commit parent or tag header declares one type for an object refs/heads/main stores as another;
		// the walk must fail whether main loaded the target first or zzz names it first.
		sig := object.Signature{Name: "t", Email: "t@t", When: time.Now()}
		parent := func(tree, target plumbing.Hash) object.Object {
			return &object.Commit{TreeHash: tree, ParentHashes: []plumbing.Hash{target}, Author: sig, Committer: sig, Message: "x"}
		}
		tag := func(typ plumbing.ObjectType) func(_, target plumbing.Hash) object.Object {
			return func(_, target plumbing.Hash) object.Object {
				return &object.Tag{Name: "x", Tagger: sig, Message: "x", TargetType: typ, Target: target}
			}
		}
		for _, tc := range []struct {
			name    string
			link    func(tree, target plumbing.Hash) object.Object
			target  string
			wantErr bool
		}{
			{"ParentBlob", parent, "blob", true},
			{"ParentTree", parent, "tree", true},
			{"TagCommitBlob", tag(plumbing.CommitObject), "blob", true},
			{"TagBlobTree", tag(plumbing.BlobObject), "tree", true},
			{"TagBlobMissing", tag(plumbing.BlobObject), "missing", true},
			{"TagBlob", tag(plumbing.BlobObject), "blob", false},
		} {
			for _, zzzFirst := range []bool{false, true} {
				store := &orderedRefs{Storage: memory.NewStorage(), desc: zzzFirst}
				gitRepo, err := git.Init(store, git.WithDefaultBranch(plumbing.NewBranchReferenceName("main")))
				if err != nil {
					t.Fatal(err)
				}
				repo := &Repository{repo: gitRepo}
				commit, err := repo.repo.CommitObject(plumbing.NewHash(mustCommit(t, repo, "main", "", add("a", "a1"))))
				if err != nil {
					t.Fatal(err)
				}
				blob, err := repo.storeBlob([]byte("a1"))
				if err != nil {
					t.Fatal(err)
				}
				target := map[string]plumbing.Hash{"tree": commit.TreeHash, "blob": blob, "missing": plumbing.NewHash(strings.Repeat("2", 40))}[tc.target]
				if err := store.SetReference(plumbing.NewHashReference("refs/heads/zzz", encode(t, store, tc.link(commit.TreeHash, target)))); err != nil {
					t.Fatal(err)
				}
				yields := 0
				err = repo.WalkObjects(t.Context(), func(hash plumbing.Hash, _ plumbing.ObjectType) error {
					if hash == target {
						yields++
					}
					return nil
				})
				switch {
				case !tc.wantErr:
					if err != nil || yields != 1 {
						t.Errorf("%s zzzFirst=%v: WalkObjects = %v, target yielded %d times; want nil, once", tc.name, zzzFirst, err, yields)
					}
				case err == nil:
					t.Errorf("%s zzzFirst=%v: WalkObjects = nil, want type conflict", tc.name, zzzFirst)
				case !strings.Contains(err.Error(), target.String()) || !strings.Contains(err.Error(), "refs/heads/zzz"):
					t.Errorf("%s zzzFirst=%v: error %q lacks target hash or ref", tc.name, zzzFirst, err)
				}
			}
		}
	})
}

func TestWalkObjectsGitBinary(t *testing.T) {
	dataDir := t.TempDir()
	bound := BindSharedObjects(osfs.New(dataDir), "/repositories", "/git/sha1")
	repo, err := Init(t.Context(), bound, "/org/repo.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	add := func(path, content string) CommitOperation {
		return CommitOperation{Type: CommitOperationAdd, Path: path, Content: []byte(content)}
	}
	mustCommit(t, repo, "main", "", add("a", "a1"), add("dir/b", "b1"))
	main := mustCommit(t, repo, "main", "", add("a", "a2"))
	branch := func(name string) string {
		t.Helper()
		if err := repo.CreateBranch(name, main); err != nil {
			t.Fatal(err)
		}
		return mustCommit(t, repo, name, "", add(name, name))
	}
	dev := branch("dev")
	tagged := branch("tagged")
	gone := branch("gone")
	if err := repo.CreateTag("v1", tagged); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"tagged", "gone"} {
		if err := repo.DeleteBranch(name); err != nil {
			t.Fatal(err)
		}
	}

	// Objects live only in the shared store, so git reaches them through the alternates file.
	want := map[string]bool{}
	for _, line := range strings.Split(gitOut(t, filepath.Join(dataDir, "repositories/org/repo.git"), "rev-list", "--objects", "--all"), "\n") {
		if fields := strings.Fields(line); len(fields) > 0 {
			want[fields[0]] = true
		}
	}
	for _, hash := range []string{main, dev, tagged, sharedBlobHash([]byte("b1")), sharedBlobHash([]byte("dev"))} {
		if !want[hash] {
			t.Fatalf("git rev-list lost %s", hash)
		}
	}
	for _, hash := range []string{gone, sharedBlobHash([]byte("gone"))} {
		if want[hash] {
			t.Fatalf("git rev-list still reaches deleted %s", hash)
		}
	}

	got := map[string]bool{}
	err = repo.WalkObjects(t.Context(), func(hash plumbing.Hash, _ plumbing.ObjectType) error {
		if got[hash.String()] {
			t.Errorf("%s yielded twice", hash)
		}
		got[hash.String()] = true
		return nil
	})
	if err != nil {
		t.Fatalf("WalkObjects: %v", err)
	}
	for hash := range want {
		if !got[hash] {
			t.Errorf("git reaches %s, WalkObjects skipped it", hash)
		}
	}
	for hash := range got {
		if !want[hash] {
			t.Errorf("WalkObjects yielded %s, git rev-list --all does not reach it", hash)
		}
	}
}
