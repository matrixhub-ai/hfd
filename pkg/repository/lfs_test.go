package repository

import (
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage/memory"
)

var errUnreadable = errors.New("blob content unreadable")

// unreadableBlob is a stored blob whose content cannot be read back.
type unreadableBlob struct {
	plumbing.EncodedObject
}

func (unreadableBlob) Reader() (io.ReadCloser, error) { return nil, errUnreadable }

// Go-git's storers otherwise read small blobs eagerly.
type unreadableStorage struct {
	*memory.Storage
}

func (s *unreadableStorage) EncodedObject(kind plumbing.ObjectType, hash plumbing.Hash) (plumbing.EncodedObject, error) {
	obj, err := s.Storage.EncodedObject(kind, hash)
	if err != nil {
		return nil, err
	}
	if obj.Type() == plumbing.BlobObject {
		obj = unreadableBlob{obj}
	}
	return obj, nil
}

type missingObjectStorage struct {
	*memory.Storage
	missing plumbing.Hash
}

func (s *missingObjectStorage) EncodedObject(kind plumbing.ObjectType, hash plumbing.Hash) (plumbing.EncodedObject, error) {
	if hash == s.missing {
		return nil, plumbing.ErrObjectNotFound
	}
	return s.Storage.EncodedObject(kind, hash)
}

func TestScanLFSPointersMissingTreeFails(t *testing.T) {
	store := &missingObjectStorage{Storage: memory.NewStorage()}
	gitRepo, err := git.Init(store, git.WithDefaultBranch(plumbing.NewBranchReferenceName("main")))
	if err != nil {
		t.Fatal(err)
	}
	repo := &Repository{repo: gitRepo}
	hash := mustCommit(t, repo, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "dir/ptr", Content: fmt.Appendf(nil,
			"version https://git-lfs.github.com/spec/v1\noid sha256:%064x\nsize 123\n", 1)},
		CommitOperation{Type: CommitOperationAdd, Path: "top", Content: []byte("plain")})
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
	if _, err := repo.ScanLFSPointers(t.Context()); !errors.Is(err, plumbing.ErrObjectNotFound) {
		t.Fatalf("scan error = %v, want missing tree error", err)
	}
}

func TestScanLFSPointersSurfacesReadErrors(t *testing.T) {
	st := &unreadableStorage{Storage: memory.NewStorage()}
	r, err := git.Init(st, git.WithDefaultBranch(plumbing.NewBranchReferenceName("main")))
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	repo := &Repository{repo: r}
	mustCommit(t, repo, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "f", Content: []byte("not a pointer")})
	if _, err := repo.ScanLFSPointers(t.Context()); !errors.Is(err, errUnreadable) {
		t.Fatalf("scan: got %v, want the blob read error surfaced", err)
	}
}

func TestScanLFSPointersReachability(t *testing.T) {
	repo := initTestRepo(t)
	pointer := func(id int) CommitOperation {
		return CommitOperation{Type: CommitOperationAdd, Path: "nested/file", Content: fmt.Appendf(nil,
			"version https://git-lfs.github.com/spec/v1\noid sha256:%064x\nsize 123\n", id)}
	}
	mustCommit(t, repo, "main", "", pointer(1),
		CommitOperation{Type: CommitOperationAdd, Path: "plain", Content: []byte("not a pointer")})
	main := mustCommit(t, repo, "main", "", pointer(2))
	for index, branch := range []string{"dev", "tmp", "gone"} {
		if err := repo.CreateBranch(branch, main); err != nil {
			t.Fatalf("create branch %s: %v", branch, err)
		}
		commit := mustCommit(t, repo, branch, "", pointer(index+3))
		if branch == "tmp" {
			if _, err := repo.repo.CreateTag("v1", plumbing.NewHash(commit), &git.CreateTagOptions{
				Message: "v1",
				Tagger:  &object.Signature{Name: "t", Email: "t@t", When: time.Now()},
			}); err != nil {
				t.Fatalf("create annotated tag: %v", err)
			}
		}
		if branch != "dev" {
			if err := repo.DeleteBranch(branch); err != nil {
				t.Fatalf("delete branch %s: %v", branch, err)
			}
		}
	}
	pointers, err := repo.ScanLFSPointers(t.Context())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	counts := map[string]int{}
	for _, ptr := range pointers {
		counts[ptr.OID()]++
	}
	for id := 1; id <= 4; id++ {
		oid := fmt.Sprintf("%064x", id)
		if counts[oid] != 1 {
			t.Errorf("pointer %s: got %d, want 1", oid, counts[oid])
		}
	}
	if len(pointers) != 4 || len(counts) != 4 {
		t.Errorf("got %d pointers: %v, want only A, B, C, D", len(pointers), counts)
	}
}

func TestScanLFSPointersTagTargets(t *testing.T) {
	repo := initTestRepo(t)
	main := mustCommit(t, repo, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "plain", Content: []byte("not a pointer")})
	// Each pointer stays reachable only through a lightweight tag on a blob or a tree.
	for index, kind := range []string{"blob", "tree"} {
		if err := repo.CreateBranch(kind, main); err != nil {
			t.Fatalf("create branch %s: %v", kind, err)
		}
		hash := mustCommit(t, repo, kind, "", CommitOperation{Type: CommitOperationAdd, Path: "nested/" + kind,
			Content: fmt.Appendf(nil, "version https://git-lfs.github.com/spec/v1\noid sha256:%064x\nsize 123\n", index+1)})
		commit, err := repo.repo.CommitObject(plumbing.NewHash(hash))
		if err != nil {
			t.Fatal(err)
		}
		target := commit.TreeHash
		if kind == "blob" {
			tree, err := commit.Tree()
			if err != nil {
				t.Fatal(err)
			}
			entry, err := tree.FindEntry("nested/blob")
			if err != nil {
				t.Fatal(err)
			}
			target = entry.Hash
		}
		tag := plumbing.NewHashReference(plumbing.NewTagReferenceName(kind+"tag"), target)
		if err := repo.repo.Storer.SetReference(tag); err != nil {
			t.Fatalf("create %s tag: %v", kind, err)
		}
		if err := repo.DeleteBranch(kind); err != nil {
			t.Fatalf("delete branch %s: %v", kind, err)
		}
	}
	pointers, err := repo.ScanLFSPointers(t.Context())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	counts := map[string]int{}
	for _, ptr := range pointers {
		counts[ptr.OID()]++
	}
	for id := 1; id <= 2; id++ {
		oid := fmt.Sprintf("%064x", id)
		if counts[oid] != 1 {
			t.Errorf("pointer %s: got %d, want 1", oid, counts[oid])
		}
	}
	if len(pointers) != 2 {
		t.Errorf("got %d pointers: %v, want the blob-tagged and tree-tagged ones", len(pointers), counts)
	}
}
