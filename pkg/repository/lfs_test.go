package repository

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/memory"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
)

var errUnreadable = errors.New("blob content unreadable")

// unreadableBlob is a stored blob whose content cannot be read back.
type unreadableBlob struct {
	plumbing.EncodedObject
}

func (unreadableBlob) Reader() (io.ReadCloser, error) { return nil, errUnreadable }

// unreadableStorage hands out every blob as unreadable; go-git's own storers read small blobs
// eagerly, so this is the only way a Blob.Reader failure reaches ScanLFSPointers.
type unreadableStorage struct {
	*memory.Storage
}

func (s *unreadableStorage) IterEncodedObjects(t plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	iter, err := s.Storage.IterEncodedObjects(t)
	if err != nil {
		return nil, err
	}
	var objs []plumbing.EncodedObject
	err = iter.ForEach(func(obj plumbing.EncodedObject) error {
		if obj.Type() == plumbing.BlobObject {
			obj = unreadableBlob{obj}
		}
		objs = append(objs, obj)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return storer.NewEncodedObjectSliceIter(objs), nil
}

func TestScanLFSPointersSurfacesReadErrors(t *testing.T) {
	st := &unreadableStorage{Storage: memory.NewStorage()}
	r, err := git.Init(st, git.WithDefaultBranch(plumbing.NewBranchReferenceName("main")))
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	blob := st.NewEncodedObject()
	blob.SetType(plumbing.BlobObject)
	w, err := blob.Writer()
	if err != nil {
		t.Fatalf("blob writer: %v", err)
	}
	if _, err := w.Write([]byte("not a pointer")); err != nil {
		t.Fatalf("write blob: %v", err)
	}
	_ = w.Close()
	if _, err := st.SetEncodedObject(blob); err != nil {
		t.Fatalf("store blob: %v", err)
	}

	repo := &Repository{repo: r}
	if _, err := repo.ScanLFSPointers(context.Background()); !errors.Is(err, errUnreadable) {
		t.Fatalf("scan: got %v, want the blob read error surfaced", err)
	}
}

func storeBlob(t *testing.T, st storer.EncodedObjectStorer, content string) plumbing.Hash {
	t.Helper()
	blob := st.NewEncodedObject()
	blob.SetType(plumbing.BlobObject)
	w, err := blob.Writer()
	if err != nil {
		t.Fatalf("blob writer: %v", err)
	}
	if _, err := w.Write([]byte(content)); err != nil {
		t.Fatalf("write blob: %v", err)
	}
	_ = w.Close()
	h, err := st.SetEncodedObject(blob)
	if err != nil {
		t.Fatalf("store blob: %v", err)
	}
	return h
}

func pointerText(oid string, size int) string {
	return fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, size)
}

// Ignored blobs are skipped by Git hash before they are read: another blob naming the same OID still counts, nil skips nothing, cancellation surfaces.
func TestScanLFSPointersExcept(t *testing.T) {
	t.Run("EmptyCancelled", func(t *testing.T) {
		empty, err := git.Init(memory.NewStorage(), git.WithDefaultBranch(plumbing.NewBranchReferenceName("main")))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := (&Repository{repo: empty}).ScanLFSPointersExcept(ctx, nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("empty scan = %v, want context.Canceled", err)
		}
	})
	st := &unreadableStorage{Storage: memory.NewStorage()}
	r, err := git.Init(st, git.WithDefaultBranch(plumbing.NewBranchReferenceName("main")))
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	oid, other := strings.Repeat("a", 64), strings.Repeat("b", 64)
	first, second, third := storeBlob(t, st, pointerText(oid, 1)), storeBlob(t, st, pointerText(oid, 2)), storeBlob(t, st, pointerText(other, 3))
	oids := func(repo *Repository, ignored ...plumbing.Hash) []string {
		t.Helper()
		ptrs, err := repo.ScanLFSPointersExcept(t.Context(), ignored)
		if err != nil {
			t.Fatalf("scan ignoring %v: %v", ignored, err)
		}
		var got []string
		for _, p := range ptrs {
			got = append(got, p.OID())
		}
		slices.Sort(got)
		return got
	}

	// Every blob is unreadable here, so the scan only succeeds when no ignored blob is read.
	unreadable := &Repository{repo: r}
	if got := oids(unreadable, first, second, third); len(got) != 0 {
		t.Fatalf("scan ignoring every blob = %v, want none", got)
	}
	if _, err := unreadable.ScanLFSPointersExcept(t.Context(), nil); !errors.Is(err, errUnreadable) {
		t.Fatalf("scan ignoring nothing: got %v, want the blob read error surfaced", err)
	}

	rr, err := git.Open(st.Storage, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	readable := &Repository{repo: rr}
	if got := oids(readable); !slices.Equal(got, []string{oid, oid, other}) {
		t.Fatalf("scan ignoring nothing = %v, want every pointer", got)
	}
	if got := oids(readable, first); !slices.Equal(got, []string{oid, other}) {
		t.Fatalf("scan ignoring one of two blobs naming %s = %v, want the OID kept by the other blob", oid, got)
	}
	if got := oids(readable, first, second); !slices.Equal(got, []string{other}) {
		t.Fatalf("scan ignoring both blobs naming %s = %v, want only %s", oid, got, other)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := readable.ScanLFSPointersExcept(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("scan with cancelled context: got %v, want context.Canceled", err)
	}
}

// As in git-lfs, an empty blob is not a pointer and candidates stay below MaxLFSPointerSize.
func TestScanLFSPointersSizeBounds(t *testing.T) {
	st := memory.NewStorage()
	r, err := git.Init(st, git.WithDefaultBranch(plumbing.NewBranchReferenceName("main")))
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	kept, dropped := pointerText(strings.Repeat("c", 64), 5), pointerText(strings.Repeat("d", 64), 4)
	storeBlob(t, st, "")
	storeBlob(t, st, dropped+strings.Repeat(" ", lfs.MaxLFSPointerSize-len(dropped)))
	storeBlob(t, st, kept+strings.Repeat(" ", lfs.MaxLFSPointerSize-1-len(kept)))

	ptrs, err := (&Repository{repo: r}).ScanLFSPointersExcept(t.Context(), nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(ptrs) != 1 || ptrs[0].OID() != strings.Repeat("c", 64) {
		t.Fatalf("scan = %v, want only the pointer below the cutoff", ptrs)
	}
}
