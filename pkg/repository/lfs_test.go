package repository

import (
	"errors"
	"io"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/memory"
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
	if _, err := repo.ScanLFSPointers(); !errors.Is(err, errUnreadable) {
		t.Fatalf("scan: got %v, want the blob read error surfaced", err)
	}
}
