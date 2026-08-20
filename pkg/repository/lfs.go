package repository

import (
	"fmt"
	"io"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
)

// ScanLFSPointers scans all branches in the repository for LFS pointer files
// and returns a list of unique LFS pointers
func (r *Repository) ScanLFSPointers() ([]*lfs.Pointer, error) {
	blobIter, err := r.repo.BlobObjects()
	if err != nil {
		return nil, fmt.Errorf("failed to get blob objects: %w", err)
	}

	result := []*lfs.Pointer{}
	err = blobIter.ForEach(func(obj *object.Blob) error {
		if obj.Size > lfs.MaxLFSPointerSize {
			return nil
		}

		ptr, _ := r.parseLFS(obj.Hash)
		if ptr == nil {
			return nil
		}

		result = append(result, ptr)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// LFSFile pairs an LFS pointer with the file path it occupies at a revision.
type LFSFile struct {
	Path    string
	Pointer *lfs.Pointer
}

// ListLFSPointers walks the tree at the given revision and returns every
// LFS-tracked file with its path.
func (r *Repository) ListLFSPointers(rev string) ([]LFSFile, error) {
	if rev == "" {
		rev = r.DefaultBranch()
	}

	hash, err := r.repo.ResolveRevision(plumbing.Revision(rev))
	if err != nil {
		return nil, fmt.Errorf("failed to resolve revision: %w", err)
	}

	commit, err := r.repo.CommitObject(*hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get commit object: %w", err)
	}

	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("failed to get tree object: %w", err)
	}

	walker := object.NewTreeWalker(tree, true, nil)
	defer walker.Close()

	var result []LFSFile
	for {
		name, entry, err := walker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to walk tree: %w", err)
		}
		if !entry.Mode.IsFile() {
			continue
		}
		ptr, _ := r.parseLFS(entry.Hash)
		if ptr == nil {
			continue
		}
		result = append(result, LFSFile{Path: name, Pointer: ptr})
	}
	return result, nil
}
