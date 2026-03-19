package repository

import (
	"fmt"

	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
)

// ScanLFSPointers scans all branches in the repository for LFS pointer files
// and returns a list of unique LFS pointers
func (r *Repository) ScanLFSPointers() ([]*lfs.Pointer, error) {
	blobIter, err := r.repo.BlobObjects()
	if err != nil {
		return nil, fmt.Errorf("failed to get blob objects: %v", err)
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
