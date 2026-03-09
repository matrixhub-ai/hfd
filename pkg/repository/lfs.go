package repository

import (
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/wzshiming/hfd/pkg/lfs"
)

// ScanLFSPointers scans all branches in the repository for LFS pointer files
// and returns a list of unique LFS pointers
func (r *Repository) ScanLFSPointers() ([]*lfs.Pointer, error) {
	// Get all branches
	branches, err := r.repo.Branches()
	if err != nil {
		return nil, err
	}

	seen := map[plumbing.Hash]bool{}

	result := []*lfs.Pointer{}

	err = branches.ForEach(func(ref *plumbing.Reference) error {
		commit, err := r.repo.CommitObject(ref.Hash())
		if err != nil {
			// Skip branches with inaccessible commits, continue with others
			return nil
		}

		tree, err := commit.Tree()
		if err != nil {
			// Skip branches with inaccessible trees, continue with others
			return nil
		}

		// Walk all files in the tree
		walker := object.NewTreeWalker(tree, true, seen)
		defer walker.Close()

		for {
			_, entry, err := walker.Next()
			if err != nil {
				break
			}

			seen[entry.Hash] = true

			if !entry.Mode.IsFile() {
				continue
			}

			blob, err := r.repo.BlobObject(entry.Hash)
			if err != nil {
				continue
			}

			if blob.Size > lfs.MaxLFSPointerSize {
				continue
			}

			reader, err := blob.Reader()
			if err != nil {
				continue
			}

			ptr, err := lfs.DecodePointer(reader)
			_ = reader.Close()
			if err != nil || ptr == nil {
				continue
			}

			result = append(result, ptr)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}
