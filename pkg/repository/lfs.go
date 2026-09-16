package repository

import (
	"bytes"
	"fmt"
	"io"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
)

// ScanLFSPointers returns LFS pointers from unique blobs reachable from
// repository refs: commits and their ancestors, or directly tagged trees and blobs.
func (r *Repository) ScanLFSPointers() ([]*lfs.Pointer, error) {
	refs, err := r.repo.References()
	if err != nil {
		return nil, fmt.Errorf("failed to get references: %w", err)
	}

	result := []*lfs.Pointer{}
	seenCommits := map[plumbing.Hash]bool{}
	seenTrees := map[plumbing.Hash]bool{}
	seenBlobs := map[plumbing.Hash]bool{}
	scanBlob := func(hash plumbing.Hash) error {
		if seenBlobs[hash] {
			return nil
		}
		seenBlobs[hash] = true
		obj, err := r.repo.BlobObject(hash)
		if err != nil {
			return fmt.Errorf("read blob %s: %w", hash, err)
		}
		if obj.Size > lfs.MaxLFSPointerSize {
			return nil
		}
		reader, err := obj.Reader()
		if err != nil {
			return fmt.Errorf("read blob %s: %w", hash, err)
		}
		data, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			return fmt.Errorf("read blob %s: %w", hash, err)
		}
		ptr, _ := lfs.DecodePointer(bytes.NewReader(data))
		if ptr != nil {
			result = append(result, ptr)
		}
		return nil
	}
	var scanTree func(*object.Tree) error
	scanTree = func(tree *object.Tree) error {
		for _, entry := range tree.Entries {
			if entry.Mode == filemode.Dir {
				if seenTrees[entry.Hash] {
					continue
				}
				seenTrees[entry.Hash] = true
				subtree, err := object.GetTree(r.repo.Storer, entry.Hash)
				if err != nil {
					return fmt.Errorf("read tree %s: %w", entry.Hash, err)
				}
				if err := scanTree(subtree); err != nil {
					return err
				}
				continue
			}
			if !entry.Mode.IsFile() {
				continue
			}
			if err := scanBlob(entry.Hash); err != nil {
				return err
			}
		}
		return nil
	}
	err = refs.ForEach(func(ref *plumbing.Reference) error {
		if ref.Type() != plumbing.HashReference {
			return nil
		}
		target, err := object.GetObject(r.repo.Storer, ref.Hash())
		if err != nil {
			return fmt.Errorf("ref %s: %w", ref.Name(), err)
		}
		for target.Type() == plumbing.TagObject {
			target, err = object.GetObject(r.repo.Storer, target.(*object.Tag).Target)
			if err != nil {
				return fmt.Errorf("ref %s: %w", ref.Name(), err)
			}
		}
		switch target := target.(type) {
		case *object.Blob:
			return scanBlob(target.Hash)
		case *object.Tree:
			if seenTrees[target.Hash] {
				return nil
			}
			seenTrees[target.Hash] = true
			return scanTree(target)
		case *object.Commit:
			commits := object.NewCommitPreorderIter(target, seenCommits, nil)
			defer commits.Close()
			return commits.ForEach(func(commit *object.Commit) error {
				seenCommits[commit.Hash] = true
				if seenTrees[commit.TreeHash] {
					return nil
				}
				tree, err := commit.Tree()
				if err != nil {
					return err
				}
				seenTrees[tree.Hash] = true
				return scanTree(tree)
			})
		}
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
