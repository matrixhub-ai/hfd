package repository

import (
	"fmt"
	"path"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/matrixhub-ai/hfd/pkg/lfs"
)

// EntryType represents the type of a tree entry, either a file or a directory.
type EntryType string

const (
	EntryTypeFile      EntryType = "file"
	EntryTypeDirectory EntryType = "directory"
)

// TreeEntry represents a file or directory in the repository.
type TreeEntry struct {
	oid        string
	path       string
	entryType  EntryType
	size       int64
	lfs        *lfs.Pointer
	lastCommit *Commit
}

// OID returns the Git object ID of the entry.
func (e *TreeEntry) OID() string { return e.oid }

// Path returns the file path of the entry relative to the repository root.
func (e *TreeEntry) Path() string { return e.path }

// Type returns the type of the entry (file or directory).
func (e *TreeEntry) Type() EntryType { return e.entryType }

// Size returns the size of the entry in bytes.
func (e *TreeEntry) Size() int64 { return e.size }

// LFSPointer returns the LFSPointer pointer information if the entry is an LFSPointer-tracked file, or nil otherwise.
func (e *TreeEntry) LFSPointer() *lfs.Pointer { return e.lfs }

// LastCommit returns the last commit that modified this entry, or nil if not expanded.
func (e *TreeEntry) LastCommit() *Commit { return e.lastCommit }

// TreeOptions provides options for the HFTree method.
type TreeOptions struct {
	// Recursive enables recursive traversal of subdirectories.
	Recursive bool
}

// blobMetadata populates the Size and LFS fields for a file entry.
func (r *Repository) blobMetadata(hfentry *TreeEntry) {
	hash := plumbing.NewHash(hfentry.oid)
	blob, err := r.repo.BlobObject(hash)
	if err != nil {
		return
	}
	hfentry.size = blob.Size
	if blob.Size <= lfs.MaxLFSPointerSize {
		reader, err := blob.Reader()
		if err != nil {
			return
		}
		ptr, err := lfs.DecodePointer(reader)
		_ = reader.Close()
		if err == nil && ptr != nil {
			hfentry.lfs = ptr
		}
	}
}

// Tree returns the list of files and directories at the given revision and path, with options for recursive traversal and metadata expansion.
func (r *Repository) Tree(rev string, path string, opts *TreeOptions) ([]*TreeEntry, error) {
	if rev == "" {
		rev = r.DefaultBranch()
	}

	if opts == nil {
		opts = &TreeOptions{}
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

	if path != "" {
		entry, err := tree.FindEntry(path)
		if err != nil {
			return nil, fmt.Errorf("path not found: %w", err)
		}

		if entry.Mode.IsFile() {
			return nil, fmt.Errorf("path is not a directory")
		}

		tree, err = r.repo.TreeObject(entry.Hash)
		if err != nil {
			return nil, fmt.Errorf("failed to get subtree object: %w", err)
		}
	}

	var entries []*TreeEntry
	err = r.walkTree(commit, tree, path, opts, func(entry *TreeEntry) error {
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return entries, nil
}

// TreeSize returns the total size of all files under the given path at the given rev.
func (r *Repository) TreeSize(rev string, treePath string) (int64, error) {
	entries, err := r.Tree(rev, treePath, &TreeOptions{Recursive: true})
	if err != nil {
		return 0, err
	}

	var total int64
	for _, entry := range entries {
		if entry.Type() == EntryTypeFile {
			if entry.LFSPointer() != nil {
				total += entry.LFSPointer().Size()
			} else {
				total += entry.Size()
			}
		}
	}
	return total, nil
}

// walkTree recursively walks a tree and returns all entries.
func (r *Repository) walkTree(commit *object.Commit, tree *object.Tree, basePath string, opts *TreeOptions, cb func(entry *TreeEntry) error) error {
	for _, entry := range tree.Entries {
		entryPath := path.Join(basePath, entry.Name)
		if entry.Mode.IsFile() {
			hfentry := TreeEntry{
				oid:       entry.Hash.String(),
				path:      entryPath,
				entryType: EntryTypeFile,
			}

			r.blobMetadata(&hfentry)
			hfentry.lastCommit = &Commit{r: r, commit: commit}

			if err := cb(&hfentry); err != nil {
				return err
			}
		} else {
			hfentry := TreeEntry{
				oid:       entry.Hash.String(),
				path:      entryPath,
				entryType: EntryTypeDirectory,
				size:      0,
			}
			hfentry.lastCommit = &Commit{r: r, commit: commit}

			if err := cb(&hfentry); err != nil {
				return err
			}

			if opts.Recursive {
				subTree, err := r.repo.TreeObject(entry.Hash)
				if err == nil {
					err = r.walkTree(commit, subTree, entryPath, opts, cb)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
