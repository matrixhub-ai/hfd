package repository

import (
	"fmt"
	"io"
	"path"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// EntryType represents the type of a tree entry, either a file or a directory.
type EntryType string

const (
	EntryTypeFile      EntryType = "file"
	EntryTypeDirectory EntryType = "directory"
)

// TreeEntry represents a file or directory in the repository.
type TreeEntry struct {
	hash       Hash
	path       string
	entryType  EntryType
	lastCommit *Commit
	r          *Repository
}

// Hash returns the Git object hash of the tree entry.
func (e *TreeEntry) Hash() Hash { return e.hash }

// Path returns the file path of the entry relative to the repository root.
func (e *TreeEntry) Path() string { return e.path }

// Type returns the type of the entry (file or directory).
func (e *TreeEntry) Type() EntryType { return e.entryType }

// LastCommit returns the last commit that modified this entry, or nil if not expanded.
func (e *TreeEntry) LastCommit() *Commit { return e.lastCommit }

// Blob returns a Blob object for this entry if it is a file, or an error if it is a directory or if there was an issue retrieving the blob.
func (e *TreeEntry) Blob() (*Blob, error) {
	if e.entryType != EntryTypeFile {
		return nil, fmt.Errorf("entry is not a file")
	}

	blob, err := e.r.repo.BlobObject(e.hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get blob object: %w", err)
	}

	return &Blob{
		name:      path.Base(e.path),
		size:      blob.Size,
		modTime:   e.lastCommit.commit.Committer.When,
		newReader: func() (io.ReadCloser, error) { return blob.Reader() },
		hash:      blob.Hash,
		r:         e.r,
	}, nil
}

// TreeOptions provides options for the HFTree method.
type TreeOptions struct {
	// Recursive enables recursive traversal of subdirectories.
	Recursive bool
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
			entryBlob, err := entry.Blob()
			if err != nil {
				return 0, fmt.Errorf("failed to get blob for entry %q: %w", entry.Path(), err)
			}
			if ptr, _ := entryBlob.LFSPointer(); ptr != nil {
				total += ptr.Size()
			} else {
				total += entryBlob.Size()
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
				hash:      entry.Hash,
				path:      entryPath,
				entryType: EntryTypeFile,
				r:         r,
			}

			hfentry.lastCommit = &Commit{r: r, commit: commit}

			if err := cb(&hfentry); err != nil {
				return err
			}
		} else {
			hfentry := TreeEntry{
				hash:      entry.Hash,
				path:      entryPath,
				entryType: EntryTypeDirectory,
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
