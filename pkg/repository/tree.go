package repository

import (
	"fmt"
	"path"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/wzshiming/hfd/pkg/lfs"
)

type EntryType string

const (
	EntryTypeFile      EntryType = "file"
	EntryTypeDirectory EntryType = "directory"
)

// TreeEntry represents a file or directory in the repository
type TreeEntry struct {
	OID        string            `json:"oid"`
	Path       string            `json:"path"`
	Type       EntryType         `json:"type"`
	Size       int64             `json:"size"`
	LFS        *TreeLFS          `json:"lfs,omitempty"`
	LastCommit *HFTreeLastCommit `json:"lastCommit,omitempty"`
}

type TreeLFS struct {
	OID         string `json:"oid"`
	Size        int64  `json:"size"`
	PointerSize int64  `json:"pointerSize"`
}

// HFTreeLastCommit represents the last commit that modified a file or directory
type HFTreeLastCommit struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Date  string `json:"date"`
}

// TreeOptions provides options for the HFTree method.
type TreeOptions struct {
	// Recursive enables recursive traversal of subdirectories.
	Recursive bool
	// Expand enables fetching additional metadata such as lastCommit info.
	Expand bool
}

// blobMetadata populates the Size and LFS fields for a file entry.
func (r *Repository) blobMetadata(hfentry *TreeEntry) {
	hash := plumbing.NewHash(hfentry.OID)
	blob, err := r.repo.BlobObject(hash)
	if err != nil {
		return
	}
	hfentry.Size = blob.Size
	if blob.Size <= lfs.MaxLFSPointerSize {
		reader, err := blob.Reader()
		if err != nil {
			return
		}
		ptr, err := lfs.DecodePointer(reader)
		_ = reader.Close()
		if err == nil && ptr != nil {
			hfentry.LFS = &TreeLFS{
				OID:         ptr.Oid,
				Size:        ptr.Size,
				PointerSize: blob.Size,
			}

			hfentry.Size = ptr.Size
		}
	}
}

// lastCommit fetches the last commit that modified the given path.
func (r *Repository) lastCommit(commit *object.Commit) (*HFTreeLastCommit, error) {
	// Get the commit log for this specific file
	iter, err := r.repo.Log(&git.LogOptions{
		From: commit.Hash,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get commit log: %w", err)
	}
	defer iter.Close()

	// Get the first (most recent) commit
	c, err := iter.Next()
	if err != nil {
		return nil, fmt.Errorf("failed to get last commit: %w", err)
	}

	// Extract just the first line of the commit message as the title
	title := c.Message
	if idx := strings.Index(title, "\n"); idx >= 0 {
		title = title[:idx]
	}

	return &HFTreeLastCommit{
		ID:    c.Hash.String(),
		Title: title,
		Date:  c.Author.When.UTC().Format(TimeFormat),
	}, nil
}

func (r *Repository) Tree(rev string, path string, opts *TreeOptions) ([]TreeEntry, error) {
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

	var entries []TreeEntry
	err = r.walkTree(commit, tree, path, opts, func(entry *TreeEntry) error {
		entries = append(entries, *entry)
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
		if entry.Type == EntryTypeFile {
			if entry.LFS != nil {
				total += entry.LFS.Size
			} else {
				total += entry.Size
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
				OID:  entry.Hash.String(),
				Path: entryPath,
				Type: EntryTypeFile,
			}

			r.blobMetadata(&hfentry)
			if opts.Expand {
				lastCommit, err := r.lastCommit(commit)
				if err != nil {
					return err
				}
				hfentry.LastCommit = lastCommit
			}

			if err := cb(&hfentry); err != nil {
				return err
			}
		} else {
			hfentry := TreeEntry{
				OID:  entry.Hash.String(),
				Path: entryPath,
				Type: EntryTypeDirectory,
				Size: 0,
			}
			if opts.Expand {
				lastCommit, err := r.lastCommit(commit)
				if err != nil {
					return err
				}
				hfentry.LastCommit = lastCommit
			}

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
