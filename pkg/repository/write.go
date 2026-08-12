package repository

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
)

// CommitOperationType represents the type of operation in a commit.
type CommitOperationType string

const (
	// CommitOperationAdd adds or updates a file.
	CommitOperationAdd CommitOperationType = "add"
	// CommitOperationDelete deletes a file.
	CommitOperationDelete CommitOperationType = "delete"
)

// CommitOperation represents a single operation in a commit.
type CommitOperation struct {
	Type    CommitOperationType
	Path    string
	Content []byte // file content for add operations
}

// CreateCommit creates a new commit on the given branch with the given operations.
// This works on bare repositories by directly manipulating git objects and refs.
// If parentCommit is non-empty, the rev update is made atomic: the current tip
// must match parentCommit, otherwise the operation fails (optimistic concurrency).
func (r *Repository) CreateCommit(ctx context.Context, rev string, message string, authorName string, authorEmail string, ops []CommitOperation, parentCommit string) (string, error) {
	if rev == "" {
		rev = r.DefaultBranch()
	}
	refName := plumbing.NewBranchReferenceName(rev)

	// Load the current tree entries from the branch tip (empty for new branches).
	entries := make(map[string]object.TreeEntry)
	var currentTip string
	var oldRef *plumbing.Reference
	var parents []plumbing.Hash
	if ref, err := r.repo.Storer.Reference(refName); err == nil && !ref.Hash().IsZero() {
		oldRef = ref
		currentTip = ref.Hash().String()
		parents = append(parents, ref.Hash())

		commit, err := r.repo.CommitObject(ref.Hash())
		if err != nil {
			return "", fmt.Errorf("failed to read branch tip commit: %w", err)
		}
		tree, err := commit.Tree()
		if err != nil {
			return "", fmt.Errorf("failed to read branch tip tree: %w", err)
		}
		walker := object.NewTreeWalker(tree, true, nil)
		defer walker.Close()
		for {
			name, entry, err := walker.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return "", fmt.Errorf("failed to walk branch tip tree: %w", err)
			}
			if entry.Mode == filemode.Dir {
				continue
			}
			entries[name] = entry
		}
	}

	// If parentCommit is specified, verify it matches the current tip
	if parentCommit != "" && currentTip != parentCommit {
		return "", fmt.Errorf("expected parent commit %s but branch tip is %s", parentCommit, currentTip)
	}

	// Apply operations
	for _, op := range ops {
		switch op.Type {
		case CommitOperationAdd:
			blobHash, err := r.storeBlob(op.Content)
			if err != nil {
				return "", fmt.Errorf("failed to create blob for %s: %w", op.Path, err)
			}
			entries[op.Path] = object.TreeEntry{
				Name: op.Path,
				Mode: filemode.Regular,
				Hash: blobHash,
			}
		case CommitOperationDelete:
			delete(entries, op.Path)
		default:
			return "", fmt.Errorf("unsupported operation type: %s", op.Type)
		}
	}

	// Write tree
	treeHash, err := r.storeTree(entries)
	if err != nil {
		return "", fmt.Errorf("failed to write tree: %w", err)
	}

	signature := object.Signature{
		Name:  authorName,
		Email: authorEmail,
		When:  time.Now(),
	}
	commitHash, err := r.storeCommit(&object.Commit{
		Author:       signature,
		Committer:    signature,
		Message:      message,
		TreeHash:     treeHash,
		ParentHashes: parents,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create commit: %w", err)
	}

	// Update ref atomically: provide old value to prevent lost updates
	newRef := plumbing.NewHashReference(refName, commitHash)
	if err := r.repo.Storer.CheckAndSetReference(newRef, oldRef); err != nil {
		return "", fmt.Errorf("failed to update rev: %w", err)
	}

	return commitHash.String(), nil
}

// storeBlob writes content as a blob object and returns its hash.
func (r *Repository) storeBlob(content []byte) (plumbing.Hash, error) {
	obj := r.repo.Storer.NewEncodedObject()
	obj.SetType(plumbing.BlobObject)
	obj.SetSize(int64(len(content)))
	w, err := obj.Writer()
	if err != nil {
		return plumbing.ZeroHash, err
	}
	if _, err := w.Write(content); err != nil {
		_ = w.Close()
		return plumbing.ZeroHash, err
	}
	if err := w.Close(); err != nil {
		return plumbing.ZeroHash, err
	}
	return r.repo.Storer.SetEncodedObject(obj)
}

// storeCommit writes the commit object and returns its hash.
func (r *Repository) storeCommit(commit *object.Commit) (plumbing.Hash, error) {
	obj := r.repo.Storer.NewEncodedObject()
	if err := commit.Encode(obj); err != nil {
		return plumbing.ZeroHash, err
	}
	return r.repo.Storer.SetEncodedObject(obj)
}

// treeNode is an intermediate representation of a tree while rebuilding the
// nested tree objects from a flat path → entry map.
type treeNode struct {
	blobs map[string]object.TreeEntry
	subs  map[string]*treeNode
}

func newTreeNode() *treeNode {
	return &treeNode{
		blobs: make(map[string]object.TreeEntry),
		subs:  make(map[string]*treeNode),
	}
}

// storeTree writes the nested tree objects for a flat map of full path → entry
// and returns the root tree hash.
func (r *Repository) storeTree(entries map[string]object.TreeEntry) (plumbing.Hash, error) {
	root := newTreeNode()
	for path, entry := range entries {
		node := root
		parts := strings.Split(path, "/")
		for _, dir := range parts[:len(parts)-1] {
			sub, ok := node.subs[dir]
			if !ok {
				sub = newTreeNode()
				node.subs[dir] = sub
			}
			node = sub
		}
		entry.Name = parts[len(parts)-1]
		node.blobs[entry.Name] = entry
	}
	return r.storeTreeNode(root)
}

// storeTreeNode recursively writes tree objects bottom-up and returns the hash of node.
func (r *Repository) storeTreeNode(node *treeNode) (plumbing.Hash, error) {
	treeEntries := make([]object.TreeEntry, 0, len(node.blobs)+len(node.subs))
	for name, sub := range node.subs {
		hash, err := r.storeTreeNode(sub)
		if err != nil {
			return plumbing.ZeroHash, err
		}
		treeEntries = append(treeEntries, object.TreeEntry{
			Name: name,
			Mode: filemode.Dir,
			Hash: hash,
		})
	}
	for _, entry := range node.blobs {
		treeEntries = append(treeEntries, entry)
	}
	sort.Sort(object.TreeEntrySorter(treeEntries))

	tree := &object.Tree{Entries: treeEntries}
	obj := r.repo.Storer.NewEncodedObject()
	if err := tree.Encode(obj); err != nil {
		return plumbing.ZeroHash, err
	}
	return r.repo.Storer.SetEncodedObject(obj)
}
