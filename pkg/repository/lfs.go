package repository

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
)

// ScanLFSPointers returns the LFS pointers of the blobs reachable from the
// repository's refs, one per blob, ordered by blob hash.
func (r *Repository) ScanLFSPointers(ctx context.Context) ([]*lfs.Pointer, error) {
	return r.ScanLFSPointersExcept(ctx, nil)
}

// ScanLFSPointersExcept is ScanLFSPointers skipping the blobs in ignored, e.g. those a GC preview would delete.
func (r *Repository) ScanLFSPointersExcept(ctx context.Context, ignored []plumbing.Hash) ([]*lfs.Pointer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	set, err := r.reachable(ctx)
	if err != nil {
		return nil, err
	}

	s := &lfsScan{
		store:    r.repo.Storer,
		ignored:  make(map[plumbing.Hash]struct{}, len(ignored)),
		pointers: map[plumbing.Hash]*lfs.Pointer{},
	}
	for _, h := range ignored {
		s.ignored[h] = struct{}{}
	}
	walk := &treeWalk{store: r.repo.Storer, seen: map[plumbing.Hash]struct{}{}}
	err = set.walkTrees(ctx, walk, func(h plumbing.Hash) error { return s.blob(ctx, h) })
	if err != nil {
		return nil, err
	}

	hashes := slices.Collect(maps.Keys(s.pointers))
	plumbing.HashesSort(hashes)
	result := make([]*lfs.Pointer, 0, len(hashes))
	for _, h := range hashes {
		result = append(result, s.pointers[h])
	}
	return result, ctx.Err()
}

// lfsScan judges the blobs a tree walk hands it.
type lfsScan struct {
	store    storer.EncodedObjectStorer
	ignored  map[plumbing.Hash]struct{}
	pointers map[plumbing.Hash]*lfs.Pointer
}

// blob records the pointer in hash, reading only blobs small enough to be one; ignored blobs are never read.
func (s *lfsScan) blob(ctx context.Context, hash plumbing.Hash) error {
	if _, ok := s.ignored[hash]; ok {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	size, err := s.store.EncodedObjectSize(hash)
	if err != nil {
		return fmt.Errorf("read blob %s: %w", hash, err)
	}
	if size >= lfs.MaxLFSPointerSize {
		return nil
	}
	obj, err := s.store.EncodedObject(plumbing.BlobObject, hash)
	if err != nil {
		return fmt.Errorf("read blob %s: %w", hash, err)
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
	// Only a decode failure means "not a pointer"; read errors above must surface.
	if ptr, _ := lfs.DecodePointer(bytes.NewReader(data)); ptr != nil {
		s.pointers[hash] = ptr
	}
	return nil
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

	hash, err := r.resolveRevision(rev)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve revision: %w", err)
	}

	commit, err := r.repo.CommitObject(hash)
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
