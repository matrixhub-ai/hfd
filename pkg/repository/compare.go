package repository

import (
	"context"
	"fmt"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// Compare compares two revisions and returns the unified diff between them.
func (r *Repository) Compare(ctx context.Context, base, head string) (object.Changes, error) {
	if base == "" || head == "" {
		return nil, fmt.Errorf("both base and head revisions must be specified")
	}

	baseHash, err := r.repo.ResolveRevision(plumbing.Revision(base))
	if err != nil {
		return nil, fmt.Errorf("failed to resolve base revision %q: %w", base, err)
	}

	headHash, err := r.repo.ResolveRevision(plumbing.Revision(head))
	if err != nil {
		return nil, fmt.Errorf("failed to resolve head revision %q: %w", head, err)
	}

	if *baseHash == *headHash {
		return nil, nil
	}

	baseCommit, err := r.repo.CommitObject(*baseHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get base commit: %w", err)
	}

	headCommit, err := r.repo.CommitObject(*headHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get head commit: %w", err)
	}

	baseTree, err := baseCommit.Tree()
	if err != nil {
		return nil, fmt.Errorf("failed to get base tree: %w", err)
	}

	headTree, err := headCommit.Tree()
	if err != nil {
		return nil, fmt.Errorf("failed to get head tree: %w", err)
	}

	return object.DiffTreeContext(ctx, baseTree, headTree)
}
