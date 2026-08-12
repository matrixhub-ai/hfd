package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
)

// SuperSquash squashes all commits in the given rev into a single root commit
// with the provided message. The action is irreversible.
func (r *Repository) SuperSquash(ctx context.Context, rev string, message string, authorName string, authorEmail string) (string, error) {
	if rev == "" {
		rev = r.DefaultBranch()
	}

	refName := plumbing.NewBranchReferenceName(rev)

	tipHash, err := r.repo.ResolveRevision(plumbing.Revision(refName))
	if err != nil {
		return "", fmt.Errorf("failed to get tree for rev %q: %w", rev, err)
	}
	tipCommit, err := r.repo.CommitObject(*tipHash)
	if err != nil {
		return "", fmt.Errorf("failed to get tree for rev %q: %w", rev, err)
	}

	// Create a new root commit (no parents) with the same tree
	signature := object.Signature{
		Name:  authorName,
		Email: authorEmail,
		When:  time.Now(),
	}
	commitHash, err := r.storeCommit(&object.Commit{
		Author:    signature,
		Committer: signature,
		Message:   message,
		TreeHash:  tipCommit.TreeHash,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create squash commit: %w", err)
	}

	// Force update the ref to point to the new root commit
	if err := r.repo.Storer.SetReference(plumbing.NewHashReference(refName, commitHash)); err != nil {
		return "", fmt.Errorf("failed to update ref: %w", err)
	}

	return commitHash.String(), nil
}
