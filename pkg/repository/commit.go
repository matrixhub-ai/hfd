package repository

import (
	"fmt"
	"io"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// CommitsOptions provides options for the Commits method.
type CommitsOptions struct {
	Offset int
}

// Commits returns a list of commits starting from the given revision.
// If rev is empty, it defaults to the repository's default branch.
// The limit parameter specifies the maximum number of commits to return.
func (r *Repository) Commits(rev string, limit int, opts *CommitsOptions) ([]Commit, error) {
	if rev == "" {
		rev = r.DefaultBranch()
	}

	if opts == nil {
		opts = &CommitsOptions{}
	}

	hash, err := r.repo.ResolveRevision(plumbing.Revision(rev))
	if err != nil {
		return nil, fmt.Errorf("failed to resolve revision: %w", err)
	}

	commitIter, err := r.repo.Log(&git.LogOptions{From: *hash})
	if err != nil {
		return nil, fmt.Errorf("failed to get commit log: %w", err)
	}

	var offset int
	var commits []Commit
	err = commitIter.ForEach(func(c *object.Commit) error {
		if offset < opts.Offset {
			offset++
			return nil // Skip until we reach the offset
		}
		commits = append(commits, Commit{
			SHA:     c.Hash.String(),
			Message: c.Message,
			Author:  c.Author.Name,
			Email:   c.Author.Email,
			Date:    c.Author.When.UTC().Format(TimeFormat),
		})
		if limit > 0 && len(commits) >= limit {
			return io.EOF // Stop after reaching the limit
		}
		return nil
	})
	if err != nil && err != io.EOF && len(commits) == 0 {
		return nil, fmt.Errorf("failed to iterate commits: %w", err)
	}

	return commits, nil
}

// Commit represents a Git commit with relevant metadata.
type Commit struct {
	SHA     string `json:"sha"`
	Message string `json:"message"`
	Author  string `json:"author"`
	Email   string `json:"email"`
	Date    string `json:"date"`
}
