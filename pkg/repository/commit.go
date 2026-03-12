package repository

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// CommitsOptions provides options for the Commits method.
type CommitsOptions struct {
	Limit  int
	Offset int
}

// Commits returns a list of commits starting from the given revision.
// If rev is empty, it defaults to the repository's default branch.
// The limit parameter specifies the maximum number of commits to return.
func (r *Repository) Commits(rev string, opts *CommitsOptions) ([]Commit, error) {
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
		commits = append(commits, Commit{r: r, commit: c})
		if opts.Limit > 0 && len(commits) >= opts.Limit {
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
	r      *Repository
	commit *object.Commit
}

// Hash returns the Git object hash of the commit.
func (c *Commit) Hash() Hash {
	return c.commit.Hash
}

// Author returns the author information of the commit, including their name, email, and the time of the commit.
func (c *Commit) Author() *Signature {
	return &Signature{c.commit.Author}
}

// Committer returns the committer information of the commit, which may differ from the author if the commit was made by someone else on behalf of the author.
func (c *Commit) Committer() *Signature {
	return &Signature{c.commit.Committer}
}

// String returns the full commit message, including the title and body.
func (c *Commit) String() string {
	return c.commit.String()
}

// ID returns the string representation of the commit hash.
func (c *Commit) Message() string {
	return c.commit.Message
}

// Title returns the first line of the commit message, which is commonly used as the commit title.
func (c *Commit) Title() string {
	title := c.commit.Message
	if idx := strings.Index(title, "\n"); idx >= 0 {
		title = title[:idx]
	}
	return title
}

// Signature represents the author or committer of a commit, including their name, email, and the time of the commit.
type Signature struct {
	signature object.Signature
}

// Name returns the name of the signature.
func (s *Signature) Name() string {
	return s.signature.Name
}

// Email returns the email of the signature.
func (s *Signature) Email() string {
	return s.signature.Email
}

// When returns the time of the signature.
func (s *Signature) When() time.Time {
	return s.signature.When
}

// String returns a string representation of the signature in the format "Name <Email> Time".
func (s *Signature) String() string {
	return s.signature.String()
}

// Hash is a type alias for plumbing.Hash to represent Git object hashes.
type Hash = plumbing.Hash
