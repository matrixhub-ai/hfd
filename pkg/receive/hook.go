package receive

import (
	"context"
	"strings"
)

const (
	zeroPad = "0000000000000000000000000000000000000000"
)

// Hook is a callback invoked after a push is accepted by git-receive-pack.
// repoName is the logical repository name (e.g. "my-model") and updates
// contains every ref that was created, updated, or deleted in the push.
type Hook func(ctx context.Context, repoName string, updates []RefUpdate) error

// RefUpdate represents a single ref update from a git push operation.
type RefUpdate struct {
	OldRev  string
	NewRev  string
	RefName string
}

// IsBranch reports whether the update targets a branch ref.
func (r RefUpdate) IsBranch() bool {
	return strings.HasPrefix(r.RefName, "refs/heads/")
}

// IsTag reports whether the update targets a tag ref.
func (r RefUpdate) IsTag() bool {
	return strings.HasPrefix(r.RefName, "refs/tags/")
}

// IsCreate reports whether the update creates a new ref.
func (r RefUpdate) IsCreate() bool {
	return r.OldRev == zeroPad
}

// IsDelete reports whether the update deletes an existing ref.
func (r RefUpdate) IsDelete() bool {
	return r.NewRev == zeroPad
}

// Name returns the short name of the ref (e.g. "main" for "refs/heads/main").
func (r RefUpdate) Name() string {
	if r.IsBranch() {
		return strings.TrimPrefix(r.RefName, "refs/heads/")
	}
	if r.IsTag() {
		return strings.TrimPrefix(r.RefName, "refs/tags/")
	}
	return r.RefName
}
