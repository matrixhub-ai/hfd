package receive

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
)

// ZeroHash is the zero hash used for ref create/delete operations.
const ZeroHash = "0000000000000000000000000000000000000000"

// BreakHash is marked as a special hash that indicates the ref update squash should be performed by the receive hook.
const BreakHash = ""

// PreReceiveHookFunc is called before a git push is processed with the list of ref updates.
// Returning a non-nil error will reject the push before any refs are updated.
type PreReceiveHookFunc func(ctx context.Context, repoName string, updates []RefUpdate) (bool, error)

// PostReceiveHookFunc is called after a successful git push with the list of ref updates.
// It is used for notifications and logging; errors are logged but do not affect the push result.
type PostReceiveHookFunc func(ctx context.Context, repoName string, updates []RefUpdate) error

// RefUpdate represents a single ref update in a git push, including the old and new revisions, the ref name, and helper methods to identify the type of update.
type RefUpdate interface {
	OldRev() string
	NewRev() string
	RefName() string
	IsBranch() bool
	IsTag() bool
	IsCreate() bool
	IsDelete() bool
	IsForce(ctx context.Context) (bool, error)
	Name() string
	String() string
}

// NewRefUpdate creates a new RefUpdate instance with the given parameters.
func NewRefUpdate(oldRev, newRev, refName, repoPath string) RefUpdate {
	return refUpdate{
		oldRev:   oldRev,
		newRev:   newRev,
		refName:  refName,
		repoPath: repoPath,
	}
}

type refUpdate struct {
	oldRev   string
	newRev   string
	refName  string
	repoPath string
}

func (r refUpdate) OldRev() string {
	return r.oldRev
}

func (r refUpdate) NewRev() string {
	return r.newRev
}

func (r refUpdate) RefName() string {
	return r.refName
}

// IsBranch returns true if the reference is a branch (refs/heads/*).
func (r refUpdate) IsBranch() bool {
	return strings.HasPrefix(r.refName, "refs/heads/")
}

// IsTag returns true if the reference is a tag (refs/tags/*).
func (r refUpdate) IsTag() bool {
	return strings.HasPrefix(r.refName, "refs/tags/")
}

// IsCreate returns true if this is a new reference (old rev is all zeros).
func (r refUpdate) IsCreate() bool {
	return r.oldRev == ZeroHash
}

// IsDelete returns true if the reference is being deleted (new rev is all zeros).
func (r refUpdate) IsDelete() bool {
	return r.newRev == ZeroHash
}

// Name returns the short name of the reference (e.g. "main", "v1.0").
func (r refUpdate) Name() string {
	if after, ok := strings.CutPrefix(r.refName, "refs/heads/"); ok {
		return after
	}
	if after, ok := strings.CutPrefix(r.refName, "refs/tags/"); ok {
		return after
	}
	return r.refName
}

// IsForce checks if this ref update is a non-fast-forward (force) push by invoking the git merge-base command.
func (r refUpdate) IsForce(ctx context.Context) (bool, error) {
	if r.oldRev == BreakHash && r.newRev == BreakHash {
		return true, nil
	}
	if r.repoPath == "" {
		return false, fmt.Errorf("repo path is empty")
	}
	return isForce(ctx, r.repoPath, r)
}

// String returns a human-readable description of the ref update, e.g. "branch_create:main".
func (r refUpdate) String() string {
	switch {
	case r.IsBranch() && r.IsCreate():
		return fmt.Sprintf("branch_create:%s", r.Name())
	case r.IsBranch() && r.IsDelete():
		return fmt.Sprintf("branch_delete:%s", r.Name())
	case r.IsBranch():
		return fmt.Sprintf("branch_push:%s", r.Name())
	case r.IsTag() && r.IsCreate():
		return fmt.Sprintf("tag_create:%s", r.Name())
	case r.IsTag() && r.IsDelete():
		return fmt.Sprintf("tag_delete:%s", r.Name())
	default:
		return fmt.Sprintf("ref_update:%s", r.refName)
	}
}

// isForce checks if a branch update is a non-fast-forward (force) push.
// Returns false for creates, deletes, tags, and non-branch refs.
// repoPath is the filesystem path to the bare git repository.
func isForce(ctx context.Context, repoPath string, r RefUpdate) (bool, error) {
	if r.IsCreate() || r.IsDelete() || !r.IsBranch() {
		return false, nil
	}
	oldRev := r.OldRev()
	newRev := r.NewRev()
	if oldRev == "" || newRev == "" {
		return false, nil
	}

	repo, err := git.PlainOpenWithOptions(repoPath, &git.PlainOpenOptions{})
	if err != nil {
		return false, fmt.Errorf("failed to check force push: %w", err)
	}
	oldCommit, err := repo.CommitObject(plumbing.NewHash(oldRev))
	if err != nil {
		return false, fmt.Errorf("failed to check force push: %w", err)
	}
	newCommit, err := repo.CommitObject(plumbing.NewHash(newRev))
	if err != nil {
		return false, fmt.Errorf("failed to check force push: %w", err)
	}
	isAncestor, err := oldCommit.IsAncestor(newCommit)
	if err != nil {
		return false, fmt.Errorf("failed to check force push: %w", err)
	}
	return !isAncestor, nil
}

// DiffRefs computes ref updates by comparing before and after ref snapshots.
// before and after are maps of full ref name (e.g. "refs/heads/main") to commit hash.
func DiffRefs(before, after map[string]string, repoPath string) []RefUpdate {
	var updates []RefUpdate

	// Detect new and changed refs
	for refName, newHash := range after {
		oldHash, existed := before[refName]
		if !existed {
			updates = append(updates, refUpdate{
				oldRev:   ZeroHash,
				newRev:   newHash,
				refName:  refName,
				repoPath: repoPath,
			})
		} else if oldHash != newHash {
			updates = append(updates, refUpdate{
				oldRev:   oldHash,
				newRev:   newHash,
				refName:  refName,
				repoPath: repoPath,
			})
		}
	}

	// Detect deleted refs
	for refName, oldHash := range before {
		if _, exists := after[refName]; !exists {
			updates = append(updates, refUpdate{
				oldRev:   oldHash,
				newRev:   ZeroHash,
				refName:  refName,
				repoPath: repoPath,
			})
		}
	}

	return updates
}
