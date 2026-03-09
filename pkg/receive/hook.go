package receive

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/wzshiming/hfd/internal/utils"
)

// ZeroHash represents a non-existent ref (used for creates and deletes).
const ZeroHash = "0000000000000000000000000000000000000000"

// RefUpdate represents a single ref update in a git push operation.
type RefUpdate struct {
	// OldRev is the old commit hash (ZeroHash for new refs).
	OldRev string
	// NewRev is the new commit hash (ZeroHash for deleted refs).
	NewRev string
	// RefName is the full ref name (e.g., "refs/heads/main" or "refs/tags/v1.0").
	RefName string
}

// IsBranch returns true if the ref is a branch (under refs/heads/).
func (r RefUpdate) IsBranch() bool {
	return strings.HasPrefix(r.RefName, "refs/heads/")
}

// IsTag returns true if the ref is a tag (under refs/tags/).
func (r RefUpdate) IsTag() bool {
	return strings.HasPrefix(r.RefName, "refs/tags/")
}

// IsCreate returns true if this update creates a new ref.
func (r RefUpdate) IsCreate() bool {
	return r.OldRev == ZeroHash
}

// IsDelete returns true if this update deletes an existing ref.
func (r RefUpdate) IsDelete() bool {
	return r.NewRev == ZeroHash
}

// Name returns the short name of the ref (without the refs/heads/ or refs/tags/ prefix).
func (r RefUpdate) Name() string {
	if r.IsBranch() {
		return strings.TrimPrefix(r.RefName, "refs/heads/")
	}
	if r.IsTag() {
		return strings.TrimPrefix(r.RefName, "refs/tags/")
	}
	return r.RefName
}

// IsForcePush checks if a branch update is a force push by verifying
// that the old revision is not an ancestor of the new revision.
// Returns false for creates, deletes, and tag updates.
func IsForcePush(ctx context.Context, repoPath string, update RefUpdate) bool {
	if !update.IsBranch() || update.IsCreate() || update.IsDelete() {
		return false
	}
	cmd := utils.Command(ctx, "git", "merge-base", "--is-ancestor", update.OldRev, update.NewRev)
	cmd.Dir = repoPath
	return cmd.Run() != nil
}

// Hook is a function called when ref updates occur during git operations.
// It receives the repository name (storage path) and the list of ref updates.
// Returning an error can be used to signal that the operation should be rejected
// (for pre-receive) or that post-receive processing failed.
type Hook func(ctx context.Context, repoName string, updates []RefUpdate) error

// ParseRefUpdates parses ref updates from a reader.
// Each line has the format: <old-value> SP <new-value> SP <ref-name> LF
// This is the standard format used by git pre-receive and post-receive hooks.
func ParseRefUpdates(r io.Reader) ([]RefUpdate, error) {
	var updates []RefUpdate
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 3 {
			continue
		}
		updates = append(updates, RefUpdate{
			OldRev:  parts[0],
			NewRev:  parts[1],
			RefName: parts[2],
		})
	}
	return updates, scanner.Err()
}

// DiffRefs compares two ref snapshots and returns the changes as RefUpdates.
// before and after are maps of ref names to commit hashes.
func DiffRefs(before, after map[string]string) []RefUpdate {
	var updates []RefUpdate
	for ref, newHash := range after {
		oldHash, existed := before[ref]
		if !existed {
			updates = append(updates, RefUpdate{OldRev: ZeroHash, NewRev: newHash, RefName: ref})
		} else if oldHash != newHash {
			updates = append(updates, RefUpdate{OldRev: oldHash, NewRev: newHash, RefName: ref})
		}
	}
	for ref, oldHash := range before {
		if _, exists := after[ref]; !exists {
			updates = append(updates, RefUpdate{OldRev: oldHash, NewRev: ZeroHash, RefName: ref})
		}
	}
	return updates
}

// FormatEvent returns a human-readable description of a ref update event.
func FormatEvent(update RefUpdate) string {
	switch {
	case update.IsBranch() && update.IsCreate():
		return fmt.Sprintf("branch_create:%s", update.Name())
	case update.IsBranch() && update.IsDelete():
		return fmt.Sprintf("branch_delete:%s", update.Name())
	case update.IsBranch():
		return fmt.Sprintf("branch_push:%s", update.Name())
	case update.IsTag() && update.IsCreate():
		return fmt.Sprintf("tag_create:%s", update.Name())
	case update.IsTag() && update.IsDelete():
		return fmt.Sprintf("tag_delete:%s", update.Name())
	default:
		return fmt.Sprintf("ref_update:%s", update.RefName)
	}
}
