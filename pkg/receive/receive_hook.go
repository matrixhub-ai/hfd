package receive

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
)

// ZeroHash is the zero hash used for ref create/delete operations.
const ZeroHash = "0000000000000000000000000000000000000000"

// PreReceiveHook is called before a git push is processed with the list of ref updates.
// Returning a non-nil error will reject the push before any refs are updated.
type PreReceiveHook func(ctx context.Context, repoName string, updates []RefUpdate) error

// PostReceiveHook is called after a successful git push with the list of ref updates.
// It is used for notifications and logging; errors are logged but do not affect the push result.
type PostReceiveHook func(ctx context.Context, repoName string, updates []RefUpdate) error

// RefUpdate holds the raw ref update parsed from the receive-pack input.
type RefUpdate struct {
	OldRev  string
	NewRev  string
	RefName string
}

// IsBranch returns true if the reference is a branch (refs/heads/*).
func (r RefUpdate) IsBranch() bool {
	return strings.HasPrefix(r.RefName, "refs/heads/")
}

// IsTag returns true if the reference is a tag (refs/tags/*).
func (r RefUpdate) IsTag() bool {
	return strings.HasPrefix(r.RefName, "refs/tags/")
}

// IsCreate returns true if this is a new reference (old rev is all zeros).
func (r RefUpdate) IsCreate() bool {
	return r.OldRev == ZeroHash
}

// IsDelete returns true if the reference is being deleted (new rev is all zeros).
func (r RefUpdate) IsDelete() bool {
	return r.NewRev == ZeroHash
}

// Name returns the short name of the reference (e.g. "main", "v1.0").
func (r RefUpdate) Name() string {
	if after, ok := strings.CutPrefix(r.RefName, "refs/heads/"); ok {
		return after
	}
	if after, ok := strings.CutPrefix(r.RefName, "refs/tags/"); ok {
		return after
	}
	return r.RefName
}

// String returns a human-readable description of the ref update, e.g. "branch_create:main".
func (r RefUpdate) String() string {
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
		return fmt.Sprintf("ref_update:%s", r.RefName)
	}
}

// ParseRefUpdates reads the pkt-line formatted ref update commands from the
// beginning of a git receive-pack input stream. It returns the parsed updates
// and a new reader that replays the consumed bytes followed by the remaining input.
func ParseRefUpdates(r io.Reader) ([]RefUpdate, io.Reader) {
	var buf bytes.Buffer
	tee := io.TeeReader(r, &buf)

	var updates []RefUpdate

	for {
		// Read 4-byte hex length prefix
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(tee, lenBuf); err != nil {
			break
		}

		pktLen, err := strconv.ParseUint(string(lenBuf), 16, 16)
		if err != nil {
			break
		}

		// Flush packet (0000) marks end of commands
		if pktLen == 0 {
			break
		}

		if pktLen < 4 {
			break
		}

		// Read the payload (length includes the 4-byte header)
		payload := make([]byte, pktLen-4)
		if _, err := io.ReadFull(tee, payload); err != nil {
			break
		}

		line := string(payload)
		line = strings.TrimRight(line, "\n")

		// Remove capabilities after null byte
		if idx := strings.IndexByte(line, 0); idx >= 0 {
			line = line[:idx]
		}

		parts := strings.SplitN(line, " ", 3)
		if len(parts) == 3 {
			updates = append(updates, RefUpdate{
				OldRev:  parts[0],
				NewRev:  parts[1],
				RefName: parts[2],
			})
		}
	}

	return updates, io.MultiReader(&buf, r)
}

// IsForcePush checks if a branch update is a non-fast-forward (force) push.
// Returns false for creates, deletes, tags, and non-branch refs.
// repoPath is the filesystem path to the bare git repository.
func IsForcePush(ctx context.Context, repoPath string, r RefUpdate) bool {
	if r.IsCreate() || r.IsDelete() || !r.IsBranch() {
		return false
	}
	cmd := exec.CommandContext(ctx, "git", "merge-base", "--is-ancestor", r.OldRev, r.NewRev)
	cmd.Dir = repoPath
	return cmd.Run() != nil
}

// DiffRefs computes ref updates by comparing before and after ref snapshots.
// before and after are maps of full ref name (e.g. "refs/heads/main") to commit hash.
func DiffRefs(before, after map[string]string) []RefUpdate {
	var updates []RefUpdate

	// Detect new and changed refs
	for refName, newHash := range after {
		oldHash, existed := before[refName]
		if !existed {
			updates = append(updates, RefUpdate{
				OldRev:  ZeroHash,
				NewRev:  newHash,
				RefName: refName,
			})
		} else if oldHash != newHash {
			updates = append(updates, RefUpdate{
				OldRev:  oldHash,
				NewRev:  newHash,
				RefName: refName,
			})
		}
	}

	// Detect deleted refs
	for refName, oldHash := range before {
		if _, exists := after[refName]; !exists {
			updates = append(updates, RefUpdate{
				OldRev:  oldHash,
				NewRev:  ZeroHash,
				RefName: refName,
			})
		}
	}

	return updates
}
