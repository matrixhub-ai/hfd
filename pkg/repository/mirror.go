package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/matrixhub-ai/hfd/internal/utils"
)

// MirrorSourceFunc defines a function type for determining the source URL of a repository mirror.
// It receives the repository name and returns the source URL, a boolean indicating whether
// the mirror should be enabled for this repository, and an error if any occurs during the process.
type MirrorSourceFunc func(ctx context.Context, repoName string) (string, bool, error)

// MirrorRefFilterFunc filters which refs should be synced during mirror operations.
// It receives the repository name and a list of remote ref names (e.g. "refs/heads/main",
// "refs/tags/v1.0") and returns the filtered list of refs to sync.
type MirrorRefFilterFunc func(ctx context.Context, repoName string, refs []string) ([]string, error)

// InitMirror initializes a new bare git repository at repoPath.
// The returned Repository is ready to be used as a mirror of the source repository.
func InitMirror(ctx context.Context, repoPath string, sourceURL string) (*Repository, error) {
	sourceURL = strings.TrimSuffix(sourceURL, "/")
	sourceURL = strings.TrimSuffix(sourceURL, ".git") + ".git"

	defaultBranch, err := GetRemoteDefaultBranch(ctx, sourceURL)
	if err != nil {
		return nil, fmt.Errorf("failed to get HEAD from source repository: %w", err)
	}

	return Init(ctx, repoPath, defaultBranch)
}

// GetRemoteDefaultBranch retrieves the default branch name of the repository at the given source URL.
func GetRemoteDefaultBranch(ctx context.Context, sourceURL string) (string, error) {
	cmd := utils.Command(ctx, "git", "ls-remote", "--symref", sourceURL)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}

	const prefix = "ref: refs/heads/"
	// Search all output lines for the symref declaration, e.g.:
	//   ref: refs/heads/main\tHEAD
	for line := range strings.SplitSeq(string(out), "\n") {
		ref, found := strings.CutSuffix(line, "\tHEAD")
		if !found {
			continue
		}
		if !strings.HasPrefix(ref, prefix) {
			continue
		}
		return strings.TrimPrefix(ref, prefix), nil
	}
	return "", fmt.Errorf("HEAD symref not found in git ls-remote output")
}

// GetRemoteRefs returns a list of all ref names from the sourceURL.
// The returned names are fully qualified (e.g. "refs/heads/main", "refs/tags/v1.0").
func GetRemoteRefs(ctx context.Context, sourceURL string) (map[string]string, error) {
	cmd := utils.Command(ctx, "git", "ls-remote", "--refs", sourceURL)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list remote refs: %w", err)
	}

	refs := make(map[string]string)
	for line := range strings.SplitSeq(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Format: <hash>\t<refname>
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			continue
		}
		refs[parts[1]] = parts[0]
	}
	return refs, nil
}

// SyncMirrorRefs syncs only the specified refs from the sourceURL.
// Local refs that are not in the specified list are pruned.
func (r *Repository) SyncMirrorRefs(ctx context.Context, sourceURL string, refs []string) error {
	if len(refs) == 0 {
		return nil
	}

	args := []string{
		"fetch",
		sourceURL,
		"--no-tags",
		"--progress",
	}

	// Add explicit refspecs for each desired ref.
	for _, ref := range refs {
		args = append(args, "+"+ref+":"+ref)
	}

	cmd := utils.Command(ctx, "git", args...)
	cmd.Dir = r.repoPath
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to fetch repository refs: %w", err)
	}

	// Prune local refs that are not in the desired list.
	desired := make(map[string]bool, len(refs))
	for _, ref := range refs {
		desired[ref] = true
	}

	localRefs, err := r.Refs()
	if err != nil {
		return err
	}

	for refName := range localRefs {
		if !desired[refName] {
			delCmd := utils.Command(ctx, "git", "update-ref", "-d", refName)
			delCmd.Dir = r.repoPath
			_ = delCmd.Run()
		}
	}

	return nil
}
