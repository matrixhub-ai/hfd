package repository

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/matrixhub-ai/hfd/internal/utils"
)

// SuperSquash squashes all commits in the given rev into a single root commit
// with the provided message. The action is irreversible.
func (r *Repository) SuperSquash(ctx context.Context, rev string, message string, authorName string, authorEmail string) (string, error) {
	if rev == "" {
		rev = r.DefaultBranch()
	}

	refName := "refs/heads/" + rev

	env := append(os.Environ(),
		"GIT_DIR="+r.repoPath,
	)

	// Get the current tree hash for this rev
	treeCmd := utils.Command(ctx, "git", "rev-parse", refName+"^{tree}")
	treeCmd.Env = env
	treeOutput, err := treeCmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get tree for rev %q: %w", rev, err)
	}
	treeHash := strings.TrimSpace(string(treeOutput))

	// Create a new root commit (no parents) with the same tree
	now := time.Now()
	commitEnv := append(append([]string{}, env...),
		"GIT_AUTHOR_NAME="+authorName,
		"GIT_AUTHOR_EMAIL="+authorEmail,
		"GIT_AUTHOR_DATE="+now.Format(time.RFC3339),
		"GIT_COMMITTER_NAME="+authorName,
		"GIT_COMMITTER_EMAIL="+authorEmail,
		"GIT_COMMITTER_DATE="+now.Format(time.RFC3339),
	)

	commitCmd := utils.Command(ctx, "git", "commit-tree", treeHash, "-m", message)
	commitCmd.Env = commitEnv
	commitOutput, err := commitCmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to create squash commit: %w", err)
	}
	commitHash := strings.TrimSpace(string(commitOutput))

	// Force update the ref to point to the new root commit
	updateCmd := utils.Command(ctx, "git", "update-ref", refName, commitHash)
	updateCmd.Env = env
	if err := updateCmd.Run(); err != nil {
		return "", fmt.Errorf("failed to update ref: %w", err)
	}

	return commitHash, nil
}
