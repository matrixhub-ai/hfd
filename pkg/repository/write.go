package repository

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/wzshiming/gitd/internal/utils"
)

// CommitOperationType represents the type of operation in a commit.
type CommitOperationType string

const (
	// CommitOperationAdd adds or updates a file.
	CommitOperationAdd CommitOperationType = "add"
	// CommitOperationDelete deletes a file.
	CommitOperationDelete CommitOperationType = "delete"
)

// CommitOperation represents a single operation in a commit.
type CommitOperation struct {
	Type    CommitOperationType
	Path    string
	Content []byte // file content for add operations
}

// CreateCommit creates a new commit on the given branch with the given operations.
// This works on bare repositories by directly manipulating git objects and refs.
// If parentCommit is non-empty, the ref update is made atomic: the current tip
// must match parentCommit, otherwise the operation fails (optimistic concurrency).
func (r *Repository) CreateCommit(ctx context.Context, ref string, message string, authorName string, authorEmail string, ops []CommitOperation, parentCommit string) (string, error) {
	if ref == "" {
		ref = r.DefaultBranch()
	}

	// Create a temporary index file path (the file must not exist yet for git)
	tmpIndex, err := os.CreateTemp("", "git-index-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp index: %w", err)
	}
	tmpIndexPath := tmpIndex.Name()
	_ = tmpIndex.Close()
	_ = os.Remove(tmpIndexPath) // Remove so git can create it fresh
	defer os.Remove(tmpIndexPath)

	env := append(os.Environ(),
		"GIT_INDEX_FILE="+tmpIndexPath,
		"GIT_DIR="+r.repoPath,
	)

	// Try to read the current tree into the index (ignore error for new branches)
	refName := "refs/heads/" + ref
	{
		cmd := utils.Command(ctx, "git", "read-tree", refName)
		cmd.Env = env
		_ = cmd.Run()
	}

	// Apply operations
	for _, op := range ops {
		switch op.Type {
		case CommitOperationAdd:
			// Create blob
			cmd := utils.Command(ctx, "git", "hash-object", "-w", "--stdin")
			cmd.Env = env
			cmd.Stdin = bytes.NewReader(op.Content)
			output, err := cmd.Output()
			if err != nil {
				return "", fmt.Errorf("failed to create blob for %s: %w", op.Path, err)
			}
			blobHash := strings.TrimSpace(string(output))

			// Update index
			cmd = utils.Command(ctx, "git", "update-index", "--add", "--cacheinfo", "100644", blobHash, op.Path)
			cmd.Env = env
			if err := cmd.Run(); err != nil {
				return "", fmt.Errorf("failed to update index for %s: %w", op.Path, err)
			}

		case CommitOperationDelete:
			cmd := utils.Command(ctx, "git", "update-index", "--force-remove", op.Path)
			cmd.Env = env
			_ = cmd.Run() // Ignore error if file doesn't exist
		}
	}

	// Write tree
	cmd := utils.Command(ctx, "git", "write-tree")
	cmd.Env = env
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to write tree: %w", err)
	}
	treeHash := strings.TrimSpace(string(output))

	// Build commit command
	now := time.Now()
	commitEnv := append(append([]string{}, env...),
		"GIT_AUTHOR_NAME="+authorName,
		"GIT_AUTHOR_EMAIL="+authorEmail,
		"GIT_AUTHOR_DATE="+now.Format(time.RFC3339),
		"GIT_COMMITTER_NAME="+authorName,
		"GIT_COMMITTER_EMAIL="+authorEmail,
		"GIT_COMMITTER_DATE="+now.Format(time.RFC3339),
	)

	args := []string{"commit-tree", treeHash, "-m", message}

	// Add parent if branch exists
	var currentTip string
	{
		parentCmd := utils.Command(ctx, "git", "rev-parse", "--verify", refName)
		parentCmd.Env = env
		parentOutput, err := parentCmd.Output()
		if err == nil {
			currentTip = strings.TrimSpace(string(parentOutput))
			if currentTip != "" {
				args = append(args, "-p", currentTip)
			}
		}
	}

	// If parentCommit is specified, verify it matches the current tip
	if parentCommit != "" && currentTip != parentCommit {
		return "", fmt.Errorf("expected parent commit %s but branch tip is %s", parentCommit, currentTip)
	}

	cmd = utils.Command(ctx, "git", args...)
	cmd.Env = commitEnv
	output, err = cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to create commit: %w", err)
	}
	commitHash := strings.TrimSpace(string(output))

	// Update ref atomically: provide old value to prevent lost updates
	updateRefArgs := []string{"update-ref", refName, commitHash}
	if currentTip != "" {
		updateRefArgs = append(updateRefArgs, currentTip)
	}
	cmd = utils.Command(ctx, "git", updateRefArgs...)
	cmd.Env = env
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to update ref: %w", err)
	}

	return commitHash, nil
}
