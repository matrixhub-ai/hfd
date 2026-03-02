package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/wzshiming/hfd/internal/utils"
)

// InitMrror initializes a new bare git repository as a mirror of the remote repository at sourceURL.
func InitMrror(ctx context.Context, repoPath string, sourceURL string) (*Repository, error) {
	sourceURL = strings.TrimSuffix(sourceURL, "/")
	sourceURL = strings.TrimSuffix(sourceURL, ".git") + ".git"

	cmd := utils.Command(ctx, "git", "clone", "--mirror", "--bare", "--depth=1", sourceURL, repoPath)
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to initialize git repository: %w", err)
	}

	repo, err := Open(repoPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open git repository: %w", err)
	}

	return repo, nil
}

func (r *Repository) SyncMirror(ctx context.Context) error {
	branch := r.DefaultBranch()
	err := r.fetchShallow(ctx, branch)
	if err != nil {
		return err
	}

	err = r.fetchShallow(ctx, "*")
	if err != nil {
		return err
	}
	return nil
}

func (r *Repository) fetchShallow(ctx context.Context, branch string) error {
	args := []string{
		"fetch",
		"--depth=1",
		"--prune",
		"origin",
		fmt.Sprintf("+refs/heads/%s:refs/heads/%s", branch, branch),
		"--progress",
	}
	cmd := utils.Command(ctx, "git", args...)
	cmd.Dir = r.repoPath
	err := cmd.Run()
	if err != nil {
		return err
	}
	return nil
}
