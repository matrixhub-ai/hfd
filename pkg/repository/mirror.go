package repository

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/wzshiming/hfd/internal/utils"
)

// MirrorSourceFunc is a callback that returns the source URL for mirroring a repository
// that does not exist locally. Returning an empty string or an error disables
// mirror creation for that repository.
type MirrorSourceFunc func(ctx context.Context, repoPath, repoName string) (string, error)

// NewMirrorSourceFunc creates a MirrorFunc that derives the source URL by appending
// repoName to baseURL.
func NewMirrorSourceFunc(baseURL string) MirrorSourceFunc {
	return func(ctx context.Context, repoPath, repoName string) (string, error) {
		return strings.TrimSuffix(baseURL, "/") + "/" + repoName, nil
	}
}

// InitMirror initializes a new bare git repository at repoPath and sets up a remote named "origin"
// that points to sourceURL. It then performs an initial shallow fetch to populate the mirror.
// The returned Repository is ready to be used as a mirror of the source repository.
func InitMirror(ctx context.Context, repoPath string, sourceURL string) (*Repository, error) {
	sourceURL = strings.TrimSuffix(sourceURL, "/")
	sourceURL = strings.TrimSuffix(sourceURL, ".git") + ".git"

	defaultBrach, err := getDefaultBranch(ctx, sourceURL)
	if err != nil {
		return nil, fmt.Errorf("failed to get HEAD from source repository: %w", err)
	}
	cmd := utils.Command(ctx, "git", "init", "--bare", repoPath, "--initial-branch", defaultBrach)
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to initialize git repository: %w", err)
	}

	cmd = utils.Command(ctx, "git", "-C", repoPath, "remote", "add", "--mirror=fetch", "origin", sourceURL)
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to add remote origin: %w", err)
	}

	repo, err := Open(repoPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open git repository: %w", err)
	}

	err = repo.shallowFetch(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to perform initial shallow fetch: %w", err)
	}

	err = repo.SyncMirror(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to sync mirror: %w", err)
	}

	return repo, nil
}

func (r *Repository) shallowFetch(ctx context.Context) error {
	args := []string{
		"fetch",
		"--depth=1",
		"--prune",
		"origin",
		"--progress",
	}
	cmd := utils.Command(ctx, "git", args...)
	cmd.Dir = r.repoPath
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to shallow fetch repository: %w", err)
	}
	return nil
}

func getDefaultBranch(ctx context.Context, sourceURL string) (string, error) {
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

// IsMirror checks if the repository is a mirror by looking for the "origin" remote and checking its configuration.
func (r *Repository) IsMirror() (bool, string, error) {
	config, err := r.repo.Config()
	if err != nil {
		return false, "", err
	}

	sourceURL := ""
	if config != nil {
		if remote, ok := config.Remotes["origin"]; ok {
			if len(remote.URLs) > 0 {
				sourceURL = remote.URLs[0]
			}
		}
	}
	return sourceURL != "", sourceURL, nil
}

// IsMirror checks if the repository is a mirror by looking for the "origin" remote and checking its configuration.
func (r *Repository) SyncMirror(ctx context.Context) error {
	args := []string{
		"fetch",
		"--prune",
		"origin",
		"--progress",
	}

	if fi, err := os.Stat(filepath.Join(r.repoPath, "shallow")); err == nil && !fi.IsDir() {
		args = append(args, "--unshallow")
	}

	cmd := utils.Command(ctx, "git", args...)
	cmd.Dir = r.repoPath
	return cmd.Run()
}
