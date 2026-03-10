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

// MirrorRefFilterFunc filters which refs should be synced during mirror operations.
// It receives the repository name and a list of remote ref names (e.g. "refs/heads/main",
// "refs/tags/v1.0") and returns the filtered list of refs to sync.
type MirrorRefFilterFunc func(ctx context.Context, repoName string, refs []string) ([]string, error)

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

	return repo, nil
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
func (r *Repository) IsMirror() (bool, error) {
	config, err := r.repo.Config()
	if err != nil {
		return false, err
	}

	if config != nil {
		if remote, ok := config.Remotes["origin"]; ok {
			if len(remote.URLs) > 0 {
				return true, nil
			}
		}
	}
	return false, nil
}

// SyncMirror syncs all refs from the origin remote, optionally unshallowing if needed.
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

// ListRemoteRefs returns a list of all ref names from the "origin" remote.
// The returned names are fully qualified (e.g. "refs/heads/main", "refs/tags/v1.0").
func (r *Repository) ListRemoteRefs(ctx context.Context) ([]string, error) {
	cmd := utils.Command(ctx, "git", "ls-remote", "--refs", "origin")
	cmd.Dir = r.repoPath
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list remote refs: %w", err)
	}

	var refs []string
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
		refs = append(refs, parts[1])
	}
	return refs, nil
}

// SyncMirrorRefs syncs only the specified refs from the origin remote.
// Local refs that are not in the specified list are pruned.
func (r *Repository) SyncMirrorRefs(ctx context.Context, refs []string) error {
	if len(refs) == 0 {
		return nil
	}

	args := []string{
		"fetch",
		"origin",
		"--no-tags",
		"--progress",
	}

	if fi, err := os.Stat(filepath.Join(r.repoPath, "shallow")); err == nil && !fi.IsDir() {
		args = append(args, "--unshallow")
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
