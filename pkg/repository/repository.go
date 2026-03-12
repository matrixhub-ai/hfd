package repository

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/matrixhub-ai/hfd/internal/utils"
)

var (
	ErrRepositoryNotExists = git.ErrRepositoryNotExists
	ErrRevisionNotFound    = plumbing.ErrReferenceNotFound
)

const (
	TimeFormat = "2006-01-02T15:04:05.000Z"
)

const (
	GitUploadPack      = "git-upload-pack"
	GitReceivePack     = "git-receive-pack"
	GitLFSAuthenticate = "git-lfs-authenticate"
	GitLFSTransfer     = "git-lfs-transfer"
)

// Repository represents a Git repository and provides methods to interact with it.
type Repository struct {
	repo     *git.Repository
	repoPath string
}

// IsRepository checks if the given path is a valid git repository by looking for the HEAD file and ensuring it's not empty.
func IsRepository(repoPath string) bool {
	stat, err := os.Stat(filepath.Join(repoPath, "HEAD"))
	if err == nil && stat.Size() != 0 {
		return true
	}
	return false
}

// IsValidGitProtocol reports whether value is a valid GIT_PROTOCOL string.
// Valid values match "version=N" where N is one or more decimal digits.
func IsValidGitProtocol(value string) bool {
	const prefix = "version="
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	ver := value[len(prefix):]
	if len(ver) == 0 {
		return false
	}
	for _, c := range ver {
		if c < '0' || c > '2' {
			return false
		}
	}
	return true
}

// Init initializes a new git repository at the given path with the specified default branch.
func Init(repoPath string, defaultBranch string) (*Repository, error) {
	cmd := utils.Command(context.Background(), "git", "init", "--bare", repoPath, "--initial-branch", defaultBranch)
	if err := cmd.Run(); err != nil {
		_ = os.RemoveAll(repoPath)
		return nil, fmt.Errorf("failed to initialize git repository: %w", err)
	}

	repo, err := Open(repoPath)
	if err != nil {
		_ = os.RemoveAll(repoPath)
		return nil, fmt.Errorf("failed to open git repository: %w", err)
	}

	return repo, nil
}

// Open opens an existing git repository at the given path.
func Open(repoPath string) (*Repository, error) {
	repo, err := git.PlainOpenWithOptions(repoPath, &git.PlainOpenOptions{})
	if err != nil {
		return nil, err
	}
	return &Repository{
		repo:     repo,
		repoPath: repoPath,
	}, nil
}

// RepoPath returns the filesystem path of the repository.
func (r *Repository) RepoPath() string {
	return r.repoPath
}

// SplitRevisionAndPath splits a refpath into a revision (branch or tag) and a file path.
func (r *Repository) SplitRevisionAndPath(refpath string) (rev string, path string, err error) {
	if refpath == "" {
		return r.DefaultBranch(), "", nil
	}

	branches, err := r.Branches()
	if err != nil {
		return "", "", err
	}

	// Sort branches by length (longest first) to match the most specific branch
	sortedBranches := make([]string, len(branches))
	copy(sortedBranches, branches)
	sort.Slice(sortedBranches, func(i, j int) bool {
		return len(sortedBranches[i]) > len(sortedBranches[j])
	})

	for _, branch := range sortedBranches {
		if refpath == branch {
			return branch, "", nil
		}
		if strings.HasPrefix(refpath, branch+"/") {
			return branch, refpath[len(branch)+1:], nil
		}
	}

	// Fallback: treat first segment as branch
	parts := strings.SplitN(refpath, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1], nil
	}
	return refpath, "", nil
}

// DefaultBranch returns the default branch name of the repository by reading the HEAD file.
func (r *Repository) DefaultBranch() string {
	head := filepath.Join(r.repoPath, "HEAD")
	data, err := os.ReadFile(head)
	if err != nil {
		return "main"
	}

	prefix := "ref: refs/heads/"
	if len(data) > len(prefix) && string(data[:len(prefix)]) == prefix {
		return string(bytes.TrimSpace(data[len(prefix):]))
	}
	return "main"
}

// Branches returns a list of branch names in the repository.
func (r *Repository) Branches() ([]string, error) {
	branchesIter, err := r.repo.Branches()
	if err != nil {
		return nil, err
	}

	var branches []string
	err = branchesIter.ForEach(func(ref *plumbing.Reference) error {
		name := ref.Name().Short()
		branches = append(branches, name)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if len(branches) == 0 {
		return []string{r.DefaultBranch()}, nil
	}
	return branches, nil
}

// Remove deletes the repository directory and all its contents from disk.
func (r *Repository) Remove() error {
	return os.RemoveAll(r.repoPath)
}

// validateRefName checks if a git rev name component is valid.
// It rejects names that could cause problems with git rev storage.
func validateRefName(name string) error {
	if name == "" {
		return fmt.Errorf("rev name cannot be empty")
	}
	if strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") {
		return fmt.Errorf("rev name cannot start or end with '/'")
	}
	if strings.HasPrefix(name, ".") || strings.Contains(name, "..") {
		return fmt.Errorf("rev name cannot start with '.' or contain '..'")
	}
	if strings.HasSuffix(name, ".lock") {
		return fmt.Errorf("rev name cannot end with '.lock'")
	}
	if strings.ContainsAny(name, " ~^:?*[\\") {
		return fmt.Errorf("rev name contains invalid characters")
	}
	if strings.Contains(name, "@{") {
		return fmt.Errorf("rev name cannot contain '@{'")
	}
	if strings.Contains(name, "//") {
		return fmt.Errorf("rev name cannot contain consecutive slashes")
	}
	return nil
}

// Tags returns a list of tag names in the repository.
func (r *Repository) Tags() ([]string, error) {
	tagsIter, err := r.repo.Tags()
	if err != nil {
		return nil, err
	}

	var tags []string
	err = tagsIter.ForEach(func(ref *plumbing.Reference) error {
		name := ref.Name().Short()
		tags = append(tags, name)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return tags, nil
}

// ResolveRevision resolves a revision string (branch name, tag, or commit SHA) to a commit hash.
func (r *Repository) ResolveRevision(rev string) (string, error) {
	if rev == "" {
		rev = r.DefaultBranch()
	}
	hash, err := r.repo.ResolveRevision(plumbing.Revision(rev))
	if err != nil {
		return "", err
	}
	return hash.String(), nil
}

// CreateBranch creates a new branch pointing to the given revision.
func (r *Repository) CreateBranch(name string, revision string) error {
	if err := validateRefName(name); err != nil {
		return fmt.Errorf("invalid branch name %q: %w", name, err)
	}
	if revision == "" {
		revision = r.DefaultBranch()
	}
	hash, err := r.repo.ResolveRevision(plumbing.Revision(revision))
	if err != nil {
		return fmt.Errorf("failed to resolve revision %q: %w", revision, err)
	}
	refName := plumbing.NewBranchReferenceName(name)
	ref := plumbing.NewHashReference(refName, *hash)
	return r.repo.Storer.SetReference(ref)
}

// DeleteBranch deletes a branch from the repository.
func (r *Repository) DeleteBranch(name string) error {
	refName := plumbing.NewBranchReferenceName(name)
	return r.repo.Storer.RemoveReference(refName)
}

// CreateTag creates a lightweight tag pointing to the given revision.
func (r *Repository) CreateTag(name string, revision string) error {
	if err := validateRefName(name); err != nil {
		return fmt.Errorf("invalid tag name %q: %w", name, err)
	}
	if revision == "" {
		revision = r.DefaultBranch()
	}
	hash, err := r.repo.ResolveRevision(plumbing.Revision(revision))
	if err != nil {
		return fmt.Errorf("failed to resolve revision %q: %w", revision, err)
	}
	refName := plumbing.NewTagReferenceName(name)
	rev := plumbing.NewHashReference(refName, *hash)
	return r.repo.Storer.SetReference(rev)
}

// DeleteTag deletes a tag from the repository.
func (r *Repository) DeleteTag(name string) error {
	refName := plumbing.NewTagReferenceName(name)
	return r.repo.Storer.RemoveReference(refName)
}

// BranchExists checks if a branch with the given name exists.
func (r *Repository) BranchExists(name string) (bool, error) {
	refName := plumbing.NewBranchReferenceName(name)
	_, err := r.repo.Storer.Reference(refName)
	if err == nil {
		return true, nil
	}
	if err == plumbing.ErrReferenceNotFound {
		return false, nil
	}
	return false, err
}

// TagExists checks if a tag with the given name exists.
func (r *Repository) TagExists(name string) (bool, error) {
	refName := plumbing.NewTagReferenceName(name)
	_, err := r.repo.Storer.Reference(refName)
	if err == nil {
		return true, nil
	}
	if err == plumbing.ErrReferenceNotFound {
		return false, nil
	}
	return false, err
}

// Refs returns a map of all reference names (e.g. "refs/heads/main", "refs/tags/v1.0")
// to their commit hashes. This is useful for snapshotting the ref state before/after operations.
func (r *Repository) Refs() (map[string]string, error) {
	refs := make(map[string]string)

	iter, err := r.repo.References()
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	err = iter.ForEach(func(ref *plumbing.Reference) error {
		name := ref.Name().String()
		// Skip HEAD as it's a symbolic reference, not a concrete ref
		if name == "HEAD" {
			return nil
		}
		refs[name] = ref.Hash().String()
		return nil
	})
	if err != nil {
		return nil, err
	}

	return refs, nil
}

// RefHash returns the commit hash a rev (branch or tag) points to.
func (r *Repository) RefHash(refName plumbing.ReferenceName) (string, error) {
	rev, err := r.repo.Storer.Reference(refName)
	if err != nil {
		return "", err
	}
	return rev.Hash().String(), nil
}

// Move renames the repository directory to newPath.
func (r *Repository) Move(newPath string) error {
	if err := os.MkdirAll(filepath.Dir(newPath), 0755); err != nil {
		return err
	}
	return os.Rename(r.repoPath, newPath)
}

// DiskUsage returns the total disk usage of the repository in bytes.
// This includes the on-disk size of the git repository directory plus the
// declared sizes of any LFS-tracked objects (from their pointer files).
func (r *Repository) DiskUsage() (int64, error) {
	var total int64
	err := filepath.Walk(r.repoPath, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	// Add declared LFS content sizes. LFS objects are stored outside the git
	// repo directory, so the walk above only captures the tiny pointer blobs.
	lfsPointers, err := r.ScanLFSPointers()
	if err != nil {
		return 0, err
	}
	for _, ptr := range lfsPointers {
		total += ptr.Size()
	}

	return total, nil
}
