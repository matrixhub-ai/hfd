package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/filesystem"
	"github.com/go-git/go-git/v6/storage/filesystem/dotgit"

	"github.com/matrixhub-ai/hfd/internal/lru"
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
	repo          *git.Repository
	fs            billy.Filesystem
	repoPath      string
	defaultBranch atomic.Pointer[string]
}

// newStorer returns a go-git storer for the bare repository at repoPath on fs.
func newStorer(fs billy.Filesystem, repoPath string) *filesystem.Storage {
	opts := filesystem.Options{}
	if bound, ok := fs.(*sharedObjectsFS); ok {
		// go-git resolves relative alternates from the filesystem root.
		opts.AlternatesFS = bound.dataFS
	}
	return filesystem.NewStorageWithOptions(chroot.New(fs, repoPath), cache.NewObjectLRUDefault(), opts)
}

// IsRepository checks if the given path is a valid git repository by looking for the HEAD file and ensuring it's not empty.
func IsRepository(fs billy.Filesystem, repoPath string) bool {
	if _, ok := lruCache.Get(cacheKey{fs, repoPath}); ok {
		return true
	}
	ok, _ := hasHEAD(fs, repoPath)
	return ok
}

// hasHEAD is IsRepository's on-disk check alone; lruCache.Get would promote the entry.
func hasHEAD(fs billy.Filesystem, repoPath string) (bool, error) {
	stat, err := fs.Stat(filepath.Join(repoPath, "HEAD"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return stat.Size() != 0, nil
}

// Walk visits bare repositories in lexical order without descending into them.
func Walk(ctx context.Context, fs billy.Filesystem, root string, fn func(path string) error) error {
	return walk(ctx, fs, root, fn)
}

func walk(ctx context.Context, fs billy.Filesystem, dir string, fn func(path string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.HasSuffix(dir, ".git") {
		ok, err := hasHEAD(fs, dir)
		if err != nil {
			return err
		}
		if ok {
			return fn(dir)
		}
	}
	entries, err := fs.ReadDir(dir)
	if err != nil {
		return err
	}
	// osfs returns raw directory order; only memfs and s3fs sort.
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		path := filepath.Join(dir, e.Name())
		err = walk(ctx, fs, path, fn)
		if err != nil {
			return err
		}
	}
	return nil
}

// Gitlinks are excluded; known blobs are yielded without reading their contents.
func (r *Repository) WalkObjects(ctx context.Context, fn func(hash plumbing.Hash, typ plumbing.ObjectType) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	refs, err := r.repo.References()
	if err != nil {
		return fmt.Errorf("failed to get references: %w", err)
	}
	defer refs.Close()
	w := &objectWalker{ctx: ctx, storer: r.repo.Storer, fn: fn, seen: map[plumbing.Hash]plumbing.ObjectType{}}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ref, err := refs.Next()
		if err == io.EOF {
			return ctx.Err()
		}
		if err != nil {
			return fmt.Errorf("read references: %w", err)
		}
		if ref.Type() != plumbing.HashReference {
			continue
		}
		if err := w.walk(ref); err != nil {
			return err
		}
	}
}

type objectWalker struct {
	ctx     context.Context
	storer  storer.EncodedObjectStorer
	fn      func(plumbing.Hash, plumbing.ObjectType) error
	seen    map[plumbing.Hash]plumbing.ObjectType
	ref     plumbing.ReferenceName
	pending []link
}

// link is a queued object with the type its referrer declared; a ref root declares AnyObject.
type link struct {
	hash plumbing.Hash
	typ  plumbing.ObjectType
}

// walk keeps commit parents and tag targets on an explicit stack so history depth never recurses.
func (w *objectWalker) walk(ref *plumbing.Reference) error {
	w.ref = ref.Name()
	w.pending = append(w.pending[:0], link{ref.Hash(), plumbing.AnyObject})
	for len(w.pending) > 0 {
		next := w.pending[len(w.pending)-1]
		w.pending = w.pending[:len(w.pending)-1]
		if err := w.visit(next.hash, next.typ); err != nil {
			return err
		}
	}
	return nil
}

// visit loads hash, checks it against the declared typ and yields it once.
// Reaching one hash as two types fails closed like C git instead of skipping a subtree.
func (w *objectWalker) visit(hash plumbing.Hash, typ plumbing.ObjectType) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if known, ok := w.seen[hash]; ok {
		if known == plumbing.BlobObject && (typ == plumbing.AnyObject || typ == plumbing.BlobObject) {
			// An unread leaf named by a ref or tag must really be a blob.
			obj, err := w.storer.EncodedObject(plumbing.AnyObject, hash)
			if err != nil {
				return fmt.Errorf("ref %s: read object %s: %w", w.ref, hash, err)
			}
			known, typ = obj.Type(), plumbing.BlobObject
		}
		if typ != plumbing.AnyObject && known != typ {
			return fmt.Errorf("ref %s: object %s is a %s, expected %s", w.ref, hash, known, typ)
		}
		return nil
	}
	obj, err := object.GetObject(w.storer, hash)
	if err != nil {
		return fmt.Errorf("ref %s: read object %s: %w", w.ref, hash, err)
	}
	if typ != plumbing.AnyObject && obj.Type() != typ {
		return fmt.Errorf("ref %s: object %s is a %s, expected %s", w.ref, hash, obj.Type(), typ)
	}
	w.seen[hash] = obj.Type()
	if err := w.fn(hash, obj.Type()); err != nil {
		return err
	}
	switch obj := obj.(type) {
	case *object.Tag:
		w.pending = append(w.pending, link{obj.Target, obj.TargetType})
	case *object.Commit:
		for _, parent := range obj.ParentHashes {
			w.pending = append(w.pending, link{parent, plumbing.CommitObject})
		}
		return w.visit(obj.TreeHash, plumbing.TreeObject)
	case *object.Tree:
		for _, entry := range obj.Entries {
			switch entry.Mode {
			case filemode.Dir:
				err = w.visit(entry.Hash, plumbing.TreeObject)
			case filemode.Submodule:
			default:
				err = w.leaf(entry.Hash)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// leaf yields a tree entry's blob without loading it.
func (w *objectWalker) leaf(hash plumbing.Hash) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if known, ok := w.seen[hash]; ok {
		if known != plumbing.BlobObject {
			return fmt.Errorf("ref %s: object %s is a %s, expected %s", w.ref, hash, known, plumbing.BlobObject)
		}
		return nil
	}
	w.seen[hash] = plumbing.BlobObject
	return w.fn(hash, plumbing.BlobObject)
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

// Init initializes a new git repository on fs at the given path with the specified default branch.
func Init(ctx context.Context, fs billy.Filesystem, repoPath string, defaultBranch string) (*Repository, error) {
	_, err := git.Init(newStorer(fs, repoPath), git.WithDefaultBranch(plumbing.NewBranchReferenceName(defaultBranch)))
	if err != nil {
		_ = util.RemoveAll(fs, repoPath)
		return nil, fmt.Errorf("failed to initialize git repository: %w", err)
	}

	repo, err := Open(fs, repoPath)
	if err != nil {
		_ = util.RemoveAll(fs, repoPath)
		return nil, fmt.Errorf("failed to open git repository: %w", err)
	}

	return repo, nil
}

// cacheKey identifies an open repository by its filesystem and path.
type cacheKey struct {
	fs   billy.Filesystem
	path string
}

var lruCache = lru.New[cacheKey, *Repository](128) // Cache up to 128 repositories in memory

// Open opens an existing git repository on fs at the given path.
func Open(fs billy.Filesystem, repoPath string) (repo *Repository, err error) {
	repo, ok := lruCache.GetOrNew(cacheKey{fs, repoPath}, func() (*Repository, bool) {
		var r *git.Repository
		st := newStorer(fs, repoPath)
		if bound, ok := fs.(*sharedObjectsFS); ok {
			objects := filesystem.NewObjectStorage(dotgit.New(bound.objectsFS), cache.NewObjectLRUDefault())
			r, err = git.Open(&sharedStorer{Storer: st, local: st, objects: objects}, nil)
			if err == nil {
				err = bound.ensure(repoPath)
			}
		} else {
			r, err = git.Open(st, nil)
		}
		if err != nil {
			return nil, false
		}
		return &Repository{
			repo:     r,
			fs:       fs,
			repoPath: repoPath,
		}, true

	})
	if ok {
		return repo, nil
	}
	return nil, err
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
	if db := r.defaultBranch.Load(); db != nil {
		return *db
	}
	db := r.parseDefaultBranch()
	r.defaultBranch.Store(&db)
	return db
}

func (r *Repository) parseDefaultBranch() string {
	head := filepath.Join(r.repoPath, "HEAD")
	data, err := util.ReadFile(r.fs, head)
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
	lruCache.Remove(cacheKey{r.fs, r.repoPath})
	return util.RemoveAll(r.fs, r.repoPath)
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
	if err := r.fs.MkdirAll(filepath.Dir(newPath), 0755); err != nil {
		return err
	}
	lruCache.Remove(cacheKey{r.fs, r.repoPath})
	lruCache.Remove(cacheKey{r.fs, newPath})
	if err := r.fs.Rename(r.repoPath, newPath); err != nil {
		return err
	}
	if bound, ok := r.fs.(*sharedObjectsFS); ok {
		// C git reads the alternates depth as-is; only the next Open would rewrite it.
		return bound.ensure(newPath)
	}
	return nil
}

// DiskUsage returns the total disk usage of the repository in bytes.
// This includes the on-disk size of the git repository directory plus the
// declared sizes of any LFS-tracked objects (from their pointer files).
func (r *Repository) DiskUsage(ctx context.Context) (int64, error) {
	var total int64
	err := util.Walk(r.fs, r.repoPath, func(_ string, info os.FileInfo, err error) error {
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
	lfsPointers, err := r.ScanLFSPointers(ctx)
	if err != nil {
		return 0, err
	}
	for _, ptr := range lfsPointers {
		total += ptr.Size()
	}

	return total, nil
}
