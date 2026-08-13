package repository

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6"
	gitconfig "github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/plumbing/client"
	"github.com/go-git/go-git/v6/plumbing/protocol"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/filesystem"
)

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

// fileLoader resolves file:// URLs to bare repository storage using absolute
// paths. go-git's transport.DefaultLoader chroots through billy's BoundOS,
// which can yield a cwd-relative root on Linux and then fail to find the
// repository when the process cwd is not /.
type fileLoader struct{}

func (fileLoader) Load(u *url.URL) (storage.Storer, error) {
	path := u.Path
	if fi, err := os.Stat(filepath.Join(path, "config")); err != nil || fi.IsDir() {
		return nil, transport.ErrRepositoryNotFound
	}
	return filesystem.NewStorageWithOptions(osfs.New(path), cache.NewObjectLRUDefault(), filesystem.Options{}), nil
}

// mirrorClientOptions configures the transport client used for mirror operations.
var mirrorClientOptions = []client.Option{client.WithLoader(fileLoader{})}

// getRemoteAdvertisedRefs opens an upload-pack session to sourceURL and returns
// the advertised refs, with HEAD resolved to a symbolic reference.
func getRemoteAdvertisedRefs(ctx context.Context, sourceURL string) (*transport.RemoteRefs, error) {
	u, err := transport.ParseURL(sourceURL)
	if err != nil {
		return nil, err
	}
	sess, err := client.New(mirrorClientOptions...).Handshake(ctx, &transport.Request{
		URL:     u,
		Command: transport.UploadPackService,
		// Prefer wire protocol v2 like the git binary: v0/v1 advertisements
		// cannot express an unborn HEAD symref.
		Protocol: protocol.V2,
	})
	if err != nil {
		return nil, err
	}
	defer sess.Close()
	return sess.GetRemoteRefs(ctx, nil)
}

// GetRemoteDefaultBranch retrieves the default branch name of the repository at the given source URL.
func GetRemoteDefaultBranch(ctx context.Context, sourceURL string) (string, error) {
	remoteRefs, err := getRemoteAdvertisedRefs(ctx, sourceURL)
	if err != nil {
		return "", err
	}

	const prefix = "refs/heads/"
	for _, ref := range remoteRefs.References {
		if ref.Name() != plumbing.HEAD || ref.Type() != plumbing.SymbolicReference {
			continue
		}
		if target := ref.Target().String(); strings.HasPrefix(target, prefix) {
			return strings.TrimPrefix(target, prefix), nil
		}
	}
	// Empty repository: HEAD points at a branch that has no commits yet.
	if target := remoteRefs.Unborn.String(); strings.HasPrefix(target, prefix) {
		return strings.TrimPrefix(target, prefix), nil
	}
	return "", fmt.Errorf("HEAD symref not found in remote advertisement")
}

// GetRemoteRefs returns a list of all ref names from the sourceURL.
// The returned names are fully qualified (e.g. "refs/heads/main", "refs/tags/v1.0").
func GetRemoteRefs(ctx context.Context, sourceURL string) (map[string]string, error) {
	remoteRefs, err := getRemoteAdvertisedRefs(ctx, sourceURL)
	if err != nil {
		// Match `git ls-remote --refs`: an empty repository lists no refs
		// instead of failing.
		if errors.Is(err, transport.ErrEmptyRemoteRepository) {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("failed to list remote refs: %w", err)
	}

	refs := make(map[string]string)
	for _, ref := range remoteRefs.References {
		if ref.Type() != plumbing.HashReference {
			continue
		}
		name := ref.Name().String()
		// Match `git ls-remote --refs`: only refs/*, excluding peeled entries.
		if !strings.HasPrefix(name, "refs/") || strings.HasSuffix(name, "^{}") {
			continue
		}
		refs[name] = ref.Hash().String()
	}
	return refs, nil
}

// mirrorRemote returns an in-memory remote bound to the given URL, without
// touching the repository configuration.
func (r *Repository) mirrorRemote(url string) *git.Remote {
	return git.NewRemote(r.repo.Storer, &gitconfig.RemoteConfig{
		Name: "mirror",
		URLs: []string{url},
	})
}

// PushMirrorRefs pushes the specified refspecs to the destination URL.
// Each refspec should be "+src:dst" for create/update or ":dst" for delete.
// When prune is true, remote refs matching the refspecs that do not exist
// locally are removed, matching `git push --prune` semantics.
// Server progress messages are written to progress if non-nil.
func (r *Repository) PushMirrorRefs(ctx context.Context, destURL string, refs []string, prune bool, progress io.Writer) error {
	if len(refs) == 0 {
		return nil
	}

	specs := make([]gitconfig.RefSpec, 0, len(refs))
	for _, refspec := range refs {
		specs = append(specs, gitconfig.RefSpec(refspec))
	}

	// Compute prune deletions ourselves instead of using go-git's
	// PushOptions.Prune: its refspec reversal keeps the force prefix on the
	// destination side, so local-existence lookups never match and every
	// matching remote ref gets deleted. It also removes local refs that are
	// missing on the remote, which push must never do.
	if prune {
		deletes, err := r.pushPruneRefSpecs(ctx, destURL, specs)
		if err != nil {
			return fmt.Errorf("failed to compute remote refs to prune: %w", err)
		}
		specs = append(specs, deletes...)
	}

	err := r.mirrorRemote(destURL).PushContext(ctx, &git.PushOptions{
		RemoteName:    "mirror",
		RemoteURL:     destURL,
		RefSpecs:      specs,
		Progress:      progress,
		ClientOptions: mirrorClientOptions,
	})
	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		return fmt.Errorf("failed to push mirror refs to remote: %w", err)
	}

	return nil
}

// pushPruneRefSpecs returns delete refspecs (":dst") for remote refs that
// match the destination side of specs but no longer have a local counterpart,
// like `git push --prune`.
func (r *Repository) pushPruneRefSpecs(ctx context.Context, destURL string, specs []gitconfig.RefSpec) ([]gitconfig.RefSpec, error) {
	remoteRefs, err := GetRemoteRefs(ctx, destURL)
	if err != nil {
		return nil, err
	}
	localRefs, err := r.Refs()
	if err != nil {
		return nil, err
	}

	// Reverse the specs manually (dst:src, force prefix stripped) so remote
	// names can be mapped back to their local source refs.
	reversed := make([]gitconfig.RefSpec, 0, len(specs))
	for _, spec := range specs {
		if spec.IsDelete() {
			continue
		}
		raw := strings.TrimPrefix(spec.String(), "+")
		src, dst, ok := strings.Cut(raw, ":")
		if !ok {
			dst = src
		}
		reversed = append(reversed, gitconfig.RefSpec(dst+":"+src))
	}

	var deletes []gitconfig.RefSpec
	for remoteName := range remoteRefs {
		name := plumbing.ReferenceName(remoteName)
		matched, hasLocal := false, false
		for _, rev := range reversed {
			if !rev.Match(name) {
				continue
			}
			matched = true
			if _, ok := localRefs[rev.Dst(name).String()]; ok {
				hasLocal = true
				break
			}
		}
		if matched && !hasLocal {
			deletes = append(deletes, gitconfig.RefSpec(":"+remoteName))
		}
	}
	return deletes, nil
}

// PullMirrorRefs fetches the specified refs from the sourceURL and updates the local mirror repository.
// Local refs that are not in the specified list are pruned.
// Server progress messages are written to progress if non-nil.
func (r *Repository) PullMirrorRefs(ctx context.Context, sourceURL string, refs []string, progress io.Writer) error {
	if len(refs) == 0 {
		return nil
	}

	specs := make([]gitconfig.RefSpec, 0, len(refs))
	for _, ref := range refs {
		specs = append(specs, gitconfig.RefSpec("+"+ref+":"+ref))
	}

	err := r.mirrorRemote(sourceURL).FetchContext(ctx, &git.FetchOptions{
		RemoteName:    "mirror",
		RemoteURL:     sourceURL,
		RefSpecs:      specs,
		Tags:          plumbing.NoTags,
		Force:         true,
		Progress:      progress,
		ClientOptions: mirrorClientOptions,
	})
	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
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
			_ = r.repo.Storer.RemoveReference(plumbing.ReferenceName(refName))
		}
	}

	return nil
}
