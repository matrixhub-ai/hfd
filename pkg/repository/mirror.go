package repository

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/client"
	"github.com/go-git/go-git/v6/plumbing/transport"

	"github.com/matrixhub-ai/hfd/internal/utils"
)

// MirrorSourceFunc defines a function type for determining the source URL of a repository mirror.
// It receives the repository name and returns the source URL, a boolean indicating whether
// the mirror should be enabled for this repository, and an error if any occurs during the process.
type MirrorSourceFunc func(ctx context.Context, repoName string) (string, bool, error)

// MirrorDestinationFunc defines a function type for determining the destination URL of a repository push mirror.
// It receives the repository name and returns the destination URL, a boolean indicating whether
// push mirroring is enabled for this repository, and an error if any occurs during the process.
type MirrorDestinationFunc func(ctx context.Context, repoName string) (string, bool, error)

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

// gitHTTPClient is shared by all remote git operations so HTTPS connections
// are pooled instead of being re-dialed per operation.
var gitHTTPClient = &http.Client{
	Transport: http.DefaultTransport.(*http.Transport).Clone(),
}

// gitClientOptions makes go-git transport clients reuse gitHTTPClient.
var gitClientOptions = []client.Option{
	client.WithHTTPClient(gitHTTPClient),
}

// gitClient is the shared transport client for direct session use.
var gitClient = client.New(gitClientOptions...)

// getRemoteRefs opens an upload-pack session to the sourceURL and returns the
// advertised refs, with HEAD resolved to a symbolic reference.
func getRemoteRefs(ctx context.Context, sourceURL string) (*transport.RemoteRefs, error) {
	u, err := transport.ParseURL(sourceURL)
	if err != nil {
		return nil, err
	}

	sess, err := gitClient.Handshake(ctx, &transport.Request{
		URL:      u,
		Command:  transport.UploadPackService,
		Protocol: config.DefaultProtocolVersion,
	})
	if err != nil {
		return nil, err
	}
	defer sess.Close()

	return sess.GetRemoteRefs(ctx, nil)
}

// GetRemoteDefaultBranch retrieves the default branch name of the repository at the given source URL.
func GetRemoteDefaultBranch(ctx context.Context, sourceURL string) (string, error) {
	remoteRefs, err := getRemoteRefs(ctx, sourceURL)
	if err != nil {
		return "", err
	}

	head := remoteRefs.Unborn
	if head == "" {
		for _, ref := range remoteRefs.References {
			if ref.Name() == plumbing.HEAD && ref.Type() == plumbing.SymbolicReference {
				head = ref.Target()
				break
			}
		}
	}
	if !head.IsBranch() {
		return "", fmt.Errorf("HEAD symref not found for %q", sourceURL)
	}
	return head.Short(), nil
}

// GetRemoteRefs returns a list of all ref names from the sourceURL.
// The returned names are fully qualified (e.g. "refs/heads/main", "refs/tags/v1.0").
func GetRemoteRefs(ctx context.Context, sourceURL string) (map[string]string, error) {
	remoteRefs, err := getRemoteRefs(ctx, sourceURL)
	if err != nil {
		// An empty repository has no refs to list, matching `git ls-remote --refs`.
		if errors.Is(err, transport.ErrEmptyRemoteRepository) {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("failed to list remote refs: %w", err)
	}

	refs := make(map[string]string, len(remoteRefs.References))
	for _, ref := range remoteRefs.References {
		if ref.Type() != plumbing.HashReference {
			continue
		}
		name := ref.Name().String()
		// Match `git ls-remote --refs`: skip HEAD and peeled tag entries.
		if !strings.HasPrefix(name, "refs/") || strings.HasSuffix(name, "^{}") {
			continue
		}
		refs[name] = ref.Hash().String()
	}
	return refs, nil
}

// PushMirrorRefs pushes the specified refspecs to the destination URL.
// Each refspec should be "+src:dst" for create/update or ":dst" for delete.
// When prune is true, remote refs matching the refspecs that do not exist
// locally are removed.
func (r *Repository) PushMirrorRefs(ctx context.Context, destURL string, refspecs []string, prune bool) error {
	if len(refspecs) == 0 {
		return nil
	}

	specs := make([]config.RefSpec, 0, len(refspecs))
	for _, refspec := range refspecs {
		specs = append(specs, config.RefSpec(refspec))
	}

	remote := git.NewRemote(r.repo.Storer, &config.RemoteConfig{
		Name: "mirror",
		URLs: []string{destURL},
	})
	err := remote.PushContext(ctx, &git.PushOptions{
		// PushContext requires RemoteName to match the remote config name.
		RemoteName:    "mirror",
		RefSpecs:      specs,
		Prune:         prune,
		Progress:      utils.CommandOutput(ctx),
		ClientOptions: gitClientOptions,
	})
	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		return fmt.Errorf("failed to push mirror refs to remote: %w", err)
	}

	return nil
}

// SyncMirrorRefs syncs only the specified refs from the sourceURL.
// Local refs that are not in the specified list are pruned.
//
//go:fix inline
func (r *Repository) SyncMirrorRefs(ctx context.Context, sourceURL string, refs []string) error {
	return r.PullMirrorRefs(ctx, sourceURL, refs)
}

// PullMirrorRefs fetches the specified refs from the sourceURL and updates the local mirror repository.
// Local refs that are not in the specified list are pruned.
func (r *Repository) PullMirrorRefs(ctx context.Context, sourceURL string, refs []string) error {
	if len(refs) == 0 {
		return nil
	}

	// Explicit force refspecs for each desired ref.
	refspecs := make([]config.RefSpec, 0, len(refs))
	for _, ref := range refs {
		refspecs = append(refspecs, config.RefSpec("+"+ref+":"+ref))
	}

	remote := git.NewRemote(r.repo.Storer, &config.RemoteConfig{
		Name: "mirror",
		URLs: []string{sourceURL},
	})
	err := remote.FetchContext(ctx, &git.FetchOptions{
		RefSpecs:      refspecs,
		Tags:          plumbing.NoTags,
		Force:         true,
		Progress:      utils.CommandOutput(ctx),
		ClientOptions: gitClientOptions,
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
