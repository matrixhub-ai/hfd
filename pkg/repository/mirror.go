package repository

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6"
	gitconfig "github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/plumbing/client"
	"github.com/go-git/go-git/v6/plumbing/protocol"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/filesystem"
)

// InitMirror initializes a new bare git repository on fs at repoPath.
// The returned Repository is ready to be used as a mirror of the source repository.
func InitMirror(ctx context.Context, fs billy.Filesystem, repoPath string, sourceURL string) (*Repository, error) {
	sourceURL = strings.TrimSuffix(sourceURL, "/")
	sourceURL = strings.TrimSuffix(sourceURL, ".git") + ".git"

	defaultBranch, err := GetRemoteDefaultBranch(ctx, sourceURL)
	if err != nil {
		return nil, fmt.Errorf("failed to get HEAD from source repository: %w", err)
	}

	return Init(ctx, fs, repoPath, defaultBranch)
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

func getRemoteAdvertisedRefs(ctx context.Context, url, service string) (*transport.RemoteRefs, error) {
	u, err := transport.ParseURL(url)
	if err != nil {
		return nil, err
	}
	req := &transport.Request{URL: u, Command: service}
	if service == transport.UploadPackService {
		// Prefer wire protocol v2 like the git binary: v0/v1 advertisements
		// cannot express an unborn HEAD symref.
		req.Protocol = protocol.V2
	}
	sess, err := client.New(mirrorClientOptions...).Handshake(ctx, req)
	if err != nil {
		return nil, err
	}
	defer sess.Close()
	return sess.GetRemoteRefs(ctx, nil)
}

// GetRemoteDefaultBranch retrieves the default branch name of the repository at the given source URL.
func GetRemoteDefaultBranch(ctx context.Context, sourceURL string) (string, error) {
	remoteRefs, err := getRemoteAdvertisedRefs(ctx, sourceURL, transport.UploadPackService)
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
	refs, err := advertisedHashRefs(ctx, sourceURL, transport.UploadPackService)
	if err != nil {
		return nil, fmt.Errorf("failed to list remote refs: %w", err)
	}
	return refs, nil
}

func advertisedHashRefs(ctx context.Context, url, service string) (map[string]string, error) {
	remoteRefs, err := getRemoteAdvertisedRefs(ctx, url, service)
	if err != nil {
		if errors.Is(err, transport.ErrEmptyRemoteRepository) {
			return map[string]string{}, nil
		}
		return nil, err
	}

	refs := make(map[string]string)
	for _, ref := range remoteRefs.References {
		if ref.Type() != plumbing.HashReference {
			continue
		}
		name := ref.Name().String()
		// Only refs/*, excluding peeled entries.
		if !strings.HasPrefix(name, "refs/") || strings.HasSuffix(name, "^{}") {
			continue
		}
		refs[name] = ref.Hash().String()
	}
	return refs, nil
}

type remoteRefsFunc func() (map[string]string, error)

// Use receive-pack so discovering push targets does not require read permission.
func pushRemoteRefs(ctx context.Context, destURL string) remoteRefsFunc {
	var refs map[string]string
	var err error
	return func() (map[string]string, error) {
		if refs == nil && err == nil {
			if refs, err = advertisedHashRefs(ctx, destURL, transport.ReceivePackService); err != nil {
				err = fmt.Errorf("failed to read destination refs: %w", err)
			}
		}
		return refs, err
	}
}

// mirrorRemote returns an in-memory remote bound to the given URL, without
// touching the repository configuration.
func (r *Repository) mirrorRemote(url string) *git.Remote {
	return git.NewRemote(r.repo.Storer, &gitconfig.RemoteConfig{
		Name: "mirror",
		URLs: []string{url},
	})
}

// PushMirrorRefs pushes refspecs with optional pruning and progress output.
func (r *Repository) PushMirrorRefs(ctx context.Context, destURL string, refs []string, prune bool, progress io.Writer) error {
	if len(refs) == 0 {
		return nil
	}

	remote := pushRemoteRefs(ctx, destURL)
	specs, patterns, err := r.pushRefSpecs(refs, remote)
	if err != nil {
		return err
	}

	// Compute prune deletions ourselves instead of using go-git's
	// PushOptions.Prune: its refspec reversal keeps the force prefix on the
	// destination side, so local-existence lookups never match and every
	// matching remote ref gets deleted. It also removes local refs that are
	// missing on the remote, which push must never do.
	var deletes []gitconfig.RefSpec
	if prune {
		if deletes, err = r.pushPruneRefSpecs(specs, patterns, remote); err != nil {
			return fmt.Errorf("failed to compute remote refs to prune: %w", err)
		}
	}

	if len(specs)+len(deletes) == 0 {
		// Nothing selected: still reach the destination's receive-pack like a real push would.
		_, err := remote()
		return err
	}

	if dir := r.gitDir(); dir != "" {
		// git resolves the caller's refspecs itself; only the deletes are ours.
		err = r.pushGit(ctx, dir, destURL, refs, deletes, progress)
	} else {
		err = r.mirrorRemote(destURL).PushContext(ctx, &git.PushOptions{
			RemoteName:    "mirror",
			RemoteURL:     destURL,
			RefSpecs:      slices.Concat(specs, deletes),
			Progress:      progress,
			ClientOptions: mirrorClientOptions,
		})
	}
	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		return fmt.Errorf("failed to push mirror refs to remote: %w", err)
	}

	return nil
}

type pushPattern struct {
	spec     gitconfig.RefSpec // unforced src:dst
	force    bool
	matching bool // ":" pushes only branches the destination already has
}

// Explicit destinations take precedence over wildcard and matching pushes.
func (r *Repository) pushRefSpecs(refs []string, remote remoteRefsFunc) (specs, patterns []gitconfig.RefSpec, err error) {
	localRefs, err := r.Refs()
	if err != nil {
		return nil, nil, err
	}
	claimed := make(map[string]bool)
	var deferred []pushPattern
	var matching, matchForce bool
	for _, refspec := range refs {
		body, force := strings.CutPrefix(refspec, "+")
		src, dst, explicitDst := strings.Cut(body, ":")
		switch {
		case body == ":":
			matching, matchForce = true, matchForce || force
		case strings.Contains(dst, ":") || (explicitDst && dst == ""):
			return nil, nil, fmt.Errorf("invalid refspec %q: %w", refspec, gitconfig.ErrRefSpecMalformedSeparator)
		case strings.Contains(body, "*"):
			if !explicitDst {
				dst = src
			}
			if strings.Count(src, "*") != 1 || strings.Count(dst, "*") != 1 {
				return nil, nil, fmt.Errorf("invalid refspec %q: %w", refspec, gitconfig.ErrRefSpecMalformedWildcard)
			}
			if !validRefSide(src) || !validRefSide(dst) {
				return nil, nil, fmt.Errorf("invalid refspec %q: %w", refspec, plumbing.ErrInvalidReferenceName)
			}
			deferred = append(deferred, pushPattern{spec: gitconfig.RefSpec(src + ":" + dst), force: force})
		default:
			if src != "" {
				if src, err = r.resolvePushSource(src, localRefs); err != nil {
					return nil, nil, err
				}
			}
			if !explicitDst {
				if _, isRef := localRefs[src]; !isRef {
					return nil, nil, fmt.Errorf("%s cannot be resolved to branch", body)
				}
				dst = src
			}
			if !validRefSide(dst) {
				return nil, nil, fmt.Errorf("invalid refspec %q: %w", refspec, plumbing.ErrInvalidReferenceName)
			}
			if dst, err = resolvePushDestination(dst, src, remote); err != nil {
				return nil, nil, err
			}
			if claimed[dst] {
				return nil, nil, fmt.Errorf("dst ref %s receives from more than one src", dst)
			}
			claimed[dst] = true
			if src == "" {
				force = false // go-git reads "+:dst" as a push from source "+".
			}
			specs = append(specs, pushSpec(src, dst, force))
		}
	}
	if matching {
		// Matching yields to every pattern, like git's get_ref_match.
		deferred = append(deferred, pushPattern{spec: "refs/heads/*:refs/heads/*", force: matchForce, matching: true})
	}
	for _, p := range deferred {
		patterns = append(patterns, p.spec)
	}

	// Git selects only the first matching pattern for each local ref.
	for _, name := range slices.Sorted(maps.Keys(localRefs)) {
		src := name
		if plumbing.NewHash(localRefs[name]).IsZero() {
			// A symbolic ref pushes its target; a broken one is skipped like for_each_ref does.
			if src, err = r.resolvedRefName(name); err != nil {
				continue
			}
		}
		for _, p := range deferred {
			if !p.spec.Match(plumbing.ReferenceName(name)) {
				continue
			}
			dst := p.spec.Dst(plumbing.ReferenceName(name)).String()
			if p.matching {
				remoteRefs, err := remote()
				if err != nil {
					return nil, nil, err
				}
				if _, ok := remoteRefs[dst]; !ok {
					break
				}
			}
			if !claimed[dst] {
				claimed[dst] = true
				specs = append(specs, pushSpec(src, dst, p.force))
			}
			break
		}
	}
	return specs, patterns, nil
}

func pushSpec(src, dst string, force bool) gitconfig.RefSpec {
	if force {
		src = "+" + src
	}
	return gitconfig.RefSpec(src + ":" + dst)
}

// Refspecs allow one-level names and a wildcard unlike concrete ref names.
func validRefSide(side string) bool {
	name := strings.Replace(side, "*", "x", 1)
	if !strings.Contains(name, "/") {
		name = "refs/" + name
	}
	return validRefName(name)
}

// Branch and tag matches take precedence over remote-tracking refs.
func refNameMatches(short string, refs map[string]string) []string {
	var strong, weak []string
	for _, rule := range plumbing.RefRevParseRules[1:] {
		full := fmt.Sprintf(rule, short)
		if _, ok := refs[full]; !ok {
			continue
		}
		if rule == "refs/%s" || strings.HasPrefix(full, "refs/heads/") || strings.HasPrefix(full, "refs/tags/") {
			strong = append(strong, full)
		} else {
			weak = append(weak, full)
		}
	}
	if len(strong) > 0 {
		return strong
	}
	return weak
}

func (r *Repository) resolvePushSource(src string, localRefs map[string]string) (string, error) {
	name := src
	if _, ok := localRefs[src]; !ok && src != plumbing.HEAD.String() {
		switch matches := refNameMatches(src, localRefs); len(matches) {
		case 1:
			name = matches[0]
		case 0:
			if plumbing.IsHash(src) {
				if _, err := r.repo.Storer.EncodedObject(plumbing.AnyObject, plumbing.NewHash(src)); err == nil {
					return src, nil
				}
			}
			// ResolveRevision peels tags, so it only serves revision expressions.
			hash, err := r.repo.ResolveRevision(plumbing.Revision(src))
			if err != nil {
				return "", fmt.Errorf("src refspec %s does not match any", src)
			}
			return hash.String(), nil
		default:
			return "", fmt.Errorf("src refspec %s matches more than one", src)
		}
	}
	resolved, err := r.resolvedRefName(name)
	if err != nil {
		// Unborn, broken and cyclic symbolic refs have nothing to push.
		return "", fmt.Errorf("src refspec %s does not match any: %w", src, err)
	}
	return resolved, nil
}

func (r *Repository) resolvedRefName(name string) (string, error) {
	ref, err := storer.ResolveReference(r.repo.Storer, plumbing.ReferenceName(name))
	if err != nil {
		return "", err
	}
	if ref.Name() == plumbing.HEAD {
		return ref.Hash().String(), nil
	}
	return ref.Name().String(), nil
}

// An existing remote match takes precedence over guessing the source namespace.
func resolvePushDestination(dst, src string, remote remoteRefsFunc) (string, error) {
	if strings.HasPrefix(dst, "refs/") {
		return dst, nil
	}
	remoteRefs, err := remote()
	if err != nil {
		return "", err
	}
	matches := refNameMatches(dst, remoteRefs)
	switch {
	case len(matches) == 1:
		return matches[0], nil
	case len(matches) > 1:
		return "", fmt.Errorf("dst refspec %s matches more than one", dst)
	case src == "":
		return "", fmt.Errorf("unable to delete '%s': remote ref does not exist", dst)
	}
	for _, ns := range []string{"refs/heads/", "refs/tags/"} {
		if strings.HasPrefix(src, ns) {
			return ns + dst, nil
		}
	}
	return "", fmt.Errorf("unable to push to unqualified destination: %s", dst)
}

// A destination selected for an update must not also be pruned.
func (r *Repository) pushPruneRefSpecs(specs, patterns []gitconfig.RefSpec, remote remoteRefsFunc) ([]gitconfig.RefSpec, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	remoteRefs, err := remote()
	if err != nil {
		return nil, err
	}
	localRefs, err := r.Refs()
	if err != nil {
		return nil, err
	}
	claimed := make(map[string]bool, len(specs))
	for _, spec := range specs {
		claimed[spec.Dst("").String()] = true
	}

	var deletes []gitconfig.RefSpec
	for _, remoteName := range slices.Sorted(maps.Keys(remoteRefs)) {
		if claimed[remoteName] {
			continue
		}
		name := plumbing.ReferenceName(remoteName)
		for _, pattern := range patterns {
			rev := pattern.Reverse()
			if !rev.Match(name) {
				continue
			}
			localName := rev.Dst(name).String()
			hash, exists := localRefs[localName]
			if exists && plumbing.NewHash(hash).IsZero() {
				_, err := r.resolvedRefName(localName)
				exists = err == nil
			}
			if !exists {
				deletes = append(deletes, gitconfig.RefSpec(":"+remoteName))
			}
			break
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

	var err error
	if dir := r.gitDir(); dir != "" {
		err = r.fetchGit(ctx, dir, sourceURL, refs, progress)
	} else {
		specs := make([]gitconfig.RefSpec, 0, len(refs))
		for _, ref := range refs {
			specs = append(specs, gitconfig.RefSpec("+"+ref+":"+ref))
		}
		err = r.mirrorRemote(sourceURL).FetchContext(ctx, &git.FetchOptions{
			RemoteName:    "mirror",
			RemoteURL:     sourceURL,
			RefSpecs:      specs,
			Tags:          plumbing.NoTags,
			Force:         true,
			Progress:      progress,
			ClientOptions: mirrorClientOptions,
		})
	}
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
