package mirror

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"sync"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// PullFromRemote syncs the mirror repository at repoPath with the source URL, firing hooks for any ref changes.
// If the repository does not exist, it is initialized as a mirror and then synced.
// A nil opts uses the Mirror's configured callbacks.
func (m *Mirror) PullFromRemote(ctx context.Context, repoPath, repoName string, opts *PullOptions) error {
	var opt PullOptions
	if opts != nil {
		opt = *opts
	}

	logctx := context.Background()
	if m.gitOutputFunc != nil {
		logctx = authenticate.WithIdentity(logctx, authenticate.IdentityFrom(ctx))

		opt.Output = m.gitOutputFunc(logctx, repoName)
	}

	if err := m.resolvePullSource(ctx, repoName, &opt); err != nil {
		return err
	}
	scan, err := m.syncAndScan(ctx, logctx, repoPath, repoName, opt)
	if err != nil {
		return err
	}
	policy := opt.Policy
	if policy == "" {
		policy = m.pullPolicy
	}
	if policy != PullOnDemand {
		scan.prefetch.Do(func() { m.prefetchLFS(scan.sourceURL, scan.oids, scan.targets) })
	}
	return nil
}

// syncAndScan syncs the mirror at repoPath and scans its LFS objects, once for all overlapping pulls of it.
func (m *Mirror) syncAndScan(ctx context.Context, logctx context.Context, repoPath, repoName string, opt PullOptions) (*lfsScan, error) {
	repo, err := repository.Open(m.repositoriesFS, repoPath)
	if err != nil {
		if err != repository.ErrRepositoryNotExists {
			return nil, fmt.Errorf("failed to open mirror repository: %w", err)
		}
		return m.initMirrorAndSync(ctx, logctx, repoPath, repoName, opt)
	}

	scan, err, _ := m.pullGroup.Do(repoPath, func() (any, error) {
		if err := m.syncMirror(ctx, repo, repoName, opt.SourceURL, opt.Refs, opt.Output); err != nil {
			return nil, err
		}
		return m.pullMirrorLFS(repo, repoName, opt.SourceURL)
	})
	if err != nil {
		return nil, err
	}
	return scan.(*lfsScan), nil
}

// resolvePullSource fills in the source URL and embedded credentials from the
// configured callbacks when not overridden by the caller.
func (m *Mirror) resolvePullSource(ctx context.Context, repoName string, opt *PullOptions) error {
	if opt.SourceURL == "" {
		if m.mirrorSourceFunc == nil {
			return fmt.Errorf("no mirror source configured for repository %q", repoName)
		}
		sourceURL, isMirror, err := m.mirrorSourceFunc(ctx, repoName)
		if err != nil {
			return err
		}
		if !isMirror {
			return fmt.Errorf("repository %q is not configured as a mirror", repoName)
		}
		opt.SourceURL = sourceURL
	}

	if opt.UserInfo == nil && m.syncUserInfoFunc != nil {
		userInfo, err := m.syncUserInfoFunc(ctx, repoName)
		if err != nil {
			return fmt.Errorf("failed to get sync user info: %w", err)
		}
		opt.UserInfo = userInfo
	}

	if opt.UserInfo != nil {
		u, err := url.Parse(opt.SourceURL)
		if err != nil {
			return fmt.Errorf("failed to parse source URL: %w", err)
		}
		u.User = opt.UserInfo
		opt.SourceURL = u.String()
	}
	return nil
}

// initMirrorAndSync initializes the mirror repository and performs the first sync.
// Initialization failures are reported as ErrRepositoryNotExists so callers treat
// the repository as absent.
func (m *Mirror) initMirrorAndSync(ctx context.Context, logctx context.Context, repoPath, repoName string, opt PullOptions) (*lfsScan, error) {
	scan, err, _ := m.pullGroup.Do(repoPath, func() (any, error) {
		repo, remoteRefs, err := repository.InitMirror(logctx, m.repositoriesFS, repoPath, opt.SourceURL)
		if err != nil {
			slog.WarnContext(ctx, "Failed to initialize mirror repository", "repo", repoName, "error", err)
			return nil, repository.ErrRepositoryNotExists
		}

		if err := m.syncMirrorRefs(ctx, repo, repoName, opt.SourceURL, remoteRefs, opt.Refs, opt.Output); err != nil {
			return nil, err
		}
		return m.pullMirrorLFS(repo, repoName, opt.SourceURL)
	})
	if err != nil {
		return nil, err
	}
	return scan.(*lfsScan), nil
}

// lfsScan holds what one pull scan registered; every pull that shared the scan shares it.
type lfsScan struct {
	sourceURL string
	oids      []string
	targets   map[string]resolveTarget
	prefetch  sync.Once // one prefetch batch per scan, however many pulls shared it
}

// pullMirrorLFS registers the LFS objects at the tips of all local refs, each keyed by the immutable commit its pointer was seen at.
func (m *Mirror) pullMirrorLFS(repo *repository.Repository, repoName, sourceURL string) (*lfsScan, error) {
	if m.xetMirror == nil {
		return &lfsScan{}, nil
	}

	refs, err := repo.Refs()
	if err != nil {
		return nil, fmt.Errorf("failed to get local refs: %w", err)
	}

	var oids []string
	targets := make(map[string]resolveTarget)
	seenCommits := make(map[string]struct{})
	name := canonicalRepoName(repoName)
	for _, commit := range refs {
		if _, ok := seenCommits[commit]; ok {
			continue
		}
		seenCommits[commit] = struct{}{}

		files, err := repo.ListLFSPointers(commit)
		if err != nil {
			slog.Warn("Mirror LFS scan failed", "repo", repoName, "rev", commit, "error", err)
			continue
		}
		for _, f := range files {
			oid := f.Pointer.OID()
			if _, ok := targets[oid]; ok {
				continue
			}
			targets[oid] = resolveTarget{repoName: name, commit: commit, path: f.Path, size: f.Pointer.Size()}
			oids = append(oids, oid)
		}
	}

	for _, oid := range oids {
		m.registerTarget(oid, targets[oid])
	}
	return &lfsScan{sourceURL: sourceURL, oids: oids, targets: targets}, nil
}
