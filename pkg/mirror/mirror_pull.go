package mirror

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"

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
		ui, _ := authenticate.GetUserInfo(ctx)
		logctx = authenticate.WithContext(logctx, ui)

		opt.Output = m.gitOutputFunc(logctx, repoName)
	}

	if err := m.resolvePullSource(ctx, repoName, &opt); err != nil {
		return err
	}

	repo, err := repository.Open(m.repositoriesFS, repoPath)
	if err != nil {
		if err != repository.ErrRepositoryNotExists {
			return fmt.Errorf("failed to open mirror repository: %w", err)
		}
		return m.initMirrorAndSync(ctx, logctx, repoPath, repoName, opt)
	}

	if !m.shouldSync(repoPath) {
		return nil
	}

	_, err, _ = m.pullGroup.Do(repoPath, func() (any, error) {
		defer m.markSynced(repoPath)

		if err := m.syncMirror(ctx, repo, repoName, opt.SourceURL, opt.Refs, opt.Output); err != nil {
			return nil, err
		}
		if err := m.pullMirrorLFS(repo, repoName, opt.SourceURL); err != nil {
			return nil, err
		}
		return repo, nil
	})
	return err
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
func (m *Mirror) initMirrorAndSync(ctx context.Context, logctx context.Context, repoPath, repoName string, opt PullOptions) error {
	_, err, _ := m.pullGroup.Do(repoPath, func() (any, error) {
		repo, err := repository.InitMirror(logctx, m.repositoriesFS, repoPath, opt.SourceURL)
		if err != nil {
			slog.WarnContext(ctx, "Failed to initialize mirror repository", "repo", repoName, "error", err)
			return nil, repository.ErrRepositoryNotExists
		}

		defer m.markSynced(repoPath)

		if err := m.syncMirror(ctx, repo, repoName, opt.SourceURL, opt.Refs, opt.Output); err != nil {
			return nil, err
		}
		if err := m.pullMirrorLFS(repo, repoName, opt.SourceURL); err != nil {
			return nil, err
		}
		return repo, nil
	})
	return err
}

// pullMirrorLFS scans the tips of all local refs for LFS pointers and
// prefetches the referenced objects through the xet data plane, keyed by the
// immutable commit each pointer was seen at.
func (m *Mirror) pullMirrorLFS(repo *repository.Repository, repoName, sourceURL string) error {
	if m.xetMirror == nil {
		return nil
	}

	refs, err := repo.Refs()
	if err != nil {
		return fmt.Errorf("failed to get local refs: %w", err)
	}

	var oids []string
	targets := make(map[string]resolveTarget)
	seenCommits := make(map[string]struct{})
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
			targets[oid] = resolveTarget{repoName: repoName, commit: commit, path: f.Path, size: f.Pointer.Size()}
			oids = append(oids, oid)
		}
	}

	m.prefetchLFS(sourceURL, oids, targets)
	return nil
}
