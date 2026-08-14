package mirror

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
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
		if err := m.pullMirrorLFS(repo, opt.SourceURL); err != nil {
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
		if err := m.pullMirrorLFS(repo, opt.SourceURL); err != nil {
			return nil, err
		}
		return repo, nil
	})
	return err
}

// pullMirrorLFS queues background fetches for all LFS objects referenced by the repository.
func (m *Mirror) pullMirrorLFS(repo *repository.Repository, sourceURL string) error {
	lfsPointers, err := repo.ScanLFSPointers()
	if err != nil {
		return fmt.Errorf("failed to scan LFS pointers: %w", err)
	}

	if len(lfsPointers) == 0 {
		return nil
	}

	objects := make([]lfs.LFSObject, 0, len(lfsPointers))
	for _, pointer := range lfsPointers {
		objects = append(objects, lfs.LFSObject{
			Oid:  pointer.OID(),
			Size: pointer.Size(),
		})
	}

	m.lfsTeeCache.Queue(sourceURL, objects)
	return nil
}
