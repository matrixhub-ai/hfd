package mirror

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/wzshiming/hfd/pkg/permission"
	"github.com/wzshiming/hfd/pkg/receive"
	"github.com/wzshiming/hfd/pkg/repository"
)

// Mirror handles repository mirror operations, including syncing from upstream and firing hooks for ref changes.
type Mirror struct {
	mirrorSourceFunc    repository.MirrorSourceFunc
	mirrorRefFilterFunc repository.MirrorRefFilterFunc
	permissionHookFunc  permission.PermissionHookFunc
	preReceiveHookFunc  receive.PreReceiveHookFunc
	postReceiveHookFunc receive.PostReceiveHookFunc
}

// Option defines a functional option for configuring the Mirror.
type Option func(*Mirror)

// WithMirrorSourceFunc sets the repository proxy callback for transparent upstream repository fetching.
func WithMirrorSourceFunc(fn repository.MirrorSourceFunc) Option {
	return func(m *Mirror) {
		m.mirrorSourceFunc = fn
	}
}

// WithMirrorRefFilterFunc sets the ref filter callback for mirror operations.
func WithMirrorRefFilterFunc(fn repository.MirrorRefFilterFunc) Option {
	return func(m *Mirror) {
		m.mirrorRefFilterFunc = fn
	}
}

// WithPermissionHookFunc sets the permission hook for verifying operations.
func WithPermissionHookFunc(fn permission.PermissionHookFunc) Option {
	return func(m *Mirror) {
		m.permissionHookFunc = fn
	}
}

// WithPreReceiveHookFunc sets the pre-receive hook called before ref changes are applied.
func WithPreReceiveHookFunc(fn receive.PreReceiveHookFunc) Option {
	return func(m *Mirror) {
		m.preReceiveHookFunc = fn
	}
}

// WithPostReceiveHookFunc sets the post-receive hook called after a git push is processed.
func WithPostReceiveHookFunc(fn receive.PostReceiveHookFunc) Option {
	return func(m *Mirror) {
		m.postReceiveHookFunc = fn
	}
}

// NewMirror creates a new Mirror with the provided options.
func NewMirror(opts ...Option) *Mirror {
	m := &Mirror{}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// OpenOrSync opens the repository at repoPath. If it doesn't exist and mirrorSourceFunc is set, it attempts to initialize a mirror from the source URL.
func (m *Mirror) OpenOrSync(ctx context.Context, repoPath, repoName string) (*repository.Repository, error) {
	if m.mirrorSourceFunc == nil {
		return repository.Open(repoPath)
	}
	repo, err := repository.Open(repoPath)
	if err != nil {
		if err != repository.ErrRepositoryNotExists {
			return nil, err
		}
		if m.permissionHookFunc != nil {
			if err := m.permissionHookFunc(ctx, permission.OperationCreateProxyRepo, repoName, permission.Context{}); err != nil {
				return nil, err
			}
		}
		sourceURL, isMirror, err := m.mirrorSourceFunc(ctx, repoName)
		if err != nil {
			return nil, err
		}
		if !isMirror {
			return nil, repository.ErrRepositoryNotExists
		}
		repo, err = repository.InitMirror(ctx, repoPath, sourceURL)
		if err != nil {
			return nil, repository.ErrRepositoryNotExists
		}
		err = m.syncMirror(ctx, repo, repoName, sourceURL)
		if err != nil {
			return nil, fmt.Errorf("failed to sync mirror: %w", err)
		}
	} else {
		sourceURL, isMirror, err := m.mirrorSourceFunc(ctx, repoName)
		if err != nil {
			slog.WarnContext(ctx, "mirrorSourceFunc error", "repo", repoName, "error", err)
			return repo, nil
		}
		if !isMirror {
			return repo, nil
		}
		err = m.syncMirror(ctx, repo, repoName, sourceURL)
		if err != nil {
			return nil, fmt.Errorf("failed to sync mirror: %w", err)
		}
	}
	return repo, nil
}

func removeKeyFromMap(m map[string]string, keys []string) map[string]string {
	if m == nil {
		return nil
	}
	result := make(map[string]string)
	for _, key := range keys {
		result[key] = m[key]
	}
	return result
}

// syncMirror syncs a mirror and fires post-receive hooks for any ref changes.
func (m *Mirror) syncMirror(ctx context.Context, repo *repository.Repository, repoName string, sourceURL string) error {
	remoteRefs, err := repo.ListRemoteRefs(ctx, sourceURL)
	if err != nil {
		return fmt.Errorf("failed to list remote refs: %w", err)
	}

	if m.mirrorRefFilterFunc != nil {
		remoteRefs, err = m.mirrorRefFilterFunc(ctx, repoName, remoteRefs)
		if err != nil {
			return fmt.Errorf("failed to filter mirror refs: %w", err)
		}
	}
	if len(remoteRefs) == 0 {
		return nil
	}

	var before map[string]string
	if m.postReceiveHookFunc != nil || m.preReceiveHookFunc != nil {
		before, _ = repo.Refs()
	}

	before = removeKeyFromMap(before, remoteRefs)

	if m.preReceiveHookFunc != nil {
		var updates []receive.RefUpdate
		for _, target := range remoteRefs {
			oldRev, ok := before[target]
			if oldRev == "" || !ok {
				oldRev = receive.ZeroHash
			}
			updates = append(updates, receive.NewRefUpdate(oldRev, receive.BreakHash, target, repo.RepoPath()))
		}
		if err := m.preReceiveHookFunc(ctx, repoName, updates); err != nil {
			return fmt.Errorf("pre-receive hook error: %w", err)
		}
	}

	if err := repo.SyncMirrorRefs(ctx, sourceURL, remoteRefs); err != nil {
		return fmt.Errorf("failed to sync mirror refs: %w", err)
	}

	if m.postReceiveHookFunc != nil {
		after, _ := repo.Refs()
		after = removeKeyFromMap(after, remoteRefs)
		updates := receive.DiffRefs(before, after, repo.RepoPath())
		if len(updates) > 0 {
			if err := m.postReceiveHookFunc(ctx, repoName, updates); err != nil {
				return fmt.Errorf("post-receive hook error: %w", err)
			}
		}
	}
	return nil
}
