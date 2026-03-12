package mirror

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/wzshiming/hfd/pkg/lfs"
	"github.com/wzshiming/hfd/pkg/receive"
	"github.com/wzshiming/hfd/pkg/repository"
	"golang.org/x/sync/singleflight"
)

// Mirror handles repository mirror operations, including syncing from upstream and firing hooks for ref changes.
type Mirror struct {
	mirrorSourceFunc    repository.MirrorSourceFunc
	mirrorRefFilterFunc repository.MirrorRefFilterFunc
	preReceiveHookFunc  receive.PreReceiveHookFunc
	postReceiveHookFunc receive.PostReceiveHookFunc
	lfsTeeCache         *lfs.TeeCache
	ttl                 time.Duration
	group               singleflight.Group
	lastSync            sync.Map // map[string]time.Time, keyed by repoName
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

// WithTTL sets a minimum duration between successive mirror syncs for the same repository.
// A zero value preserves the existing behavior of syncing on every read.
func WithTTL(ttl time.Duration) Option {
	return func(m *Mirror) {
		m.ttl = ttl
	}
}

// WithLFSCache sets the LFS tee cache for transparent upstream object fetching during mirror sync.
func WithLFSCache(tc *lfs.TeeCache) Option {
	return func(m *Mirror) {
		m.lfsTeeCache = tc
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

// IsMirror checks if a repository is configured as a mirror. Returns false if mirrorSourceFunc is not set.
func (m *Mirror) IsMirror(ctx context.Context, repoName string) (bool, error) {
	if m.mirrorSourceFunc == nil {
		return false, nil
	}
	_, isMirror, err := m.mirrorSourceFunc(ctx, repoName)
	return isMirror, err
}

// OpenOrSync opens the repository at repoPath. If it doesn't exist and mirrorSourceFunc is set, it attempts to initialize a mirror from the source URL.
func (m *Mirror) OpenOrSync(ctx context.Context, repoPath, repoName string) (*repository.Repository, error) {
	sourceURL, isMirror, err := m.mirrorSourceFunc(ctx, repoName)
	if err != nil {
		return nil, err
	}
	if !isMirror {
		return repository.Open(repoPath)
	}

	repo, err := repository.Open(repoPath)
	if err == nil {
		if !m.shouldSync(repoName) {
			return repo, nil
		}
		_, err, _ := m.group.Do(repoPath, func() (any, error) {
			defer m.markSynced(repoName)
			return nil, m.syncMirror(ctx, repo, repoName, sourceURL)
		})
		if err != nil {
			return nil, err
		}
		return repo, nil
	}

	if err != repository.ErrRepositoryNotExists {
		return nil, err
	}

	v, err, _ := m.group.Do(repoPath, func() (any, error) {
		repo, err = repository.InitMirror(ctx, repoPath, sourceURL)
		if err != nil {
			slog.WarnContext(ctx, "Failed to initialize mirror repository", "repo", repoName, "error", err)
			return nil, repository.ErrRepositoryNotExists
		}
		defer m.markSynced(repoName)
		return repo, m.syncMirror(ctx, repo, repoName, sourceURL)
	})
	if err != nil {
		return nil, err
	}

	return v.(*repository.Repository), nil
}

func filterKeyFromMap(m map[string]string, keys []string) map[string]string {
	if m == nil {
		return nil
	}
	result := make(map[string]string)
	for _, key := range keys {
		result[key] = m[key]
	}
	return result
}

func keys(m map[string]string) []string {
	var result []string
	for k := range m {
		result = append(result, k)
	}
	return result
}

func (m *Mirror) shouldSync(repoName string) bool {
	if m.ttl <= 0 {
		return true
	}

	last, ok := m.lastSync.Load(repoName)
	if !ok {
		return true
	}

	return time.Since(last.(time.Time)) >= m.ttl
}

func (m *Mirror) markSynced(repoName string) {
	if m.ttl <= 0 {
		return
	}

	m.lastSync.Store(repoName, time.Now())
}

// syncMirror syncs a mirror and fires post-receive hooks for any ref changes.
func (m *Mirror) syncMirror(ctx context.Context, repo *repository.Repository, repoName string, sourceURL string) error {
	remoteRefsMap, err := repo.RemoteRefs(ctx, sourceURL)
	if err != nil {
		return fmt.Errorf("failed to list remote refs: %w", err)
	}

	refsFilter := keys(remoteRefsMap)
	if m.mirrorRefFilterFunc != nil {
		refsFilter, err = m.mirrorRefFilterFunc(ctx, repoName, refsFilter)
		if err != nil {
			return fmt.Errorf("failed to filter mirror refs: %w", err)
		}
	}
	if len(refsFilter) == 0 {
		return nil
	}

	before, err := repo.Refs()
	if err != nil {
		return fmt.Errorf("failed to get local refs: %w", err)
	}
	before = filterKeyFromMap(before, refsFilter)

	remoteMap := filterKeyFromMap(remoteRefsMap, refsFilter)
	preReceiveUpdates := receive.DiffRefs(before, remoteMap, repo.RepoPath())
	if len(preReceiveUpdates) == 0 {
		return nil
	}
	if m.preReceiveHookFunc != nil {
		if ok, err := m.preReceiveHookFunc(ctx, repoName, preReceiveUpdates); err != nil {
			return fmt.Errorf("pre-receive hook error: %w", err)
		} else if !ok {
			return nil
		}
	}

	if err := repo.SyncMirrorRefs(ctx, sourceURL, refsFilter); err != nil {
		return fmt.Errorf("failed to sync mirror refs: %w", err)
	}

	if m.postReceiveHookFunc != nil {
		after, err := repo.Refs()
		if err != nil {
			return fmt.Errorf("failed to get local refs after sync: %w", err)
		}
		after = filterKeyFromMap(after, refsFilter)
		postReceiveUpdates := receive.DiffRefs(before, after, repo.RepoPath())
		if len(postReceiveUpdates) > 0 {
			if err := m.postReceiveHookFunc(ctx, repoName, postReceiveUpdates); err != nil {
				return fmt.Errorf("post-receive hook error: %w", err)
			}
		}
	}
	return nil
}
