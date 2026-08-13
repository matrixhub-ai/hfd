package mirror

import (
	"context"
	"io"
	"net/url"
	"sync"
	"time"

	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"golang.org/x/sync/singleflight"
)

// SourceFunc determines the source URL of a repository mirror.
// It receives the repository name and returns the source URL, a boolean indicating whether
// the mirror should be enabled for this repository, and an error if any occurs during the process.
type SourceFunc func(ctx context.Context, repoName string) (string, bool, error)

// DestinationFunc determines the destination URL of a repository push mirror.
// It receives the repository name and returns the destination URL, a boolean indicating whether
// push mirroring is enabled for this repository, and an error if any occurs during the process.
type DestinationFunc func(ctx context.Context, repoName string) (string, bool, error)

// RefFilterFunc filters which refs should be synced during mirror operations.
// It receives the repository name and a list of remote ref names (e.g. "refs/heads/main",
// "refs/tags/v1.0") and returns the filtered list of refs to sync.
type RefFilterFunc func(ctx context.Context, repoName string, refs []string) ([]string, error)

// Mirror handles repository mirror operations, including syncing from upstream and firing hooks for ref changes.
type Mirror struct {
	mirrorSourceFunc      SourceFunc
	mirrorDestinationFunc DestinationFunc
	mirrorRefFilterFunc   RefFilterFunc
	preReceiveHookFunc    receive.PreReceiveHookFunc
	postReceiveHookFunc   receive.PostReceiveHookFunc
	syncUserInfoFunc      SyncUserInfoFunc
	gitOutputFunc         GitOutputFunc
	lfsStorage            lfs.Storage
	concurrency           int
	enablePullXET         bool
	enablePushXET         bool
	cacheDir              string
	xetEvictMaxBytes      int64
	xetEvictBeforeFunc    func() time.Time
	lfsTeeCache           *teeCache
	pullGroup             singleflight.Group
	pushGroup             singleflight.Group
	progressFunc          func(name string, downloaded, total int64)
	ttl                   time.Duration
	lastSync              sync.Map // map[string]time.Time, keyed by repoName
}

// Option defines a functional option for configuring the Mirror.
type Option func(*Mirror)

// WithMirrorSourceFunc sets the repository proxy callback for transparent upstream repository fetching.
func WithMirrorSourceFunc(fn SourceFunc) Option {
	return func(m *Mirror) {
		m.mirrorSourceFunc = fn
	}
}

// WithMirrorDestinationFunc sets the repository destination callback for pushing local changes to a remote repository.
func WithMirrorDestinationFunc(fn DestinationFunc) Option {
	return func(m *Mirror) {
		m.mirrorDestinationFunc = fn
	}
}

// WithMirrorRefFilterFunc sets the ref filter callback for mirror operations.
func WithMirrorRefFilterFunc(fn RefFilterFunc) Option {
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

// WithLFSStorage configures the Mirror to use the provided LFS storage backend for caching fetched objects.
func WithLFSStorage(storage lfs.Storage) Option {
	return func(m *Mirror) {
		m.lfsStorage = storage
	}
}

// WithPullXET enables or disables the use of XET for fetching LFS objects during mirror pull operations.
// When enabled, LFS objects will be fetched directly to the configured storage backend, bypassing local disk caching.
func WithPullXET(b bool) Option {
	return func(m *Mirror) {
		m.enablePullXET = b
	}
}

// WithPushXET enables or disables the use of XET for fetching LFS objects during mirror push operations.
// When enabled, LFS objects will be fetched directly to the configured storage backend, bypassing local disk caching.
func WithPushXET(b bool) Option {
	return func(m *Mirror) {
		m.enablePushXET = b
	}
}

// WithXETIdleEvictMaxBytes sets the maximum XET disk cache size after an idle cleanup pass.
// A value of 0 evicts all eligible inactive entries.
func WithXETIdleEvictMaxBytes(maxBytes int64) Option {
	return func(m *Mirror) {
		m.xetEvictMaxBytes = maxBytes
	}
}

// WithXETIdleEvictBeforeFunc sets the cutoff time used for XET disk cache eviction when downloads become idle.
// Entries updated before the returned time are eligible for eviction.
func WithXETIdleEvictBeforeFunc(fn func() time.Time) Option {
	return func(m *Mirror) {
		m.xetEvictBeforeFunc = fn
	}
}

// WithConcurrency sets the concurrency level for concurrent fetching of LFS objects during mirror syncs.
func WithConcurrency(concurrency int) Option {
	return func(m *Mirror) {
		m.concurrency = concurrency
	}
}

// WithProgressFunc sets a callback function to receive progress updates during LFS object fetches.
func WithProgressFunc(fn func(name string, downloaded, total int64)) Option {
	return func(m *Mirror) {
		m.progressFunc = fn
	}
}

// WithCacheDir sets the directory path for caching LFS objects during mirror syncs. If not set, a temporary directory will be used.
func WithCacheDir(dir string) Option {
	return func(m *Mirror) {
		m.cacheDir = dir
	}
}

// WithTTL sets the time-to-live duration for cached LFS objects in the mirror. Objects not accessed within this duration may be evicted from the cache.
func WithTTL(d time.Duration) Option {
	return func(m *Mirror) {
		m.ttl = d
	}
}

// GitOutputFunc defines a function type for providing an io.Writer to capture git command output for a given repository.
type GitOutputFunc func(ctx context.Context, repoName string) io.Writer

// WithGitOutputFunc sets a callback function to provide an io.Writer for capturing git command output for a given repository.
func WithGitOutputFunc(fn GitOutputFunc) Option {
	return func(m *Mirror) {
		m.gitOutputFunc = fn
	}
}

// SyncUserInfoFunc defines a function type for generating a sync token for a given repository, used to coordinate concurrent sync operations.
type SyncUserInfoFunc func(ctx context.Context, repoName string) (*url.Userinfo, error)

// WithSyncUserInfoFunc sets a callback function to generate a sync token for a given repository, used to coordinate concurrent sync operations.
func WithSyncUserInfoFunc(fn SyncUserInfoFunc) Option {
	return func(m *Mirror) {
		m.syncUserInfoFunc = fn
	}
}

// NewMirror creates a new Mirror with the provided options.
func NewMirror(opts ...Option) *Mirror {
	m := &Mirror{}
	for _, opt := range opts {
		opt(m)
	}
	if m.xetEvictBeforeFunc == nil {
		m.xetEvictBeforeFunc = time.Now
	}

	m.lfsTeeCache = newTeeCache(m.lfsStorage, m.concurrency, m.enablePullXET, m.enablePushXET, m.cacheDir, m.xetEvictMaxBytes, m.xetEvictBeforeFunc, m.progressFunc)
	return m
}

// IsMirrorSource checks if the given repository is a mirror source by invoking the mirrorSourceFunc callback. Returns false if the callback is not set.
func (m *Mirror) IsMirrorSource(ctx context.Context, repoName string) (bool, error) {
	if m.mirrorSourceFunc == nil {
		return false, nil
	}
	_, isMirror, err := m.mirrorSourceFunc(ctx, repoName)
	return isMirror, err
}

// IsMirrorDestination checks if the given repository is a mirror destination by invoking the mirrorDestinationFunc callback. Returns false if the callback is not set.
func (m *Mirror) IsMirrorDestination(ctx context.Context, repoName string) (bool, error) {
	if m.mirrorDestinationFunc == nil {
		return false, nil
	}
	_, isMirror, err := m.mirrorDestinationFunc(ctx, repoName)
	return isMirror, err
}

// PullOptions carries per-call overrides for PullFromRemote.
// The zero value uses the Mirror's configured callbacks for every field.
type PullOptions struct {
	// SourceURL overrides the mirrorSourceFunc lookup.
	SourceURL string
	// Refs restricts the sync to the given refs, overriding the mirrorRefFilterFunc.
	Refs []string
	// UserInfo sets credentials for the sync, overriding the SyncUserInfoFunc.
	UserInfo *url.Userinfo
	// Output captures git command output, overriding the GitOutputFunc.
	Output io.Writer
}

// PushOptions carries per-call overrides for PushToRemote.
// The zero value uses the Mirror's configured callbacks for every field.
type PushOptions struct {
	// DestinationURL overrides the mirrorDestinationFunc lookup.
	DestinationURL string
	// Refs restricts the push to the given refs; empty mirrors all branches and tags with pruning.
	Refs []string
	// UserInfo sets credentials for the push, overriding the SyncUserInfoFunc.
	UserInfo *url.Userinfo
	// Output captures git command output, overriding the GitOutputFunc.
	Output io.Writer
}

func (m *Mirror) shouldSync(repoPath string) bool {
	if m.ttl <= 0 {
		return true
	}

	last, ok := m.lastSync.Load(repoPath)
	if !ok {
		return true
	}

	return time.Since(last.(time.Time)) >= m.ttl
}

func (m *Mirror) markSynced(repoPath string) {
	if m.ttl <= 0 {
		return
	}

	m.lastSync.Store(repoPath, time.Now())
}
