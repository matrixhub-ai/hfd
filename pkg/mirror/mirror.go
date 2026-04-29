package mirror

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/matrixhub-ai/hfd/internal/utils"
	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"golang.org/x/sync/singleflight"
)

// Mirror handles repository mirror operations, including syncing from upstream and firing hooks for ref changes.
type Mirror struct {
	mirrorSourceFunc    repository.MirrorSourceFunc
	mirrorRefFilterFunc repository.MirrorRefFilterFunc
	preReceiveHookFunc  receive.PreReceiveHookFunc
	postReceiveHookFunc receive.PostReceiveHookFunc
	syncUserInfoFunc    SyncUserInfoFunc
	gitOutputFunc       GitOutputFunc
	lfsStorage          lfs.Storage
	concurrency         int
	enableXET           bool
	cacheDir            string
	lfsTeeCache         *teeCache
	ttl                 time.Duration
	group               singleflight.Group
	lastSync            sync.Map // map[string]time.Time, keyed by repoName
	progressFunc        func(name string, downloaded, total int64)
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

// WithLFSStorage configures the Mirror to use the provided LFS storage backend for caching fetched objects.
func WithLFSStorage(storage lfs.Storage) Option {
	return func(m *Mirror) {
		m.lfsStorage = storage
	}
}

// WithXET enables or disables the use of XET for fetching LFS objects during mirror syncs.
// When enabled, LFS objects will be fetched directly to the configured storage backend, bypassing local disk caching.
func WithXET(b bool) Option {
	return func(m *Mirror) {
		m.enableXET = b
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

	m.lfsTeeCache = newTeeCache(m.lfsStorage, m.concurrency, m.enableXET, m.cacheDir, m.progressFunc)
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

type SyncOption func(*syncOption)

type syncOption struct {
	SourceURL string
	Refs      []string
	UserInfo  *url.Userinfo
	Output    io.Writer
}

// WithSyncMirrorSourceURL sets the source URL for mirror sync operations, overriding the default mirrorSourceFunc lookup.
func WithSyncMirrorSourceURL(url string) SyncOption {
	return func(o *syncOption) {
		o.SourceURL = url
	}
}

// WithSyncMirrorRefs sets the specific refs to sync during mirror operations, overriding the default mirrorRefFilterFunc.
func WithSyncMirrorRefs(refs []string) SyncOption {
	return func(o *syncOption) {
		o.Refs = refs
	}
}

// WithSyncUserInfo sets a sync user info for the mirror sync operation, used to coordinate concurrent syncs. This is an alternative to setting a global SyncUserInfoFunc.
func WithSyncUserInfo(userInfo *url.Userinfo) SyncOption {
	return func(o *syncOption) {
		o.UserInfo = userInfo
	}
}

// WithSyncOutput sets an io.Writer to capture git command output during the mirror sync operation, overriding the default GitOutputFunc.
func WithSyncOutput(output io.Writer) SyncOption {
	return func(o *syncOption) {
		o.Output = output
	}
}

// OpenOrSync opens the mirror repository at repoPath, syncing with the source URL if necessary based on TTL.
func (m *Mirror) OpenOrSync(ctx context.Context, repoPath, repoName string, opts ...SyncOption) (*repository.Repository, error) {
	var opt syncOption
	for _, o := range opts {
		o(&opt)
	}

	logctx := context.Background()
	if m.gitOutputFunc != nil {
		ui, _ := authenticate.GetUserInfo(ctx)
		logctx = authenticate.WithContext(logctx, ui)

		opt.Output = m.gitOutputFunc(logctx, repoName)
	}

	if opt.Output != nil {
		logctx = utils.WithCommandOutput(logctx, opt.Output)
	}

	if opt.SourceURL == "" {
		sourceURL, isMirror, err := m.mirrorSourceFunc(ctx, repoName)
		if err != nil {
			return nil, err
		}
		if !isMirror {
			return repository.Open(repoPath)
		}
		opt.SourceURL = sourceURL
	}

	if opt.UserInfo == nil && m.syncUserInfoFunc != nil {
		userInfo, err := m.syncUserInfoFunc(ctx, repoName)
		if err != nil {
			return nil, fmt.Errorf("failed to get sync user info: %w", err)
		}
		opt.UserInfo = userInfo
	}

	if opt.UserInfo != nil {
		u, err := url.Parse(opt.SourceURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse source URL: %w", err)
		}
		u.User = opt.UserInfo
		opt.SourceURL = u.String()
	}

	repo, err := repository.Open(repoPath)
	if err == nil {
		if !m.shouldSync(repoPath) {
			return repo, nil
		}
		_, err, _ := m.group.Do(repoPath, func() (any, error) {
			defer m.markSynced(repoPath)

			err := m.syncMirror(logctx, repo, repoName, opt.SourceURL, opt.Refs)
			if err != nil {
				return nil, err
			}

			err = m.syncMirrorLFS(context.Background(), repo, repoName, opt.SourceURL)
			if err != nil {
				return nil, err
			}
			return repo, nil
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
		repo, err = repository.InitMirror(logctx, repoPath, opt.SourceURL)
		if err != nil {
			slog.WarnContext(ctx, "Failed to initialize mirror repository", "repo", repoName, "error", err)
			return nil, repository.ErrRepositoryNotExists
		}
		defer m.markSynced(repoPath)

		err = m.syncMirror(logctx, repo, repoName, opt.SourceURL, opt.Refs)
		if err != nil {
			return nil, err
		}
		err = m.syncMirrorLFS(context.Background(), repo, repoName, opt.SourceURL)
		if err != nil {
			return nil, err
		}
		return repo, nil
	})
	if err != nil {
		return nil, err
	}

	return v.(*repository.Repository), nil
}

// Sync forcefully syncs the mirror repository at repoPath with the source URL, regardless of TTL.
func (m *Mirror) Sync(ctx context.Context, repoPath, repoName string, opts ...SyncOption) error {
	var opt syncOption
	for _, o := range opts {
		o(&opt)
	}

	logctx := context.Background()
	if m.gitOutputFunc != nil {
		ui, _ := authenticate.GetUserInfo(ctx)
		logctx = authenticate.WithContext(logctx, ui)

		opt.Output = m.gitOutputFunc(logctx, repoName)
	}

	if opt.Output != nil {
		logctx = utils.WithCommandOutput(logctx, opt.Output)
	}

	if opt.SourceURL == "" {
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

	repo, err := repository.Open(repoPath)
	if err != nil {
		return fmt.Errorf("failed to open mirror repository: %w", err)
	}

	_, err, _ = m.group.Do(repoPath, func() (any, error) {
		defer m.markSynced(repoPath)
		err = m.syncMirror(ctx, repo, repoName, opt.SourceURL, opt.Refs)
		if err != nil {
			return nil, err
		}
		err = m.syncMirrorLFS(ctx, repo, repoName, opt.SourceURL)
		if err != nil {
			return nil, err
		}
		return repo, nil
	})
	if err != nil {
		return err
	}
	return nil
}

func filterKeyFromMap(m map[string]string, keys []string) map[string]string {
	if m == nil {
		return nil
	}
	result := make(map[string]string)
	for _, key := range keys {
		val, ok := m[key]
		if !ok {
			continue
		}
		result[key] = val
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

// syncMirror syncs a mirror and fires post-receive hooks for any ref changes.
func (m *Mirror) syncMirror(ctx context.Context, repo *repository.Repository, repoName string, sourceURL string, refs []string) error {
	remoteRefsMap, err := repository.GetRemoteRefs(ctx, sourceURL)
	if err != nil {
		return fmt.Errorf("failed to list remote refs: %w", err)
	}

	refsFilter := keys(remoteRefsMap)
	if len(refs) > 0 {
		refsFilter = refs
	} else if m.mirrorRefFilterFunc != nil {
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

func (m *Mirror) syncMirrorLFS(ctx context.Context, repo *repository.Repository, repoName string, sourceURL string) error {
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
