package mirror

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/matrixhub-ai/hfd/internal/utils"
	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	xetclient "github.com/wzshiming/xet/client"
	"golang.org/x/sync/singleflight"
)

// Mirror handles repository mirror operations, including syncing from upstream and firing hooks for ref changes.
type Mirror struct {
	mirrorSourceFunc      repository.MirrorSourceFunc
	mirrorDestinationFunc repository.MirrorDestinationFunc
	mirrorRefFilterFunc   repository.MirrorRefFilterFunc
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
}

// Option defines a functional option for configuring the Mirror.
type Option func(*Mirror)

// WithMirrorSourceFunc sets the repository proxy callback for transparent upstream repository fetching.
func WithMirrorSourceFunc(fn repository.MirrorSourceFunc) Option {
	return func(m *Mirror) {
		m.mirrorSourceFunc = fn
	}
}

// WithMirrorDestinationFunc sets the repository destination callback for pushing local changes to a remote repository.
func WithMirrorDestinationFunc(fn repository.MirrorDestinationFunc) Option {
	return func(m *Mirror) {
		m.mirrorDestinationFunc = fn
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

type SyncOption func(*syncOption)

type syncOption struct {
	SourceURL      string
	DestinationURL string
	Refs           []string
	UserInfo       *url.Userinfo
	Output         io.Writer
}

// WithSyncMirrorSourceURL sets the source URL for mirror sync operations, overriding the default mirrorSourceFunc lookup.
func WithSyncMirrorSourceURL(url string) SyncOption {
	return func(o *syncOption) {
		o.SourceURL = url
	}
}

// WithSyncMirrorDestinationURL sets the destination URL for push mirror operations, overriding the default mirrorDestFunc lookup.
func WithSyncMirrorDestinationURL(url string) SyncOption {
	return func(o *syncOption) {
		o.DestinationURL = url
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

// PullFromRemote syncs the mirror repository at repoPath with the source URL, firing hooks for any ref changes. If the repository does not exist, it is initialized as a mirror and then synced.
func (m *Mirror) PullFromRemote(ctx context.Context, repoPath, repoName string, opts ...SyncOption) error {
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
		if err != repository.ErrRepositoryNotExists {
			return fmt.Errorf("failed to open mirror repository: %w", err)
		}

		_, err, _ = m.pullGroup.Do(repoPath, func() (any, error) {
			repo, err = repository.InitMirror(logctx, repoPath, opt.SourceURL)
			if err != nil {
				slog.WarnContext(ctx, "Failed to initialize mirror repository", "repo", repoName, "error", err)
				return nil, repository.ErrRepositoryNotExists
			}

			err = m.syncMirror(ctx, repo, repoName, opt.SourceURL, opt.Refs)
			if err != nil {
				return nil, err
			}
			err = m.pullMirrorLFS(repo, opt.SourceURL)
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

	_, err, _ = m.pullGroup.Do(repoPath, func() (any, error) {
		err = m.syncMirror(ctx, repo, repoName, opt.SourceURL, opt.Refs)
		if err != nil {
			return nil, err
		}
		err = m.pullMirrorLFS(repo, opt.SourceURL)
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

// PushToRemote pushes the given ref updates to the configured remote destination.
// It is typically called after a successful push to the local repository (post-receive hook)
// to keep the remote destination in sync with local changes.
// If mirrorDestFunc is not set and no DestURL is provided via opts, the function returns nil.
func (m *Mirror) PushToRemote(ctx context.Context, repoPath, repoName string, opts ...SyncOption) error {
	if m.mirrorDestinationFunc == nil {
		return nil
	}

	var opt syncOption
	for _, o := range opts {
		o(&opt)
	}

	if m.gitOutputFunc != nil {
		logctx := context.Background()
		ui, _ := authenticate.GetUserInfo(ctx)
		logctx = authenticate.WithContext(logctx, ui)
		opt.Output = m.gitOutputFunc(logctx, repoName)
	}

	if opt.Output != nil {
		ctx = utils.WithCommandOutput(ctx, opt.Output)
	}

	if opt.DestinationURL == "" {
		destURL, isPushMirror, err := m.mirrorDestinationFunc(ctx, repoName)
		if err != nil {
			return err
		}
		if !isPushMirror {
			return nil
		}
		opt.DestinationURL = destURL
	}

	if opt.UserInfo == nil && m.syncUserInfoFunc != nil {
		userInfo, err := m.syncUserInfoFunc(ctx, repoName)
		if err != nil {
			return fmt.Errorf("failed to get sync user info: %w", err)
		}
		opt.UserInfo = userInfo
	}

	if opt.UserInfo != nil {
		u, err := url.Parse(opt.DestinationURL)
		if err != nil {
			return fmt.Errorf("failed to parse dest URL: %w", err)
		}
		u.User = opt.UserInfo
		opt.DestinationURL = u.String()
	}

	repo, err := repository.Open(repoPath)
	if err != nil {
		return fmt.Errorf("failed to open repository: %w", err)
	}

	_, err, _ = m.pushGroup.Do(repoPath, func() (any, error) {
		if err := m.pushMirrorLFS(repo, opt.DestinationURL); err != nil {
			return nil, fmt.Errorf("failed to push LFS objects to remote: %w", err)
		}

		var refspecs []string
		var prune bool
		if len(opt.Refs) > 0 {
			for _, ref := range opt.Refs {
				refspecs = append(refspecs, "+"+ref+":"+ref)
			}
		} else {
			prune = true
			refspecs = []string{
				"+refs/heads/*:refs/heads/*",
				"+refs/tags/*:refs/tags/*",
			}
		}

		if err := repo.PushMirrorRefs(ctx, opt.DestinationURL, refspecs, prune); err != nil {
			return nil, err
		}

		return nil, nil
	})
	if err != nil {
		return err
	}

	return nil
}

// pushMirrorLFS uploads LFS objects referenced by the repository to the remote LFS endpoint.
func (m *Mirror) pushMirrorLFS(repo *repository.Repository, destURL string) error {
	if m.lfsStorage == nil {
		return nil
	}

	ctx := context.Background()

	getter, ok := m.lfsStorage.(lfs.Getter)
	if !ok {
		return nil
	}

	lfsPointers, err := repo.ScanLFSPointers()
	if err != nil {
		return fmt.Errorf("failed to scan LFS pointers: %w", err)
	}

	if len(lfsPointers) == 0 {
		return nil
	}

	objects := make([]lfs.LFSObject, 0, len(lfsPointers))
	for _, ptr := range lfsPointers {
		if m.lfsStorage.Exists(ptr.OID()) {
			objects = append(objects, lfs.LFSObject{Oid: ptr.OID(), Size: ptr.Size()})
		}
	}

	if len(objects) == 0 {
		return nil
	}

	lfsClient := lfs.NewClient(utils.HTTPClient)

	// When XET is enabled and the xet client is initialized, advertise the xet transfer
	// protocol so the remote can select it. Fall back to a basic-only request on error.
	var batchResp *lfs.BatchResponse
	var xetC *xetclient.Client
	if m.enablePushXET && m.lfsTeeCache != nil {
		xetC = m.lfsTeeCache.xetClient
	}
	xetUpload := xetC != nil
	if xetUpload {
		batchResp, err = lfsClient.UploadBatch(ctx, destURL, lfs.TransferWithXETCapabilities, objects)
		if err != nil {
			return fmt.Errorf("failed to get LFS upload batch from remote with XET capabilities: %w", err)
		}

		if !strings.EqualFold(batchResp.Transfer, "xet") {
			xetUpload = false
		}
	} else {
		batchResp, err = lfsClient.UploadBatch(ctx, destURL, lfs.TransferCapabilities, objects)
		if err != nil {
			return fmt.Errorf("failed to get LFS upload batch from remote: %w", err)
		}
	}

	for _, obj := range batchResp.Objects {
		if obj.Error != nil {
			slog.WarnContext(ctx, "LFS push mirror: remote returned error for object", "oid", obj.Oid, "error", obj.Error)
			continue
		}

		uploadAction, ok := obj.Actions["upload"]
		if !ok {
			// Remote already has this object; skip.
			continue
		}

		if xetUpload {
			if err := m.doXETUpload(ctx, obj.Oid, uploadAction, obj.Actions["verify"], getter, xetC); err != nil {
				slog.WarnContext(ctx, "LFS push mirror: XET upload failed", "oid", obj.Oid, "error", err)
				continue
			}
			slog.InfoContext(ctx, "LFS push mirror: uploaded object via XET", "oid", obj.Oid)
			continue
		}

		content, info, err := getter.Get(obj.Oid)
		if err != nil {
			slog.WarnContext(ctx, "LFS push mirror: failed to read local object", "oid", obj.Oid, "error", err)
			continue
		}

		req, err := uploadAction.UploadRequest(ctx, content, info.Size())
		if err != nil {
			_ = content.Close()
			slog.WarnContext(ctx, "LFS push mirror: failed to build upload request", "oid", obj.Oid, "error", err)
			continue
		}

		resp, err := utils.HTTPClient.Do(req)
		_ = content.Close()
		if err != nil {
			slog.WarnContext(ctx, "LFS push mirror: failed to upload object", "oid", obj.Oid, "error", err)
			continue
		}
		_ = resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			slog.WarnContext(ctx, "LFS push mirror: upload returned unexpected status", "oid", obj.Oid, "status", resp.StatusCode)
			continue
		}

		// If a verify action is present, call it.
		if verifyAction, ok := obj.Actions["verify"]; ok {
			verifyReq, err := verifyAction.Request(ctx)
			if err != nil {
				slog.WarnContext(ctx, "LFS push mirror: failed to build verify request", "oid", obj.Oid, "error", err)
				continue
			}
			verifyReq.Method = http.MethodPost
			verifyResp, err := utils.HTTPClient.Do(verifyReq)
			if err != nil {
				slog.WarnContext(ctx, "LFS push mirror: failed to verify object", "oid", obj.Oid, "error", err)
				continue
			}
			_ = verifyResp.Body.Close()
		}

		slog.InfoContext(ctx, "LFS push mirror: uploaded object", "oid", obj.Oid)
	}

	return nil
}

// doXETUpload uploads an LFS object to the remote XET CAS using the credentials
// embedded in uploadAction.Header, then fires the optional verify action.
func (m *Mirror) doXETUpload(ctx context.Context, oid string, uploadAction, verifyAction lfs.Action, getter lfs.Getter, xetC *xetclient.Client) error {
	casURL := uploadAction.Header["X-Xet-Cas-Url"]
	casToken := uploadAction.Header["X-Xet-Access-Token"]

	provider := xetclient.StaticAuthProvider(casURL, casToken)

	content, _, err := getter.Get(oid)
	if err != nil {
		return fmt.Errorf("read local object: %w", err)
	}
	defer content.Close()

	if _, err := xetC.UploadFileWithAuthProvider(ctx, provider, content); err != nil {
		return fmt.Errorf("XET upload: %w", err)
	}

	if verifyAction.Href != "" {
		verifyReq, err := verifyAction.Request(ctx)
		if err != nil {
			slog.WarnContext(ctx, "LFS push mirror: failed to build XET verify request", "oid", oid, "error", err)
			return nil
		}
		verifyReq.Method = http.MethodPost
		verifyResp, err := utils.HTTPClient.Do(verifyReq)
		if err != nil {
			slog.WarnContext(ctx, "LFS push mirror: failed to verify XET upload", "oid", oid, "error", err)
			return nil
		}
		_ = verifyResp.Body.Close()
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
