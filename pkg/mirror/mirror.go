package mirror

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
	xetclient "github.com/wzshiming/xet/client"
	xetmirror "github.com/wzshiming/xet/mirror"
	xetserver "github.com/wzshiming/xet/server"
	xetstorage "github.com/wzshiming/xet/storage"
	xettoken "github.com/wzshiming/xet/token"
	"golang.org/x/sync/singleflight"

	"github.com/matrixhub-ai/hfd/internal/stallguard"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/receive"
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

// GitOutputFunc defines a function type for providing an io.Writer to capture git command output for a given repository.
type GitOutputFunc func(ctx context.Context, repoName string) io.Writer

// SyncUserInfoFunc defines a function type for generating a sync token for a given repository, used to coordinate concurrent sync operations.
type SyncUserInfoFunc func(ctx context.Context, repoName string) (*url.Userinfo, error)

// Mirror handles repository mirror operations: git ref syncing (pull and
// push) plus an xet-backed data plane that caches upstream file bytes as
// xorbs/shards and serves them to both xet-capable and plain LFS clients.
type Mirror struct {
	mirrorSourceFunc      SourceFunc
	mirrorDestinationFunc DestinationFunc
	mirrorRefFilterFunc   RefFilterFunc
	preReceiveHookFunc    receive.PreReceiveHookFunc
	postReceiveHookFunc   receive.PostReceiveHookFunc
	permissionHookFunc    permission.PermissionHookFunc
	syncUserInfoFunc      SyncUserInfoFunc
	gitOutputFunc         GitOutputFunc
	repositoriesFS        billy.Filesystem
	concurrency           int
	progressFunc          func(name string, downloaded, total int64)
	ttl                   time.Duration
	pullGroup             singleflight.Group
	pushGroup             singleflight.Group
	lastSync              sync.Map // map[string]time.Time, keyed by repoPath
	background            sync.WaitGroup

	// data plane
	upstreamURL    string
	upstreamToken  string
	externalURL    string
	dataDir        string
	xetCacheSize   int64
	xetStorage     xetstorage.Storage
	xetClient      *xetclient.Client
	issuer         *xettoken.Issuer
	dataPlane      http.Handler
	mirrorHandler  *xetmirror.Handler // nil without a pull upstream
	httpClient     *http.Client       // LFS batch/upload/verify; no timeout, uploads may run long
	downloadClient *http.Client       // object content downloads, resuming interrupted streams

	oidIndex    sync.Map // oid -> resolveTarget, populated by pull syncs
	prefetching sync.Map // oid -> struct{}, in-flight prefetch dedupe
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

// WithPermissionHookFunc sets the permission hook enforced on the data
// plane's user-credential routes (token minting and the sha256 bridge).
func WithPermissionHookFunc(fn permission.PermissionHookFunc) Option {
	return func(m *Mirror) {
		m.permissionHookFunc = fn
	}
}

// WithRepositoriesFS sets the filesystem used to access local mirror
// repositories. The default is the host OS.
func WithRepositoriesFS(fs billy.Filesystem) Option {
	return func(m *Mirror) {
		m.repositoriesFS = fs
	}
}

// WithConcurrency sets the concurrency level for xet chunk transfers.
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

// WithTTL sets the minimum interval between pull syncs of the same repository.
func WithTTL(d time.Duration) Option {
	return func(m *Mirror) {
		m.ttl = d
	}
}

// WithGitOutputFunc sets a callback function to provide an io.Writer for capturing git command output for a given repository.
func WithGitOutputFunc(fn GitOutputFunc) Option {
	return func(m *Mirror) {
		m.gitOutputFunc = fn
	}
}

// WithSyncUserInfoFunc sets a callback function to generate a sync token for a given repository, used to coordinate concurrent sync operations.
func WithSyncUserInfoFunc(fn SyncUserInfoFunc) Option {
	return func(m *Mirror) {
		m.syncUserInfoFunc = fn
	}
}

// WithUpstream sets the upstream hub base URL for the data plane (e.g.
// https://huggingface.co). Setting it enables the xet mirror data plane.
func WithUpstream(upstream string) Option {
	return func(m *Mirror) {
		m.upstreamURL = upstream
	}
}

// WithUpstreamToken sets the credential the data plane uses against the upstream hub.
func WithUpstreamToken(token string) Option {
	return func(m *Mirror) {
		m.upstreamToken = token
	}
}

// WithExternalURL sets the externally visible base URL used in xet Link
// headers and minted CAS tokens. When empty it is derived from each request.
func WithExternalURL(external string) Option {
	return func(m *Mirror) {
		m.externalURL = external
	}
}

// WithDataDir sets the directory holding the xet storage, the mirror
// ingest cache, and the xet client chunk cache.
func WithDataDir(dir string) Option {
	return func(m *Mirror) {
		m.dataDir = dir
	}
}

// WithXETStorage overrides the xet storage backend. When unset a file
// storage rooted under the data directory is created.
func WithXETStorage(s xetstorage.Storage) Option {
	return func(m *Mirror) {
		m.xetStorage = s
	}
}

// WithXETCacheSize bounds the xet client chunk cache size in bytes.
func WithXETCacheSize(sizeBytes int64) Option {
	return func(m *Mirror) {
		m.xetCacheSize = sizeBytes
	}
}

// NewMirror creates a new Mirror with the provided options. When an upstream
// is configured it assembles the xet data plane: a CAS server whose
// fallthrough is the xet mirror handler, sharing one storage and one token
// issuer, following the composition of xetd.
func NewMirror(opts ...Option) (*Mirror, error) {
	m := &Mirror{}
	for _, opt := range opts {
		opt(m)
	}
	if m.repositoriesFS == nil {
		m.repositoriesFS = osfs.Default
	}
	if m.dataDir == "" {
		m.dataDir = filepath.Join(".", "xet-mirror-data")
	}
	// The batch/upload client has no overall timeout because uploads may run
	// long; the stall guard bounds no-progress phases instead.
	m.httpClient = http.DefaultClient
	m.downloadClient = newDownloadClient()

	chunksDir := filepath.Join(m.dataDir, "chunks")
	if err := os.MkdirAll(chunksDir, 0755); err != nil {
		return nil, fmt.Errorf("create xet chunk cache dir: %w", err)
	}
	clientOpts := []xetclient.Options{
		xetclient.WithCacheDir(chunksDir),
		// The xet client wraps this transport with its own httpseek layer, so
		// stalled chunk downloads abort and resume instead of hanging.
		xetclient.WithHTTPClient(&http.Client{Transport: stallguard.NewTransport(http.DefaultTransport, stallIdleWindow)}),
	}
	if m.concurrency > 0 {
		clientOpts = append(clientOpts, xetclient.WithConcurrency(m.concurrency))
	}
	if m.progressFunc != nil {
		clientOpts = append(clientOpts, xetclient.WithProgressFunc(m.progressFunc))
	}
	if m.xetCacheSize > 0 {
		clientOpts = append(clientOpts, xetclient.WithCacheSize(m.xetCacheSize))
	}
	xetC, err := xetclient.NewClient(clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("create xet client: %w", err)
	}
	m.xetClient = xetC

	if m.xetStorage == nil {
		stor, err := xetstorage.NewFileStorage(
			xetstorage.WithBasePath(filepath.Join(m.dataDir, "storage")),
		)
		if err != nil {
			return nil, fmt.Errorf("create xet storage: %w", err)
		}
		m.xetStorage = stor
	}

	issuer, err := xettoken.NewIssuer(nil, 15*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("create token issuer: %w", err)
	}
	m.issuer = issuer

	// The CAS server always runs so xet clients can upload LFS objects; the
	// upstream ingest handler is added only when a pull upstream exists.
	casNext := http.Handler(http.NotFoundHandler())
	if m.upstreamURL != "" {
		mirrorHandler, err := xetmirror.NewHandler(
			xetmirror.WithStorage(m.xetStorage),
			xetmirror.WithUpstream(m.upstreamURL),
			xetmirror.WithUpstreamToken(m.upstreamToken),
			xetmirror.WithCacheDir(filepath.Join(m.dataDir, "mirror")),
			xetmirror.WithClient(m.xetClient),
			xetmirror.WithMintToken(issuer.Mint),
			xetmirror.WithExternalURL(m.externalURL),
			// hfd serves its own control plane; unmatched requests must not
			// be proxied upstream.
			xetmirror.WithNext(http.NotFoundHandler()),
		)
		if err != nil {
			return nil, fmt.Errorf("create xet mirror handler: %w", err)
		}
		casNext = mirrorHandler
		m.mirrorHandler = mirrorHandler
	}

	m.dataPlane = xetserver.NewHandler(
		xetserver.WithStorage(m.xetStorage),
		xetserver.WithAuthFunc(func(tok string) bool { return issuer.Validate(tok, time.Now()) }),
		xetserver.WithNext(casNext),
	)
	m.dataPlane = m.handleWriteToken(m.dataPlane)
	if m.permissionHookFunc != nil {
		m.dataPlane = m.gateUserPaths(m.dataPlane)
	}

	return m, nil
}

// IsMirrorSource checks if the given repository is a mirror source by invoking the mirrorSourceFunc callback. Returns false if the callback is not set.
func (m *Mirror) IsMirrorSource(ctx context.Context, repoName string) (bool, error) {
	if m.mirrorSourceFunc == nil {
		return false, nil
	}
	_, isMirror, err := m.mirrorSourceFunc(ctx, repoName)
	return isMirror, err
}

// Wait blocks until background work (LFS prefetches and the ingests they
// drive) has finished. Call it before tearing down the data directory.
func (m *Mirror) Wait() {
	m.background.Wait()
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
