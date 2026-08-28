package mirror

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
	"golang.org/x/sync/singleflight"

	xetclient "github.com/wzshiming/xet/client"
	xetmirror "github.com/wzshiming/xet/mirror"
	xetstorage "github.com/wzshiming/xet/storage"

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
// push) plus an xet-backed data plane that ingests file bytes as
// xorbs/shards and answers object queries. It also carries the HTTP
// data-plane surface: xet Link headers, CAS token minting, and OID
// resolves served directly from the ingest engine.
type Mirror struct {
	mirrorSourceFunc      SourceFunc
	mirrorDestinationFunc DestinationFunc
	mirrorRefFilterFunc   RefFilterFunc
	preReceiveHookFunc    receive.PreReceiveHookFunc
	postReceiveHookFunc   receive.PostReceiveHookFunc
	syncUserInfoFunc      SyncUserInfoFunc
	gitOutputFunc         GitOutputFunc
	repositoriesFS        billy.Filesystem
	ttl                   time.Duration
	pullGroup             singleflight.Group
	pushGroup             singleflight.Group
	lastSync              sync.Map // map[string]time.Time, keyed by repoPath
	background            sync.WaitGroup

	xetStorage     xetstorage.Storage
	xetClient      *xetclient.Client
	xetMirror      *xetmirror.Mirror // ingest engine; nil without a pull upstream
	mint           func(time.Time) (string, int64)
	externalURL    string
	concurrency    int
	dataDir        string
	httpClient     *http.Client // LFS batch/upload/verify; no timeout, uploads may run long
	downloadClient *http.Client // object content downloads, resuming interrupted streams

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

// WithXETStorage sets the xet storage backend the mirror ingests into and serves from.
func WithXETStorage(s xetstorage.Storage) Option {
	return func(m *Mirror) {
		m.xetStorage = s
	}
}

// WithXETClient sets the xet client used for chunk transfers.
func WithXETClient(c *xetclient.Client) Option {
	return func(m *Mirror) {
		m.xetClient = c
	}
}

// WithXETMirror sets the xet mirror engine that ingests upstream files into
// the xet storage; optional — without it the upstream features are off.
func WithXETMirror(x *xetmirror.Mirror) Option {
	return func(m *Mirror) {
		m.xetMirror = x
	}
}

// WithMintToken sets the function that mints short-lived CAS access tokens;
// see authenticate.NewXETTokenScheme.
func WithMintToken(fn func(time.Time) (string, int64)) Option {
	return func(m *Mirror) {
		m.mint = fn
	}
}

// WithExternalURL sets the externally visible base URL for minted casUrl and
// Link headers; derived from the request when empty.
func WithExternalURL(u string) Option {
	return func(m *Mirror) {
		m.externalURL = u
	}
}

// WithConcurrency sets the xet upload concurrency for ingests.
func WithConcurrency(concurrency int) Option {
	return func(m *Mirror) {
		m.concurrency = concurrency
	}
}

// WithDataDir sets the mirror's scratch directory; the ingest spool lives under it.
func WithDataDir(dir string) Option {
	return func(m *Mirror) {
		m.dataDir = dir
	}
}

// WithRepositoriesFS sets the filesystem used to access local mirror
// repositories. The default is the host OS.
func WithRepositoriesFS(fs billy.Filesystem) Option {
	return func(m *Mirror) {
		m.repositoriesFS = fs
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

// NewMirror creates a new Mirror with the provided options. It does not
// assemble the xet stack; the caller (cmd/hfd) builds the client, storage,
// and mirror engine and injects each piece.
func NewMirror(opts ...Option) (*Mirror, error) {
	m := &Mirror{}
	for _, opt := range opts {
		opt(m)
	}
	if m.repositoriesFS == nil {
		m.repositoriesFS = osfs.Default
	}
	// The batch/upload client has no overall timeout because uploads may run
	// long; the stall guard bounds no-progress phases instead.
	m.httpClient = http.DefaultClient
	m.downloadClient = newDownloadClient()
	if m.xetStorage == nil || m.xetClient == nil {
		return nil, fmt.Errorf("mirror requires the xet pieces: WithXETStorage, WithXETClient")
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
