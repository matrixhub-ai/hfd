package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gorilla/handlers"
	s3fs "github.com/wzshiming/go-billy-s3fs"
	xetclient "github.com/wzshiming/xet/client"
	xetmirror "github.com/wzshiming/xet/mirror"
	xetserver "github.com/wzshiming/xet/server"
	xetinternalapi "github.com/wzshiming/xet/server/internalapi"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/internal/stallguard"
	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	backendcas "github.com/matrixhub-ai/hfd/pkg/backend/cas"
	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendinternalapi "github.com/matrixhub-ai/hfd/pkg/backend/internalapi"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	backendssh "github.com/matrixhub-ai/hfd/pkg/backend/ssh"
	"github.com/matrixhub-ai/hfd/pkg/gc"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	pkgssh "github.com/matrixhub-ai/hfd/pkg/ssh"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// s3Configured reports whether the S3 storage backend is configured.
func s3Configured(cfg *config) bool {
	return cfg.S3Endpoint != "" && cfg.S3Bucket != ""
}

// newS3Client builds the S3 client shared by the repository filesystem and
// the xet content storage.
func newS3Client(cfg *config) *s3.Client {
	awsCfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(cfg.S3AccessKey, cfg.S3SecretKey, ""),
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
		}
		o.UsePathStyle = cfg.S3UsePathStyle
		// only checksum when required, for S3-compatible stores (e.g. MinIO)
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
}

// newS3Filesystem returns a billy filesystem rooted at the S3 bucket, holding
// git repositories. Metadata and small object bodies are kept in a local
// write-through cache; writes from other processes become visible after the
// TTL.
func newS3Filesystem(cfg *config) *s3fs.S3FS {
	client := newS3Client(cfg)
	var presignOpts []func(*s3.PresignOptions)
	if cfg.S3SignEndpoint != "" {
		presignOpts = append(presignOpts, s3fs.WithPresignEndpoint(cfg.S3SignEndpoint))
	}
	return s3fs.New(cfg.S3Bucket,
		s3fs.WithClient(client),
		s3fs.WithPresignClient(s3.NewPresignClient(client, presignOpts...)),
		s3fs.WithMemCache(256<<20, time.Minute),
	)
}

// buildStorage resolves the data directory and creates the storage layout,
// backed by S3 when configured and by the local data directory otherwise.
func buildStorage(ctx context.Context, cfg *config) (*storage.Storage, error) {
	if (cfg.S3Endpoint != "") != (cfg.S3Bucket != "") {
		return nil, fmt.Errorf("S3 storage requires both --s3-endpoint and --s3-bucket (endpoint %q, bucket %q)", cfg.S3Endpoint, cfg.S3Bucket)
	}
	absRootDir, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve data directory %q: %w", cfg.DataDir, err)
	}
	if err := os.MkdirAll(absRootDir, 0755); err != nil {
		return nil, fmt.Errorf("create data directory %q: %w", absRootDir, err)
	}
	opts := []storage.Option{storage.WithRootDir(absRootDir)}
	if s3Configured(cfg) {
		slog.InfoContext(ctx, "Using S3-backed storage filesystem", "bucket", cfg.S3Bucket)
		opts = append(opts, storage.WithFilesystem(newS3Filesystem(cfg)))
	}
	return storage.NewStorage(opts...), nil
}

// buildXETStorage creates the xet content storage holding all LFS bytes: in
// the S3 bucket when configured, under the data directory otherwise. In S3
// mode xorb downloads presign straight to S3 — everything else is proxied.
func buildXETStorage(ctx context.Context, cfg *config) (xetStore, error) {
	var xs xetStore
	var err error
	if s3Configured(cfg) {
		s3Opts := []xetstorage.S3Option{
			xetstorage.WithS3Client(newS3Client(cfg)),
			xetstorage.WithS3Bucket(cfg.S3Bucket),
			xetstorage.WithS3Prefix("xet"),
		}
		if cfg.S3SignEndpoint != "" {
			s3Opts = append(s3Opts, xetstorage.WithS3PresignEndpoint(cfg.S3SignEndpoint))
		}
		xs, err = xetstorage.NewS3Storage(ctx, s3Opts...)
		if err != nil {
			return nil, fmt.Errorf("create xet S3 storage: %w", err)
		}
	} else {
		xs, err = xetstorage.NewFileStorage(
			xetstorage.WithBasePath(filepath.Join(cfg.DataDir, "xet", "storage")),
		)
		if err != nil {
			return nil, fmt.Errorf("create xet storage: %w", err)
		}
	}
	return xs, nil
}

// buildXETClient creates the xet client with its chunk cache under the data
// directory.
func buildXETClient(cfg *config) (*xetclient.Client, error) {
	chunksDir := filepath.Join(cfg.DataDir, "xet", "chunks")
	if err := os.MkdirAll(chunksDir, 0755); err != nil {
		return nil, fmt.Errorf("create xet chunk cache dir: %w", err)
	}
	clientOpts := []xetclient.Options{
		xetclient.WithCacheDir(chunksDir),
		// The xet client wraps this transport with its own httpseek layer, so
		// stalled chunk downloads abort and resume instead of hanging.
		xetclient.WithHTTPClient(&http.Client{Transport: stallguard.NewTransport(http.DefaultTransport, 15*time.Second)}),
	}
	if cfg.ProxyConcurrencyPerFile > 0 {
		clientOpts = append(clientOpts, xetclient.WithConcurrency(cfg.ProxyConcurrencyPerFile))
	}
	if cfg.ProxyCacheSize > 0 {
		clientOpts = append(clientOpts, xetclient.WithCacheSize(cfg.ProxyCacheSize))
	}
	xetC, err := xetclient.NewClient(clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("create xet client: %w", err)
	}
	return xetC, nil
}

// buildXETMirror creates the upstream ingest engine when a pull upstream is
// configured; nil otherwise.
func buildXETMirror(cfg *config, xs xetstorage.Storage, xetC *xetclient.Client) (*xetmirror.Mirror, error) {
	if cfg.PullMirrorURL == "" {
		return nil, nil
	}
	engine, err := xetmirror.NewMirror(
		xetmirror.WithStorage(xs),
		xetmirror.WithUpstream(strings.TrimSuffix(cfg.PullMirrorURL, "/")),
		xetmirror.WithUpstreamToken(cfg.ProxyToken),
		xetmirror.WithCacheDir(filepath.Join(cfg.DataDir, "xet", "mirror")),
		xetmirror.WithClient(xetC),
	)
	if err != nil {
		return nil, fmt.Errorf("create xet mirror engine: %w", err)
	}
	return engine, nil
}

// buildMirror builds the shared mirror around the injected xet pieces; the
// mirror carries the data plane (token mint, external URL) and serves OID
// resolves straight off the ingest engine. Pull and push mirroring activate
// when their URLs are configured.
func buildMirror(ctx context.Context, cfg *config, st *storage.Storage, xs xetstorage.Storage, hooks *serverHooks, xetC *xetclient.Client, engine *xetmirror.Mirror, mint func(time.Time) (string, int64)) (*mirror.Mirror, error) {
	opts := []mirror.Option{
		mirror.WithXETStorage(xs),
		mirror.WithXETClient(xetC),
		mirror.WithXETMirror(engine),
		mirror.WithMintToken(mint),
		mirror.WithExternalURL(cfg.HostURL),
		mirror.WithDataDir(filepath.Join(cfg.DataDir, "xet")),
		mirror.WithConcurrency(cfg.ProxyConcurrencyPerFile),
		mirror.WithPreReceiveHookFunc(hooks.preReceive),
		mirror.WithPostReceiveHookFunc(hooks.postReceive),
		mirror.WithRepositoriesFS(st.RepositoriesFS()),
		mirror.WithGitOutputFunc(hooks.gitOutput),
		mirror.WithSyncUserInfoFunc(hooks.syncUserInfo),
		mirror.WithTTL(cfg.ProxyCacheTTL),
		mirror.WithMirrorRefFilterFunc(hooks.mirrorRefFilter),
	}

	if cfg.PullMirrorURL != "" {
		slog.InfoContext(ctx, "Pull mirror mode enabled", "source", cfg.PullMirrorURL)
		baseURL := strings.TrimSuffix(cfg.PullMirrorURL, "/")
		opts = append(opts,
			mirror.WithMirrorSourceFunc(
				func(ctx context.Context, repoName string) (string, bool, error) {
					return baseURL + "/" + strings.TrimPrefix(repoName, "/"), true, nil
				}))
	}

	if cfg.PushMirrorURL != "" {
		slog.InfoContext(ctx, "Push mirror mode enabled", "destination", cfg.PushMirrorURL)
		baseURL := strings.TrimSuffix(cfg.PushMirrorURL, "/")
		opts = append(opts, mirror.WithMirrorDestinationFunc(
			func(ctx context.Context, repoName string) (string, bool, error) {
				return baseURL + "/" + strings.TrimPrefix(repoName, "/"), true, nil
			}))
	}

	return mirror.NewMirror(opts...)
}

// buildAuthenticators creates validators for each configured authentication scheme.
func buildAuthenticators(ctx context.Context, cfg *config) (*authenticate.Authenticators, error) {
	auth := &authenticate.Authenticators{}
	if cfg.AuthPassword != "" {
		auth.BasicAuth = authenticate.NewSimpleBasicAuthValidator(cfg.AuthUsername, cfg.AuthPassword)
	}
	if cfg.AuthToken != "" {
		auth.Token = authenticate.NewSimpleTokenValidator(cfg.AuthUsername, cfg.AuthToken)
	}
	if cfg.AuthSignKey != "" {
		auth.TokenSign = authenticate.NewTokenSignValidator([]byte(cfg.AuthSignKey))
	}
	if cfg.SSHAuthorizedKey != "" {
		authKeysData, err := os.ReadFile(cfg.SSHAuthorizedKey)
		if err != nil {
			return nil, fmt.Errorf("read SSH authorized keys file %q: %w", cfg.SSHAuthorizedKey, err)
		}
		parsedKeys, err := pkgssh.ParseAuthorizedKeys(authKeysData)
		if err != nil {
			return nil, fmt.Errorf("parse SSH authorized keys %q: %w", cfg.SSHAuthorizedKey, err)
		}
		var authorizedKeys [][]byte
		for _, k := range parsedKeys {
			authorizedKeys = append(authorizedKeys, k.Marshal())
		}
		slog.InfoContext(ctx, "Loaded SSH authorized keys", "count", len(parsedKeys))
		auth.PublicKey = authenticate.NewSimplePublicKeyValidator(authorizedKeys)
	}
	return auth, nil
}

type middleware func(next http.Handler) http.Handler

// xetStore is the xet storage together with the GC surface both xet backends implement.
type xetStore interface {
	xetstorage.Storage
	xetstorage.GCStore
}

// chain composes middlewares so the first one is the outermost, i.e. the
// argument order is the request order.
func chain(tail http.Handler, mws ...middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		tail = mws[i](tail)
	}
	return tail
}

// passthrough is the middleware for a layer that is not enabled.
func passthrough(next http.Handler) http.Handler { return next }

func requestLogging(w io.Writer) middleware {
	return func(next http.Handler) http.Handler {
		return handlers.CombinedLoggingHandler(w, next)
	}
}

// internalAPI mounts the unauthenticated /internal/ management endpoints when enabled: hfd's
// POST /internal/gc and /internal/gc/sweep on one lock, in front of xet's file listing and unlink.
func internalAPI(ctx context.Context, cfg *config, st *storage.Storage, xs xetStore) middleware {
	if !cfg.Internal {
		return passthrough
	}
	slog.WarnContext(ctx, "Internal management API enabled; /internal/ endpoints are unauthenticated")
	collector := gc.NewCollector(st.RepositoriesFS(), xs)
	return func(next http.Handler) http.Handler {
		return backendinternalapi.NewHandler(
			backendinternalapi.WithCollector(collector),
			backendinternalapi.WithGCGrace(time.Hour),
			backendinternalapi.WithNext(xetinternalapi.NewHandler(
				xetinternalapi.WithStorage(xs),
				xetinternalapi.WithNext(next),
			)),
		)
	}
}

// casTokenRecognizer maps hfd-signed CAS credentials, which carry a fixed
// scope rather than a URL, to the xet-cas user.
func casTokenRecognizer(authFn func(string) bool) middleware {
	return func(next http.Handler) http.Handler {
		return authenticate.TokenValidatorHandler(authenticate.NewTokenRecognizer("xet-cas", authFn), next)
	}
}

func authentication(auth *authenticate.Authenticators) middleware {
	return func(next http.Handler) http.Handler {
		return authenticate.NewHandler(
			authenticate.WithNext(next),
			authenticate.WithBasicAuthValidator(auth.BasicAuth),
			authenticate.WithTokenValidator(auth.Token),
			authenticate.WithTokenSignValidator(auth.TokenSign),
		)
	}
}

// gitHTTPBackend serves the git smart HTTP protocol.
func gitHTTPBackend(st *storage.Storage, hooks *serverHooks, m *mirror.Mirror) middleware {
	return func(next http.Handler) http.Handler {
		return backendhttp.NewHandler(
			backendhttp.WithStorage(st),
			backendhttp.WithNext(next),
			backendhttp.WithMirror(m),
			backendhttp.WithPreOpenHookFunc(hooks.preOpen),
			backendhttp.WithPermissionHookFunc(hooks.permission),
			backendhttp.WithPreReceiveHookFunc(hooks.preReceive),
			backendhttp.WithPostReceiveHookFunc(hooks.postReceive),
		)
	}
}

// lfsBackend serves the git LFS API from the mirror's data plane.
func lfsBackend(st *storage.Storage, hooks *serverHooks, m *mirror.Mirror, auth *authenticate.Authenticators) middleware {
	return func(next http.Handler) http.Handler {
		return backendlfs.NewHandler(
			backendlfs.WithStorage(st),
			backendlfs.WithNext(next),
			backendlfs.WithMirror(m),
			backendlfs.WithPermissionHookFunc(hooks.permission),
			backendlfs.WithTokenSignValidator(auth.TokenSign),
		)
	}
}

// hfBackend serves the HF hub API (resolve/tree) from the mirror's data plane.
func hfBackend(st *storage.Storage, hooks *serverHooks, m *mirror.Mirror) middleware {
	return func(next http.Handler) http.Handler {
		return backendhf.NewHandler(
			backendhf.WithStorage(st),
			backendhf.WithNext(next),
			backendhf.WithMirror(m),
			backendhf.WithPreOpenHookFunc(hooks.preOpen),
			backendhf.WithPermissionHookFunc(hooks.permission),
			backendhf.WithPreReceiveHookFunc(hooks.preReceive),
			backendhf.WithPostReceiveHookFunc(hooks.postReceive),
		)
	}
}

// casBackend serves the CAS token routes from the mirror's data plane.
func casBackend(hooks *serverHooks, m *mirror.Mirror) middleware {
	return func(next http.Handler) http.Handler {
		return backendcas.NewHandler(
			backendcas.WithMirror(m),
			backendcas.WithPermissionHookFunc(hooks.permission),
			backendcas.WithNext(next),
		)
	}
}

// xetCASServer serves the xet CAS transfer routes over the xet storage. Unlike
// xetd there is no hub front end behind it — hfd's own backends serve the hub
// control plane (resolve/tree in hfBackend, the token routes in casBackend).
func xetCASServer(xs xetstorage.Storage, authFn func(string) bool) middleware {
	return func(next http.Handler) http.Handler {
		return xetserver.NewHandler(
			xetserver.WithStorage(xs),
			xetserver.WithAuthFunc(authFn),
			xetserver.WithNext(next),
		)
	}
}

// buildHTTPHandler lists the HTTP layers in request order, outermost first.
func buildHTTPHandler(ctx context.Context, cfg *config, st *storage.Storage, xs xetStore, hooks *serverHooks, m *mirror.Mirror, auth *authenticate.Authenticators, authFn func(string) bool) http.Handler {
	return chain(http.NotFoundHandler(),
		requestLogging(os.Stderr),
		internalAPI(ctx, cfg, st, xs), // operator endpoints bypass user auth
		casTokenRecognizer(authFn),    // hfd-signed CAS credentials would 401 in the per-URL validators
		authentication(auth),
		gitHTTPBackend(st, hooks, m),
		lfsBackend(st, hooks, m, auth),
		hfBackend(st, hooks, m),
		casBackend(hooks, m),
		xetCASServer(xs, authFn),
	)
}

// loadOrGenerateHostKey loads the SSH host key from the configured path, or
// generates and saves one under the data directory when absent.
func loadOrGenerateHostKey(ctx context.Context, cfg *config, st *storage.Storage) (pkgssh.Signer, error) {
	hostKeyPath := cfg.SSHHostKeyFile
	if hostKeyPath == "" {
		hostKeyPath = filepath.Join(cfg.DataDir, "ssh_host_rsa_key")
	}
	data, err := os.ReadFile(hostKeyPath)
	if err == nil {
		hostKeySigner, err := pkgssh.ParseHostKeyFile(data)
		if err != nil {
			return nil, fmt.Errorf("parse SSH host key file %q: %w", hostKeyPath, err)
		}
		slog.InfoContext(ctx, "Loaded SSH host key", "path", hostKeyPath)
		return hostKeySigner, nil
	}
	if cfg.SSHHostKeyFile != "" || !os.IsNotExist(err) {
		return nil, fmt.Errorf("read SSH host key file %q: %w", hostKeyPath, err)
	}
	hostKeySigner, err := pkgssh.GenerateAndSaveHostKey(hostKeyPath, pkgssh.KeyTypeRSA)
	if err != nil {
		return nil, fmt.Errorf("generate SSH host key %q: %w", hostKeyPath, err)
	}
	slog.InfoContext(ctx, "Generated SSH host key", "path", hostKeyPath)
	return hostKeySigner, nil
}

// buildSSHServer assembles the SSH protocol server.
func buildSSHServer(ctx context.Context, cfg *config, st *storage.Storage, hooks *serverHooks, sharedMirror *mirror.Mirror, auth *authenticate.Authenticators) (*backendssh.Server, error) {
	hostKeySigner, err := loadOrGenerateHostKey(ctx, cfg, st)
	if err != nil {
		return nil, err
	}

	return backendssh.NewServer(
		backendssh.WithStorage(st),
		backendssh.WithHostKey(hostKeySigner),
		backendssh.WithPermissionHookFunc(hooks.permission),
		backendssh.WithPreOpenHookFunc(hooks.preOpen),
		backendssh.WithPreReceiveHookFunc(hooks.preReceive),
		backendssh.WithPostReceiveHookFunc(hooks.postReceive),
		backendssh.WithMirror(sharedMirror),
		backendssh.WithLFSURL(cfg.HostURL),
		backendssh.WithBasicAuthValidator(auth.BasicAuth),
		backendssh.WithPublicKeyValidator(auth.PublicKey),
		backendssh.WithTokenSignValidator(auth.TokenSign),
	), nil
}
