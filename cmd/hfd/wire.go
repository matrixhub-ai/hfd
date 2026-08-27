package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3fs "github.com/wzshiming/go-billy-s3fs"
	xetclient "github.com/wzshiming/xet/client"
	xetmirror "github.com/wzshiming/xet/mirror"
	xetserver "github.com/wzshiming/xet/server"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/internal/stallguard"
	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	backendcas "github.com/matrixhub-ai/hfd/pkg/backend/cas"
	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	backendssh "github.com/matrixhub-ai/hfd/pkg/backend/ssh"
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
func buildXETStorage(ctx context.Context, cfg *config) (xetstorage.Storage, error) {
	var xs xetstorage.Storage
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

// buildXETMirrorHandler creates the upstream ingest handler when a pull
// upstream is configured; nil otherwise.
func buildXETMirrorHandler(cfg *config, xs xetstorage.Storage, xetC *xetclient.Client, mint func(time.Time) (string, int64)) (*xetmirror.Handler, error) {
	if cfg.PullMirrorURL == "" {
		return nil, nil
	}
	mirrorHandler, err := xetmirror.NewHandler(
		xetmirror.WithStorage(xs),
		xetmirror.WithUpstream(strings.TrimSuffix(cfg.PullMirrorURL, "/")),
		xetmirror.WithUpstreamToken(cfg.ProxyToken),
		xetmirror.WithCacheDir(filepath.Join(cfg.DataDir, "xet", "mirror")),
		xetmirror.WithClient(xetC),
		xetmirror.WithMintToken(mint),
		xetmirror.WithExternalURL(cfg.HostURL),
		// hfd serves its own control plane; unmatched requests must not
		// be proxied upstream.
		xetmirror.WithNext(http.NotFoundHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("create xet mirror handler: %w", err)
	}
	return mirrorHandler, nil
}

// buildMirror builds the shared mirror around the injected xet pieces. Pull
// and push mirroring activate when their URLs are configured.
func buildMirror(ctx context.Context, cfg *config, st *storage.Storage, xs xetstorage.Storage, hooks *serverHooks, mint func(time.Time) (string, int64), xetC *xetclient.Client, mirrorHandler *xetmirror.Handler) (*mirror.Mirror, error) {
	opts := []mirror.Option{
		mirror.WithXETStorage(xs),
		mirror.WithXETClient(xetC),
		mirror.WithMirrorHandler(mirrorHandler),
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

// buildXETComposition assembles the xet CAS composition — the CAS server
// falling through to the mirror handler, following xetd.
func buildXETComposition(xs xetstorage.Storage, authFn func(string) bool, mirrorHandler *xetmirror.Handler) http.Handler {
	// The CAS server always runs so xet clients can upload LFS objects; the
	// upstream ingest handler is added only when a pull upstream exists.
	handler := http.Handler(http.NotFoundHandler())
	if mirrorHandler != nil {
		handler = mirrorHandler
	}
	handler = xetserver.NewHandler(
		xetserver.WithStorage(xs),
		xetserver.WithAuthFunc(authFn),
		xetserver.WithNext(handler),
	)
	return handler
}

// buildHTTPHandler composes the HTTP handler chain: HF API → LFS → HTTP Git
// backends over the CAS write-token backend and the injected xet composition
// tail, with authentication and CAS-credential recognition at the head.
func buildHTTPHandler(st *storage.Storage, hooks *serverHooks, sharedMirror *mirror.Mirror, auth *authenticate.Authenticators, authFn func(string) bool, xetComposition http.Handler) http.Handler {
	handler := xetComposition

	handler = backendcas.NewHandler(
		backendcas.WithMirror(sharedMirror),
		backendcas.WithPermissionHookFunc(hooks.permission),
		backendcas.WithNext(handler),
	)

	handler = backendhf.NewHandler(
		backendhf.WithStorage(st),
		backendhf.WithNext(handler),
		backendhf.WithMirror(sharedMirror),
		backendhf.WithPreOpenHookFunc(hooks.preOpen),
		backendhf.WithPermissionHookFunc(hooks.permission),
		backendhf.WithPreReceiveHookFunc(hooks.preReceive),
		backendhf.WithPostReceiveHookFunc(hooks.postReceive),
	)

	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(st),
		backendlfs.WithNext(handler),
		backendlfs.WithMirror(sharedMirror),
		backendlfs.WithPermissionHookFunc(hooks.permission),
		backendlfs.WithTokenSignValidator(auth.TokenSign),
	)

	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(st),
		backendhttp.WithNext(handler),
		backendhttp.WithMirror(sharedMirror),
		backendhttp.WithPreOpenHookFunc(hooks.preOpen),
		backendhttp.WithPermissionHookFunc(hooks.permission),
		backendhttp.WithPreReceiveHookFunc(hooks.preReceive),
		backendhttp.WithPostReceiveHookFunc(hooks.postReceive),
	)

	handler = authenticate.NewHandler(
		authenticate.WithNext(handler),
		authenticate.WithBasicAuthValidator(auth.BasicAuth),
		authenticate.WithTokenValidator(auth.Token),
		authenticate.WithTokenSignValidator(auth.TokenSign),
	)

	// CAS credentials are hfd-signed with a fixed scope; recognize them ahead
	// of the per-URL validators, which would otherwise 401 them.
	handler = authenticate.TokenValidatorHandler(authenticate.NewTokenRecognizer("xet-cas", authFn), handler)

	return handler
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
