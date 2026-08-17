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
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
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

// buildMirror creates the shared mirror. The xet data plane is always
// enabled and holds all LFS content; pull and push mirroring activate when
// their URLs are configured. In S3 mode the xet storage lives in the bucket,
// and xorb downloads presign straight to S3 — everything else is proxied.
func buildMirror(ctx context.Context, cfg *config, st *storage.Storage, hooks *serverHooks) (*mirror.Mirror, error) {
	opts := []mirror.Option{
		mirror.WithPreReceiveHookFunc(hooks.preReceive),
		mirror.WithPostReceiveHookFunc(hooks.postReceive),
		mirror.WithPermissionHookFunc(hooks.permission),
		mirror.WithRepositoriesFS(st.RepositoriesFS()),
		mirror.WithConcurrency(cfg.ProxyConcurrencyPerFile),
		mirror.WithDataDir(filepath.Join(cfg.DataDir, "xet")),
		mirror.WithXETCacheSize(cfg.ProxyCacheSize),
		mirror.WithExternalURL(cfg.HostURL),
		mirror.WithGitOutputFunc(hooks.gitOutput),
		mirror.WithSyncUserInfoFunc(hooks.syncUserInfo),
		mirror.WithTTL(cfg.ProxyCacheTTL),
		mirror.WithMirrorRefFilterFunc(hooks.mirrorRefFilter),
	}

	if s3Configured(cfg) {
		s3Opts := []xetstorage.S3Option{
			xetstorage.WithS3Client(newS3Client(cfg)),
			xetstorage.WithS3Bucket(cfg.S3Bucket),
			xetstorage.WithS3Prefix("xet"),
		}
		if cfg.S3SignEndpoint != "" {
			s3Opts = append(s3Opts, xetstorage.WithS3PresignEndpoint(cfg.S3SignEndpoint))
		}
		xs, err := xetstorage.NewS3Storage(ctx, s3Opts...)
		if err != nil {
			return nil, fmt.Errorf("create xet S3 storage: %w", err)
		}
		opts = append(opts, mirror.WithXETStorage(xs))
	}

	if cfg.PullMirrorURL != "" {
		slog.InfoContext(ctx, "Pull mirror mode enabled", "source", cfg.PullMirrorURL)
		baseURL := strings.TrimSuffix(cfg.PullMirrorURL, "/")
		opts = append(opts,
			mirror.WithUpstream(baseURL),
			mirror.WithUpstreamToken(cfg.ProxyToken),
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

// authenticators bundles the optional authentication validators.
type authenticators struct {
	basicAuth authenticate.BasicAuthValidator
	token     authenticate.TokenValidator
	publicKey authenticate.PublicKeyValidator
	tokenSign authenticate.TokenSignValidator
}

// buildAuthenticators creates validators for each configured authentication scheme.
func buildAuthenticators(ctx context.Context, cfg *config) (*authenticators, error) {
	auth := &authenticators{}
	if cfg.AuthPassword != "" {
		auth.basicAuth = authenticate.NewSimpleBasicAuthValidator(cfg.AuthUsername, cfg.AuthPassword)
	}
	if cfg.AuthToken != "" {
		auth.token = authenticate.NewSimpleTokenValidator(cfg.AuthUsername, cfg.AuthToken)
	}
	if cfg.AuthSignKey != "" {
		auth.tokenSign = authenticate.NewTokenSignValidator([]byte(cfg.AuthSignKey))
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
		auth.publicKey = authenticate.NewSimplePublicKeyValidator(authorizedKeys)
	}
	return auth, nil
}

// buildHTTPHandler assembles the HTTP handler chain:
// HF API → LFS → HTTP Git → authentication middlewares.
func buildHTTPHandler(st *storage.Storage, hooks *serverHooks, sharedMirror *mirror.Mirror, auth *authenticators) http.Handler {
	var handler http.Handler

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
		backendlfs.WithTokenSignValidator(auth.tokenSign),
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

	// Token minting and bridge routes carry user credentials: they ride
	// inside the authentication chain so the mirror's permission gate sees
	// the requesting user.
	if sharedMirror != nil {
		if dp := sharedMirror.DataPlane(); dp != nil {
			inner := handler
			handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mirror.IsDataPlaneUserPath(r.URL.Path) {
					dp.ServeHTTP(w, r)
					return
				}
				inner.ServeHTTP(w, r)
			})
		}
	}

	handler = authenticate.AnonymousAuthenticateHandler(handler)
	handler = authenticate.TokenValidatorHandler(auth.token, handler)
	handler = authenticate.TokenSignValidatorHandler(auth.tokenSign, handler)
	handler = authenticate.BasicAuthHandler(auth.basicAuth, handler)

	// CAS transfer routes authenticate with their own minted tokens, so they
	// bypass the hfd authentication chain.
	if sharedMirror != nil {
		if dp := sharedMirror.DataPlane(); dp != nil {
			inner := handler
			handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mirror.IsCASPath(r.URL.Path) {
					dp.ServeHTTP(w, r)
					return
				}
				inner.ServeHTTP(w, r)
			})
		}
	}

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
func buildSSHServer(ctx context.Context, cfg *config, st *storage.Storage, hooks *serverHooks, sharedMirror *mirror.Mirror, auth *authenticators) (*backendssh.Server, error) {
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
		backendssh.WithBasicAuthValidator(auth.basicAuth),
		backendssh.WithPublicKeyValidator(auth.publicKey),
		backendssh.WithTokenSignValidator(auth.tokenSign),
	), nil
}
