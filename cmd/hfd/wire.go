package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/matrixhub-ai/hfd/internal/server"
	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	pkgssh "github.com/matrixhub-ai/hfd/pkg/ssh"
	"github.com/matrixhub-ai/hfd/pkg/storage"
	s3fs "github.com/wzshiming/go-billy-s3fs"
	xetauth "github.com/wzshiming/xet/auth"
	xetclient "github.com/wzshiming/xet/client"
	xetmirror "github.com/wzshiming/xet/mirror"
	xets3 "github.com/wzshiming/xet/storage/s3"
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

// buildStorage keeps xet caches local even when content and repositories use S3.
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
		s3Opts := []xets3.Option{
			xets3.WithS3Client(newS3Client(cfg)),
			xets3.WithBucket(cfg.S3Bucket),
			xets3.WithPrefix("xet"),
		}
		if cfg.S3SignEndpoint != "" {
			s3Opts = append(s3Opts, xets3.WithPresignEndpoint(cfg.S3SignEndpoint))
		}
		xs, err := xets3.NewStorage(ctx, s3Opts...)
		if err != nil {
			return nil, fmt.Errorf("create xet S3 storage: %w", err)
		}
		opts = append(opts, storage.WithXETStorage(xs))
	}
	return storage.NewStorage(opts...)
}

// buildXETClient applies proxy tuning to the storage-rooted chunk cache.
func buildXETClient(cfg *config, st *storage.Storage) (*xetclient.Client, error) {
	clientOpts := []xetclient.Options{
		xetclient.WithCacheDir(filepath.Join(st.XETDir(), "chunks")),
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
func buildXETMirror(cfg *config, st *storage.Storage, xetC *xetclient.Client) (*xetmirror.Mirror, error) {
	if cfg.PullMirrorURL == "" {
		return nil, nil
	}
	upstream, err := xetmirror.StaticUpstream(strings.TrimSuffix(cfg.PullMirrorURL, "/"), cfg.ProxyToken)
	if err != nil {
		return nil, fmt.Errorf("create xet mirror engine: %w", err)
	}
	engine, err := xetmirror.NewMirror(
		xetmirror.WithStorage(st.XETStorage()),
		xetmirror.WithUpstream(upstream),
		xetmirror.WithCacheDir(filepath.Join(st.XETDir(), "mirror")),
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
func buildMirror(ctx context.Context, cfg *config, st *storage.Storage, hooks *server.Hooks, xetC *xetclient.Client, engine *xetmirror.Mirror, mint func(xetauth.Grant) (string, int64, error)) (*mirror.Mirror, error) {
	opts := []mirror.Option{
		mirror.WithXETStorage(st.XETStorage()),
		mirror.WithXETClient(xetC),
		mirror.WithXETMirror(engine),
		mirror.WithMintToken(mint),
		mirror.WithExternalURL(cfg.HostURL),
		mirror.WithDataDir(st.XETDir()),
		mirror.WithConcurrency(cfg.ProxyConcurrencyPerFile),
		mirror.WithPreReceiveHookFunc(hooks.PreReceive),
		mirror.WithPostReceiveHookFunc(hooks.PostReceive),
		mirror.WithRepositoriesFS(st.RepositoriesFS()),
		mirror.WithGitOutputFunc(hooks.GitOutput),
		mirror.WithSyncUserInfoFunc(hooks.SyncUserInfo),
		mirror.WithMirrorRefFilterFunc(hooks.MirrorRefFilter),
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
		authorizedKeys := make(map[string]string, len(parsedKeys))
		for _, k := range parsedKeys {
			authorizedKeys[string(k.Key.Marshal())] = k.Comment
		}
		slog.InfoContext(ctx, "Loaded SSH authorized keys", "count", len(parsedKeys))
		auth.PublicKey = authenticate.NewSimplePublicKeyValidator(authorizedKeys)
	}
	return auth, nil
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
