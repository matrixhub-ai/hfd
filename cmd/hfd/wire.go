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

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	backendssh "github.com/matrixhub-ai/hfd/pkg/backend/ssh"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/s3fs"
	pkgssh "github.com/matrixhub-ai/hfd/pkg/ssh"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// buildStorage resolves the data directory and creates the storage layout.
func buildStorage(cfg *config) (*storage.Storage, error) {
	absRootDir, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve data directory %q: %w", cfg.DataDir, err)
	}
	if err := os.MkdirAll(absRootDir, 0755); err != nil {
		return nil, fmt.Errorf("create data directory %q: %w", absRootDir, err)
	}
	return storage.NewStorage(storage.WithRootDir(absRootDir)), nil
}

// buildLFSStorage selects the LFS backend (S3 when configured, local otherwise)
// and mounts the S3 repositories filesystem when requested. The returned cleanup
// unmounts S3 and must be called on shutdown.
func buildLFSStorage(ctx context.Context, cfg *config, st *storage.Storage) (lfs.Storage, func(), error) {
	cleanup := func() {}

	if cfg.S3Endpoint == "" || cfg.S3Bucket == "" {
		return lfs.NewLocal(st.LFSDir()), cleanup, nil
	}

	if cfg.S3Repositories {
		repositoriesDir := st.RepositoriesDir()
		slog.InfoContext(ctx, "Mounting S3 bucket", "bucket", cfg.S3Bucket, "path", repositoriesDir)
		err := s3fs.Mount(
			context.Background(),
			repositoriesDir,
			cfg.S3Endpoint,
			cfg.S3AccessKey,
			cfg.S3SecretKey,
			cfg.S3Bucket,
			"/repositories/",
			cfg.S3UsePathStyle,
		)
		if err != nil {
			return nil, cleanup, fmt.Errorf("mount S3 bucket %q at %q: %w", cfg.S3Bucket, repositoriesDir, err)
		}
		cleanup = func() {
			slog.InfoContext(ctx, "Unmounting S3 bucket", "path", repositoriesDir)
			if err := s3fs.Unmount(context.Background(), repositoriesDir); err != nil {
				slog.ErrorContext(ctx, "Error unmounting S3 bucket", "path", repositoriesDir, "error", err)
			}
		}
	}

	lfsStorage := lfs.NewS3(
		"lfs",
		cfg.S3Endpoint,
		cfg.S3AccessKey,
		cfg.S3SecretKey,
		cfg.S3Bucket,
		cfg.S3UsePathStyle,
		cfg.S3SignEndpoint,
	)
	return lfsStorage, cleanup, nil
}

// buildMirror creates the shared mirror when pull or push mirroring is
// configured, or returns nil otherwise.
func buildMirror(ctx context.Context, cfg *config, st *storage.Storage, hooks *serverHooks, lfsStorage lfs.Storage) *mirror.Mirror {
	if cfg.PullMirrorURL == "" && cfg.PushMirrorURL == "" {
		return nil
	}

	opts := []mirror.Option{
		mirror.WithPreReceiveHookFunc(hooks.preReceive),
		mirror.WithPostReceiveHookFunc(hooks.postReceive),
		mirror.WithLFSStorage(lfsStorage),
		mirror.WithPullXET(cfg.PullMirrorXET),
		mirror.WithPushXET(cfg.PushMirrorXET),
		mirror.WithXETIdleEvictMaxBytes(cfg.ProxyXETEvictMaxBytes),
		mirror.WithXETIdleEvictBeforeFunc(func() time.Time {
			if cfg.ProxyXETEvictBefore < 0 {
				return time.Time{}
			}
			return time.Now().Add(-cfg.ProxyXETEvictBefore)
		}),
		mirror.WithConcurrency(cfg.ProxyConcurrencyPerFile),
		mirror.WithCacheDir(st.TmpDir()),
		mirror.WithGitOutputFunc(hooks.gitOutput),
		mirror.WithSyncUserInfoFunc(hooks.syncUserInfo),
		mirror.WithTTL(cfg.ProxyCacheTTL),
		mirror.WithMirrorRefFilterFunc(hooks.mirrorRefFilter),
	}

	if cfg.PullMirrorURL != "" {
		slog.InfoContext(ctx, "Pull mirror mode enabled", "source", cfg.PullMirrorURL)
		baseURL := strings.TrimSuffix(cfg.PullMirrorURL, "/")
		opts = append(opts, mirror.WithMirrorSourceFunc(
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
func buildHTTPHandler(st *storage.Storage, hooks *serverHooks, sharedMirror *mirror.Mirror, lfsStorage lfs.Storage, auth *authenticators) http.Handler {
	var handler http.Handler

	handler = backendhf.NewHandler(
		backendhf.WithStorage(st),
		backendhf.WithNext(handler),
		backendhf.WithMirror(sharedMirror),
		backendhf.WithPreOpenHookFunc(hooks.preOpen),
		backendhf.WithPermissionHookFunc(hooks.permission),
		backendhf.WithPreReceiveHookFunc(hooks.preReceive),
		backendhf.WithPostReceiveHookFunc(hooks.postReceive),
		backendhf.WithLFSStorage(lfsStorage),
	)

	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(st),
		backendlfs.WithNext(handler),
		backendlfs.WithMirror(sharedMirror),
		backendlfs.WithPermissionHookFunc(hooks.permission),
		backendlfs.WithTokenSignValidator(auth.tokenSign),
		backendlfs.WithLFSStorage(lfsStorage),
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

	handler = authenticate.AnonymousAuthenticateHandler(handler)
	handler = authenticate.TokenValidatorHandler(auth.token, handler)
	handler = authenticate.TokenSignValidatorHandler(auth.tokenSign, handler)
	handler = authenticate.BasicAuthHandler(auth.basicAuth, handler)

	return handler
}

// loadOrGenerateHostKey loads the SSH host key from the configured path, or
// generates and saves one under the data directory when absent.
func loadOrGenerateHostKey(ctx context.Context, cfg *config, st *storage.Storage) (pkgssh.Signer, error) {
	hostKeyPath := cfg.SSHHostKeyFile
	if hostKeyPath == "" {
		hostKeyPath = filepath.Join(st.RootDir(), "ssh_host_rsa_key")
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
