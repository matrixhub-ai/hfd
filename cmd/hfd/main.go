package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gorilla/handlers"
	"github.com/wzshiming/hfd/internal/utils"
	"github.com/wzshiming/hfd/pkg/authenticate"
	backendhttp "github.com/wzshiming/hfd/pkg/backend/http"
	backendhuggingface "github.com/wzshiming/hfd/pkg/backend/huggingface"
	backendlfs "github.com/wzshiming/hfd/pkg/backend/lfs"
	backendssh "github.com/wzshiming/hfd/pkg/backend/ssh"
	"github.com/wzshiming/hfd/pkg/lfs"
	"github.com/wzshiming/hfd/pkg/permission"
	"github.com/wzshiming/hfd/pkg/receive"
	"github.com/wzshiming/hfd/pkg/repository"
	"github.com/wzshiming/hfd/pkg/s3fs"
	pkgssh "github.com/wzshiming/hfd/pkg/ssh"
	"github.com/wzshiming/hfd/pkg/storage"
)

var (
	addr           = ":8080"
	sshAddr        = ":2222"
	sshHostKeyFile = ""
	dataDir        = "./data"
	s3Repositories = false
	s3SignEndpoint = ""
	s3Endpoint     = ""
	s3AccessKey    = ""
	s3SecretKey    = ""
	s3Bucket       = ""
	s3UsePathStyle = false

	// Authentication flags
	sshAuthorizedKey = ""
	authUsername     = "admin"
	authPassword     = ""
	authToken        = ""
	authSignKey      = "secret-sign-key"

	proxyURL = ""
	lfsURL   = ""
)

func init() {
	flag.StringVar(&addr, "addr", addr, "HTTP server address")
	flag.StringVar(&sshAddr, "ssh-addr", sshAddr, "SSH protocol server address")
	flag.StringVar(&sshHostKeyFile, "ssh-host-key", sshHostKeyFile, "Path to SSH host key file (PEM format); if empty, a key is generated")
	flag.StringVar(&dataDir, "data", dataDir, "Directory containing git repositories")
	flag.BoolVar(&s3Repositories, "s3-repositories", s3Repositories, "Store repositories in S3")
	flag.StringVar(&s3Endpoint, "s3-endpoint", s3Endpoint, "S3 endpoint")
	flag.StringVar(&s3SignEndpoint, "s3-sign-endpoint", s3SignEndpoint, "S3 signing endpoint (if different from s3-endpoint)")
	flag.StringVar(&s3AccessKey, "s3-access-key", s3AccessKey, "S3 access key")
	flag.StringVar(&s3SecretKey, "s3-secret-key", s3SecretKey, "S3 secret key")
	flag.StringVar(&s3Bucket, "s3-bucket", s3Bucket, "S3 bucket name")
	flag.BoolVar(&s3UsePathStyle, "s3-use-path-style", s3UsePathStyle, "Use path style for S3 URLs")

	flag.StringVar(&sshAuthorizedKey, "ssh-authorized-key", sshAuthorizedKey, "Path to SSH authorized_keys file for public key authentication")
	flag.StringVar(&authUsername, "username", authUsername, "Username for authentication (HTTP basic auth and SSH password auth)")
	flag.StringVar(&authPassword, "password", authPassword, "Password for authentication (HTTP basic auth, bearer token, and SSH password auth)")
	flag.StringVar(&authToken, "token", authToken, "Static token for authentication (alternative to username/password)")
	flag.StringVar(&authSignKey, "sign-key", authSignKey, "Key for signing authentication tokens (enables token signing)")

	flag.StringVar(&proxyURL, "proxy", proxyURL, "Proxy source URL for fetching repositories that don't exist locally (e.g. https://huggingface.co)")
	flag.StringVar(&lfsURL, "lfs-url", lfsURL, "External LFS URL for the server, used by git-lfs-authenticate over SSH (e.g. http://localhost:8080)")

	flag.Parse()

	if lfsURL == "" {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid address format: %v\n", err)
			os.Exit(1)
		}
		if host == "" {
			host = "localhost"
		}
		lfsURL = fmt.Sprintf("http://%s:%s", host, port)
	}
}

func main() {
	ctx := context.Background()
	absRootDir, err := filepath.Abs(dataDir)
	if err != nil {
		slog.ErrorContext(ctx, "Error getting absolute path of repo directory", "path", dataDir, "error", err)
		os.Exit(1)
	}

	storage := storage.NewStorage(
		storage.WithRootDir(absRootDir),
	)

	slog.InfoContext(ctx, "Starting hfd server", "addr", addr, "data", absRootDir)

	var lfsStore = lfs.NewLocal(storage.LFSDir())
	if s3Endpoint != "" && s3Bucket != "" {
		if s3Repositories {
			repositoriesDir := filepath.Join(absRootDir, "repositories")
			slog.InfoContext(ctx, "Mounting S3 bucket", "bucket", s3Bucket, "path", repositoriesDir)
			err := s3fs.Mount(
				context.Background(),
				repositoriesDir,
				s3Endpoint,
				s3AccessKey,
				s3SecretKey,
				s3Bucket,
				"/repositories/",
				s3UsePathStyle,
			)
			if err != nil {
				slog.ErrorContext(ctx, "Error mounting S3 bucket", "bucket", s3Bucket, "path", repositoriesDir, "error", err)
				os.Exit(1)
			}
			defer func() {
				slog.InfoContext(ctx, "Unmounting S3 bucket", "path", repositoriesDir)
				if err := s3fs.Unmount(context.Background(), repositoriesDir); err != nil {
					slog.ErrorContext(ctx, "Error unmounting S3 bucket", "path", repositoriesDir, "error", err)
				}
			}()
		}

		lfsStore = lfs.NewS3(
			"lfs",
			s3Endpoint,
			s3AccessKey,
			s3SecretKey,
			s3Bucket,
			s3UsePathStyle,
			s3SignEndpoint,
		)
	}

	var mirrorSourceFunc repository.MirrorSourceFunc
	var lfsTeeCache *lfs.TeeCache
	if proxyURL != "" {
		slog.InfoContext(ctx, "Proxy mode enabled", "source", proxyURL)
		mirrorSourceFunc = repository.NewMirrorSourceFunc(proxyURL)
		lfsTeeCache = lfs.NewTeeCache(
			utils.HTTPClient,
			lfsStore,
		)
	}

	permissionHook := func(ctx context.Context, op permission.Operation, repoName string, opCtx permission.Context) error {
		userInfo, _ := authenticate.GetUserInfo(ctx)
		slog.InfoContext(ctx, "Permission check", "user", userInfo.User, "op", op, "repo", repoName, "context", opCtx)
		return nil // or return an error to deny permission
	}

	preReceiveHook := func(ctx context.Context, repoName string, updates []receive.RefUpdate) error {
		userInfo, _ := authenticate.GetUserInfo(ctx)
		for _, e := range updates {
			slog.InfoContext(ctx, "Pre-receive hook", "user", userInfo.User, "repo", repoName, "event", e.String(),
				"ref", e.RefName, "old", e.OldRev, "new", e.NewRev)
		}
		return nil // or return an error to deny the push
	}

	postReceiveHook := func(ctx context.Context, repoName string, updates []receive.RefUpdate) error {
		userInfo, _ := authenticate.GetUserInfo(ctx)
		for _, e := range updates {
			slog.InfoContext(ctx, "Post-receive hook", "user", userInfo.User, "repo", repoName, "event", e.String(),
				"ref", e.RefName, "old", e.OldRev, "new", e.NewRev)
		}
		return nil
	}

	mirrorRefFilterFunc := func(ctx context.Context, repoName string, remoteRefs []string) ([]string, error) {
		var filtered []string
		for _, ref := range remoteRefs {
			if strings.HasPrefix(ref, "refs/heads/") || strings.HasPrefix(ref, "refs/tags/") {
				filtered = append(filtered, ref)
			}
		}
		slog.InfoContext(ctx, "Mirror ref filter", "repo", repoName, "remoteRefs", remoteRefs, "filteredRefs", filtered)
		return filtered, nil
	}

	var basicAuthValidator authenticate.BasicAuthValidator
	var tokenValidator authenticate.TokenValidator
	var publicKeyValidator authenticate.PublicKeyValidator
	var tokenSignValidator authenticate.TokenSignValidator
	if authPassword != "" {
		basicAuthValidator = authenticate.NewSimpleBasicAuthValidator(authUsername, authPassword)
	}
	if authToken != "" {
		tokenValidator = authenticate.NewSimpleTokenValidator(authUsername, authToken)
	}
	if authSignKey != "" {
		tokenSignValidator = authenticate.NewTokenSignValidator([]byte(authSignKey))
	}
	if sshAuthorizedKey != "" {
		var authorizedKeys [][]byte
		authKeysData, err := os.ReadFile(sshAuthorizedKey)
		if err != nil {
			slog.ErrorContext(ctx, "Error reading SSH authorized keys file", "path", sshAuthorizedKey, "error", err)
			os.Exit(1)
		}
		parsedKeys, err := pkgssh.ParseAuthorizedKeys(authKeysData)
		if err != nil {
			slog.ErrorContext(ctx, "Error parsing SSH authorized keys", "path", sshAuthorizedKey, "error", err)
			os.Exit(1)
		}
		for _, k := range parsedKeys {
			authorizedKeys = append(authorizedKeys, k.Marshal())
		}
		slog.InfoContext(ctx, "Loaded SSH authorized keys", "count", len(parsedKeys))
		publicKeyValidator = authenticate.NewSimplePublicKeyValidator(authorizedKeys)
	}

	var handler http.Handler

	handler = backendhuggingface.NewHandler(
		backendhuggingface.WithStorage(storage),
		backendhuggingface.WithNext(handler),
		backendhuggingface.WithMirrorSourceFunc(mirrorSourceFunc),
		backendhuggingface.WithLFSTeeCache(lfsTeeCache),
		backendhuggingface.WithPermissionHookFunc(permissionHook),
		backendhuggingface.WithPreReceiveHookFunc(preReceiveHook),
		backendhuggingface.WithPostReceiveHookFunc(postReceiveHook),
		backendhuggingface.WithLFSStore(lfsStore),
		backendhuggingface.WithMirrorRefFilterFunc(mirrorRefFilterFunc),
	)

	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(storage),
		backendlfs.WithNext(handler),
		backendlfs.WithLFSTeeCache(lfsTeeCache),
		backendlfs.WithPermissionHookFunc(permissionHook),
		backendlfs.WithTokenSignValidator(tokenSignValidator),
		backendlfs.WithLFSStore(lfsStore),
		backendlfs.WithMirrorSourceFunc(mirrorSourceFunc),
	)

	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(storage),
		backendhttp.WithNext(handler),
		backendhttp.WithMirrorSourceFunc(mirrorSourceFunc),
		backendhttp.WithPermissionHookFunc(permissionHook),
		backendhttp.WithPreReceiveHookFunc(preReceiveHook),
		backendhttp.WithPostReceiveHookFunc(postReceiveHook),
		backendhttp.WithMirrorRefFilterFunc(mirrorRefFilterFunc),
	)

	handler = authenticate.AnonymousAuthenticateHandler(handler)
	handler = authenticate.TokenValidatorHandler(tokenValidator, handler)
	handler = authenticate.TokenSignValidatorHandler(tokenSignValidator, handler)
	handler = authenticate.BasicAuthHandler(basicAuthValidator, handler)

	if sshAddr != "" {
		var hostKeySigner pkgssh.Signer
		hostKeyPath := sshHostKeyFile
		if hostKeyPath == "" {
			hostKeyPath = filepath.Join(absRootDir, "ssh_host_rsa_key")
		}
		data, err := os.ReadFile(hostKeyPath)
		if err == nil {
			hostKeySigner, err = pkgssh.ParseHostKeyFile(data)
			if err != nil {
				slog.ErrorContext(ctx, "Error parsing SSH host key file", "path", hostKeyPath, "error", err)
				os.Exit(1)
			}
			slog.InfoContext(ctx, "Loaded SSH host key", "path", hostKeyPath)
		} else if sshHostKeyFile != "" || !os.IsNotExist(err) {
			slog.ErrorContext(ctx, "Error reading SSH host key file", "path", hostKeyPath, "error", err)
			os.Exit(1)
		} else {
			hostKeySigner, err = pkgssh.GenerateAndSaveHostKey(hostKeyPath, pkgssh.KeyTypeRSA)
			if err != nil {
				slog.ErrorContext(ctx, "Error generating SSH host key", "path", hostKeyPath, "error", err)
				os.Exit(1)
			}
			slog.InfoContext(ctx, "Generated SSH host key", "path", hostKeyPath)
		}
		sshOpts := []backendssh.Option{
			backendssh.WithPermissionHookFunc(permissionHook),
			backendssh.WithPreReceiveHookFunc(preReceiveHook),
			backendssh.WithPostReceiveHookFunc(postReceiveHook),
			backendssh.WithMirrorSourceFunc(mirrorSourceFunc),
			backendssh.WithLFSURL(lfsURL),
			backendssh.WithBasicAuthValidator(basicAuthValidator),
			backendssh.WithPublicKeyValidator(publicKeyValidator),
			backendssh.WithTokenSignValidator(tokenSignValidator),
			backendssh.WithMirrorRefFilterFunc(mirrorRefFilterFunc),
		}

		sshServer := backendssh.NewServer(storage.RepositoriesDir(), hostKeySigner, sshOpts...)
		slog.InfoContext(ctx, "Starting SSH protocol server", "addr", sshAddr)
		go func() {
			if err := sshServer.ListenAndServe(ctx, sshAddr); err != nil {
				slog.ErrorContext(ctx, "Error starting SSH protocol server", "addr", sshAddr, "error", err)
				os.Exit(1)
			}
		}()
	}

	handler = handlers.CombinedLoggingHandler(os.Stderr, handler)
	if err := http.ListenAndServe(addr, handler); err != nil {
		slog.ErrorContext(ctx, "Error starting server", "error", err)
		os.Exit(1)
	}
}
