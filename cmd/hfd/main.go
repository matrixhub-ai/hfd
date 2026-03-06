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

	"github.com/gorilla/handlers"
	"github.com/wzshiming/hfd/internal/utils"
	"github.com/wzshiming/hfd/pkg/authenticate"
	backendhttp "github.com/wzshiming/hfd/pkg/backend/http"
	backendhuggingface "github.com/wzshiming/hfd/pkg/backend/huggingface"
	backendlfs "github.com/wzshiming/hfd/pkg/backend/lfs"
	backendssh "github.com/wzshiming/hfd/pkg/backend/ssh"
	"github.com/wzshiming/hfd/pkg/lfs"
	"github.com/wzshiming/hfd/pkg/permission"
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
	absRootDir, err := filepath.Abs(dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting absolute path of repo directory: %v\n", err)
		os.Exit(1)
	}

	storage := storage.NewStorage(
		storage.WithRootDir(absRootDir),
	)

	slog.Info("Starting hfd server", "addr", addr, "data", absRootDir)

	var lfsStore = lfs.NewLocal(storage.LFSDir())
	if s3Endpoint != "" && s3Bucket != "" {
		if s3Repositories {
			repositoriesDir := filepath.Join(absRootDir, "repositories")
			slog.Info("Mounting S3 bucket", "bucket", s3Bucket, "path", repositoriesDir)
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
				fmt.Fprintf(os.Stderr, "Error mounting S3 bucket: %v\n", err)
				os.Exit(1)
			}
			defer func() {
				slog.Info("Unmounting S3 bucket", "path", repositoriesDir)
				if err := s3fs.Unmount(context.Background(), repositoriesDir); err != nil {
					fmt.Fprintf(os.Stderr, "Error unmounting S3 bucket: %v\n", err)
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

	var proxyManager *repository.ProxyManager
	var lfsProxyManager *lfs.ProxyManager
	if proxyURL != "" {
		slog.Info("Proxy mode enabled", "source", proxyURL)
		proxyManager = repository.NewProxyManager(proxyURL)
		lfsProxyManager = lfs.NewProxyManager(
			utils.HTTPClient,
			lfsStore,
		)
	}

	permissionHook := func(ctx context.Context, op permission.Operation, repoPath string, opCtx permission.Context) error {
		user, _ := authenticate.GetUser(ctx)
		slog.Info("Permission check", "user", user, "op", op, "repo", repoPath, "context", opCtx)
		return nil // or return an error to deny permission
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
			fmt.Fprintf(os.Stderr, "Error reading SSH authorized keys file: %v\n", err)
			os.Exit(1)
		}
		parsedKeys, err := pkgssh.ParseAuthorizedKeys(authKeysData)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing SSH authorized keys: %v\n", err)
			os.Exit(1)
		}
		for _, k := range parsedKeys {
			authorizedKeys = append(authorizedKeys, k.Marshal())
		}
		slog.Info("Loaded SSH authorized keys", "count", len(parsedKeys))
		publicKeyValidator = authenticate.NewSimplePublicKeyValidator(authorizedKeys)
	}

	var handler http.Handler

	handler = backendhuggingface.NewHandler(
		backendhuggingface.WithStorage(storage),
		backendhuggingface.WithNext(handler),
		backendhuggingface.WithProxyManager(proxyManager),
		backendhuggingface.WithLFSProxyManager(lfsProxyManager),
		backendhuggingface.WithPermissionHookFunc(permissionHook),
		backendhuggingface.WithLFSStore(lfsStore),
	)

	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(storage),
		backendlfs.WithNext(handler),
		backendlfs.WithLFSProxyManager(lfsProxyManager),
		backendlfs.WithPermissionHookFunc(permissionHook),
		backendlfs.WithTokenSignValidator(tokenSignValidator),
		backendlfs.WithLFSStore(lfsStore),
	)

	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(storage),
		backendhttp.WithNext(handler),
		backendhttp.WithProxyManager(proxyManager),
		backendhttp.WithPermissionHookFunc(permissionHook),
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
				fmt.Fprintf(os.Stderr, "Error parsing SSH host key file: %v\n", err)
				os.Exit(1)
			}
			slog.Info("Loaded SSH host key", "path", hostKeyPath)
		} else if sshHostKeyFile != "" || !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "Error reading SSH host key file: %v\n", err)
			os.Exit(1)
		} else {
			hostKeySigner, err = pkgssh.GenerateAndSaveHostKey(hostKeyPath, pkgssh.KeyTypeRSA)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error generating SSH host key: %v\n", err)
				os.Exit(1)
			}
			slog.Info("Generated SSH host key", "path", hostKeyPath)
		}
		sshOpts := []backendssh.Option{
			backendssh.WithPermissionHookFunc(permissionHook),
			backendssh.WithProxyManager(proxyManager),
			backendssh.WithLFSURL(lfsURL),
			backendssh.WithBasicAuthValidator(basicAuthValidator),
			backendssh.WithPublicKeyValidator(publicKeyValidator),
			backendssh.WithTokenSignValidator(tokenSignValidator),
		}

		sshServer := backendssh.NewServer(storage.RepositoriesDir(), hostKeySigner, sshOpts...)
		slog.Info("Starting SSH protocol server", "addr", sshAddr)
		go func() {
			if err := sshServer.ListenAndServe(sshAddr); err != nil {
				fmt.Fprintf(os.Stderr, "Error starting SSH protocol server on %s: %v\n", sshAddr, err)
				os.Exit(1)
			}
		}()
	}

	handler = handlers.CombinedLoggingHandler(os.Stderr, handler)
	if err := http.ListenAndServe(addr, handler); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %v\n", err)
		os.Exit(1)
	}
}
