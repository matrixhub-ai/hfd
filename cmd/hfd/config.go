package main

import (
	"flag"
	"fmt"
	"net"
	"time"
)

// config groups all command line flags for the hfd server.
type config struct {
	// HTTP
	Addr    string
	HostURL string

	// SSH
	SSHAddr          string
	SSHHostKeyFile   string
	SSHAuthorizedKey string

	// Storage
	DataDir string

	// S3
	S3Endpoint     string
	S3SignEndpoint string
	S3AccessKey    string
	S3SecretKey    string
	S3Bucket       string
	S3UsePathStyle bool

	// Authentication
	AuthUsername string
	AuthPassword string
	AuthToken    string
	AuthSignKey  string

	// Mirror / proxy
	Proxy                   string
	ProxyToken              string
	PullMirrorURL           string
	PushMirrorURL           string
	ProxyCacheTTL           time.Duration
	ProxyConcurrencyPerFile int
	ProxyCacheSize          int64

	// Internal management
	Internal bool
}

func defaultConfig() *config {
	return &config{
		Addr:                    ":8080",
		SSHAddr:                 ":2222",
		DataDir:                 "./data",
		AuthUsername:            "admin",
		AuthSignKey:             "secret-sign-key",
		ProxyCacheTTL:           time.Minute,
		ProxyConcurrencyPerFile: 2,
		ProxyCacheSize:          10 * 1024 * 1024 * 1024, // 10 GB
	}
}

// parseConfig registers all flags, parses the command line, and applies
// derived defaults (host URL inference and proxy fallbacks).
func parseConfig() (*config, error) {
	cfg := defaultConfig()

	flag.StringVar(&cfg.Addr, "addr", cfg.Addr, "HTTP server address")
	flag.StringVar(&cfg.SSHAddr, "ssh-addr", cfg.SSHAddr, "SSH protocol server address")
	flag.StringVar(&cfg.SSHHostKeyFile, "ssh-host-key", cfg.SSHHostKeyFile, "Path to SSH host key file (PEM format); if empty, a key is generated")
	flag.StringVar(&cfg.DataDir, "data", cfg.DataDir, "Directory containing git repositories")
	flag.StringVar(&cfg.S3Endpoint, "s3-endpoint", cfg.S3Endpoint, "S3 endpoint")
	flag.StringVar(&cfg.S3SignEndpoint, "s3-sign-endpoint", cfg.S3SignEndpoint, "S3 signing endpoint (if different from s3-endpoint)")
	flag.StringVar(&cfg.S3AccessKey, "s3-access-key", cfg.S3AccessKey, "S3 access key")
	flag.StringVar(&cfg.S3SecretKey, "s3-secret-key", cfg.S3SecretKey, "S3 secret key")
	flag.StringVar(&cfg.S3Bucket, "s3-bucket", cfg.S3Bucket, "S3 bucket name")
	flag.BoolVar(&cfg.S3UsePathStyle, "s3-use-path-style", cfg.S3UsePathStyle, "Use path style for S3 URLs")

	flag.StringVar(&cfg.SSHAuthorizedKey, "ssh-authorized-key", cfg.SSHAuthorizedKey, "Path to SSH authorized_keys file for public key authentication")
	flag.StringVar(&cfg.AuthUsername, "username", cfg.AuthUsername, "Username for authentication (HTTP basic auth and SSH password auth)")
	flag.StringVar(&cfg.AuthPassword, "password", cfg.AuthPassword, "Password for authentication (HTTP basic auth, bearer token, and SSH password auth)")
	flag.StringVar(&cfg.AuthToken, "token", cfg.AuthToken, "Static token for authentication (alternative to username/password)")
	flag.StringVar(&cfg.AuthSignKey, "sign-key", cfg.AuthSignKey, "Key for signing authentication tokens (enables token signing)")

	flag.StringVar(&cfg.HostURL, "host-url", cfg.HostURL, "External URL for the server (e.g. http://localhost:8080); if not set, it is inferred from the listen address")

	flag.StringVar(&cfg.Proxy, "proxy", cfg.Proxy, "Proxy URL for fetching repositories from a remote during pull-mirror syncs (e.g. https://huggingface.co)")
	flag.StringVar(&cfg.ProxyToken, "proxy-token", cfg.ProxyToken, "Static token for authenticating to the pull mirror proxy")
	flag.StringVar(&cfg.PullMirrorURL, "pull-mirror", cfg.PullMirrorURL, "Pull mirror source base URL for syncing repositories from a remote (e.g. https://huggingface.co)")
	flag.StringVar(&cfg.PushMirrorURL, "push-mirror", cfg.PushMirrorURL, "Push mirror destination base URL for syncing local pushes to a remote (e.g. https://huggingface.co)")
	flag.DurationVar(&cfg.ProxyCacheTTL, "proxy-cache-ttl", cfg.ProxyCacheTTL, "Duration to cache proxy-fetched repositories locally")
	flag.IntVar(&cfg.ProxyConcurrencyPerFile, "proxy-concurrency-per-file", cfg.ProxyConcurrencyPerFile, "Number of concurrent fetches per file when syncing from proxy")
	flag.Int64Var(&cfg.ProxyCacheSize, "proxy-cache-size", cfg.ProxyCacheSize, "Maximum size in bytes of the content chunk cache used for proxy transfers")

	flag.BoolVar(&cfg.Internal, "internal", cfg.Internal, "Enable unauthenticated management endpoints under /internal/ (file listing, unlink, GC sweep, repository-aware GC via POST /internal/gc); expose only on trusted networks")
	flag.Parse()

	if cfg.HostURL == "" {
		host, port, err := net.SplitHostPort(cfg.Addr)
		if err != nil {
			return nil, fmt.Errorf("invalid address format: %w", err)
		}
		if host == "" {
			host = "localhost"
		}
		cfg.HostURL = fmt.Sprintf("http://%s:%s", host, port)
	}

	if cfg.Proxy != "" {
		if cfg.PullMirrorURL == "" {
			cfg.PullMirrorURL = cfg.Proxy
		}
		if cfg.PushMirrorURL == "" {
			cfg.PushMirrorURL = cfg.Proxy
		}
	}

	return cfg, nil
}
