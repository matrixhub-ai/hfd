package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/matrixhub-ai/hfd/internal/server"
	backendssh "github.com/matrixhub-ai/hfd/pkg/backend/ssh"
	"github.com/matrixhub-ai/hfd/pkg/gc"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	xetauth "github.com/wzshiming/xet/auth"
)

func main() {
	ctx := context.Background()

	cfg, err := parseConfig()
	if err != nil {
		slog.ErrorContext(ctx, "Invalid configuration", "error", err)
		os.Exit(1)
	}

	if err := run(ctx, cfg); err != nil {
		slog.ErrorContext(ctx, "hfd exited", "error", err)
		os.Exit(1)
	}
}

// run assembles all components in dependency order and serves until a listener fails.
func run(ctx context.Context, cfg *config) error {
	// Phase 1: storage layer.
	st, err := buildStorage(ctx, cfg)
	if err != nil {
		return fmt.Errorf("prepare storage: %w", err)
	}

	slog.InfoContext(ctx, "Starting hfd server", "addr", cfg.Addr, "data", cfg.DataDir)

	// Phase 2: auth layer.
	hooks := &server.Hooks{ProxyToken: cfg.ProxyToken, PullTTL: cfg.ProxyCacheTTL}
	auth, err := buildAuthenticators(ctx, cfg)
	if err != nil {
		return fmt.Errorf("prepare authenticators: %w", err)
	}
	issuer, err := xetauth.NewIssuer([]byte(cfg.AuthSignKey), time.Hour, nil)
	if err != nil {
		return fmt.Errorf("prepare XET issuer: %w", err)
	}

	// Phase 3: xet/mirror layer.
	xetC, err := buildXETClient(cfg, st)
	if err != nil {
		return fmt.Errorf("prepare XET client: %w", err)
	}
	engine, err := buildXETMirror(cfg, st, xetC)
	if err != nil {
		return fmt.Errorf("prepare XET mirror engine: %w", err)
	}
	// The mirror is built with the hooks and the hooks call back into the mirror.
	sharedMirror, err := buildMirror(ctx, cfg, st, hooks, xetC, engine, issuer.Sign)
	if err != nil {
		return fmt.Errorf("prepare mirror: %w", err)
	}
	hooks.Mirror = sharedMirror
	// Integrators may assemble e.g. permission.SplitReadWrite(permission.AllowAll(), permission.RequireAuthenticated()).
	policy := permission.Logged(permission.PullMirrorReadOnly(sharedMirror))

	// Phase 4: frontends.
	opts := server.Options{
		Storage:        st,
		Mirror:         sharedMirror,
		Authenticators: auth,
		CASAuthorizer:  issuer,
		Permission:     policy,
		PreOpen:        hooks.PreOpen,
		PreReceive:     hooks.PreReceive,
		PostReceive:    hooks.PostReceive,
		AccessLog:      os.Stderr,
		HostURL:        cfg.HostURL,
	}
	if cfg.Internal {
		slog.WarnContext(ctx, "Internal management API enabled; /internal/ endpoints are unauthenticated")
		opts.InternalGC = gc.NewCollector(st.RepositoriesFS(), st.XETStorage())
		opts.GCGrace = time.Hour
	}
	handler := server.NewHTTPHandler(opts)
	var sshServer *backendssh.Server
	if cfg.SSHAddr != "" {
		hostKeySigner, err := loadOrGenerateHostKey(ctx, cfg, st)
		if err != nil {
			return fmt.Errorf("prepare SSH server: %w", err)
		}
		sshServer = server.NewSSHServer(opts, hostKeySigner)
	}

	return serve(ctx, cfg, handler, sshServer)
}

// serve starts the optional SSH listener and the HTTP listener, returning the first failure.
func serve(ctx context.Context, cfg *config, handler http.Handler, sshServer *backendssh.Server) error {
	errCh := make(chan error, 2)
	if sshServer != nil {
		slog.InfoContext(ctx, "Starting SSH server", "addr", cfg.SSHAddr)
		go func() {
			errCh <- fmt.Errorf("SSH server: %w", sshServer.ListenAndServe(ctx, cfg.SSHAddr))
		}()
	}
	go func() {
		errCh <- fmt.Errorf("HTTP server: %w", http.ListenAndServe(cfg.Addr, handler))
	}()
	return <-errCh
}
