package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/gorilla/handlers"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	backendssh "github.com/matrixhub-ai/hfd/pkg/backend/ssh"
	"github.com/matrixhub-ai/hfd/pkg/permission"
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
	xs, err := buildXETStorage(ctx, cfg)
	if err != nil {
		return fmt.Errorf("prepare XET storage: %w", err)
	}

	slog.InfoContext(ctx, "Starting hfd server", "addr", cfg.Addr, "data", cfg.DataDir)

	// Phase 2: auth layer.
	// Integrators may assemble e.g. permission.SplitReadWrite(permission.AllowAll(), permission.RequireAuthenticated()).
	hooks := &serverHooks{
		storage:    st,
		proxyToken: cfg.ProxyToken,
		permission: permission.Logged(permission.AllowAll()),
	}
	auth, err := buildAuthenticators(ctx, cfg)
	if err != nil {
		return fmt.Errorf("prepare authenticators: %w", err)
	}
	mint, authFn, err := authenticate.NewXETTokenScheme(auth.TokenSign)
	if err != nil {
		return fmt.Errorf("prepare XET token scheme: %w", err)
	}

	// Phase 3: xet/mirror layer.
	xetC, err := buildXETClient(cfg)
	if err != nil {
		return fmt.Errorf("prepare XET client: %w", err)
	}
	engine, err := buildXETMirror(cfg, xs, xetC)
	if err != nil {
		return fmt.Errorf("prepare XET mirror engine: %w", err)
	}
	hubHandler := buildXETHubHandler(cfg, engine, mint)
	// The mirror is built with the hooks and the hooks call back into the mirror.
	sharedMirror, err := buildMirror(ctx, cfg, st, xs, hooks, xetC, engine, mint)
	if err != nil {
		return fmt.Errorf("prepare mirror: %w", err)
	}
	hooks.mirror = sharedMirror

	// Phase 4: frontends.
	// The backends serve from the mirror's data plane.
	xetComposition := buildXETComposition(xs, authFn, hubHandler)
	handler := buildHTTPHandler(st, hooks, sharedMirror, auth, authFn, xetComposition)
	handler = wrapInternalAPI(ctx, cfg, xs, handler)
	var sshServer *backendssh.Server
	if cfg.SSHAddr != "" {
		sshServer, err = buildSSHServer(ctx, cfg, st, hooks, sharedMirror, auth)
		if err != nil {
			return fmt.Errorf("prepare SSH server: %w", err)
		}
	}

	return serve(ctx, cfg, handler, sshServer)
}

// serve starts the optional SSH listener and the HTTP listener, returning the first failure.
func serve(ctx context.Context, cfg *config, handler http.Handler, sshServer *backendssh.Server) error {
	loggedHandler := handlers.CombinedLoggingHandler(os.Stderr, handler)

	errCh := make(chan error, 2)
	if sshServer != nil {
		slog.InfoContext(ctx, "Starting SSH server", "addr", cfg.SSHAddr)
		go func() {
			errCh <- fmt.Errorf("SSH server: %w", sshServer.ListenAndServe(ctx, cfg.SSHAddr))
		}()
	}
	go func() {
		errCh <- fmt.Errorf("HTTP server: %w", http.ListenAndServe(cfg.Addr, loggedHandler))
	}()
	return <-errCh
}
