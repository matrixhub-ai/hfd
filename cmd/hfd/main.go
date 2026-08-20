package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/gorilla/handlers"
)

func main() {
	ctx := context.Background()

	cfg, err := parseConfig()
	if err != nil {
		slog.ErrorContext(ctx, "Invalid configuration", "error", err)
		os.Exit(1)
	}

	st, err := buildStorage(ctx, cfg)
	if err != nil {
		slog.ErrorContext(ctx, "Error preparing storage", "error", err)
		os.Exit(1)
	}

	slog.InfoContext(ctx, "Starting hfd server", "addr", cfg.Addr, "data", cfg.DataDir)

	hooks := &serverHooks{
		storage:    st,
		proxyToken: cfg.ProxyToken,
	}
	// The mirror is built with the hooks and the hooks call back into the mirror.
	hooks.mirror, err = buildMirror(ctx, cfg, st, hooks)
	if err != nil {
		slog.ErrorContext(ctx, "Error preparing mirror", "error", err)
		os.Exit(1)
	}

	auth, err := buildAuthenticators(ctx, cfg)
	if err != nil {
		slog.ErrorContext(ctx, "Error preparing authenticators", "error", err)
		os.Exit(1)
	}

	handler := buildHTTPHandler(st, hooks, hooks.mirror, auth)

	if cfg.SSHAddr != "" {
		sshServer, err := buildSSHServer(ctx, cfg, st, hooks, hooks.mirror, auth)
		if err != nil {
			slog.ErrorContext(ctx, "Error preparing SSH protocol server", "error", err)
			os.Exit(1)
		}
		slog.InfoContext(ctx, "Starting SSH protocol server", "addr", cfg.SSHAddr)
		go func() {
			if err := sshServer.ListenAndServe(ctx, cfg.SSHAddr); err != nil {
				slog.ErrorContext(ctx, "Error starting SSH protocol server", "addr", cfg.SSHAddr, "error", err)
				os.Exit(1)
			}
		}()
	}

	handler = handlers.CombinedLoggingHandler(os.Stderr, handler)
	if err := http.ListenAndServe(cfg.Addr, handler); err != nil {
		slog.ErrorContext(ctx, "Error starting server", "error", err)
		os.Exit(1)
	}
}
