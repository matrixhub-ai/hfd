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

	st, err := buildStorage(cfg)
	if err != nil {
		slog.ErrorContext(ctx, "Error preparing storage", "error", err)
		os.Exit(1)
	}

	slog.InfoContext(ctx, "Starting hfd server", "addr", cfg.Addr, "data", st.RootDir())

	lfsStorage, cleanup, err := buildLFSStorage(ctx, cfg, st)
	if err != nil {
		slog.ErrorContext(ctx, "Error preparing LFS storage", "error", err)
		os.Exit(1)
	}
	defer cleanup()

	hooks := &serverHooks{
		storage:    st,
		proxyToken: cfg.ProxyToken,
	}
	// The mirror is built with the hooks and the hooks call back into the mirror.
	hooks.mirror = buildMirror(ctx, cfg, st, hooks, lfsStorage)

	auth, err := buildAuthenticators(ctx, cfg)
	if err != nil {
		slog.ErrorContext(ctx, "Error preparing authenticators", "error", err)
		os.Exit(1)
	}

	handler := buildHTTPHandler(st, hooks, hooks.mirror, lfsStorage, auth)

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
