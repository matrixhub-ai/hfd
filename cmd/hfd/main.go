package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/gorilla/handlers"

	"github.com/matrixhub-ai/hfd/pkg/mirror"
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

	xs, err := buildXETStorage(ctx, cfg)
	if err != nil {
		slog.ErrorContext(ctx, "Error preparing XET storage", "error", err)
		os.Exit(1)
	}

	slog.InfoContext(ctx, "Starting hfd server", "addr", cfg.Addr, "data", cfg.DataDir)

	hooks := &serverHooks{
		storage:    st,
		proxyToken: cfg.ProxyToken,
	}
	auth, err := buildAuthenticators(ctx, cfg)
	if err != nil {
		slog.ErrorContext(ctx, "Error preparing authenticators", "error", err)
		os.Exit(1)
	}

	mint, authFn, err := mirror.NewXETTokenScheme(auth.TokenSign)
	if err != nil {
		slog.ErrorContext(ctx, "Error preparing XET token scheme", "error", err)
		os.Exit(1)
	}

	xetC, err := buildXETClient(cfg)
	if err != nil {
		slog.ErrorContext(ctx, "Error preparing XET client", "error", err)
		os.Exit(1)
	}

	mirrorHandler, err := buildXETMirrorHandler(cfg, xs, xetC, mint)
	if err != nil {
		slog.ErrorContext(ctx, "Error preparing XET mirror handler", "error", err)
		os.Exit(1)
	}

	// The mirror is built with the hooks and the hooks call back into the mirror.
	sharedMirror, err := buildMirror(ctx, cfg, st, xs, hooks, mint, xetC, mirrorHandler)
	if err != nil {
		slog.ErrorContext(ctx, "Error preparing mirror", "error", err)
		os.Exit(1)
	}
	hooks.mirror = sharedMirror

	xetComposition := buildXETComposition(xs, authFn, mirrorHandler)
	handler := buildHTTPHandler(st, hooks, sharedMirror, auth, authFn, xetComposition)

	if cfg.SSHAddr != "" {
		sshServer, err := buildSSHServer(ctx, cfg, st, hooks, sharedMirror, auth)
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
