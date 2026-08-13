package main

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// serverHooks bundles the request hooks shared by all backends.
// The mirror field is set after construction because the mirror itself is
// built with these hooks.
type serverHooks struct {
	storage    *storage.Storage
	proxyToken string
	mirror     *mirror.Mirror
}

// permission logs and allows every operation.
func (h *serverHooks) permission(ctx context.Context, op permission.Operation, repoName string, opCtx permission.Context) (bool, error) {
	userInfo, _ := authenticate.GetUserInfo(ctx)
	slog.InfoContext(ctx, "Permission check", "user", userInfo.User, "op", op, "repo", repoName, "context", opCtx)
	return true, nil // or return false, nil to deny, or return an error to indicate an error
}

// preOpen syncs mirror sources from the remote before the repository is opened.
func (h *serverHooks) preOpen(ctx context.Context, repoName string, write bool) error {
	if h.mirror == nil {
		return nil
	}
	repoPath := h.storage.ResolvePath(repoName)
	if repoPath == "" {
		slog.WarnContext(ctx, "Cannot resolve repo path for push mirror", "repo", repoName)
		return nil
	}

	isMirror, err := h.mirror.IsMirrorSource(ctx, repoName)
	if err != nil {
		return err
	}
	if !isMirror {
		return nil
	}

	return h.mirror.PullFromRemote(context.Background(), repoPath, repoName, nil)
}

// preReceive logs and allows every ref update.
func (h *serverHooks) preReceive(ctx context.Context, repoName string, updates []receive.RefUpdate) (bool, error) {
	userInfo, _ := authenticate.GetUserInfo(ctx)
	for _, e := range updates {
		slog.InfoContext(ctx, "Pre-receive hook", "user", userInfo.User, "repo", repoName, "event", e.String(),
			"ref", e.RefName(), "old", e.OldRev(), "new", e.NewRev())
	}
	return true, nil // or return false, nil to deny, or return an error to indicate an error
}

// postReceive logs ref updates and pushes branch/tag changes to the push mirror.
func (h *serverHooks) postReceive(ctx context.Context, repoName string, updates []receive.RefUpdate) error {
	userInfo, _ := authenticate.GetUserInfo(ctx)
	for _, e := range updates {
		slog.InfoContext(ctx, "Post-receive hook", "user", userInfo.User, "repo", repoName, "event", e.String(),
			"ref", e.RefName(), "old", e.OldRev(), "new", e.NewRev())
	}

	if h.mirror == nil {
		return nil
	}

	repoPath := h.storage.ResolvePath(repoName)
	if repoPath == "" {
		slog.WarnContext(ctx, "Cannot resolve repo path for push mirror", "repo", repoName)
		return nil
	}

	shouldPush := false
	for _, u := range updates {
		if strings.HasPrefix(u.RefName(), "refs/heads/") || strings.HasPrefix(u.RefName(), "refs/tags/") {
			shouldPush = true
			break
		}
	}
	if !shouldPush {
		slog.InfoContext(ctx, "Skip push mirror for non-branch/tag refs", "repo", repoName)
		return nil
	}

	return h.mirror.PushToRemote(context.Background(), repoPath, repoName, nil)
}

// gitOutput provides the writer for mirror git command output.
func (h *serverHooks) gitOutput(ctx context.Context, repoName string) io.Writer {
	userInfo, _ := authenticate.GetUserInfo(ctx)
	slog.InfoContext(ctx, "Git command output", "user", userInfo.User, "repo", repoName)
	return os.Stdout
}

// syncUserInfo supplies credentials for mirror syncs from the proxy token.
func (h *serverHooks) syncUserInfo(ctx context.Context, repoName string) (*url.Userinfo, error) {
	userInfo, _ := authenticate.GetUserInfo(ctx)
	slog.InfoContext(ctx, "Get sync user info", "user", userInfo.User, "repo", repoName)
	if h.proxyToken != "" {
		return url.UserPassword("git", h.proxyToken), nil
	}
	return nil, nil
}

// mirrorRefFilter restricts mirror syncs to branches and tags.
func (h *serverHooks) mirrorRefFilter(ctx context.Context, repoName string, remoteRefs []string) ([]string, error) {
	var filtered []string
	for _, ref := range remoteRefs {
		if strings.HasPrefix(ref, "refs/heads/") || strings.HasPrefix(ref, "refs/tags/") {
			filtered = append(filtered, ref)
		}
	}
	slog.InfoContext(ctx, "Mirror ref filter", "repo", repoName, "remoteRefs", remoteRefs, "filteredRefs", filtered)
	return filtered, nil
}
