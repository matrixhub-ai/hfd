package server

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// Hooks is the default hook set: mirror pull before opens (at most once per PullTTL per repository), logging on receive, push-mirroring after receive.
type Hooks struct {
	Mirror     *mirror.Mirror
	ProxyToken string
	PullTTL    time.Duration
	lastPull   sync.Map
}

// PreOpen syncs mirror sources before the repository is opened, at most once per PullTTL per repository.
func (h *Hooks) PreOpen(ctx context.Context, repoName string, write bool) error {
	if h.Mirror == nil {
		return nil
	}
	repoPath := repository.ResolvePath(repoName)
	if repoPath == "" {
		slog.WarnContext(ctx, "Cannot resolve repo path for push mirror", "repo", repoName)
		return nil
	}

	isMirror, err := h.Mirror.IsMirrorSource(ctx, repoName)
	if err != nil {
		return err
	}
	if !isMirror {
		return nil
	}

	if h.PullTTL > 0 {
		if last, ok := h.lastPull.Load(repoPath); ok && time.Since(last.(time.Time)) < h.PullTTL {
			return nil
		}
	}
	if err := h.Mirror.PullFromRemote(context.Background(), repoPath, repoName, nil); err != nil {
		return err
	}
	if h.PullTTL > 0 {
		h.lastPull.Store(repoPath, time.Now())
	}
	return nil
}

// PreReceive logs and allows every ref update.
func (h *Hooks) PreReceive(ctx context.Context, repoName string, updates []receive.RefUpdate) (bool, error) {
	for _, e := range updates {
		slog.InfoContext(ctx, "Pre-receive hook", "user", authenticate.IdentityFrom(ctx).Name(), "repo", repoName, "event", e.String(),
			"ref", e.RefName(), "old", e.OldRev(), "new", e.NewRev())
	}
	return true, nil
}

// PostReceive logs ref updates and pushes branch/tag changes to the push mirror.
func (h *Hooks) PostReceive(ctx context.Context, repoName string, updates []receive.RefUpdate) error {
	for _, e := range updates {
		slog.InfoContext(ctx, "Post-receive hook", "user", authenticate.IdentityFrom(ctx).Name(), "repo", repoName, "event", e.String(),
			"ref", e.RefName(), "old", e.OldRev(), "new", e.NewRev())
	}

	if h.Mirror == nil {
		return nil
	}

	repoPath := repository.ResolvePath(repoName)
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

	return h.Mirror.PushToRemote(context.Background(), repoPath, repoName, nil)
}

// GitOutput provides the writer for mirror git command output.
func (h *Hooks) GitOutput(ctx context.Context, repoName string) io.Writer {
	slog.InfoContext(ctx, "Git command output", "user", authenticate.IdentityFrom(ctx).Name(), "repo", repoName)
	return os.Stdout
}

// SyncUserInfo supplies credentials for mirror syncs from the proxy token.
func (h *Hooks) SyncUserInfo(ctx context.Context, repoName string) (*url.Userinfo, error) {
	slog.InfoContext(ctx, "Get sync user info", "user", authenticate.IdentityFrom(ctx).Name(), "repo", repoName)
	if h.ProxyToken != "" {
		return url.UserPassword("git", h.ProxyToken), nil
	}
	return nil, nil
}

// MirrorRefFilter restricts mirror syncs to branches and tags.
func (h *Hooks) MirrorRefFilter(ctx context.Context, repoName string, remoteRefs []string) ([]string, error) {
	var filtered []string
	for _, ref := range remoteRefs {
		if strings.HasPrefix(ref, "refs/heads/") || strings.HasPrefix(ref, "refs/tags/") {
			filtered = append(filtered, ref)
		}
	}
	slog.InfoContext(ctx, "Mirror ref filter", "repo", repoName, "remoteRefs", remoteRefs, "filteredRefs", filtered)
	return filtered, nil
}
