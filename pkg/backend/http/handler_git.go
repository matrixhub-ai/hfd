package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"

	"github.com/gorilla/mux"

	"github.com/wzshiming/hfd/pkg/permission"
	"github.com/wzshiming/hfd/pkg/receive"
	"github.com/wzshiming/hfd/pkg/repository"
)

// gitProtocolEnv returns a GIT_PROTOCOL environment variable derived from the
// request's Git-Protocol header if the value is present and valid, or nil otherwise.
func gitProtocolEnv(r *http.Request) []string {
	value := r.Header.Get("Git-Protocol")
	if value == "" || !repository.IsValidGitProtocol(value) {
		return nil
	}
	return []string{"GIT_PROTOCOL=" + value}
}

// handleInfoRefs handles the /info/refs endpoint for git service discovery.
func (h *Handler) handleInfoRefs(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repoName := vars["repo"]

	service := r.URL.Query().Get("service")
	if service == "" {
		responseText(w, "service parameter is required", http.StatusBadRequest)
		return
	}

	if service != repository.GitUploadPack && service != repository.GitReceivePack {
		responseText(w, "unsupported service", http.StatusForbidden)
		return
	}

	if h.permissionHook != nil {
		op := permission.OperationReadRepo
		if service == repository.GitReceivePack {
			op = permission.OperationUpdateRepo
		}
		if err := h.permissionHook(r.Context(), op, repoName, permission.Context{}); err != nil {
			responseText(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), repoName)
	if repoPath == "" {
		responseText(w, fmt.Sprintf("repository %q not found", repoName), http.StatusNotFound)
		return
	}

	repo, err := h.openRepo(r.Context(), repoPath, repoName, service)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			if sourceURL := h.tryGitProxy(r.Context(), repoPath, repoName, service); sourceURL != "" {
				if proxyErr := h.gitTeeCache.ProxyInfoRefs(w, r, sourceURL); proxyErr != nil {
					responseText(w, fmt.Sprintf("Failed to proxy info refs for %q: %v", repoName, proxyErr), http.StatusBadGateway)
				}
				return
			}
			responseText(w, fmt.Sprintf("repository %q not found", repoName), http.StatusNotFound)
			return
		}
		responseText(w, fmt.Sprintf("Failed to open repository %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}
	if service == repository.GitReceivePack {
		isMirror, _, err := repo.IsMirror()
		if err != nil {
			responseText(w, fmt.Sprintf("Failed to check repository type for %q: %v", repoName, err), http.StatusInternalServerError)
			return
		}
		if isMirror {
			responseText(w, fmt.Sprintf("push to mirror repository %q is not allowed", repoName), http.StatusForbidden)
			return
		}
	}

	w.Header().Set("Content-Type", fmt.Sprintf("application/x-%s-advertisement", service))
	w.Header().Set("Cache-Control", "no-cache")

	err = repo.Stateless(r.Context(), w, nil, service, true, gitProtocolEnv(r)...)
	if err != nil {
		responseText(w, fmt.Sprintf("Failed to get info refs for %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}
}

// handleUploadPack handles the git-upload-pack endpoint (fetch/clone).
func (h *Handler) handleUploadPack(w http.ResponseWriter, r *http.Request) {
	h.handleService(w, r, repository.GitUploadPack)
}

// handleReceivePack handles the git-receive-pack endpoint (push).
func (h *Handler) handleReceivePack(w http.ResponseWriter, r *http.Request) {
	h.handleService(w, r, repository.GitReceivePack)
}

// handleService handles a git service request.
func (h *Handler) handleService(w http.ResponseWriter, r *http.Request, service string) {
	vars := mux.Vars(r)
	repoName := vars["repo"]

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), repoName)
	if repoPath == "" {
		responseText(w, fmt.Sprintf("repository %q not found", repoName), http.StatusNotFound)
		return
	}

	// For receive-pack, parse ref updates early so they can be included in the permission check
	var input io.Reader = r.Body
	var updates []receive.RefUpdate
	if service == repository.GitReceivePack {
		updates, input = receive.ParseRefUpdates(r.Body)
	}

	if h.permissionHook != nil {
		op := permission.OperationReadRepo
		if service == repository.GitReceivePack {
			op = permission.OperationUpdateRepo
		}
		if err := h.permissionHook(r.Context(), op, repoName, permission.Context{}); err != nil {
			responseText(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	// Pre-receive hook — can reject the push before git-receive-pack processes it.
	if service == repository.GitReceivePack && h.preReceiveHook != nil && len(updates) > 0 {
		if err := h.preReceiveHook(r.Context(), repoName, updates); err != nil {
			responseText(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	repo, err := h.openRepo(r.Context(), repoPath, repoName, service)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			if sourceURL := h.tryGitProxy(r.Context(), repoPath, repoName, service); sourceURL != "" {
				if proxyErr := h.gitTeeCache.ProxyService(w, r, sourceURL, service); proxyErr != nil {
					responseText(w, fmt.Sprintf("Failed to proxy %s for %q: %v", service, repoName, proxyErr), http.StatusBadGateway)
				}
				return
			}
			responseText(w, fmt.Sprintf("repository %q not found", repoName), http.StatusNotFound)
			return
		}
		responseText(w, fmt.Sprintf("Failed to open repository %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}
	if service == repository.GitReceivePack {
		isMirror, _, err := repo.IsMirror()
		if err != nil {
			responseText(w, fmt.Sprintf("Failed to check repository type for %q: %v", repoName, err), http.StatusInternalServerError)
			return
		}
		if isMirror {
			responseText(w, fmt.Sprintf("push to mirror repository %q is not allowed", repoName), http.StatusForbidden)
			return
		}
	}

	w.Header().Set("Content-Type", fmt.Sprintf("application/x-%s-result", service))
	w.Header().Set("Cache-Control", "no-cache")

	err = repo.Stateless(r.Context(), w, input, service, false, gitProtocolEnv(r)...)
	if err != nil {
		responseText(w, fmt.Sprintf("Failed to get info refs for %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}

	if service == repository.GitReceivePack && h.postReceiveHook != nil && len(updates) > 0 {
		if hookErr := h.postReceiveHook(r.Context(), repoName, updates); hookErr != nil {
			slog.Warn("post-receive hook error", "repo", repoName, "error", hookErr)
		}
	}
}

// openRepo opens a repository, optionally creating a mirror from the proxy source
// if the repository doesn't exist locally and proxy mode is enabled.
// Proxy is only used for read operations (git-upload-pack).
func (h *Handler) openRepo(ctx context.Context, repoPath, repoName, service string) (*repository.Repository, error) {
	// If a mirror init is in flight via tee cache, skip opening the partially-initialized repo.
	if h.gitTeeCache != nil && h.gitTeeCache.IsInFlight(repoPath) {
		return nil, repository.ErrRepositoryNotExists
	}

	repo, err := repository.Open(repoPath)
	if err == nil {
		if mirror, _, err := repo.IsMirror(); err == nil && mirror {
			err = h.syncMirrorWithHook(ctx, repo, repoName)
			if err != nil {
				return nil, fmt.Errorf("failed to sync mirror: %w", err)
			}
		}
		return repo, nil
	}
	// Only proxy for read operations
	if service != repository.GitUploadPack {
		return nil, err
	}
	if err == repository.ErrRepositoryNotExists && h.mirrorSourceFunc != nil {
		// When git tee cache is available, skip blocking mirror init.
		// The handler will proxy the request and start mirror init in the background.
		if h.gitTeeCache != nil {
			return nil, repository.ErrRepositoryNotExists
		}

		if h.permissionHook != nil {
			if err := h.permissionHook(ctx, permission.OperationCreateProxyRepo, repoName, permission.Context{}); err != nil {
				return nil, err
			}
		}
		sourceURL, err := h.mirrorSourceFunc(ctx, repoPath, repoName)
		if err != nil {
			return nil, err
		}
		repo, err := repository.InitMirror(ctx, repoPath, sourceURL)
		if err != nil {
			_ = os.RemoveAll(repoPath)
			return nil, repository.ErrRepositoryNotExists
		}
		err = h.syncMirrorWithHook(ctx, repo, repoName)
		if err != nil {
			return nil, fmt.Errorf("failed to sync mirror: %w", err)
		}
		return repo, nil
	}
	return nil, err
}

// tryGitProxy checks if a request should be proxied via the git tee cache.
// Returns the source URL if proxying should proceed, or empty string if not.
// Also starts the background mirror initialization if not already in progress.
func (h *Handler) tryGitProxy(ctx context.Context, repoPath, repoName, service string) string {
	if h.gitTeeCache == nil || h.mirrorSourceFunc == nil {
		return ""
	}
	if service != repository.GitUploadPack {
		return ""
	}
	if h.permissionHook != nil {
		if err := h.permissionHook(ctx, permission.OperationCreateProxyRepo, repoName, permission.Context{}); err != nil {
			return ""
		}
	}
	sourceURL, err := h.mirrorSourceFunc(ctx, repoPath, repoName)
	if err != nil || sourceURL == "" {
		return ""
	}
	h.gitTeeCache.StartInitMirror(repoPath, sourceURL)
	return sourceURL
}

// syncMirrorWithHook syncs a mirror and fires post-receive hooks for any ref changes.
func (h *Handler) syncMirrorWithHook(ctx context.Context, repo *repository.Repository, repoName string) error {
	var before map[string]string
	if h.postReceiveHook != nil {
		before, _ = repo.Refs()
	}

	if err := repo.SyncMirror(ctx); err != nil {
		return fmt.Errorf("failed to sync mirror: %w", err)
	}

	if h.postReceiveHook != nil {
		after, _ := repo.Refs()
		updates := receive.DiffRefs(before, after)
		if len(updates) > 0 {
			if err := h.postReceiveHook(ctx, repoName, updates); err != nil {
				return fmt.Errorf("post-receive hook error: %w", err)
			}
		}
	}
	return nil
}
