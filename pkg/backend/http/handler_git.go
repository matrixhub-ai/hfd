package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/wzshiming/hfd/pkg/permission"
	"github.com/wzshiming/hfd/pkg/receive"
	"github.com/wzshiming/hfd/pkg/repository"
)

func (h *Handler) registryGit(r *mux.Router) {
	r.HandleFunc("/{repo:.+}.git/info/refs", h.handleInfoRefs).Methods(http.MethodGet)
	r.HandleFunc("/{repo:.+}/info/refs", h.handleInfoRefs).Methods(http.MethodGet)
	r.HandleFunc("/{repo:.+}.git/git-upload-pack", h.handleUploadPack).Methods(http.MethodPost)
	r.HandleFunc("/{repo:.+}/git-upload-pack", h.handleUploadPack).Methods(http.MethodPost)
	r.HandleFunc("/{repo:.+}.git/git-receive-pack", h.handleReceivePack).Methods(http.MethodPost)
	r.HandleFunc("/{repo:.+}/git-receive-pack", h.handleReceivePack).Methods(http.MethodPost)
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

	err = repo.Stateless(r.Context(), w, nil, service, true)
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

	err = repo.Stateless(r.Context(), w, input, service, false)
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
	repo, err := repository.Open(repoPath)
	if err == nil {
		if mirror, _, err := repo.IsMirror(); err == nil && mirror {
			h.syncMirrorWithHook(ctx, repo, repoPath, repoName)
		}
		return repo, nil
	}
	// Only proxy for read operations
	if service != repository.GitUploadPack {
		return nil, err
	}
	if err == repository.ErrRepositoryNotExists && h.proxyManager != nil {
		if h.permissionHook != nil {
			if err := h.permissionHook(ctx, permission.OperationCreateProxyRepo, repoName, permission.Context{}); err != nil {
				return nil, err
			}
		}
		repo, err := h.proxyManager.Init(ctx, repoPath, repoName)
		if err != nil {
			return nil, err
		}
		h.fireHookForNewMirror(ctx, repo, repoPath, repoName)
		return repo, nil
	}
	return nil, err
}

// syncMirrorWithHook syncs a mirror and fires post-receive hooks for any ref changes.
func (h *Handler) syncMirrorWithHook(ctx context.Context, repo *repository.Repository, repoPath, repoName string) {
	var before map[string]string
	if h.postReceiveHook != nil {
		before, _ = repo.Refs()
	}

	if err := repo.SyncMirror(ctx); err != nil {
		slog.Warn("failed to sync mirror", "repo", repoName, "error", err)
		return
	}

	if h.postReceiveHook != nil {
		after, _ := repo.Refs()
		updates := receive.DiffRefs(before, after)
		if len(updates) > 0 {
			if err := h.postReceiveHook(ctx, repoName, updates); err != nil {
				slog.Warn("post-receive hook error", "repo", repoName, "error", err)
			}
		}
	}
}

// fireHookForNewMirror fires post-receive hooks for all refs in a newly created mirror repo.
func (h *Handler) fireHookForNewMirror(ctx context.Context, repo *repository.Repository, repoPath, repoName string) {
	if h.postReceiveHook == nil {
		return
	}
	after, _ := repo.Refs()
	updates := receive.DiffRefs(nil, after)
	if len(updates) > 0 {
		if err := h.postReceiveHook(ctx, repoName, updates); err != nil {
			slog.Warn("post-receive hook error", "repo", repoName, "error", err)
		}
	}
}
