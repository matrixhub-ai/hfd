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

	if h.permissionHookFunc != nil {
		op := permission.OperationReadRepo
		if service == repository.GitReceivePack {
			op = permission.OperationUpdateRepo
		}
		if err := h.permissionHookFunc(r.Context(), op, repoName, permission.Context{}); err != nil {
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
	if service == repository.GitReceivePack && h.mirrorSourceFunc != nil {
		if _, isMirror, err := h.mirrorSourceFunc(r.Context(), repoName); err == nil && isMirror {
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
		updates, input = receive.ParseRefUpdates(r.Body, repoPath)
	}

	if h.permissionHookFunc != nil {
		op := permission.OperationReadRepo
		if service == repository.GitReceivePack {
			op = permission.OperationUpdateRepo
		}
		if err := h.permissionHookFunc(r.Context(), op, repoName, permission.Context{}); err != nil {
			responseText(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	// Pre-receive hook — can reject the push before git-receive-pack processes it.
	if service == repository.GitReceivePack && h.preReceiveHookFunc != nil && len(updates) > 0 {
		if err := h.preReceiveHookFunc(r.Context(), repoName, updates); err != nil {
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
	if service == repository.GitReceivePack && h.mirrorSourceFunc != nil {
		if _, isMirror, err := h.mirrorSourceFunc(r.Context(), repoName); err == nil && isMirror {
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

	if service == repository.GitReceivePack && h.postReceiveHookFunc != nil && len(updates) > 0 {
		if hookErr := h.postReceiveHookFunc(r.Context(), repoName, updates); hookErr != nil {
			slog.WarnContext(r.Context(), "post-receive hook error", "repo", repoName, "error", hookErr)
		}
	}
}

// openRepo opens a repository, optionally creating a mirror from the proxy source
// if the repository doesn't exist locally and proxy mode is enabled.
// Proxy is only used for read operations (git-upload-pack).
func (h *Handler) openRepo(ctx context.Context, repoPath, repoName, service string) (*repository.Repository, error) {
	if h.mirrorSourceFunc == nil || service != repository.GitUploadPack {
		return repository.Open(repoPath)
	}
	repo, err := repository.Open(repoPath)
	if err != nil {
		if err != repository.ErrRepositoryNotExists {
			return nil, err
		}
		if h.permissionHookFunc != nil {
			if err := h.permissionHookFunc(ctx, permission.OperationCreateProxyRepo, repoName, permission.Context{}); err != nil {
				return nil, err
			}
		}
		sourceURL, isMirror, err := h.mirrorSourceFunc(ctx, repoName)
		if err != nil {
			return nil, err
		}
		if !isMirror {
			return nil, repository.ErrRepositoryNotExists
		}
		repo, err = repository.InitMirror(ctx, repoPath, sourceURL)
		if err != nil {
			return nil, repository.ErrRepositoryNotExists
		}
		err = h.syncMirror(ctx, repo, repoName, sourceURL)
		if err != nil {
			return nil, fmt.Errorf("failed to sync mirror: %w", err)
		}
	} else {
		sourceURL, isMirror, err := h.mirrorSourceFunc(ctx, repoName)
		if err != nil {
			slog.WarnContext(ctx, "mirrorSourceFunc error", "repo", repoName, "error", err)
			return repo, nil
		}
		if !isMirror {
			return repo, nil
		}
		err = h.syncMirror(ctx, repo, repoName, sourceURL)
		if err != nil {
			return nil, fmt.Errorf("failed to sync mirror: %w", err)
		}
	}
	return repo, nil
}

func removeKeyFromMap(m map[string]string, keys []string) map[string]string {
	if m == nil {
		return nil
	}
	result := make(map[string]string)
	for _, key := range keys {
		result[key] = m[key]
	}
	return result
}

// syncMirror syncs a mirror and fires post-receive hooks for any ref changes.
func (h *Handler) syncMirror(ctx context.Context, repo *repository.Repository, repoName string, sourceURL string) error {
	remoteRefs, err := repo.ListRemoteRefs(ctx, sourceURL)
	if err != nil {
		return fmt.Errorf("failed to list remote refs: %w", err)
	}

	if h.mirrorRefFilterFunc != nil {
		remoteRefs, err = h.mirrorRefFilterFunc(ctx, repoName, remoteRefs)
		if err != nil {
			return fmt.Errorf("failed to filter mirror refs: %w", err)
		}
	}
	if len(remoteRefs) == 0 {
		return nil
	}

	var before map[string]string
	if h.postReceiveHookFunc != nil || h.preReceiveHookFunc != nil {
		before, _ = repo.Refs()
	}

	before = removeKeyFromMap(before, remoteRefs)

	if h.preReceiveHookFunc != nil {
		var updates []receive.RefUpdate
		for _, target := range remoteRefs {
			oldRev, ok := before[target]
			if oldRev == "" || !ok {
				oldRev = receive.ZeroHash
			}
			updates = append(updates, receive.NewRefUpdate(oldRev, receive.BreakHash, target, repo.RepoPath()))
		}
		if err := h.preReceiveHookFunc(ctx, repoName, updates); err != nil {
			return fmt.Errorf("pre-receive hook error: %w", err)
		}
	}

	if err := repo.SyncMirrorRefs(ctx, sourceURL, remoteRefs); err != nil {
		return fmt.Errorf("failed to sync mirror refs: %w", err)
	}

	if h.postReceiveHookFunc != nil {
		after, _ := repo.Refs()
		after = removeKeyFromMap(after, remoteRefs)
		updates := receive.DiffRefs(before, after, repo.RepoPath())
		if len(updates) > 0 {
			if err := h.postReceiveHookFunc(ctx, repoName, updates); err != nil {
				return fmt.Errorf("post-receive hook error: %w", err)
			}
		}
	}
	return nil
}
