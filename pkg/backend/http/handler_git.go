package backend

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// gitProtocol returns the client's GIT_PROTOCOL request value derived from the
// request's Git-Protocol header if the value is present and valid, or "" otherwise.
func gitProtocol(r *http.Request) string {
	value := r.Header.Get("Git-Protocol")
	if value == "" || !repository.IsValidGitProtocol(value) {
		return ""
	}
	return value
}

// requestBody returns the request body, transparently inflating it when the
// client sent Content-Encoding: gzip. Git clients gzip-compress smart-HTTP
// request bodies larger than 1KiB (remote-curl.c post_rpc), and git
// http-backend inflates them the same way (http-backend.c inflate_request).
func requestBody(r *http.Request) (io.ReadCloser, error) {
	if r.Header.Get("Content-Encoding") != "gzip" {
		return r.Body, nil
	}
	return gzip.NewReader(r.Body)
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

	if h.mirror != nil {
		switch service {
		case repository.GitUploadPack:
			isMirrorSrc, err := h.mirror.IsMirrorSource(r.Context(), repoName)
			if err != nil {
				responseText(w, fmt.Sprintf("Failed to check mirror status: %v", err), http.StatusInternalServerError)
				return
			}
			if !isMirrorSrc {
				responseText(w, "pull from mirror repository is not allowed", http.StatusForbidden)
				return
			}
		case repository.GitReceivePack:
			isMirrorDest, err := h.mirror.IsMirrorDestination(r.Context(), repoName)
			if err != nil {
				responseText(w, fmt.Sprintf("Failed to check mirror destination status: %v", err), http.StatusInternalServerError)
				return
			}
			if !isMirrorDest {
				responseText(w, "push to mirror destination repository is not allowed", http.StatusForbidden)
				return
			}
		}
	}

	if h.permissionHookFunc != nil {
		op := permission.OperationReadRepo
		if service == repository.GitReceivePack {
			op = permission.OperationUpdateRepo
		}
		if ok, err := h.permissionHookFunc(r.Context(), op, repoName, permission.Context{}); err != nil {
			responseText(w, err.Error(), http.StatusInternalServerError)
			return
		} else if !ok {
			responseText(w, "permission denied", http.StatusForbidden)
			return
		}
	}

	repoPath := h.storage.ResolvePath(repoName)
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

	w.Header().Set("Content-Type", fmt.Sprintf("application/x-%s-advertisement", service))
	w.Header().Set("Cache-Control", "no-cache")

	err = repo.AdvertiseRefs(r.Context(), w, service, gitProtocol(r))
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

	repoPath := h.storage.ResolvePath(repoName)
	if repoPath == "" {
		responseText(w, fmt.Sprintf("repository %q not found", repoName), http.StatusNotFound)
		return
	}

	if h.mirror != nil {
		switch service {
		case repository.GitUploadPack:
			isMirrorSrc, err := h.mirror.IsMirrorSource(r.Context(), repoName)
			if err != nil {
				responseText(w, fmt.Sprintf("Failed to check mirror status: %v", err), http.StatusInternalServerError)
				return
			}
			if !isMirrorSrc {
				responseText(w, "pull from mirror repository is not allowed", http.StatusForbidden)
				return
			}
		case repository.GitReceivePack:
			isMirrorDest, err := h.mirror.IsMirrorDestination(r.Context(), repoName)
			if err != nil {
				responseText(w, fmt.Sprintf("Failed to check mirror destination status: %v", err), http.StatusInternalServerError)
				return
			}
			if !isMirrorDest {
				responseText(w, "push to mirror destination repository is not allowed", http.StatusForbidden)
				return
			}
		}
	}

	if h.permissionHookFunc != nil {
		op := permission.OperationReadRepo
		if service == repository.GitReceivePack {
			op = permission.OperationUpdateRepo
		}
		if ok, err := h.permissionHookFunc(r.Context(), op, repoName, permission.Context{}); err != nil {
			responseText(w, err.Error(), http.StatusInternalServerError)
			return
		} else if !ok {
			responseText(w, "permission denied", http.StatusForbidden)
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

	body, err := requestBody(r)
	if err != nil {
		responseText(w, fmt.Sprintf("Failed to read request body: %v", err), http.StatusBadRequest)
		return
	}
	defer func() {
		_ = body.Close()
	}()

	w.Header().Set("Content-Type", fmt.Sprintf("application/x-%s-result", service))
	w.Header().Set("Cache-Control", "no-cache")

	err = repo.Stateless(r.Context(), w, body, service, gitProtocol(r), h.receivePackHooks(repoName))
	if err != nil {
		// A pre-receive rejection has already been reported to the client
		// in-protocol via report-status.
		if errors.Is(err, errPreReceiveDenied) {
			slog.WarnContext(r.Context(), "pre-receive hook denied push", "repo", repoName, "error", err)
			return
		}
		slog.ErrorContext(r.Context(), "failed to serve git service", "service", service, "repo", repoName, "error", err)
		responseText(w, fmt.Sprintf("Failed to serve %s for %q: %v", service, repoName, err), http.StatusInternalServerError)
		return
	}
}

// errPreReceiveDenied marks a push rejected by the pre-receive hook; the
// rejection is reported to the client in-protocol via report-status.
var errPreReceiveDenied = errors.New("pre-receive hook denied the push")

// receivePackHooks wires the handler's pre/post-receive hook functions into
// the go-git receive-pack serving path.
func (h *Handler) receivePackHooks(repoName string) repository.ReceivePackHooks {
	var hooks repository.ReceivePackHooks
	if h.preReceiveHookFunc != nil {
		hooks.PreReceive = func(ctx context.Context, updates []receive.RefUpdate) error {
			if len(updates) == 0 {
				return nil
			}
			if ok, err := h.preReceiveHookFunc(ctx, repoName, updates); err != nil {
				return fmt.Errorf("%w: %v", errPreReceiveDenied, err)
			} else if !ok {
				return errPreReceiveDenied
			}
			return nil
		}
	}
	hooks.PostReceive = func(ctx context.Context, updates []receive.RefUpdate) {
		h.afterReceivePack(ctx, repoName, updates)
	}
	return hooks
}

func (h *Handler) afterReceivePack(ctx context.Context, repoName string, updates []receive.RefUpdate) {
	if len(updates) == 0 {
		return
	}

	if h.postReceiveHookFunc != nil {
		if hookErr := h.postReceiveHookFunc(ctx, repoName, updates); hookErr != nil {
			slog.WarnContext(ctx, "post-receive hook error", "repo", repoName, "error", hookErr)
		}
	}
}

func (h *Handler) openRepo(ctx context.Context, repoPath, repoName, service string) (*repository.Repository, error) {
	if err := h.preOpenHook(ctx, repoName, service == repository.GitReceivePack); err != nil {
		return nil, err
	}
	return repository.Open(repoPath)
}

func (h *Handler) preOpenHook(ctx context.Context, repoName string, write bool) error {
	if h.preOpenHookFunc == nil {
		return nil
	}
	return h.preOpenHookFunc(ctx, repoName, write)
}
