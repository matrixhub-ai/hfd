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
	switch r.Header.Get("Content-Encoding") {
	case "gzip", "x-gzip":
		return gzip.NewReader(r.Body)
	}
	return r.Body, nil
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

	if !h.checkMirrorAccess(w, r, repoName, service) {
		return
	}
	if !h.checkPermission(w, r, repoName, service) {
		return
	}

	repoPath := repository.ResolvePath(repoName)
	if repoPath == "" {
		responseText(w, fmt.Sprintf("repository %q not found", repoName), http.StatusNotFound)
		return
	}

	repo, ok := h.openRepoChecked(w, r, repoPath, repoName, service)
	if !ok {
		return
	}

	w.Header().Set("Content-Type", fmt.Sprintf("application/x-%s-advertisement", service))
	w.Header().Set("Cache-Control", "no-cache")

	if err := repo.AdvertiseRefs(r.Context(), w, service, gitProtocol(r)); err != nil {
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

	repoPath := repository.ResolvePath(repoName)
	if repoPath == "" {
		responseText(w, fmt.Sprintf("repository %q not found", repoName), http.StatusNotFound)
		return
	}

	if !h.checkMirrorAccess(w, r, repoName, service) {
		return
	}
	if !h.checkPermission(w, r, repoName, service) {
		return
	}

	repo, ok := h.openRepoChecked(w, r, repoPath, repoName, service)
	if !ok {
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
	return repository.Open(h.storage.RepositoriesFS(), repoPath)
}

// checkMirrorAccess enforces mirror-only access rules, writing the failure
// response. It returns true when the request may proceed.
func (h *Handler) checkMirrorAccess(w http.ResponseWriter, r *http.Request, repoName, service string) bool {
	if h.mirror == nil {
		return true
	}
	switch service {
	case repository.GitUploadPack:
		isMirrorSrc, err := h.mirror.IsMirrorSource(r.Context(), repoName)
		if err != nil {
			responseText(w, fmt.Sprintf("Failed to check mirror status: %v", err), http.StatusInternalServerError)
			return false
		}
		if !isMirrorSrc {
			responseText(w, "pull from mirror repository is not allowed", http.StatusForbidden)
			return false
		}
	case repository.GitReceivePack:
		isMirrorDest, err := h.mirror.IsMirrorDestination(r.Context(), repoName)
		if err != nil {
			responseText(w, fmt.Sprintf("Failed to check mirror destination status: %v", err), http.StatusInternalServerError)
			return false
		}
		if !isMirrorDest {
			responseText(w, "push to mirror destination repository is not allowed", http.StatusForbidden)
			return false
		}
	}
	return true
}

// checkPermission runs the permission hook for the service, writing the
// failure response. It returns true when the request may proceed.
func (h *Handler) checkPermission(w http.ResponseWriter, r *http.Request, repoName, service string) bool {
	op := permission.OperationReadRepo
	if service == repository.GitReceivePack {
		op = permission.OperationUpdateRepo
	}
	return permission.Guard{
		Hook:    h.permissionHookFunc,
		Respond: func(w http.ResponseWriter, msg string, sc int) { responseText(w, msg, sc) },
	}.Allow(w, r, op, repoName, permission.Context{})
}

// openRepoChecked opens the repository, mapping open errors to HTTP responses.
func (h *Handler) openRepoChecked(w http.ResponseWriter, r *http.Request, repoPath, repoName, service string) (*repository.Repository, bool) {
	repo, err := h.openRepo(r.Context(), repoPath, repoName, service)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseText(w, fmt.Sprintf("repository %q not found", repoName), http.StatusNotFound)
			return nil, false
		}
		responseText(w, fmt.Sprintf("Failed to open repository %q: %v", repoName, err), http.StatusInternalServerError)
		return nil, false
	}
	return repo, true
}

func (h *Handler) preOpenHook(ctx context.Context, repoName string, write bool) error {
	if h.preOpenHookFunc == nil {
		return nil
	}
	return h.preOpenHookFunc(ctx, repoName, write)
}
