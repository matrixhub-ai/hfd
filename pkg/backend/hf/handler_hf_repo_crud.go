package hf

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// handleDeleteRepo handles DELETE /api/repos/delete
func (h *Handler) handleDeleteRepo(w http.ResponseWriter, r *http.Request) {
	var req deleteRepoRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responseJSON(w, fmt.Errorf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	repoName := req.Name
	if req.Organization != "" {
		repoName = req.Organization + "/" + repoName
	}

	prefix := repoTypePrefix(req.Type)
	storageName := repoName
	if prefix != "" {
		storageName = prefix + "/" + repoName
	}

	if !h.checkPermission(w, r, permission.OperationDeleteRepo, storageName, permission.Context{}) {
		return
	}
	repoPath, ok := h.resolveRepoPath(w, storageName, repoName)
	if !ok {
		return
	}
	repo, ok := h.openRepoDirect(w, repoPath, repoName)
	if !ok {
		return
	}

	if err := repo.Remove(); err != nil {
		responseJSON(w, fmt.Errorf("failed to delete repository %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleMoveRepo handles POST /api/repos/move
func (h *Handler) handleMoveRepo(w http.ResponseWriter, r *http.Request) {
	var req moveRepoRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responseJSON(w, fmt.Errorf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	prefix := repoTypePrefix(req.Type)

	fromName := req.FromRepo
	if prefix != "" {
		fromName = prefix + "/" + fromName
	}
	toName := req.ToRepo
	if prefix != "" {
		toName = prefix + "/" + toName
	}

	if !h.checkPermission(w, r, permission.OperationUpdateRepo, fromName, permission.Context{DestRepo: toName}) {
		return
	}

	fromPath := h.storage.ResolvePath(fromName)
	if fromPath == "" {
		responseJSON(w, fmt.Errorf("invalid source repository: %q", req.FromRepo), http.StatusBadRequest)
		return
	}

	toPath := h.storage.ResolvePath(toName)
	if toPath == "" {
		responseJSON(w, fmt.Errorf("invalid destination repository: %q", req.ToRepo), http.StatusBadRequest)
		return
	}

	repo, ok := h.openRepoDirect(w, fromPath, req.FromRepo)
	if !ok {
		return
	}

	// Check that destination doesn't already exist
	if repository.IsRepository(h.storage.RepositoriesFS(), toPath) {
		responseJSON(w, fmt.Errorf("destination repository %q already exists", req.ToRepo), http.StatusConflict)
		return
	}

	if err := repo.Move(toPath); err != nil {
		responseJSON(w, fmt.Errorf("failed to move repository: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleRepoSettings handles PUT /api/{repoType}/{repo}/settings
func (h *Handler) handleRepoSettings(w http.ResponseWriter, r *http.Request) {
	ri := getRepoInformation(r)

	if !h.checkPermission(w, r, permission.OperationUpdateRepo, ri.RepoName, permission.Context{}) {
		return
	}
	repoPath, ok := h.resolveRepoPath(w, ri.RepoName, ri.RepoName)
	if !ok {
		return
	}

	if !repository.IsRepository(h.storage.RepositoriesFS(), repoPath) {
		responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoName), http.StatusNotFound)
		return
	}

	// Accept the settings payload but don't enforce private/gated in this server
	var req repoSettingsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responseJSON(w, fmt.Errorf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}
