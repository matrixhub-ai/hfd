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
	repo, ok := h.openRepoDirect(w, storageName)
	if !ok {
		return
	}

	if err := repo.Remove(); err != nil {
		responseJSON(w, fmt.Errorf("failed to delete repository %q: %v", storageName, err), http.StatusInternalServerError)
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

	repo, ok := h.openRepoDirect(w, fromName)
	if !ok {
		return
	}

	toPath := repository.ResolvePath(toName)
	if toPath == "" {
		responseJSON(w, fmt.Errorf("invalid destination repository: %q", req.ToRepo), http.StatusBadRequest)
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
	if _, ok := h.openRepoDirect(w, ri.RepoName); !ok {
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
