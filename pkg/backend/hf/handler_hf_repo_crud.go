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
	if repository.ResolvePath(storageName) == "" {
		respondOpenRepoError(w, storageName, repository.ErrRepositoryNotExists)
		return
	}

	if err := h.deleteRepoFunc(r.Context(), storageName); err != nil {
		respondCatalogError(w, err)
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
	if repository.ResolvePath(fromName) == "" {
		respondOpenRepoError(w, fromName, repository.ErrRepositoryNotExists)
		return
	}
	if repository.ResolvePath(toName) == "" {
		responseJSON(w, fmt.Errorf("invalid destination repository: %q", req.ToRepo), http.StatusBadRequest)
		return
	}

	if err := h.moveRepoFunc(r.Context(), fromName, toName); err != nil {
		respondCatalogError(w, err)
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
	if repository.ResolvePath(ri.RepoName) == "" {
		respondOpenRepoError(w, ri.RepoName, repository.ErrRepositoryNotExists)
		return
	}

	var req RepoSettings
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responseJSON(w, fmt.Errorf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if err := h.updateRepoSettingsFunc(r.Context(), ri.RepoName, req); err != nil {
		respondCatalogError(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}
