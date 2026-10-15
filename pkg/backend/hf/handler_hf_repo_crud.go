package hf

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"

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

// isRepoID reports whether s is a clean namespace/name with both parts non-empty.
func isRepoID(s string) bool {
	namespace, name, ok := strings.Cut(s, "/")
	return ok && namespace != "" && name != "" && !strings.Contains(name, "/") && path.Clean(s) == s
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

	if !h.checkPermission(w, r, permission.OperationMoveRepo, fromName, permission.Context{DestRepo: toName}) {
		return
	}
	if !isRepoID(req.FromRepo) || repository.ResolvePath(fromName) == "" {
		respondOpenRepoError(w, fromName, repository.ErrRepositoryNotExists)
		return
	}
	if !isRepoID(req.ToRepo) || repository.ResolvePath(toName) == "" {
		responseJSON(w, fmt.Errorf("invalid destination repository: %q", req.ToRepo), http.StatusBadRequest)
		return
	}
	if err := h.preOpenHook(r.Context(), fromName, false); err != nil {
		respondOpenRepoError(w, fromName, err)
		return
	}

	if err := h.moveRepoFunc(r.Context(), fromName, toName); err != nil {
		respondCatalogError(w, err)
		return
	}

	responseJSON(w, moveRepoResponse{URL: fmt.Sprintf("%s/%s", requestOrigin(r), toName)}, http.StatusOK)
}

// handleDuplicateRepo handles POST /api/{repoType}/{namespace}/{repo}/duplicate
func (h *Handler) handleDuplicateRepo(w http.ResponseWriter, r *http.Request) {
	ri := getRepoInformation(r)

	var req DuplicateRepoRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responseJSON(w, fmt.Errorf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	toName := req.Repository
	if ri.RepoType != "models" {
		toName = ri.RepoType + "/" + req.Repository
	}

	if !h.checkPermission(w, r, permission.OperationDuplicateRepo, ri.RepoName, permission.Context{DestRepo: toName}) {
		return
	}
	if repository.ResolvePath(ri.RepoName) == "" {
		respondOpenRepoError(w, ri.RepoName, repository.ErrRepositoryNotExists)
		return
	}
	if !isRepoID(req.Repository) || repository.ResolvePath(toName) == "" {
		responseJSON(w, fmt.Errorf("invalid destination repository: %q", req.Repository), http.StatusBadRequest)
		return
	}
	if err := h.preOpenHook(r.Context(), ri.RepoName, false); err != nil {
		respondOpenRepoError(w, ri.RepoName, err)
		return
	}

	err := h.duplicateRepoFunc(r.Context(), ri.RepoName, toName, req)
	repoURL := requestOrigin(r) + "/" + toName
	if errors.Is(err, repository.ErrRepositoryAlreadyExists) {
		// huggingface_hub's exist_ok path reads url from the 409 body.
		responseJSON(w, duplicateRepoResponse{Error: err.Error(), URL: repoURL}, http.StatusConflict)
		return
	}
	if err != nil {
		respondCatalogError(w, err)
		return
	}

	// LFS and xet content is shared by OID, so no file copy is ever pending.
	responseJSON(w, duplicateRepoResponse{URL: repoURL}, http.StatusOK)
}

// handleDuplicateStatus handles GET /api/{repoType}/{namespace}/{repo}/duplicate/status
func (h *Handler) handleDuplicateStatus(w http.ResponseWriter, r *http.Request) {
	ri := getRepoInformation(r)

	if !h.checkPermission(w, r, permission.OperationReadRepo, ri.RepoName, permission.Context{}) {
		return
	}
	if _, ok := h.openRepoChecked(w, r, ri.RepoName, false); !ok {
		return
	}

	responseJSON(w, duplicateStatusResponse{}, http.StatusOK)
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
