package huggingface

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/gorilla/mux"

	"github.com/wzshiming/gitd/pkg/repository"
)

// HFDeleteRepoRequest represents the delete repo request body.
type HFDeleteRepoRequest struct {
	Type         string `json:"type"`
	Name         string `json:"name"`
	Organization string `json:"organization,omitempty"`
}

// HFMoveRepoRequest represents the move repo request body.
type HFMoveRepoRequest struct {
	FromRepo string `json:"fromRepo"`
	ToRepo   string `json:"toRepo"`
	Type     string `json:"type"`
}

// HFRepoSettingsRequest represents the repo settings update request body.
type HFRepoSettingsRequest struct {
	Private *bool `json:"private,omitempty"`
	Gated   any   `json:"gated,omitempty"`
}

// HFCreateBranchRequest represents the create branch request body.
type HFCreateBranchRequest struct {
	StartingPoint string `json:"startingPoint,omitempty"`
}

// HFCreateTagRequest represents the create tag request body.
type HFCreateTagRequest struct {
	Tag     string `json:"tag"`
	Message string `json:"message,omitempty"`
}

// HFGitRefInfo represents a single git ref (branch or tag).
type HFGitRefInfo struct {
	Name         string `json:"name"`
	Ref          string `json:"ref"`
	TargetCommit string `json:"targetCommit"`
}

// HFGitRefs represents the response for listing repo refs.
type HFGitRefs struct {
	Branches []HFGitRefInfo `json:"branches"`
	Converts []HFGitRefInfo `json:"converts"`
	Tags     []HFGitRefInfo `json:"tags"`
}

// handleHFDeleteRepo handles DELETE /api/repos/delete
func (h *Handler) handleHFDeleteRepo(w http.ResponseWriter, r *http.Request) {
	var req HFDeleteRepoRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responseJSON(w, fmt.Errorf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	repoName := req.Name
	if req.Organization != "" {
		repoName = req.Organization + "/" + repoName
	}

	storagePrefix := repoTypeStoragePrefix(req.Type)
	storageName := repoName
	if storagePrefix != "" {
		storageName = storagePrefix + "/" + repoName
	}

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), storageName)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
		return
	}

	repo, err := repository.Open(repoPath)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}

	if err := repo.Remove(); err != nil {
		responseJSON(w, fmt.Errorf("failed to delete repository %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleHFMoveRepo handles POST /api/repos/move
func (h *Handler) handleHFMoveRepo(w http.ResponseWriter, r *http.Request) {
	var req HFMoveRepoRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responseJSON(w, fmt.Errorf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	storagePrefix := repoTypeStoragePrefix(req.Type)

	fromName := req.FromRepo
	if storagePrefix != "" {
		fromName = storagePrefix + "/" + fromName
	}
	toName := req.ToRepo
	if storagePrefix != "" {
		toName = storagePrefix + "/" + toName
	}

	fromPath := repository.ResolvePath(h.storage.RepositoriesDir(), fromName)
	if fromPath == "" {
		responseJSON(w, fmt.Errorf("invalid source repository: %q", req.FromRepo), http.StatusBadRequest)
		return
	}

	toPath := repository.ResolvePath(h.storage.RepositoriesDir(), toName)
	if toPath == "" {
		responseJSON(w, fmt.Errorf("invalid destination repository: %q", req.ToRepo), http.StatusBadRequest)
		return
	}

	repo, err := repository.Open(fromPath)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", req.FromRepo), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", req.FromRepo, err), http.StatusInternalServerError)
		return
	}

	// Check that destination doesn't already exist
	if repository.IsRepository(toPath) {
		responseJSON(w, fmt.Errorf("destination repository %q already exists", req.ToRepo), http.StatusConflict)
		return
	}

	if err := repo.Move(toPath); err != nil {
		responseJSON(w, fmt.Errorf("failed to move repository: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleHFRepoSettings handles PUT /api/{repoType}s/{repo}/settings
func (h *Handler) handleHFRepoSettings(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repoName := vars["repo"]

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), repoStorageName(r))
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
		return
	}

	if !repository.IsRepository(repoPath) {
		responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
		return
	}

	// Accept the settings payload but don't enforce private/gated in this server
	var req HFRepoSettingsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responseJSON(w, fmt.Errorf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleHFCreateBranch handles POST /api/{repoType}s/{repo}/branch/{branch}
func (h *Handler) handleHFCreateBranch(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repoName := vars["repo"]
	branch := vars["branch"]

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), repoStorageName(r))
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
		return
	}

	repo, err := repository.Open(repoPath)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}

	// Check if branch already exists
	exists, err := repo.BranchExists(branch)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to check branch %q: %v", branch, err), http.StatusInternalServerError)
		return
	}
	if exists {
		responseJSON(w, fmt.Errorf("branch %q already exists", branch), http.StatusConflict)
		return
	}

	var req HFCreateBranchRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			responseJSON(w, fmt.Errorf("invalid request body: %v", err), http.StatusBadRequest)
			return
		}
	}

	revision := req.StartingPoint
	if err := repo.CreateBranch(branch, revision); err != nil {
		responseJSON(w, fmt.Errorf("failed to create branch %q: %v", branch, err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleHFDeleteBranch handles DELETE /api/{repoType}s/{repo}/branch/{branch}
func (h *Handler) handleHFDeleteBranch(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repoName := vars["repo"]
	branch := vars["branch"]

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), repoStorageName(r))
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
		return
	}

	repo, err := repository.Open(repoPath)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}

	// Prevent deleting the default branch
	if branch == repo.DefaultBranch() {
		responseJSON(w, fmt.Errorf("cannot delete default branch %q", branch), http.StatusForbidden)
		return
	}

	exists, err := repo.BranchExists(branch)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to check branch %q: %v", branch, err), http.StatusInternalServerError)
		return
	}
	if !exists {
		responseJSON(w, fmt.Errorf("branch %q not found", branch), http.StatusNotFound)
		return
	}

	if err := repo.DeleteBranch(branch); err != nil {
		responseJSON(w, fmt.Errorf("failed to delete branch %q: %v", branch, err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleHFCreateTag handles POST /api/{repoType}s/{repo}/tag/{tag}
func (h *Handler) handleHFCreateTag(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repoName := vars["repo"]
	revision := vars["tag"] // URL variable is the revision to tag from

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), repoStorageName(r))
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
		return
	}

	repo, err := repository.Open(repoPath)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}

	var req HFCreateTagRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			responseJSON(w, fmt.Errorf("invalid request body: %v", err), http.StatusBadRequest)
			return
		}
	}

	if req.Tag == "" {
		responseJSON(w, fmt.Errorf("tag name is required"), http.StatusBadRequest)
		return
	}

	// Check if tag already exists
	exists, err := repo.TagExists(req.Tag)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to check tag %q: %v", req.Tag, err), http.StatusInternalServerError)
		return
	}
	if exists {
		responseJSON(w, fmt.Errorf("tag %q already exists", req.Tag), http.StatusConflict)
		return
	}

	if err := repo.CreateTag(req.Tag, revision); err != nil {
		responseJSON(w, fmt.Errorf("failed to create tag %q: %v", req.Tag, err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleHFDeleteTag handles DELETE /api/{repoType}s/{repo}/tag/{tag}
func (h *Handler) handleHFDeleteTag(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repoName := vars["repo"]
	tag := vars["tag"]

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), repoStorageName(r))
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
		return
	}

	repo, err := repository.Open(repoPath)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}

	exists, err := repo.TagExists(tag)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to check tag %q: %v", tag, err), http.StatusInternalServerError)
		return
	}
	if !exists {
		responseJSON(w, fmt.Errorf("tag %q not found", tag), http.StatusNotFound)
		return
	}

	if err := repo.DeleteTag(tag); err != nil {
		responseJSON(w, fmt.Errorf("failed to delete tag %q: %v", tag, err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleHFListRefs handles GET /api/{repoType}s/{repo}/refs
func (h *Handler) handleHFListRefs(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repoName := vars["repo"]

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), repoStorageName(r))
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
		return
	}

	repo, err := h.openRepo(r.Context(), repoPath, repoStorageName(r))
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}

	// List branches
	branchNames, err := repo.Branches()
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to list branches: %v", err), http.StatusInternalServerError)
		return
	}

	var branches []HFGitRefInfo
	for _, name := range branchNames {
		refName := plumbing.NewBranchReferenceName(name)
		hash, err := repo.RefHash(refName)
		if err != nil {
			continue
		}
		branches = append(branches, HFGitRefInfo{
			Name:         name,
			Ref:          refName.String(),
			TargetCommit: hash,
		})
	}

	// List tags
	tagNames, err := repo.Tags()
	if err != nil {
		tagNames = nil
	}

	var tags []HFGitRefInfo
	for _, name := range tagNames {
		refName := plumbing.NewTagReferenceName(name)
		hash, err := repo.RefHash(refName)
		if err != nil {
			continue
		}
		tags = append(tags, HFGitRefInfo{
			Name:         name,
			Ref:          refName.String(),
			TargetCommit: hash,
		})
	}

	if branches == nil {
		branches = []HFGitRefInfo{}
	}
	if tags == nil {
		tags = []HFGitRefInfo{}
	}

	refs := HFGitRefs{
		Branches: branches,
		Converts: []HFGitRefInfo{},
		Tags:     tags,
	}
	responseJSON(w, refs, http.StatusOK)
}
