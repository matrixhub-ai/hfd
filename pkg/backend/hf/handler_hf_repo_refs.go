package hf

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/receive"
)

// handleCreateBranch handles POST /api/{repoType}/{repo}/branch/{rev}
func (h *Handler) handleCreateBranch(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := getRepoInformation(r)
	rev := vars["rev"]

	if !h.checkPermission(w, r, permission.OperationUpdateRepo, ri.RepoName, permission.Context{Ref: rev}) {
		return
	}
	repoPath, ok := h.resolveRepoPath(w, ri.RepoName, ri.RepoName)
	if !ok {
		return
	}
	repo, ok := h.openRepoDirect(w, repoPath, ri.RepoName)
	if !ok {
		return
	}

	// Check if branch already exists
	exists, err := repo.BranchExists(rev)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to check branch %q: %v", rev, err), http.StatusInternalServerError)
		return
	}
	if exists {
		responseJSON(w, fmt.Errorf("branch %q already exists", rev), http.StatusConflict)
		return
	}

	var req createBranchRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			responseJSON(w, fmt.Errorf("invalid request body: %v", err), http.StatusBadRequest)
			return
		}
	}

	if h.preReceiveHookFunc != nil {
		// Resolve the starting point to a hash so the hook has the target commit
		newRev, _ := repo.ResolveRevision(req.StartingPoint)
		if newRev == "" {
			newRev, _ = repo.RefHash(plumbing.NewBranchReferenceName(repo.DefaultBranch()))
		}
		if !h.checkPreReceive(w, r, ri.RepoName, []receive.RefUpdate{
			repo.RefUpdate(receive.ZeroHash, newRev, "refs/heads/"+rev),
		}, "pre-receive hook denied the branch creation") {
			return
		}
	}

	revision := req.StartingPoint
	if err := repo.CreateBranch(rev, revision); err != nil {
		responseJSON(w, fmt.Errorf("failed to create branch %q: %v", rev, err), http.StatusInternalServerError)
		return
	}

	hash, _ := repo.RefHash(plumbing.NewBranchReferenceName(rev))
	h.afterReceivePack(r.Context(), ri.RepoName, []receive.RefUpdate{
		repo.RefUpdate(receive.ZeroHash, hash, "refs/heads/"+rev),
	})

	w.WriteHeader(http.StatusOK)
}

// handleDeleteBranch handles DELETE /api/{repoType}/{repo}/branch/{rev}
func (h *Handler) handleDeleteBranch(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := getRepoInformation(r)
	rev := vars["rev"]

	if !h.checkPermission(w, r, permission.OperationUpdateRepo, ri.RepoName, permission.Context{Ref: rev}) {
		return
	}
	repoPath, ok := h.resolveRepoPath(w, ri.RepoName, ri.RepoName)
	if !ok {
		return
	}
	repo, ok := h.openRepoChecked(w, r, repoPath, ri.RepoName, true)
	if !ok {
		return
	}

	// Prevent deleting the default branch
	if rev == repo.DefaultBranch() {
		responseJSON(w, fmt.Errorf("cannot delete default branch %q", rev), http.StatusForbidden)
		return
	}

	exists, err := repo.BranchExists(rev)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to check branch %q: %v", rev, err), http.StatusInternalServerError)
		return
	}
	if !exists {
		responseJSON(w, fmt.Errorf("branch %q not found", rev), http.StatusNotFound)
		return
	}

	// Capture hash before deletion for pre/post hooks
	oldHash, _ := repo.RefHash(plumbing.NewBranchReferenceName(rev))

	updates := []receive.RefUpdate{
		repo.RefUpdate(oldHash, receive.ZeroHash, "refs/heads/"+rev),
	}

	if !h.checkPreReceive(w, r, ri.RepoName, updates, "pre-receive hook denied the branch deletion") {
		return
	}

	if err := repo.DeleteBranch(rev); err != nil {
		responseJSON(w, fmt.Errorf("failed to delete branch %q: %v", rev, err), http.StatusInternalServerError)
		return
	}

	h.afterReceivePack(r.Context(), ri.RepoName, updates)

	w.WriteHeader(http.StatusOK)
}

// handleCreateTag handles POST /api/{repoType}/{repo}/tag/{rev}
func (h *Handler) handleCreateTag(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := getRepoInformation(r)
	rev := vars["rev"]

	if !h.checkPermission(w, r, permission.OperationUpdateRepo, ri.RepoName, permission.Context{Ref: rev}) {
		return
	}
	repoPath, ok := h.resolveRepoPath(w, ri.RepoName, ri.RepoName)
	if !ok {
		return
	}
	repo, ok := h.openRepoChecked(w, r, repoPath, ri.RepoName, true)
	if !ok {
		return
	}

	var req createTagRequest
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

	if h.preReceiveHookFunc != nil {
		// Resolve the revision to a hash so the hook has the target commit
		newRev, _ := repo.ResolveRevision(rev)
		if !h.checkPreReceive(w, r, ri.RepoName, []receive.RefUpdate{
			repo.RefUpdate(receive.ZeroHash, newRev, "refs/tags/"+req.Tag),
		}, "pre-receive hook denied the tag creation") {
			return
		}
	}

	if err := repo.CreateTag(req.Tag, rev); err != nil {
		responseJSON(w, fmt.Errorf("failed to create tag %q: %v", req.Tag, err), http.StatusInternalServerError)
		return
	}

	hash, _ := repo.RefHash(plumbing.NewTagReferenceName(req.Tag))
	h.afterReceivePack(r.Context(), ri.RepoName, []receive.RefUpdate{
		repo.RefUpdate(receive.ZeroHash, hash, "refs/tags/"+req.Tag),
	})

	w.WriteHeader(http.StatusOK)
}

// handleDeleteTag handles DELETE /api/{repoType}/{repo}/tag/{rev}
func (h *Handler) handleDeleteTag(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := getRepoInformation(r)
	rev := vars["rev"]

	if !h.checkPermission(w, r, permission.OperationUpdateRepo, ri.RepoName, permission.Context{Ref: rev}) {
		return
	}
	repoPath, ok := h.resolveRepoPath(w, ri.RepoName, ri.RepoName)
	if !ok {
		return
	}
	repo, ok := h.openRepoChecked(w, r, repoPath, ri.RepoName, true)
	if !ok {
		return
	}

	exists, err := repo.TagExists(rev)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to check tag %q: %v", rev, err), http.StatusInternalServerError)
		return
	}
	if !exists {
		responseJSON(w, fmt.Errorf("tag %q not found", rev), http.StatusNotFound)
		return
	}

	// Capture hash before deletion for pre/post hooks
	oldHash, _ := repo.RefHash(plumbing.NewTagReferenceName(rev))

	updates := []receive.RefUpdate{
		repo.RefUpdate(oldHash, receive.ZeroHash, "refs/tags/"+rev),
	}

	if !h.checkPreReceive(w, r, ri.RepoName, updates, "pre-receive hook denied the tag deletion") {
		return
	}

	if err := repo.DeleteTag(rev); err != nil {
		responseJSON(w, fmt.Errorf("failed to delete tag %q: %v", rev, err), http.StatusInternalServerError)
		return
	}

	h.afterReceivePack(r.Context(), ri.RepoName, updates)

	w.WriteHeader(http.StatusOK)
}

// handleListRefs handles GET /api/{repoType}/{repo}/refs
func (h *Handler) handleListRefs(w http.ResponseWriter, r *http.Request) {
	ri := getRepoInformation(r)

	if !h.checkPermission(w, r, permission.OperationReadRepo, ri.RepoName, permission.Context{}) {
		return
	}
	repoPath, ok := h.resolveRepoPath(w, ri.RepoName, ri.RepoName)
	if !ok {
		return
	}
	repo, ok := h.openRepoChecked(w, r, repoPath, ri.RepoName, false)
	if !ok {
		return
	}

	// List branches
	branchNames, err := repo.Branches()
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to list branches: %v", err), http.StatusInternalServerError)
		return
	}

	var branches []gitRefInfo
	for _, name := range branchNames {
		refName := plumbing.NewBranchReferenceName(name)
		hash, err := repo.RefHash(refName)
		if err != nil {
			continue
		}
		branches = append(branches, gitRefInfo{
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

	var tags []gitRefInfo
	for _, name := range tagNames {
		refName := plumbing.NewTagReferenceName(name)
		hash, err := repo.RefHash(refName)
		if err != nil {
			continue
		}
		tags = append(tags, gitRefInfo{
			Name:         name,
			Ref:          refName.String(),
			TargetCommit: hash,
		})
	}

	if branches == nil {
		branches = []gitRefInfo{}
	}
	if tags == nil {
		tags = []gitRefInfo{}
	}

	refs := gitRefs{
		Branches: branches,
		Converts: []gitRefInfo{},
		Tags:     tags,
	}
	responseJSON(w, refs, http.StatusOK)
}
