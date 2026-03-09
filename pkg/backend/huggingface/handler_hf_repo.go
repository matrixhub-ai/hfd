package huggingface

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/gorilla/mux"

	"github.com/wzshiming/hfd/pkg/permission"
	"github.com/wzshiming/hfd/pkg/receive"
	"github.com/wzshiming/hfd/pkg/repository"
)

// HFRepoInfo represents the info response for HuggingFace API
type HFRepoInfo struct {
	ID           string      `json:"id"`
	ModelID      string      `json:"modelId,omitempty"`
	SHA          string      `json:"sha"`
	Private      bool        `json:"private"`
	Disabled     bool        `json:"disabled"`
	Gated        bool        `json:"gated"`
	Downloads    int         `json:"downloads"`
	Likes        int         `json:"likes"`
	Tags         []string    `json:"tags"` // This is not git tags, but the tags in HuggingFace card metadata
	CardData     any         `json:"cardData,omitempty"`
	Siblings     []HFSibling `json:"siblings"`
	CreatedAt    string      `json:"createdAt,omitempty"`
	LastModified string      `json:"lastModified,omitempty"`
	UsedStorage  int64       `json:"usedStorage"`
}

// HFSibling represents a file in the model repository
type HFSibling struct {
	RFilename string `json:"rfilename"`
}

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

// handleInfoRevision handles the /api/{repoType}/{repo_id}/revision/{rev} and /api/{repoType}/{repo_id} endpoint
func (h *Handler) handleInfoRevision(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	ri := repoInfo(r)
	rev := vars["rev"]

	if h.permissionHook != nil {
		if err := h.permissionHook(r.Context(), permission.OperationReadRepo, ri.RepoPath, permission.Context{Ref: rev}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), ri.RepoPath)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
		return
	}

	repo, err := h.openRepo(r.Context(), repoPath, ri.RepoPath)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", ri.RepoPath, err), http.StatusInternalServerError)
		return
	}

	if rev == "" {
		rev = repo.DefaultBranch()
	}

	// Get list of files in the repository at the specified revision (recursive to include files in subdirectories)
	// An empty repository (no commits yet) is a valid state; treat it as having no files.
	hfEntries, err := repo.Tree(rev, "", &repository.TreeOptions{Recursive: true})
	if err != nil && !errors.Is(err, repository.ErrRevisionNotFound) {
		responseJSON(w, fmt.Errorf("failed to get tree for repo %q at rev %q: %v", ri.RepoPath, rev, err), http.StatusInternalServerError)
		return
	}

	var siblings []HFSibling
	for _, entry := range hfEntries {
		if entry.Type == repository.EntryTypeFile {
			siblings = append(siblings, HFSibling{
				RFilename: entry.Path,
			})
		}
	}

	usedStorage, _ := repo.DiskUsage()

	// Get the commit SHA for this revision
	commitHash := ""
	commits, err := repo.Commits(rev, 1, nil)
	if err == nil && len(commits) > 0 {
		commitHash = commits[0].SHA
	}

	// Collect metadata (tags, cardData, pipeline_tag, etc.) from README.md and config.json.
	meta := collectRepoMetadata(repo, rev)

	tags := meta.tags
	if tags == nil {
		tags = []string{}
	}

	hfInfo := HFRepoInfo{
		ID:          ri.FullName,
		SHA:         commitHash,
		Private:     false,
		Disabled:    false,
		Gated:       false,
		Downloads:   0,
		Likes:       0,
		Tags:        tags,
		Siblings:    siblings,
		CardData:    meta.cardData,
		UsedStorage: usedStorage,
	}

	// For models, also set the modelId field which is required by some HuggingFace clients. For datasets and spaces, the client doesn't require it and it can be confusing to have it be different from the ID, so we leave it empty.
	if ri.RepoType == "models" {
		hfInfo.ModelID = hfInfo.ID
	}

	responseJSON(w, hfInfo, http.StatusOK)
}

// handleDeleteRepo handles DELETE /api/repos/delete
func (h *Handler) handleDeleteRepo(w http.ResponseWriter, r *http.Request) {
	var req HFDeleteRepoRequest
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

	if h.permissionHook != nil {
		if err := h.permissionHook(r.Context(), permission.OperationDeleteRepo, storageName, permission.Context{}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
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

// handleMoveRepo handles POST /api/repos/move
func (h *Handler) handleMoveRepo(w http.ResponseWriter, r *http.Request) {
	var req HFMoveRepoRequest
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

	if h.permissionHook != nil {
		if err := h.permissionHook(r.Context(), permission.OperationUpdateRepo, fromName, permission.Context{DestRepo: toName}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
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

// handleRepoSettings handles PUT /api/{repoType}/{repo}/settings
func (h *Handler) handleRepoSettings(w http.ResponseWriter, r *http.Request) {
	ri := repoInfo(r)

	if h.permissionHook != nil {
		if err := h.permissionHook(r.Context(), permission.OperationUpdateRepo, ri.RepoPath, permission.Context{}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), ri.RepoPath)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
		return
	}

	if !repository.IsRepository(repoPath) {
		responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
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

// handleCreateBranch handles POST /api/{repoType}/{repo}/branch/{rev}
func (h *Handler) handleCreateBranch(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := repoInfo(r)
	rev := vars["rev"]

	if h.permissionHook != nil {
		if err := h.permissionHook(r.Context(), permission.OperationUpdateRepo, ri.RepoPath, permission.Context{
			Ref: rev,
		}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), ri.RepoPath)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
		return
	}

	repo, err := repository.Open(repoPath)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", ri.RepoPath, err), http.StatusInternalServerError)
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

	var req HFCreateBranchRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			responseJSON(w, fmt.Errorf("invalid request body: %v", err), http.StatusBadRequest)
			return
		}
	}

	if h.preReceiveHook != nil {
		// Resolve the starting point to a hash so the hook has the target commit
		newRev, _ := repo.ResolveRevision(req.StartingPoint)
		if newRev == "" {
			newRev, _ = repo.RefHash(plumbing.NewBranchReferenceName(repo.DefaultBranch()))
		}
		if err := h.preReceiveHook(r.Context(), ri.RepoPath, []receive.RefUpdate{
			{OldRev: receive.ZeroHash, NewRev: newRev, RefName: "refs/heads/" + rev},
		}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	revision := req.StartingPoint
	if err := repo.CreateBranch(rev, revision); err != nil {
		responseJSON(w, fmt.Errorf("failed to create branch %q: %v", rev, err), http.StatusInternalServerError)
		return
	}

	if h.postReceiveHook != nil {
		hash, _ := repo.RefHash(plumbing.NewBranchReferenceName(rev))
		if hookErr := h.postReceiveHook(r.Context(), ri.RepoPath, []receive.RefUpdate{
			{OldRev: receive.ZeroHash, NewRev: hash, RefName: "refs/heads/" + rev},
		}); hookErr != nil {
			slog.Warn("post-receive hook error", "repo", ri.RepoPath, "error", hookErr)
		}
	}

	w.WriteHeader(http.StatusOK)
}

// handleDeleteBranch handles DELETE /api/{repoType}/{repo}/branch/{rev}
func (h *Handler) handleDeleteBranch(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := repoInfo(r)
	rev := vars["rev"]

	if h.permissionHook != nil {
		if err := h.permissionHook(r.Context(), permission.OperationUpdateRepo, ri.RepoPath, permission.Context{
			Ref: rev,
		}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), ri.RepoPath)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
		return
	}

	repo, err := repository.Open(repoPath)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", ri.RepoPath, err), http.StatusInternalServerError)
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
		{OldRev: oldHash, NewRev: receive.ZeroHash, RefName: "refs/heads/" + rev},
	}

	if h.preReceiveHook != nil {
		if err := h.preReceiveHook(r.Context(), ri.RepoPath, updates); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	if err := repo.DeleteBranch(rev); err != nil {
		responseJSON(w, fmt.Errorf("failed to delete branch %q: %v", rev, err), http.StatusInternalServerError)
		return
	}

	if h.postReceiveHook != nil {
		if hookErr := h.postReceiveHook(r.Context(), ri.RepoPath, updates); hookErr != nil {
			slog.Warn("post-receive hook error", "repo", ri.RepoPath, "error", hookErr)
		}
	}

	w.WriteHeader(http.StatusOK)
}

// handleCreateTag handles POST /api/{repoType}/{repo}/tag/{rev}
func (h *Handler) handleCreateTag(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := repoInfo(r)
	rev := vars["rev"]

	if h.permissionHook != nil {
		if err := h.permissionHook(r.Context(), permission.OperationUpdateRepo, ri.RepoPath, permission.Context{
			Ref: rev,
		}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), ri.RepoPath)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
		return
	}

	repo, err := repository.Open(repoPath)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", ri.RepoPath, err), http.StatusInternalServerError)
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

	if h.preReceiveHook != nil {
		// Resolve the revision to a hash so the hook has the target commit
		newRev, _ := repo.ResolveRevision(rev)
		if err := h.preReceiveHook(r.Context(), ri.RepoPath, []receive.RefUpdate{
			{OldRev: receive.ZeroHash, NewRev: newRev, RefName: "refs/tags/" + req.Tag},
		}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	if err := repo.CreateTag(req.Tag, rev); err != nil {
		responseJSON(w, fmt.Errorf("failed to create tag %q: %v", req.Tag, err), http.StatusInternalServerError)
		return
	}

	if h.postReceiveHook != nil {
		hash, _ := repo.RefHash(plumbing.NewTagReferenceName(req.Tag))
		if hookErr := h.postReceiveHook(r.Context(), ri.RepoPath, []receive.RefUpdate{
			{OldRev: receive.ZeroHash, NewRev: hash, RefName: "refs/tags/" + req.Tag},
		}); hookErr != nil {
			slog.Warn("post-receive hook error", "repo", ri.RepoPath, "error", hookErr)
		}
	}

	w.WriteHeader(http.StatusOK)
}

// handleDeleteTag handles DELETE /api/{repoType}/{repo}/tag/{rev}
func (h *Handler) handleDeleteTag(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := repoInfo(r)
	rev := vars["rev"]

	if h.permissionHook != nil {
		if err := h.permissionHook(r.Context(), permission.OperationUpdateRepo, ri.RepoPath, permission.Context{
			Ref: rev,
		}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), ri.RepoPath)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
		return
	}

	repo, err := repository.Open(repoPath)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", ri.RepoPath, err), http.StatusInternalServerError)
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
		{OldRev: oldHash, NewRev: receive.ZeroHash, RefName: "refs/tags/" + rev},
	}

	if h.preReceiveHook != nil {
		if err := h.preReceiveHook(r.Context(), ri.RepoPath, updates); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	if err := repo.DeleteTag(rev); err != nil {
		responseJSON(w, fmt.Errorf("failed to delete tag %q: %v", rev, err), http.StatusInternalServerError)
		return
	}

	if h.postReceiveHook != nil {
		if hookErr := h.postReceiveHook(r.Context(), ri.RepoPath, updates); hookErr != nil {
			slog.Warn("post-receive hook error", "repo", ri.RepoPath, "error", hookErr)
		}
	}

	w.WriteHeader(http.StatusOK)
}

// handleListRefs handles GET /api/{repoType}/{repo}/refs
func (h *Handler) handleListRefs(w http.ResponseWriter, r *http.Request) {
	ri := repoInfo(r)

	if h.permissionHook != nil {
		if err := h.permissionHook(r.Context(), permission.OperationReadRepo, ri.RepoPath, permission.Context{}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), ri.RepoPath)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
		return
	}

	repo, err := h.openRepo(r.Context(), repoPath, ri.RepoPath)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", ri.RepoPath, err), http.StatusInternalServerError)
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

// HFCommitAuthor represents the author entry in a commit's authors list.
type HFCommitAuthor struct {
	User string `json:"user"`
}

// HFCommitInfo represents a single commit in the list commits response.
type HFCommitInfo struct {
	ID      string           `json:"id"`
	Title   string           `json:"title"`
	Message string           `json:"message"`
	Authors []HFCommitAuthor `json:"authors"`
	Date    string           `json:"date"`
}

// handleListCommits handles GET /api/{repoType}/{repo}/commits/{rev}
// It returns a paginated list of commits for the given revision.
func (h *Handler) handleListCommits(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := repoInfo(r)
	rev := vars["rev"]

	if h.permissionHook != nil {
		if err := h.permissionHook(r.Context(), permission.OperationReadRepo, ri.RepoPath, permission.Context{}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), ri.RepoPath)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
		return
	}

	repo, err := h.openRepo(r.Context(), repoPath, ri.RepoPath)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", ri.RepoPath, err), http.StatusInternalServerError)
		return
	}

	query := r.URL.Query()

	limit := 50
	if l := query.Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}

	var page int
	var offset int
	if pageStr := query.Get("p"); pageStr != "" {
		if page, err = strconv.Atoi(pageStr); err == nil && page >= 0 {
			offset = page * limit
		}
	}

	// Fetch one extra commit so we can detect whether a next page exists.
	rawCommits, err := repo.Commits(rev, limit+1, &repository.CommitsOptions{Offset: offset})
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to list commits for %q: %v", rev, err), http.StatusInternalServerError)
		return
	}

	if len(rawCommits) > limit {
		rawCommits = rawCommits[:limit]
		nextURL := buildNextCommitPageURL(r, page+1, limit)
		w.Header().Set("Link", fmt.Sprintf("<%s>; rel=\"next\"", nextURL))
	}

	commitInfos := make([]HFCommitInfo, 0, len(rawCommits))
	for _, c := range rawCommits {
		title := c.Message
		if idx := strings.IndexByte(title, '\n'); idx >= 0 {
			title = title[:idx]
		}
		commitInfos = append(commitInfos, HFCommitInfo{
			ID:      c.SHA,
			Title:   title,
			Message: c.Message,
			Authors: []HFCommitAuthor{{User: c.Author}},
			Date:    c.Date,
		})
	}

	responseJSON(w, commitInfos, http.StatusOK)
}

// buildNextCommitPageURL constructs the URL for the next commits page,
// replacing the p parameter with the given page number.
func buildNextCommitPageURL(r *http.Request, nextPage, limit int) string {
	origin := requestOrigin(r)
	q := r.URL.Query()
	q.Set("p", strconv.Itoa(nextPage))
	q.Set("limit", strconv.Itoa(limit))
	return origin + r.URL.Path + "?" + q.Encode()
}

// handleCompare handles GET /api/{repoType}/{repo}/compare/{compare}
func (h *Handler) handleCompare(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := repoInfo(r)
	compare := vars["compare"]

	if h.permissionHook != nil {
		if err := h.permissionHook(r.Context(), permission.OperationReadRepo, ri.RepoPath, permission.Context{}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), ri.RepoPath)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
		return
	}

	repo, err := h.openRepo(r.Context(), repoPath, ri.RepoPath)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", ri.RepoPath, err), http.StatusInternalServerError)
		return
	}

	base, head, found := strings.Cut(compare, "..")
	if !found || base == "" || head == "" ||
		strings.HasPrefix(head, ".") || strings.Contains(head, "..") {
		responseJSON(w, fmt.Errorf("invalid compare format %q, expected base..head", compare), http.StatusBadRequest)
		return
	}

	changes, err := repo.Compare(r.Context(), base, head)
	if err != nil {
		if errors.Is(err, repository.ErrRevisionNotFound) || errors.Is(err, plumbing.ErrReferenceNotFound) {
			responseJSON(w, fmt.Errorf("failed to resolve compare revisions %q: %v", compare, err), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to compare %q: %v", compare, err), http.StatusInternalServerError)
		return
	}

	patch, err := changes.PatchContext(r.Context())
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to generate patch for compare %q: %v", compare, err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = patch.Encode(w)
}

// HFSuperSquashRequest represents the super-squash request body.
type HFSuperSquashRequest struct {
	Message string `json:"message,omitempty"`
}

// handleSuperSquash handles POST /api/{repoType}/{repo}/super-squash/{rev}
// It squashes all commits in the current rev into a single commit with the given message.
// The action is irreversible.
func (h *Handler) handleSuperSquash(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := repoInfo(r)
	rev := vars["rev"]

	if h.permissionHook != nil {
		if err := h.permissionHook(r.Context(), permission.OperationUpdateRepo, ri.RepoPath, permission.Context{Ref: rev}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), ri.RepoPath)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
		return
	}

	repo, err := repository.Open(repoPath)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", ri.RepoPath, err), http.StatusInternalServerError)
		return
	}

	var req HFSuperSquashRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			responseJSON(w, fmt.Errorf("invalid request body: %v", err), http.StatusBadRequest)
			return
		}
	}

	message := req.Message
	if message == "" {
		message = "Super-squash branch '" + rev + "' using huggingface_hub"
	}

	if _, err := repo.SuperSquash(r.Context(), rev, message, "HuggingFace", "hf@users.noreply.huggingface.co"); err != nil {
		responseJSON(w, fmt.Errorf("failed to squash repository %q rev %q: %v", ri.RepoPath, rev, err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
