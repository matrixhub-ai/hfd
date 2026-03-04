package huggingface

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/gorilla/mux"

	"github.com/wzshiming/hfd/pkg/hf"
	"github.com/wzshiming/hfd/pkg/repository"
)

// HFRepoInfo represents the info response for HuggingFace API
type HFRepoInfo struct {
	ID            string      `json:"id"`
	ModelID       string      `json:"modelId,omitempty"`
	SHA           string      `json:"sha"`
	Private       bool        `json:"private"`
	Disabled      bool        `json:"disabled"`
	Gated         bool        `json:"gated"`
	Downloads     int         `json:"downloads"`
	Likes         int         `json:"likes"`
	Tags          []string    `json:"tags"` // This is not git tags, but the tags in HuggingFace card metadata
	CardData      any         `json:"cardData,omitempty"`
	Siblings      []HFSibling `json:"siblings"`
	CreatedAt     string      `json:"createdAt,omitempty"`
	LastModified  string      `json:"lastModified,omitempty"`
	DefaultBranch string      `json:"defaultBranch,omitempty"`
	UsedStorage   int64       `json:"usedStorage"`
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
		responseJSON(w, fmt.Errorf("failed to get tree for repo %q at ref %q: %v", ri.RepoPath, rev, err), http.StatusInternalServerError)
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
	sha := ""
	commits, err := repo.Commits(rev, 1)
	if err == nil && len(commits) > 0 {
		sha = commits[0].SHA
	}

	// Collect tags from all sources, deduplicating across them.
	seen := make(map[string]struct{})
	tags := []string{}
	addTag := func(tag string) {
		if tag == "" {
			return
		}
		if _, ok := seen[tag]; !ok {
			seen[tag] = struct{}{}
			tags = append(tags, tag)
		}
	}

	var cardData any

	// Source 1: README.md YAML front matter (HuggingFace card metadata)
	if blob, err := repo.Blob(rev, "README.md"); err == nil {
		if rc, err := blob.NewReader(); err == nil {
			if rm, err := hf.ParseReadme(rc); err == nil {
				for _, tag := range rm.Tags() {
					addTag(tag)
				}
				cardData = rm.CardData
			}
			rc.Close()
		}
	}

	// Source 2: config.json (model_type and other fields)
	if blob, err := repo.Blob(rev, "config.json"); err == nil {
		if rc, err := blob.NewReader(); err == nil {
			if cfg, err := hf.ParseConfigData(rc); err == nil {
				for _, tag := range cfg.Tags() {
					addTag(tag)
				}
			}
			rc.Close()
		}
	}

	hfInfo := HFRepoInfo{
		ID:            ri.FullName,
		SHA:           sha,
		Private:       false,
		Disabled:      false,
		Gated:         false,
		Downloads:     0,
		Likes:         0,
		Tags:          tags,
		Siblings:      siblings,
		DefaultBranch: rev,
		CardData:      cardData,
		UsedStorage:   usedStorage,
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

	revision := req.StartingPoint
	if err := repo.CreateBranch(rev, revision); err != nil {
		responseJSON(w, fmt.Errorf("failed to create branch %q: %v", rev, err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleDeleteBranch handles DELETE /api/{repoType}/{repo}/branch/{rev}
func (h *Handler) handleDeleteBranch(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := repoInfo(r)
	rev := vars["rev"]

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

	if err := repo.DeleteBranch(rev); err != nil {
		responseJSON(w, fmt.Errorf("failed to delete branch %q: %v", rev, err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleCreateTag handles POST /api/{repoType}/{repo}/tag/{rev}
func (h *Handler) handleCreateTag(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := repoInfo(r)
	rev := vars["rev"]

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

	if err := repo.CreateTag(req.Tag, rev); err != nil {
		responseJSON(w, fmt.Errorf("failed to create tag %q: %v", req.Tag, err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleDeleteTag handles DELETE /api/{repoType}/{repo}/tag/{rev}
func (h *Handler) handleDeleteTag(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := repoInfo(r)
	rev := vars["rev"]

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

	if err := repo.DeleteTag(rev); err != nil {
		responseJSON(w, fmt.Errorf("failed to delete tag %q: %v", rev, err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleListRefs handles GET /api/{repoType}/{repo}/refs
func (h *Handler) handleListRefs(w http.ResponseWriter, r *http.Request) {
	ri := repoInfo(r)

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
