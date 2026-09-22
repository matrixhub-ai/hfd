package hf

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// handleInfoRevision handles the /api/{repoType}/{repo_id}/revision/{rev} and /api/{repoType}/{repo_id} endpoint
func (h *Handler) handleInfoRevision(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	ri := getRepoInformation(r)
	rev := vars["rev"]

	if !h.checkPermission(w, r, permission.OperationReadRepo, ri.RepoName, permission.Context{Ref: rev}) {
		return
	}
	repo, ok := h.openRepoChecked(w, r, ri.RepoName, false)
	if !ok {
		return
	}

	if rev == "" {
		rev = repo.DefaultBranch()
	}

	// Get list of files in the repository at the specified revision (recursive to include files in subdirectories)
	// An empty repository (no commits yet) is a valid state; treat it as having no files.
	siblings, err := repoSiblings(repo, rev)
	if err != nil && !errors.Is(err, repository.ErrRevisionNotFound) {
		responseJSON(w, fmt.Errorf("failed to get tree for repo %q at rev %q: %v", ri.RepoName, rev, err), http.StatusInternalServerError)
		return
	}

	usedStorage, _ := repo.DiskUsage(r.Context())

	// The tip commit gives sha and lastModified; createdAt is the earliest reachable author date.
	dates := h.tips.dates(repo, repository.ResolvePath(ri.RepoName), rev, true)

	// Collect metadata (tags, cardData, pipeline_tag, etc.) from README.md and config.json.
	meta := collectRepoMetadata(repo, rev, ri.RepoType)

	tags := meta.tags
	if tags == nil {
		tags = []string{}
	}

	hfInfo := repoInfo{
		ID:          ri.FullName,
		Author:      ri.Namespace,
		SHA:         dates.sha,
		Private:     false,
		Disabled:    false,
		Gated:       false,
		Downloads:   0,
		Likes:       0,
		Tags:        tags,
		Siblings:    siblings,
		UsedStorage: usedStorage,
	}
	// A card the Hub cannot serialize is left out rather than breaking the response.
	if raw := meta.cardJSON(); raw != nil {
		hfInfo.CardData = raw
	}
	if dates.sha != "" {
		hfInfo.CreatedAt = hubTime(dates.createdAt)
		hfInfo.LastModified = hubTime(dates.lastModified)
	}

	// For models, also set the modelId field which is required by some HuggingFace clients. For datasets and spaces, the client doesn't require it and it can be confusing to have it be different from the ID, so we leave it empty.
	if ri.RepoType == "models" {
		hfInfo.ModelID = hfInfo.ID
		hfInfo.PipelineTag = meta.pipelineTag
		hfInfo.LibraryName = meta.libraryName
	}

	responseJSON(w, hfInfo, http.StatusOK)
}
