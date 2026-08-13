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
	repoPath, ok := h.resolveRepoPath(w, ri.RepoName, ri.RepoName)
	if !ok {
		return
	}
	repo, ok := h.openRepoChecked(w, r, repoPath, ri.RepoName, false)
	if !ok {
		return
	}

	if rev == "" {
		rev = repo.DefaultBranch()
	}

	// Get list of files in the repository at the specified revision (recursive to include files in subdirectories)
	// An empty repository (no commits yet) is a valid state; treat it as having no files.
	hfEntries, err := repo.Tree(rev, "", &repository.TreeOptions{Recursive: true})
	if err != nil && !errors.Is(err, repository.ErrRevisionNotFound) {
		responseJSON(w, fmt.Errorf("failed to get tree for repo %q at rev %q: %v", ri.RepoName, rev, err), http.StatusInternalServerError)
		return
	}

	var siblings []sibling
	for _, entry := range hfEntries {
		if entry.Type() == repository.EntryTypeFile {
			siblings = append(siblings, sibling{
				RFilename: entry.Path(),
			})
		}
	}

	usedStorage, _ := repo.DiskUsage()

	// Get the commit SHA for this revision
	commitHash := ""
	commits, err := repo.Commits(rev, &repository.CommitsOptions{Limit: 1})
	if err == nil && len(commits) > 0 {
		commitHash = commits[0].Hash().String()
	}

	// Collect metadata (tags, cardData, pipeline_tag, etc.) from README.md and config.json.
	meta := collectRepoMetadata(repo, rev)

	tags := meta.tags
	if tags == nil {
		tags = []string{}
	}

	hfInfo := repoInfo{
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
