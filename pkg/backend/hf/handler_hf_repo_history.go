package hf

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// handleListCommits handles GET /api/{repoType}/{repo}/commits/{rev}
// It returns a paginated list of commits for the given revision.
func (h *Handler) handleListCommits(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := getRepoInformation(r)
	rev := vars["rev"]

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
		if v, err := strconv.Atoi(pageStr); err == nil {
			page = v
			if page >= 0 {
				offset = page * limit
			}
		}
	}

	// Fetch one extra commit so we can detect whether a next page exists.
	rawCommits, err := repo.Commits(rev, &repository.CommitsOptions{Limit: limit + 1, Offset: offset})
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to list commits for %q: %v", rev, err), http.StatusInternalServerError)
		return
	}

	if len(rawCommits) > limit {
		rawCommits = rawCommits[:limit]
		nextURL := buildNextCommitPageURL(r, page+1, limit)
		w.Header().Set("Link", fmt.Sprintf("<%s>; rel=\"next\"", nextURL))
	}

	commitInfos := make([]commitInfo, 0, len(rawCommits))
	for _, c := range rawCommits {
		commitInfos = append(commitInfos, commitInfo{
			ID:      c.Hash().String(),
			Title:   c.Title(),
			Message: c.Message(),
			Authors: []commitAuthor{{User: c.Author().Name()}},
			Date:    c.Author().When().UTC().Format(repository.TimeFormat),
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
	ri := getRepoInformation(r)
	compare := vars["compare"]

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

// handleSuperSquash handles POST /api/{repoType}/{repo}/super-squash/{rev}
// It squashes all commits in the current rev into a single commit with the given message.
// The action is irreversible.
func (h *Handler) handleSuperSquash(w http.ResponseWriter, r *http.Request) {
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

	var req superSquashRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			responseJSON(w, fmt.Errorf("invalid request body: %v", err), http.StatusBadRequest)
			return
		}
	}

	if !h.checkPreReceive(w, r, ri.RepoName, []receive.RefUpdate{
		repo.RefUpdate(receive.BreakHash, receive.BreakHash, "refs/heads/"+rev),
	}, "pre-receive hook denied the super-squash operation") {
		return
	}

	user, ok := authenticate.GetUserInfo(r.Context())
	if !ok {
		user = authenticate.UserInfo{
			User:  "HuggingFace",
			Email: "hf@users.noreply.huggingface.co",
		}
	}

	message := req.Message
	if message == "" {
		message = "Super-squash branch '" + rev + "'"
	}

	if _, err := repo.SuperSquash(r.Context(), rev, message, user.User, user.Email); err != nil {
		responseJSON(w, fmt.Errorf("failed to squash repository %q rev %q: %v", ri.RepoName, rev, err), http.StatusInternalServerError)
		return
	}

	h.afterReceivePack(r.Context(), ri.RepoName, []receive.RefUpdate{
		repo.RefUpdate(receive.BreakHash, receive.BreakHash, "refs/heads/"+rev),
	})

	w.WriteHeader(http.StatusOK)
}
