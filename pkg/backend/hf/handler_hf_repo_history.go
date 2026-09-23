package hf

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

const (
	defaultCommitsLimit = 50
	maxCommitsLimit     = 1000
)

// handleListCommits handles GET /api/{repoType}/{repo}/commits/{rev}
// One history walk yields both X-Total-Count and the requested page.
func (h *Handler) handleListCommits(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := getRepoInformation(r)
	rev := vars["rev"]

	query := r.URL.Query()
	limit, page := defaultCommitsLimit, 0
	var err error
	if v := query.Get("limit"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil || limit <= 0 {
			responseJSON(w, fmt.Errorf("invalid limit parameter: %s", v), http.StatusBadRequest)
			return
		}
		limit = min(limit, maxCommitsLimit)
	}
	if v := query.Get("p"); v != "" {
		if page, err = strconv.Atoi(v); err != nil || page < 0 {
			responseJSON(w, fmt.Errorf("invalid page parameter: %s", v), http.StatusBadRequest)
			return
		}
	}

	if !h.checkPermission(w, r, permission.OperationReadRepo, ri.RepoName, permission.Context{Ref: rev}) {
		return
	}
	repo, ok := h.openRepoChecked(w, r, ri.RepoName, false)
	if !ok {
		return
	}

	hash, err := repo.ResolveRevision(rev)
	if err != nil {
		status, rerr := revisionError(rev, err)
		if status == http.StatusNotFound {
			refs, lerr := repo.Refs()
			switch {
			case lerr != nil:
				status, rerr = http.StatusInternalServerError, fmt.Errorf("failed to read references for %q: %v", rev, lerr)
			case len(refs) == 0 && rev == repo.DefaultBranch():
				// An unborn default branch has no commits rather than no revision.
				w.Header().Set("X-Total-Count", "0")
				responseJSON(w, []commitInfo{}, http.StatusOK)
				return
			case unreadableRef(repo, refs, rev, err):
				// go-git reports a reference whose target object cannot be read as not found.
				status, rerr = http.StatusInternalServerError, fmt.Errorf("failed to resolve revision %q: reference target is unreadable", rev)
			}
		}
		responseJSON(w, rerr, status)
		return
	}
	all, err := repo.Commits(hash, nil)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to list commits for %q: %v", rev, err), http.StatusInternalServerError)
		return
	}

	total := len(all)
	// Comparing against total/limit keeps page*limit from overflowing on a huge p.
	start := total
	if page <= total/limit {
		start = page * limit
	}
	end := min(start+limit, total)
	w.Header().Set("X-Total-Count", strconv.Itoa(total))
	if end < total {
		nextURL := buildNextCommitPageURL(r, page+1, limit)
		w.Header().Set("Link", fmt.Sprintf("<%s>; rel=\"next\"", nextURL))
	}

	commitInfos := make([]commitInfo, 0, end-start)
	for _, c := range all[start:end] {
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
// replacing the p parameter with the given page number. The escaped path
// keeps an encoded slash or percent inside the revision as one segment.
func buildNextCommitPageURL(r *http.Request, nextPage, limit int) string {
	origin := requestOrigin(r)
	q := r.URL.Query()
	q.Set("p", strconv.Itoa(nextPage))
	q.Set("limit", strconv.Itoa(limit))
	return origin + r.URL.EscapedPath() + "?" + q.Encode()
}

// revisionError maps unknown, invalid or exhausted revisions (main~999 runs
// out of parents as io.EOF) to 404 and anything else to 500.
func revisionError(rev string, err error) (int, error) {
	// go-git's invalid-revision error type is internal, so its message prefix is the only handle.
	if errors.Is(err, repository.ErrRevisionNotFound) || errors.Is(err, io.EOF) || strings.HasPrefix(err.Error(), "Revision invalid") {
		return http.StatusNotFound, fmt.Errorf("revision %q not found", rev)
	}
	return http.StatusInternalServerError, fmt.Errorf("failed to resolve revision %q: %v", rev, err)
}

// unreadableRef reports whether rev, or the reference before its ~ or ^ path
// when go-git found no revision, names a listed reference that fails to resolve alone.
func unreadableRef(repo *repository.Repository, refs map[string]string, rev string, err error) bool {
	if namesRef(refs, rev, repo.DefaultBranch()) {
		return true
	}
	i := strings.IndexAny(rev, "~^")
	if i < 0 || !errors.Is(err, repository.ErrRevisionNotFound) || !namesRef(refs, rev[:i], repo.DefaultBranch()) {
		return false
	}
	_, err = repo.ResolveRevision(rev[:i])
	return err != nil
}

// namesRef reports whether rev names a listed reference under go-git's rev-parse rules; Refs omits HEAD.
func namesRef(refs map[string]string, rev, defaultBranch string) bool {
	if rev == "HEAD" || rev == "@" {
		rev = defaultBranch
	}
	for _, rule := range plumbing.RefRevParseRules {
		if _, ok := refs[fmt.Sprintf(rule, rev)]; ok {
			return true
		}
	}
	return false
}

// handleCompare handles GET /api/{repoType}/{repo}/compare/{compare}
func (h *Handler) handleCompare(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := getRepoInformation(r)
	compare := vars["compare"]

	if !h.checkPermission(w, r, permission.OperationReadRepo, ri.RepoName, permission.Context{}) {
		return
	}
	repo, ok := h.openRepoChecked(w, r, ri.RepoName, false)
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
	repo, ok := h.openRepoDirect(w, ri.RepoName)
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

	name, email := commitAuthorIdentity(r.Context())

	message := req.Message
	if message == "" {
		message = "Super-squash branch '" + rev + "'"
	}

	if _, err := repo.SuperSquash(r.Context(), rev, message, name, email); err != nil {
		responseJSON(w, fmt.Errorf("failed to squash repository %q rev %q: %v", ri.RepoName, rev, err), http.StatusInternalServerError)
		return
	}

	h.afterReceivePack(r.Context(), ri.RepoName, []receive.RefUpdate{
		repo.RefUpdate(receive.BreakHash, receive.BreakHash, "refs/heads/"+rev),
	})

	w.WriteHeader(http.StatusOK)
}
