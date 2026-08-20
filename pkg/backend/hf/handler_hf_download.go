package hf

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

func (h *Handler) handleTree(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	ri := getRepoInformation(r)
	revpath := vars["revpath"]

	query := r.URL.Query()
	recursive, _ := strconv.ParseBool(query.Get("recursive"))
	expand, _ := strconv.ParseBool(query.Get("expand"))

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

	rev, path, err := repo.SplitRevisionAndPath(revpath)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to parse rev and path for repository %q: %v", ri.RepoName, err), http.StatusInternalServerError)
		return
	}

	entries, err := repo.Tree(rev, path, &repository.TreeOptions{
		Recursive: recursive,
	})
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to get tree for repo %q at rev %q and path %q: %v", ri.RepoName, rev, path, err), http.StatusInternalServerError)
		return
	}

	responseJSON(w, toHFTreeEntries(r.Context(), entries, expand), http.StatusOK)
}

func toHFTreeEntries(ctx context.Context, entries []*repository.TreeEntry, expand bool) []treeEntry {
	result := make([]treeEntry, len(entries))
	for i, e := range entries {

		blob, err := e.Blob()
		if err != nil {
			slog.WarnContext(ctx, "failed to get blob for tree entry, skipping", "path", e.Path(), "error", err)
			continue
		}

		result[i] = treeEntry{
			OID:  e.Hash().String(),
			Path: e.Path(),
			Type: e.Type(),
			Size: blob.Size(),
		}
		if ptr, _ := blob.LFSPointer(); ptr != nil {
			result[i].LFS = &lfsPointer{
				OID:         ptr.OID(),
				Size:        ptr.Size(),
				PointerSize: blob.Size(),
			}
			result[i].Size = ptr.Size()
		}
		if lastCommit := e.LastCommit(); expand && lastCommit != nil {
			result[i].LastCommit = &treeLastCommit{
				ID:    lastCommit.Hash().String(),
				Title: lastCommit.Title(),
				Date:  lastCommit.Author().When().UTC().Format(repository.TimeFormat),
			}
		}
	}
	return result
}

// handleTreeSize handles GET /api/{repoType}/{namespace}/{repo}/treesize/{revpath}
func (h *Handler) handleTreeSize(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	ri := getRepoInformation(r)
	revpath := vars["revpath"]

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

	rev, path, err := repo.SplitRevisionAndPath(revpath)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to parse rev and path for repository %q: %v", ri.RepoName, err), http.StatusInternalServerError)
		return
	}

	size, err := repo.TreeSize(rev, path)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to get tree size for repo %q at rev %q and path %q: %v", ri.RepoName, rev, path, err), http.StatusInternalServerError)
		return
	}

	responseJSON(w, treeSize{
		Path: "/" + path,
		Size: size,
	}, http.StatusOK)
}

// handleResolve handles the /{repo_id}/resolve/{revision}/{path} endpoint
// This is used by huggingface_hub to download files
func (h *Handler) handleResolve(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	ri := getRepoInformation(r)
	revpath := vars["revpath"]

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

	rev, path, err := repo.SplitRevisionAndPath(revpath)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to parse rev and path for repository %q: %v", ri.RepoName, err), http.StatusInternalServerError)
		return
	}

	// Get commit hash for the HuggingFace client requirements
	commits, err := repo.Commits(rev, &repository.CommitsOptions{Limit: 1})
	commitHash := ""
	if err == nil && len(commits) > 0 {
		commitHash = commits[0].Hash().String()
	}

	blob, err := repo.Blob(rev, path)
	if err != nil {
		responseJSON(w, fmt.Errorf("file %q not found in repository %q at revision %q", path, ri.RepoName, rev), http.StatusNotFound)
		return
	}

	if ptr, _ := blob.LFSPointer(); ptr != nil {
		name := blob.Name()
		w.Header().Set("Content-Disposition", mime.FormatMediaType("inline", map[string]string{"filename": name}))

		// This is an LFS file, serve it from the xet data plane
		// Set HuggingFace-required headers first
		w.Header().Set("X-Repo-Commit", commitHash)

		if h.mirror != nil {
			// Hub parity: fully ingested files answer with metadata and a
			// redirect to the sha256 bridge; only in-flight ingests stream
			// bytes on this response (via the mirror's spool).
			if h.mirror.ServeIngestedRedirect(w, r, ptr.OID()) {
				return
			}
			// Delegate to the xet mirror data plane: it streams while
			// ingesting and ingests on miss. Prefer the commit hash so cache
			// entries stay immutable.
			resolveRev := commitHash
			if resolveRev == "" {
				resolveRev = rev
			}
			if h.mirror.ServeResolve(w, r, ri.RepoName, resolveRev, path) {
				return
			}
		}
		responseJSON(w, fmt.Errorf("LFS object %q not found for file %q in repository %q at revision %q", ptr.OID(), path, ri.RepoName, rev), http.StatusNotFound)
		return
	}

	name := blob.Name()
	w.Header().Set("Content-Disposition", mime.FormatMediaType("inline", map[string]string{"filename": name}))

	// Set HuggingFace-required headers
	// X-Repo-Commit is required by huggingface_hub to identify the commit
	w.Header().Set("X-Repo-Commit", commitHash)

	w.Header().Set("ETag", fmt.Sprintf("\"%s\"", blob.Hash()))

	// Serve regular file content
	w.Header().Set("Content-Length", strconv.FormatInt(blob.Size(), 10))
	w.Header().Set("Last-Modified", blob.ModTime().UTC().Format(http.TimeFormat))

	// Handle HEAD request
	if r.Method == http.MethodHead {
		return
	}

	reader, err := blob.NewReader()
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to get blob reader for file %q in repository %q at revision %q: %v", path, ri.RepoName, rev, err), http.StatusInternalServerError)
		return
	}
	defer func() {
		_ = reader.Close()
	}()

	if r.Header.Get("Range") != "" {
		// TODO: Unfortunately, go-git does not support ranged reading of blobs,
		// so we have to read the entire content into memory before serving.
		// This is not ideal for large files.
		// We should consider implementing ranged reading in go-git in the future.
		content, err := io.ReadAll(reader)
		if err != nil {
			responseJSON(w, fmt.Errorf("failed to read blob content for file %q in repository %q at revision %q: %v", path, ri.RepoName, rev, err), http.StatusInternalServerError)
			return
		}
		http.ServeContent(w, r, blob.Name(), blob.ModTime(), bytes.NewReader(content))
	} else {
		_, err = io.Copy(w, reader)
		if err != nil {
			// Log but don't send error - we may have already written partial content
			return
		}
	}
}
