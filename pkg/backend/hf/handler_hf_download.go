package hf

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"strconv"

	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// treeErrorStatus maps a missing revision or path to 404; anything else is a server error.
func treeErrorStatus(err error) int {
	if errors.Is(err, repository.ErrRevisionNotFound) || errors.Is(err, object.ErrEntryNotFound) || errors.Is(err, object.ErrDirectoryNotFound) {
		return http.StatusNotFound
	}
	return http.StatusInternalServerError
}

func (h *Handler) handleTree(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	ri := getRepoInformation(r)
	revpath := vars["revpath"]

	query := r.URL.Query()
	recursive, err := queryBool(query, "recursive")
	if err != nil {
		responseJSON(w, err, http.StatusBadRequest)
		return
	}
	expand, err := queryBool(query, "expand")
	if err != nil {
		responseJSON(w, err, http.StatusBadRequest)
		return
	}

	if !h.checkPermission(w, r, permission.OperationReadRepo, ri.RepoName, permission.Context{}) {
		return
	}
	repo, ok := h.openRepoChecked(w, r, ri.RepoName, false)
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
		responseJSON(w, fmt.Errorf("failed to get tree for repo %q at rev %q and path %q: %v", ri.RepoName, rev, path, err), treeErrorStatus(err))
		return
	}

	responseJSON(w, toHFTreeEntries(r.Context(), entries, expand), http.StatusOK)
}

// queryBool reads an optional boolean query flag; an unset or empty value is false.
func queryBool(query url.Values, key string) (bool, error) {
	value := query.Get(key)
	if value == "" {
		return false, nil
	}
	flag, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("invalid %s value %q: expected a boolean", key, value)
	}
	return flag, nil
}

func toHFTreeEntries(ctx context.Context, entries []*repository.TreeEntry, expand bool) []treeEntry {
	result := make([]treeEntry, 0, len(entries))
	for _, e := range entries {
		item := treeEntry{
			OID:  e.Hash().String(),
			Path: e.Path(),
			Type: e.Type(),
		}
		if e.Type() == repository.EntryTypeFile {
			blob, err := e.Blob()
			if err != nil {
				slog.WarnContext(ctx, "failed to get blob for tree entry, skipping", "path", e.Path(), "error", err)
				continue
			}
			item.Size = blob.Size()
			if ptr, _ := blob.LFSPointer(); ptr != nil {
				item.LFS = &lfsPointer{
					OID:         ptr.OID(),
					Size:        ptr.Size(),
					PointerSize: blob.Size(),
				}
				item.Size = ptr.Size()
			}
		}
		if lastCommit := e.LastCommit(); expand && lastCommit != nil {
			item.LastCommit = &treeLastCommit{
				ID:    lastCommit.Hash().String(),
				Title: lastCommit.Title(),
				Date:  lastCommit.Author().When().UTC().Format(repository.TimeFormat),
			}
		}
		result = append(result, item)
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
	repo, ok := h.openRepoChecked(w, r, ri.RepoName, false)
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
		responseJSON(w, fmt.Errorf("failed to get tree size for repo %q at rev %q and path %q: %v", ri.RepoName, rev, path, err), treeErrorStatus(err))
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
	repo, ok := h.openRepoChecked(w, r, ri.RepoName, false)
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
			if fileHash := h.mirror.FileHash(r.Context(), ptr.OID()); fileHash != "" {
				base := h.mirror.ExternalBase(r)
				lfs.SetObjectHeaders(w, ptr.OID(), ptr.Size())
				lfs.SetXETLinkHeaders(w, fileHash, base, base+"/api/"+ri.RepoType+"/"+ri.FullName+"/xet-read-token/"+url.PathEscape(rev))
				// huggingface_hub >= 1.30 follows same-host redirects on its
				// metadata HEAD and reads the headers off the final response,
				// which the bridge cannot supply; answer the probe here.
				if ptr.Size() == 0 || r.Method == http.MethodHead {
					w.Header().Set("Content-Length", strconv.FormatInt(ptr.Size(), 10))
					w.WriteHeader(http.StatusOK)
					return
				}
				http.Redirect(w, r, base+"/xet-bridge/"+ptr.OID(), http.StatusFound)
				return
			}
			resolveRev := commitHash
			if resolveRev == "" {
				resolveRev = rev
			}
			// Register the pointer's target so the OID-keyed data plane
			// (and later LFS batch lookups) also covers revisions outside
			// pull-scan tips. ServeOID streams while ingesting and ingests
			// on miss; prefer the commit hash so cache entries stay
			// immutable.
			h.mirror.RegisterObject(ptr.OID(), ri.RepoName, resolveRev, path, ptr.Size())
			if h.mirror.ServeOID(w, r, ptr.OID()) {
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
