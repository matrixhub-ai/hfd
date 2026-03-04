package huggingface

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"github.com/wzshiming/hfd/pkg/lfs"
	"github.com/wzshiming/hfd/pkg/repository"
)

func (h *Handler) handleTree(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	ri := repoInfo(r)
	revpath := vars["revpath"]

	query := r.URL.Query()
	recursive, _ := strconv.ParseBool(query.Get("recursive"))
	expand, _ := strconv.ParseBool(query.Get("expand"))

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

	rev, path, err := repo.SplitRevisionAndPath(revpath)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to parse rev and path for repository %q: %v", ri.RepoPath, err), http.StatusInternalServerError)
		return
	}

	entries, err := repo.Tree(rev, path, &repository.TreeOptions{
		Recursive: recursive,
		Expand:    expand,
	})
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to get tree for repo %q at rev %q and path %q: %v", ri.RepoPath, rev, path, err), http.StatusInternalServerError)
		return
	}

	responseJSON(w, entries, http.StatusOK)
}

// HFTreeSize represents the response for the Get folder size API.
type HFTreeSize struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

// handleTreeSize handles GET /api/{repoType}/{namespace}/{repo}/treesize/{revpath}
func (h *Handler) handleTreeSize(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	ri := repoInfo(r)
	revpath := vars["revpath"]

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

	rev, path, err := repo.SplitRevisionAndPath(revpath)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to parse rev and path for repository %q: %v", ri.RepoPath, err), http.StatusInternalServerError)
		return
	}

	size, err := repo.TreeSize(rev, path)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to get tree size for repo %q at rev %q and path %q: %v", ri.RepoPath, rev, path, err), http.StatusInternalServerError)
		return
	}

	responseJSON(w, HFTreeSize{
		Path: "/" + path,
		Size: size,
	}, http.StatusOK)
}

// handleResolve handles the /{repo_id}/resolve/{revision}/{path} endpoint
// This is used by huggingface_hub to download files
func (h *Handler) handleResolve(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	ri := repoInfo(r)
	revpath := vars["revpath"]

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

	rev, path, err := repo.SplitRevisionAndPath(revpath)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to parse rev and path for repository %q: %v", ri.RepoPath, err), http.StatusInternalServerError)
		return
	}

	// Get commit hash for the HuggingFace client requirements
	commits, err := repo.Commits(rev, 1)
	commitHash := ""
	if err == nil && len(commits) > 0 {
		commitHash = commits[0].SHA
	}

	blob, err := repo.Blob(rev, path)
	if err != nil {
		responseJSON(w, fmt.Errorf("file %q not found in repository %q at revision %q", path, ri.RepoPath, rev), http.StatusNotFound)
		return
	}

	// Check if this is an LFS pointer file
	if blob.Size() <= lfs.MaxLFSPointerSize {
		reader, err := blob.NewReader()
		if err == nil {
			defer func() {
				_ = reader.Close()
			}()
			ptr, err := lfs.DecodePointer(reader)
			if err == nil && ptr != nil {
				// This is an LFS file, redirect to the LFS object
				// Set HuggingFace-required headers before redirect
				w.Header().Set("X-Repo-Commit", commitHash)
				w.Header().Set("ETag", fmt.Sprintf("\"%s\"", ptr.Oid))

				if !h.storage.LFSStore().Exists(ptr.Oid) {
					// Try proxy fetch if proxy manager is configured
					if h.lfsProxyManager != nil {
						sourceURL := h.getLFSProxySourceURL(repoPath)
						if sourceURL != "" {
							h.lfsProxyManager.FetchFromProxy(context.Background(), sourceURL, []lfs.LFSObject{
								{Oid: ptr.Oid, Size: ptr.Size},
							})

							pf := h.lfsProxyManager.GetFlight(ptr.Oid)
							// TODO(@wzshiming) We should ideally have a better way to wait for the proxy fetch to complete instead of polling like this,
							// but this is good enough for now since the client will retry if the file is not ready yet.
							for i := 0; i != 5; i++ {
								time.Sleep(500 * time.Millisecond)
								pf = h.lfsProxyManager.GetFlight(ptr.Oid)
								if pf != nil {
									break
								}
							}

							if pf != nil {
								rs := pf.NewReadSeeker()
								defer rs.Close()
								http.ServeContent(w, r, ptr.Oid, time.Now(), rs)
								return
							}
						}
					}
					responseJSON(w, fmt.Errorf("LFS object %q not found for file %q in repository %q at revision %q", ptr.Oid, path, ri.RepoPath, rev), http.StatusNotFound)
					return
				}
				if signer, ok := h.storage.LFSStore().(lfs.SignGetter); ok {
					url, err := signer.SignGet(ptr.Oid)
					if err != nil {
						responseJSON(w, fmt.Errorf("failed to sign URL for LFS object %q: %v", ptr.Oid, err), http.StatusInternalServerError)
						return
					}
					http.Redirect(w, r, url, http.StatusTemporaryRedirect)
					return
				}
				if getter, ok := h.storage.LFSStore().(lfs.Getter); ok {
					content, stat, err := getter.Get(ptr.Oid)
					if err != nil {
						if os.IsNotExist(err) {
							responseJSON(w, fmt.Errorf("LFS object %q not found for file %q in repository %q at revision %q", ptr.Oid, path, ri.RepoPath, rev), http.StatusNotFound)
							return
						}
						responseJSON(w, fmt.Errorf("failed to get LFS object %q: %v", ptr.Oid, err), http.StatusInternalServerError)
						return
					}
					defer func() {
						_ = content.Close()
					}()

					http.ServeContent(w, r, ptr.Oid, stat.ModTime(), content)
					return
				}
				responseJSON(w, fmt.Errorf("LFS store does not support direct content retrieval for object %q", ptr.Oid), http.StatusNotImplemented)
				return
			}
		}
	}

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
		responseJSON(w, fmt.Errorf("failed to get blob reader for file %q in repository %q at revision %q: %v", path, ri.RepoPath, rev, err), http.StatusInternalServerError)
		return
	}
	defer func() {
		_ = reader.Close()
	}()

	_, err = io.Copy(w, reader)
	if err != nil {
		// Log but don't send error - we may have already written partial content
		return
	}
}

// getLFSProxySourceURL returns the upstream LFS source URL for a proxied mirror repository.
// Returns empty string if proxy is not configured or the repo is not a mirror.
func (h *Handler) getLFSProxySourceURL(repoPath string) string {
	if h.lfsProxyManager == nil {
		return ""
	}

	repo, err := repository.Open(repoPath)
	if err != nil {
		return ""
	}

	isMirror, sourceURL, err := repo.IsMirror()
	if err != nil || !isMirror || sourceURL == "" {
		return ""
	}

	return sourceURL
}
