package hf

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"github.com/wzshiming/hfd/pkg/lfs"
	"github.com/wzshiming/hfd/pkg/permission"
	"github.com/wzshiming/hfd/pkg/repository"
)

func (h *Handler) handleTree(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	ri := getRepoInformation(r)
	revpath := vars["revpath"]

	query := r.URL.Query()
	recursive, _ := strconv.ParseBool(query.Get("recursive"))
	expand, _ := strconv.ParseBool(query.Get("expand"))

	if h.permissionHookFunc != nil {
		if err := h.permissionHookFunc(r.Context(), permission.OperationReadRepo, ri.RepoName, permission.Context{}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), ri.RepoName)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoName), http.StatusNotFound)
		return
	}

	repo, err := h.openRepo(r.Context(), repoPath, ri.RepoName, repository.GitUploadPack)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoName), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", ri.RepoName, err), http.StatusInternalServerError)
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

	responseJSON(w, toHFTreeEntries(entries, expand), http.StatusOK)
}

func toHFTreeEntries(entries []*repository.TreeEntry, expand bool) []treeEntry {
	result := make([]treeEntry, len(entries))
	for i, e := range entries {
		result[i] = treeEntry{
			OID:  e.OID(),
			Path: e.Path(),
			Type: e.Type(),
			Size: e.Size(),
		}
		if ptr := e.LFSPointer(); ptr != nil {
			result[i].LFS = &lfsPointer{
				OID:         ptr.OID(),
				Size:        ptr.Size(),
				PointerSize: e.Size(),
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

	if h.permissionHookFunc != nil {
		if err := h.permissionHookFunc(r.Context(), permission.OperationReadRepo, ri.RepoName, permission.Context{}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), ri.RepoName)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoName), http.StatusNotFound)
		return
	}

	repo, err := h.openRepo(r.Context(), repoPath, ri.RepoName, repository.GitUploadPack)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoName), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", ri.RepoName, err), http.StatusInternalServerError)
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

	if h.permissionHookFunc != nil {
		if err := h.permissionHookFunc(r.Context(), permission.OperationReadRepo, ri.RepoName, permission.Context{}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), ri.RepoName)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoName), http.StatusNotFound)
		return
	}

	repo, err := h.openRepo(r.Context(), repoPath, ri.RepoName, repository.GitUploadPack)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoName), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", ri.RepoName, err), http.StatusInternalServerError)
		return
	}

	rev, path, err := repo.SplitRevisionAndPath(revpath)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to parse rev and path for repository %q: %v", ri.RepoName, err), http.StatusInternalServerError)
		return
	}

	// Get commit hash for the HuggingFace client requirements
	commits, err := repo.Commits(rev, 1, nil)
	commitHash := ""
	if err == nil && len(commits) > 0 {
		commitHash = commits[0].Hash().String()
	}

	blob, err := repo.Blob(rev, path)
	if err != nil {
		responseJSON(w, fmt.Errorf("file %q not found in repository %q at revision %q", path, ri.RepoName, rev), http.StatusNotFound)
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
				w.Header().Set("ETag", fmt.Sprintf("\"%s\"", ptr.OID()))

				if h.mirror != nil && !h.lfsStore.Exists(ptr.OID()) {
					// Try tee cache fetch if configured
					if h.mirror != nil {
						sourceURL, started, err := h.mirror.StartLFSFetch(r.Context(), ri.RepoName, []lfs.LFSObject{
							{Oid: ptr.OID(), Size: ptr.Size()},
						})
						if err != nil {
							responseJSON(w, fmt.Errorf("failed to fetch LFS object %q from upstream source %q: %v", ptr.OID(), sourceURL, err), http.StatusInternalServerError)
							return
						}

						if started {
							pf := h.mirror.Get(ptr.OID())
							rs := pf.NewReadSeeker()
							defer rs.Close()
							http.ServeContent(w, r, ptr.OID(), time.Now(), rs)
							return
						}
					}
					responseJSON(w, fmt.Errorf("LFS object %q not found for file %q in repository %q at revision %q", ptr.OID(), path, ri.RepoName, rev), http.StatusNotFound)
					return
				}
				if signer, ok := h.lfsStore.(lfs.SignGetter); ok {
					url, err := signer.SignGet(ptr.OID())
					if err != nil {
						responseJSON(w, fmt.Errorf("failed to sign URL for LFS object %q: %v", ptr.OID(), err), http.StatusInternalServerError)
						return
					}
					http.Redirect(w, r, url, http.StatusTemporaryRedirect)
					return
				}
				if getter, ok := h.lfsStore.(lfs.Getter); ok {
					content, stat, err := getter.Get(ptr.OID())
					if err != nil {
						if os.IsNotExist(err) {
							responseJSON(w, fmt.Errorf("LFS object %q not found for file %q in repository %q at revision %q", ptr.OID(), path, ri.RepoName, rev), http.StatusNotFound)
							return
						}
						responseJSON(w, fmt.Errorf("failed to get LFS object %q: %v", ptr.OID(), err), http.StatusInternalServerError)
						return
					}
					defer func() {
						_ = content.Close()
					}()

					http.ServeContent(w, r, ptr.OID(), stat.ModTime(), content)
					return
				}
				responseJSON(w, fmt.Errorf("LFS store does not support direct content retrieval for object %q", ptr.OID()), http.StatusNotImplemented)
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
		responseJSON(w, fmt.Errorf("failed to get blob reader for file %q in repository %q at revision %q: %v", path, ri.RepoName, rev, err), http.StatusInternalServerError)
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
