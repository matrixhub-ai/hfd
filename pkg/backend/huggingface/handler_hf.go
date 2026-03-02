package huggingface

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"github.com/wzshiming/hfd/pkg/lfs"
	"github.com/wzshiming/hfd/pkg/repository"
)

// HFModelInfo represents the model info response for HuggingFace API
type HFModelInfo struct {
	ID            string      `json:"id"`
	ModelID       string      `json:"modelId"`
	SHA           string      `json:"sha"`
	Private       bool        `json:"private"`
	Disabled      bool        `json:"disabled"`
	Gated         bool        `json:"gated"`
	Downloads     int         `json:"downloads"`
	Likes         int         `json:"likes"`
	Tags          []string    `json:"tags"`
	Siblings      []HFSibling `json:"siblings"`
	CreatedAt     string      `json:"createdAt,omitempty"`
	LastModified  string      `json:"lastModified,omitempty"`
	DefaultBranch string      `json:"defaultBranch,omitempty"`
}

// HFSibling represents a file in the model repository
type HFSibling struct {
	RFilename string `json:"rfilename"`
}

// handleInfo handles the /api/models/{repo_id} endpoint
// This is used by huggingface_hub to get model metadata
func (h *Handler) handleInfo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repoName := vars["repo"]

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), repoStorageName(r))
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
		return
	}

	repo, err := h.openRepo(r.Context(), repoPath, repoStorageName(r))
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}

	defaultBranch := repo.DefaultBranch()

	// Get list of files in the repository (recursive to include files in subdirectories)
	hfEntries, err := repo.Tree(defaultBranch, "", &repository.TreeOptions{Recursive: true})
	if err != nil {
		// Return empty siblings if we can't get the tree
		hfEntries = nil
	}

	var siblings []HFSibling
	for _, entry := range hfEntries {
		if entry.Type == repository.EntryTypeFile {
			siblings = append(siblings, HFSibling{
				RFilename: entry.Path,
			})
		}
	}

	// Get the latest commit SHA if available
	sha := ""
	commits, err := repo.Commits(defaultBranch, 1)
	if err == nil && len(commits) > 0 {
		sha = commits[0].SHA
	}

	modelInfo := HFModelInfo{
		ID:            repoName,
		ModelID:       repoName,
		SHA:           sha,
		Private:       false,
		Disabled:      false,
		Gated:         false,
		Downloads:     0,
		Likes:         0,
		Tags:          []string{},
		Siblings:      siblings,
		DefaultBranch: defaultBranch,
	}

	responseJSON(w, modelInfo, http.StatusOK)
}

func (h *Handler) handleTree(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	repoName := vars["repo"]
	refpath := vars["refpath"]

	query := r.URL.Query()
	recursive, _ := strconv.ParseBool(query.Get("recursive"))
	expand, _ := strconv.ParseBool(query.Get("expand"))

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), repoStorageName(r))
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
		return
	}

	repo, err := h.openRepo(r.Context(), repoPath, repoStorageName(r))
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}

	ref, path, err := repo.SplitRevisionAndPath(refpath)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to parse ref and path for repository %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}

	entries, err := repo.Tree(ref, path, &repository.TreeOptions{
		Recursive: recursive,
		Expand:    expand,
	})
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to get tree for repo %q at ref %q and path %q: %v", repoName, ref, path, err), http.StatusInternalServerError)
		return
	}

	responseJSON(w, entries, http.StatusOK)
}

// handleInfoRevision handles the /api/models/{repo_id}/revision/{revision} endpoint
// This is used by huggingface_hub for snapshot_download to get model info at specific revision
func (h *Handler) handleInfoRevision(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	repoName := vars["repo"]
	ref := vars["revision"]

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), repoStorageName(r))
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
		return
	}

	repo, err := h.openRepo(r.Context(), repoPath, repoStorageName(r))
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}

	// Get list of files in the repository at the specified revision (recursive to include files in subdirectories)
	hfEntries, err := repo.Tree(ref, "", &repository.TreeOptions{Recursive: true})
	if err != nil {
		//TODO(@wzshiming): In hf binarys, it only use main as default branch,
		// if the ref is main but not exist, we can try to fallback to the real default branch to be compatible with hf client.
		defaultBranch := repo.DefaultBranch()
		if ref == "main" && defaultBranch != ref {
			hfEntries, err = repo.Tree(defaultBranch, "", &repository.TreeOptions{Recursive: true})
			if err != nil {
				hfEntries = nil
			} else {
				ref = defaultBranch
			}
		}
	}

	var siblings []HFSibling
	for _, entry := range hfEntries {
		if entry.Type == repository.EntryTypeFile {
			siblings = append(siblings, HFSibling{
				RFilename: entry.Path,
			})
		}
	}

	// Get the commit SHA for this revision
	sha := ""
	commits, err := repo.Commits(ref, 1)
	if err == nil && len(commits) > 0 {
		sha = commits[0].SHA
	}

	modelInfo := HFModelInfo{
		ID:            repoName,
		ModelID:       repoName,
		SHA:           sha,
		Private:       false,
		Disabled:      false,
		Gated:         false,
		Downloads:     0,
		Likes:         0,
		Tags:          []string{},
		Siblings:      siblings,
		DefaultBranch: repo.DefaultBranch(),
	}

	responseJSON(w, modelInfo, http.StatusOK)
}

// handleResolve handles the /{repo_id}/resolve/{revision}/{path} endpoint
// This is used by huggingface_hub to download files
func (h *Handler) handleResolve(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	repoName := vars["repo"]
	refpath := vars["refpath"]

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), repoStorageName(r))
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
		return
	}

	repo, err := h.openRepo(r.Context(), repoPath, repoStorageName(r))
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", repoName), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}

	ref, path, err := repo.SplitRevisionAndPath(refpath)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to parse ref and path for repository %q: %v", repoName, err), http.StatusInternalServerError)
		return
	}

	// Get commit hash for the HuggingFace client requirements
	commits, err := repo.Commits(ref, 1)
	commitHash := ""
	if err == nil && len(commits) > 0 {
		commitHash = commits[0].SHA
	}

	blob, err := repo.Blob(ref, path)
	if err != nil {
		responseJSON(w, fmt.Errorf("file %q not found in repository %q at revision %q", path, repoName, ref), http.StatusNotFound)
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
				if h.storage.S3Store() != nil {
					url, err := h.storage.S3Store().SignGet(ptr.Oid)
					if err != nil {
						responseJSON(w, fmt.Errorf("failed to sign S3 URL for LFS object %q: %v", ptr.Oid, err), http.StatusInternalServerError)
						return
					}
					http.Redirect(w, r, url, http.StatusTemporaryRedirect)
					return
				}
				content, stat, err := h.storage.ContentStore().Get(ptr.Oid)
				if err != nil {
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
					responseJSON(w, fmt.Errorf("LFS object %q not found for file %q in repository %q at revision %q", ptr.Oid, path, repoName, ref), http.StatusNotFound)
					return
				}
				defer func() {
					_ = content.Close()
				}()

				http.ServeContent(w, r, ptr.Oid, stat.ModTime(), content)
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
		responseJSON(w, fmt.Errorf("failed to get blob reader for file %q in repository %q at revision %q: %v", path, repoName, ref, err), http.StatusInternalServerError)
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
