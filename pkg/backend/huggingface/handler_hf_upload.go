package huggingface

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gorilla/mux"

	"github.com/wzshiming/hfd/pkg/permission"
	"github.com/wzshiming/hfd/pkg/repository"
)

const (
	// lfsThreshold is the file size threshold for LFS upload mode.
	// Files larger than this will be uploaded via LFS.
	lfsThreshold = 10 * 1024 * 1024 // 10MB
)

// HFPreuploadRequest represents the preupload request body.
type HFPreuploadRequest struct {
	Files []HFPreuploadFile `json:"files"`
}

// HFPreuploadFile represents a file in the preupload request.
type HFPreuploadFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	Sample string `json:"sample"`
}

// HFPreuploadResponse represents the preupload response body.
type HFPreuploadResponse struct {
	Files []HFPreuploadResponseFile `json:"files"`
}

// HFPreuploadResponseFile represents a file in the preupload response.
type HFPreuploadResponseFile struct {
	Path         string `json:"path"`
	UploadMode   string `json:"uploadMode"`
	ShouldIgnore bool   `json:"shouldIgnore"`
}

// HFCreateRepoRequest represents the create repo request body.
type HFCreateRepoRequest struct {
	Type         string `json:"type"`
	Name         string `json:"name"`
	Organization string `json:"organization,omitempty"`
	Private      bool   `json:"private"`
}

// HFCreateRepoResponse represents the create repo response body.
type HFCreateRepoResponse struct {
	URL string `json:"url"`
}

// HFCommitResponse represents the commit response body.
type HFCommitResponse struct {
	CommitURL     string `json:"commitUrl"`
	CommitOid     string `json:"commitOid"`
	CommitMessage string `json:"commitMessage"`
}

// HFCommitOperation represents a single operation in the NDJSON commit request.
type HFCommitOperation struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

// HFCommitHeader represents the header operation in the commit request.
type HFCommitHeader struct {
	Summary      string `json:"summary"`
	Description  string `json:"description"`
	ParentCommit string `json:"parentCommit"`
}

// HFCommitFile represents a regular file operation in the commit request.
type HFCommitFile struct {
	Content  string `json:"content"`
	Path     string `json:"path"`
	Encoding string `json:"encoding"`
}

// HFCommitLFSFile represents an LFS file operation in the commit request.
type HFCommitLFSFile struct {
	Path string `json:"path"`
	Algo string `json:"algo"`
	OID  string `json:"oid"`
	Size int64  `json:"size"`
}

// HFCommitDeletedFile represents a delete file operation in the commit request.
type HFCommitDeletedFile struct {
	Path string `json:"path"`
}

func requestOrigin(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if fwdProto := r.Header.Get("X-Forwarded-Proto"); fwdProto != "" {
		scheme = fwdProto
	}
	return fmt.Sprintf("%s://%s", scheme, r.Host)
}

// handleValidateYAML handles POST /api/validate-yaml
// This endpoint is called by huggingface_hub to validate YAML front matter in files like README.md.
// https://github.com/huggingface/huggingface_hub/blob/8708631a463d9a6dc8ec7b046e748cc14844474e/src/huggingface_hub/repocard.py#L189-L224
func (h *Handler) handleValidateYAML(w http.ResponseWriter, r *http.Request) {
	// Return a successful validation response
	responseJSON(w, struct {
		Errors   []string `json:"errors"`
		Warnings []string `json:"warnings"`
	}{
		Errors:   []string{},
		Warnings: []string{},
	}, http.StatusOK)
}

func repoTypePrefix(repoType string) string {
	switch repoType {
	case "dataset":
		return "datasets"
	case "space":
		return "spaces"
	default:
		return ""
	}
}

// handleCreateRepo handles POST /api/repos/create
func (h *Handler) handleCreateRepo(w http.ResponseWriter, r *http.Request) {
	var req HFCreateRepoRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responseJSON(w, fmt.Errorf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	repoName := req.Name
	if req.Organization != "" {
		repoName = req.Organization + "/" + repoName
	}

	storageName := repoName
	prefix := repoTypePrefix(req.Type)
	if prefix != "" {
		storageName = prefix + "/" + repoName
	}

	if h.permissionHook != nil {
		if err := h.permissionHook(r.Context(), permission.OperationCreateRepo, storageName, permission.Context{}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	urlName := "/" + storageName

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), storageName)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("invalid repository name: %q", repoName), http.StatusBadRequest)
		return
	}

	// Check if repository already exists
	if repository.IsRepository(repoPath) {
		resp := HFCreateRepoResponse{
			URL: fmt.Sprintf("%s%s", requestOrigin(r), urlName),
		}
		responseJSON(w, resp, http.StatusOK)
		return
	}

	// Create repository directory
	if err := os.MkdirAll(filepath.Dir(repoPath), 0755); err != nil {
		responseJSON(w, fmt.Errorf("failed to create repository directory: %v", err), http.StatusInternalServerError)
		return
	}

	defaultBranch := "main"

	// Initialize bare repository
	repo, err := repository.Init(repoPath, defaultBranch)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to initialize repository: %v", err), http.StatusInternalServerError)
		return
	}

	// Create initial commit with default .gitattributes
	_, err = repo.CreateCommit(context.Background(), defaultBranch, "Initial commit", "HuggingFace", "hf@users.noreply.huggingface.co", []repository.CommitOperation{
		{
			Type:    repository.CommitOperationAdd,
			Path:    repository.GitattributesFileName,
			Content: repository.GitattributesText,
		},
	}, "")
	if err != nil {
		// Cleanup partially initialized repository so subsequent create
		// attempts don't see an inconsistent repo as already existing.
		_ = os.RemoveAll(repoPath)
		responseJSON(w, fmt.Errorf("failed to create initial commit: %v", err), http.StatusInternalServerError)
		return
	}

	resp := HFCreateRepoResponse{
		URL: fmt.Sprintf("%s%s", requestOrigin(r), urlName),
	}
	responseJSON(w, resp, http.StatusOK)
}

// handlePreupload handles POST /api/{repoType}/{repo}/preupload/{rev}
func (h *Handler) handlePreupload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := repoInfo(r)
	rev := vars["rev"]

	if h.permissionHook != nil {
		if err := h.permissionHook(r.Context(), permission.OperationUpdateRepo, ri.RepoPath, permission.Context{Ref: rev}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	var req HFPreuploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responseJSON(w, fmt.Errorf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), ri.RepoPath)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository not found"), http.StatusNotFound)
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

	gitAttrs, err := repo.GitAttributes(rev)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to read .gitattributes for repository %q: %v", ri.RepoPath, err), http.StatusInternalServerError)
		return
	}

	var respFiles []HFPreuploadResponseFile
	for _, file := range req.Files {
		uploadMode := "regular"
		if file.Size > lfsThreshold || gitAttrs.IsLFS(file.Path) {
			uploadMode = "lfs"
		}

		respFiles = append(respFiles, HFPreuploadResponseFile{
			Path:       file.Path,
			UploadMode: uploadMode,
		})
	}

	resp := HFPreuploadResponse{
		Files: respFiles,
	}
	responseJSON(w, resp, http.StatusOK)
}

// handleCommit handles POST /api/{repoType}/{repo}/commit/{rev}
func (h *Handler) handleCommit(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := repoInfo(r)
	rev := vars["rev"]

	if h.permissionHook != nil {
		if err := h.permissionHook(r.Context(), permission.OperationUpdateRepo, ri.RepoPath, permission.Context{Ref: rev}); err != nil {
			responseJSON(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	repoPath := repository.ResolvePath(h.storage.RepositoriesDir(), ri.RepoPath)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
		return
	}

	// Parse NDJSON body
	scanner := bufio.NewScanner(r.Body)
	scanner.Buffer(make([]byte, 1024*1024), 100*1024*1024) // Allow up to 100MB lines

	var header HFCommitHeader
	var ops []repository.CommitOperation

	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}

		var op HFCommitOperation
		if err := json.Unmarshal([]byte(line), &op); err != nil {
			responseJSON(w, fmt.Errorf("invalid NDJSON line: %v", err), http.StatusBadRequest)
			return
		}

		switch op.Key {
		case "header":
			if err := json.Unmarshal(op.Value, &header); err != nil {
				responseJSON(w, fmt.Errorf("invalid header: %v", err), http.StatusBadRequest)
				return
			}

		case "file":
			var file HFCommitFile
			if err := json.Unmarshal(op.Value, &file); err != nil {
				responseJSON(w, fmt.Errorf("invalid file operation: %v", err), http.StatusBadRequest)
				return
			}

			content := []byte(file.Content)
			if file.Encoding == "base64" {
				decoded, err := base64.StdEncoding.DecodeString(file.Content)
				if err != nil {
					responseJSON(w, fmt.Errorf("failed to decode base64 content for %s: %v", file.Path, err), http.StatusBadRequest)
					return
				}
				content = decoded
			}

			ops = append(ops, repository.CommitOperation{
				Type:    repository.CommitOperationAdd,
				Path:    file.Path,
				Content: content,
			})

		case "lfsFile":
			var lfsFile HFCommitLFSFile
			if err := json.Unmarshal(op.Value, &lfsFile); err != nil {
				responseJSON(w, fmt.Errorf("invalid LFS file operation: %v", err), http.StatusBadRequest)
				return
			}

			// Create an LFS pointer content
			pointerContent := fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", lfsFile.OID, lfsFile.Size)
			ops = append(ops, repository.CommitOperation{
				Type:    repository.CommitOperationAdd,
				Path:    lfsFile.Path,
				Content: []byte(pointerContent),
			})

		case "deletedFile":
			var deleted HFCommitDeletedFile
			if err := json.Unmarshal(op.Value, &deleted); err != nil {
				responseJSON(w, fmt.Errorf("invalid delete operation: %v", err), http.StatusBadRequest)
				return
			}

			ops = append(ops, repository.CommitOperation{
				Type: repository.CommitOperationDelete,
				Path: deleted.Path,
			})
		}
	}

	if err := scanner.Err(); err != nil {
		responseJSON(w, fmt.Errorf("failed to read request body: %v", err), http.StatusBadRequest)
		return
	}

	message := header.Summary
	if message == "" {
		message = "Upload files"
	}
	if header.Description != "" {
		message += "\n\n" + header.Description
	}

	// Open the repository
	repo, err := repository.Open(repoPath)
	if err != nil {
		if errors.Is(err, repository.ErrRepositoryNotExists) {
			responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoPath), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Errorf("failed to open repository %q: %v", ri.RepoPath, err), http.StatusInternalServerError)
		return
	}

	// TODO: Add support for specifying author/committer in the request body
	commitHash, err := repo.CreateCommit(r.Context(), rev, message, "HuggingFace", "hf@users.noreply.huggingface.co", ops, header.ParentCommit)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to create commit in repository %q: %v", ri.RepoPath, err), http.StatusInternalServerError)
		return
	}

	resp := HFCommitResponse{
		CommitURL:     fmt.Sprintf("%s/%s/commit/%s", requestOrigin(r), ri.RepoPath, commitHash),
		CommitOid:     commitHash,
		CommitMessage: message,
	}
	responseJSON(w, resp, http.StatusOK)
}
