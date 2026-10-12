package hf

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

func commitAuthorIdentity(ctx context.Context) (name, email string) {
	id := authenticate.IdentityFrom(ctx)
	if authenticate.IsAnonymous(id) {
		return "HuggingFace", "hf@users.noreply.huggingface.co"
	}
	return id.Name(), id.Email()
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
	case "kernel":
		return "kernels"
	default:
		return ""
	}
}

// handleCreateRepo handles POST /api/repos/create
func (h *Handler) handleCreateRepo(w http.ResponseWriter, r *http.Request) {
	var req CreateRepoRequest
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

	if !h.checkPermission(w, r, permission.OperationCreateRepo, storageName, permission.Context{}) {
		return
	}

	if repository.ResolvePath(storageName) == "" {
		responseJSON(w, fmt.Errorf("invalid repository name: %q", repoName), http.StatusBadRequest)
		return
	}

	updates, err := h.createRepoFunc(r.Context(), storageName, req)
	if err != nil {
		respondCatalogError(w, err)
		return
	}
	h.afterReceivePack(r.Context(), storageName, updates)

	resp := createRepoResponse{
		URL: fmt.Sprintf("%s/%s", requestOrigin(r), storageName),
	}
	responseJSON(w, resp, http.StatusOK)
}

// handlePreupload handles POST /api/{repoType}/{repo}/preupload/{rev}
func (h *Handler) handlePreupload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := getRepoInformation(r)
	rev := vars["rev"]

	if !h.checkPermission(w, r, permission.OperationUpdateRepo, ri.RepoName, permission.Context{Ref: rev}) {
		return
	}

	var req preuploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responseJSON(w, fmt.Errorf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	repo, ok := h.openRepoDirect(w, ri.RepoName)
	if !ok {
		return
	}

	var gitAttrs *repository.GitAttributes
	var err error
	if req.GitAttributes != nil {
		gitAttrs = repository.ParseGitAttributes([]byte(*req.GitAttributes))
	} else if gitAttrs, err = repo.GitAttributes(rev); err != nil {
		responseJSON(w, fmt.Errorf("failed to read .gitattributes for repository %q: %v", ri.RepoName, err), http.StatusInternalServerError)
		return
	}

	var respFiles []preuploadResponseFile
	for _, file := range req.Files {
		uploadMode := "regular"
		if gitAttrs.IsLFS(file.Path) || h.routesToLFS(file) {
			uploadMode = "lfs"
		}

		respFiles = append(respFiles, preuploadResponseFile{
			Path:       file.Path,
			UploadMode: uploadMode,
		})
	}

	resp := preuploadResponse{
		Files: respFiles,
	}
	responseJSON(w, resp, http.StatusOK)
}

// routesToLFS reports whether huggingface.co's preupload sends file to LFS by its size and sample alone.
func (h *Handler) routesToLFS(file preuploadFile) bool {
	p := h.lfsPolicy
	if p.MaxRegularFileSize > 0 && file.Size > p.MaxRegularFileSize {
		return true
	}
	if p.MaxBinaryFileSize <= 0 || file.Size <= p.MaxBinaryFileSize {
		return false
	}
	sample, err := base64.StdEncoding.DecodeString(file.Sample)
	if err != nil {
		sample = nil // the Hub only checks that the sample is a string
	}
	// The Hub asks isbinaryfile alone; asking libgit2 too sends to LFS the text its commit refuses as
	// binary (UTF-16, control bytes) whenever the sample shows it.
	return lfs.IsBinarySample(sample) || lfs.IsBinaryContent(sample)
}

// policyError is huggingface.co's refusal of the blobs ops add to ref, nil when the policy accepts them.
// Large files outrank binary ones, and each content is named once, at its first path.
func (h *Handler) policyError(ops []repository.CommitOperation, ref string) error {
	var large, binary []lfs.OffendingFile
	seen := map[[sha256.Size]byte]bool{}
	for _, op := range ops {
		v := h.lfsPolicy.Check(int64(len(op.Content)), op.Content[:min(len(op.Content), lfs.BinarySniffSize)])
		if v == lfs.ViolationNone {
			continue
		}
		sum := sha256.Sum256(op.Content)
		if seen[sum] {
			continue
		}
		seen[sum] = true
		file := lfs.OffendingFile{Path: op.Path, Ref: ref}
		if v == lfs.ViolationLarge {
			large = append(large, file)
		} else {
			binary = append(binary, file)
		}
	}
	switch {
	case len(large) > 0:
		return &lfs.PolicyError{Kind: lfs.ViolationLarge, Files: large, Limit: h.lfsPolicy.MaxRegularFileSize}
	case len(binary) > 0:
		return &lfs.PolicyError{Kind: lfs.ViolationBinary, Files: binary}
	}
	return nil
}

// commitGitattributes returns the .gitattributes ops leave on branch: the last op on the file decides
// (nil for a delete), else the branch tip's.
func commitGitattributes(repo *repository.Repository, branch string, ops []repository.CommitOperation) ([]byte, error) {
	for _, op := range slices.Backward(ops) {
		if op.Path == repository.GitattributesFileName {
			return op.Content, nil
		}
	}
	return repo.BranchGitattributes(branch)
}

// handleCommit handles POST /api/{repoType}/{repo}/commit/{rev}
func (h *Handler) handleCommit(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := getRepoInformation(r)
	rev := vars["rev"]

	if !h.checkPermission(w, r, permission.OperationUpdateRepo, ri.RepoName, permission.Context{Ref: rev}) {
		return
	}

	name, email := commitAuthorIdentity(r.Context())

	// Parse NDJSON body
	scanner := bufio.NewScanner(r.Body)
	scanner.Buffer(make([]byte, 1024*1024), 100*1024*1024) // Allow up to 100MB lines

	var header commitHeader
	var ops []repository.CommitOperation
	var lfsPaths []string

	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}

		var op commitOperation
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
			var file commitFile
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
			var lfsFile commitLFSFile
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
			lfsPaths = append(lfsPaths, lfsFile.Path)

		case "deletedFile":
			var deleted commitDeletedFile
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

	if err := h.policyError(ops, plumbing.NewBranchReferenceName(rev).String()); err != nil {
		responseJSON(w, err, http.StatusBadRequest)
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
	repo, ok := h.openRepoChecked(w, r, ri.RepoName, true)
	if !ok {
		return
	}

	// The branch head before the commit; ZeroHash when the branch is new.
	oldRev := header.ParentCommit
	if oldRev == "" {
		oldRev, _ = repo.RefHash(plumbing.NewBranchReferenceName(rev))
		if oldRev == "" {
			oldRev = receive.ZeroHash
		}
	}

	// Mock pre-receive hook with current branch head as OldRev
	if h.preReceiveHookFunc != nil {
		if !h.checkPreReceive(w, r, ri.RepoName, []receive.RefUpdate{
			repo.RefUpdate(oldRev, receive.ZeroHash, "refs/heads/"+rev),
		}, "pre-receive hook denied the commit") {
			return
		}
	}

	// Like huggingface.co, route every uploaded LFS file to LFS in the commit that adds it.
	if len(lfsPaths) > 0 {
		attrs, err := commitGitattributes(repo, rev, ops)
		if err != nil {
			responseJSON(w, fmt.Errorf("failed to read .gitattributes for repository %q: %v", ri.RepoName, err), http.StatusInternalServerError)
			return
		}
		if tracked, changed := repository.TrackLFS(attrs, lfsPaths...); changed {
			ops = append(ops, repository.CommitOperation{Type: repository.CommitOperationAdd, Path: repository.GitattributesFileName, Content: tracked})
		}
	}

	commitHash, err := repo.CreateCommit(r.Context(), rev, message, name, email, ops, header.ParentCommit)
	if err != nil {
		if errors.Is(err, repository.ErrParentMismatch) {
			responseJSON(w, fmt.Errorf("a commit has happened since on %q: %v", rev, err), http.StatusPreconditionFailed)
			return
		}
		responseJSON(w, fmt.Errorf("failed to create commit in repository %q: %v", ri.RepoName, err), http.StatusInternalServerError)
		return
	}

	h.afterReceivePack(r.Context(), ri.RepoName, []receive.RefUpdate{
		repo.RefUpdate(oldRev, commitHash, "refs/heads/"+rev),
	})

	resp := commitResponse{
		CommitURL:     fmt.Sprintf("%s/%s/commit/%s", requestOrigin(r), ri.RepoName, commitHash),
		CommitOid:     commitHash,
		CommitMessage: message,
	}
	responseJSON(w, resp, http.StatusOK)
}
