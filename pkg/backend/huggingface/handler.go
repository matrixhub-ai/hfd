package huggingface

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/wzshiming/hfd/pkg/lfs"
	"github.com/wzshiming/hfd/pkg/permission"
	"github.com/wzshiming/hfd/pkg/receive"
	"github.com/wzshiming/hfd/pkg/repository"
	"github.com/wzshiming/hfd/pkg/storage"
)

// Handler
type Handler struct {
	storage             *storage.Storage
	root                *mux.Router
	next                http.Handler
	mirrorSourceFunc    repository.MirrorSourceFunc
	mirrorRefFilterFunc repository.MirrorRefFilterFunc
	lfsTeeCache         *lfs.TeeCache
	permissionHook      permission.PermissionHook
	preReceiveHook      receive.PreReceiveHook
	postReceiveHook     receive.PostReceiveHook
	lfsStore            lfs.Store
}

// Option defines a functional option for configuring the Handler.
type Option func(*Handler)

// WithStorage sets the storage backend for the handler. This is required.
func WithStorage(storage *storage.Storage) Option {
	return func(h *Handler) {
		h.storage = storage
	}
}

// WithMirrorSourceFunc sets the repository proxy callback for transparent upstream repository fetching.
func WithMirrorSourceFunc(fn repository.MirrorSourceFunc) Option {
	return func(h *Handler) {
		h.mirrorSourceFunc = fn
	}
}

// WithMirrorRefFilterFunc sets the ref filter callback for mirror operations.
// When set, only refs accepted by the filter will be synced from the upstream.
func WithMirrorRefFilterFunc(fn repository.MirrorRefFilterFunc) Option {
	return func(h *Handler) {
		h.mirrorRefFilterFunc = fn
	}
}

// WithLFSTeeCache sets the LFS tee cache for transparent upstream object fetching.
func WithLFSTeeCache(tc *lfs.TeeCache) Option {
	return func(h *Handler) {
		h.lfsTeeCache = tc
	}
}

// WithNext sets the next http.Handler to call if the request is not handled by this handler.
func WithNext(next http.Handler) Option {
	return func(h *Handler) {
		h.next = next
	}
}

// WithPermissionHookFunc sets the permission hook for verifying operations.
func WithPermissionHookFunc(hook permission.PermissionHook) Option {
	return func(h *Handler) {
		h.permissionHook = hook
	}
}

// WithPreReceiveHookFunc sets the pre-receive hook called before ref changes are applied.
// If the hook returns an error, the operation is rejected.
func WithPreReceiveHookFunc(hook receive.PreReceiveHook) Option {
	return func(h *Handler) {
		h.preReceiveHook = hook
	}
}

// WithPostReceiveHookFunc sets the post-receive hook called after a git push is processed.
// Errors from this hook are logged but do not affect the push result.
func WithPostReceiveHookFunc(hook receive.PostReceiveHook) Option {
	return func(h *Handler) {
		h.postReceiveHook = hook
	}
}

// WithLFSStore configures the LFS storage backend.
func WithLFSStore(store lfs.Store) Option {
	return func(h *Handler) {
		h.lfsStore = store
	}
}

// NewHandler creates a new Handler with the given repository directory.
func NewHandler(opts ...Option) *Handler {
	h := &Handler{
		root: mux.NewRouter(),
	}

	for _, opt := range opts {
		opt(h)
	}

	h.register()
	return h
}

// ServeHTTP implements the http.Handler interface.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.root.ServeHTTP(w, r)
}

// Router returns the underlying mux.Router for route inspection.
func (h *Handler) Router() *mux.Router {
	return h.root
}

func (h *Handler) register() {
	// HuggingFace-compatible API endpoints
	h.registryHuggingFace(h.root)

	h.root.NotFoundHandler = h.next
}

// registryHuggingFace registers the HuggingFace-compatible API endpoints.
// These endpoints allow using huggingface-cli and huggingface_hub library
// with HF_ENDPOINT pointing to this server.
func (h *Handler) registryHuggingFace(r *mux.Router) {
	// Auth endpoint - used by huggingface-cli auth commands (login, whoami)
	r.HandleFunc("/api/whoami-v2", h.handleWhoami).Methods(http.MethodGet)

	// Repository management endpoints - used by huggingface_hub for repo CRUD
	r.HandleFunc("/api/repos/create", h.handleCreateRepo).Methods(http.MethodPost)
	r.HandleFunc("/api/repos/delete", h.handleDeleteRepo).Methods(http.MethodDelete)
	r.HandleFunc("/api/repos/move", h.handleMoveRepo).Methods(http.MethodPost)

	// YAML validation endpoint - used by huggingface_hub to validate README YAML front matter
	r.HandleFunc("/api/validate-yaml", h.handleValidateYAML).Methods(http.MethodPost)

	// Repository settings, branch, tag, and refs endpoints
	// These must be registered before the generic model info catch-all route.
	r.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/settings", h.handleRepoSettings).Methods(http.MethodPut)
	r.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/branch/{rev}", h.handleCreateBranch).Methods(http.MethodPost)
	r.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/branch/{rev}", h.handleDeleteBranch).Methods(http.MethodDelete)
	r.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/tag/{rev}", h.handleCreateTag).Methods(http.MethodPost)
	r.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/tag/{rev}", h.handleDeleteTag).Methods(http.MethodDelete)
	r.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/refs", h.handleListRefs).Methods(http.MethodGet)
	r.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/commits/{rev}", h.handleListCommits).Methods(http.MethodGet)
	r.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/compare/{compare}", h.handleCompare).Methods(http.MethodGet)
	r.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/super-squash/{rev}", h.handleSuperSquash).Methods(http.MethodPost)

	// API endpoints for all repo types (models, datasets, spaces)
	r.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/preupload/{rev}", h.handlePreupload).Methods(http.MethodPost)
	r.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/commit/{rev}", h.handleCommit).Methods(http.MethodPost)
	r.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/treesize/{revpath:.*}", h.handleTreeSize).Methods(http.MethodGet)
	r.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/tree/{revpath:.*}", h.handleTree).Methods(http.MethodGet)
	r.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/revision/{rev}", h.handleInfoRevision).Methods(http.MethodGet)
	r.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}", h.handleInfoRevision).Methods(http.MethodGet)
	r.HandleFunc("/api/{repoType:models|datasets|spaces}", h.handleList).Methods(http.MethodGet)

	// File download endpoints - datasets and spaces use a type prefix, models use the root
	r.HandleFunc("/{repoType:datasets|spaces}/{namespace}/{repo}/resolve/{revpath:.*}", h.handleResolve).Methods(http.MethodGet, http.MethodHead)
	r.HandleFunc("/{namespace}/{repo}/resolve/{revpath:.*}", h.handleResolve).Methods(http.MethodGet, http.MethodHead)
	r.HandleFunc("/api/resolve-cache/{repoType:models|datasets|spaces}/{namespace}/{repo}/{revpath:.*}", h.handleResolve).Methods(http.MethodGet, http.MethodHead)
}

type repoInformation struct {
	RepoType string
	RepoName string

	FullName  string
	Namespace string
	Name      string
}

// getRepoInformation returns the repository information extracted from the request, including repo type, storage path, namespace, and name.
func getRepoInformation(r *http.Request) repoInformation {
	vars := mux.Vars(r)
	repoType := vars["repoType"]
	if repoType == "" {
		repoType = "models"
	}
	namespace := vars["namespace"]
	name := vars["repo"]
	fullName := namespace + "/" + name

	var repoName string
	switch repoType {
	case "datasets", "spaces":
		repoName = repoType + "/" + fullName
	default:
		repoName = fullName
	}

	return repoInformation{
		RepoType:  repoType,
		RepoName:  repoName,
		Namespace: namespace,
		Name:      name,
		FullName:  fullName,
	}
}

// openRepo opens a repository, optionally creating a mirror from the proxy source
// if the repository doesn't exist locally and proxy mode is enabled.
func (h *Handler) openRepo(ctx context.Context, repoPath, repoName string) (*repository.Repository, error) {
	if h.mirrorSourceFunc == nil {
		return repository.Open(repoPath)
	}
	repo, err := repository.Open(repoPath)
	if err != nil {
		if err != repository.ErrRepositoryNotExists {
			return nil, err
		}
		if h.permissionHook != nil {
			if err := h.permissionHook(ctx, permission.OperationCreateProxyRepo, repoName, permission.Context{}); err != nil {
				return nil, err
			}
		}
		sourceURL, isMirror, err := h.mirrorSourceFunc(ctx, repoName)
		if err != nil {
			return nil, err
		}
		if !isMirror {
			return nil, repository.ErrRepositoryNotExists
		}
		repo, err = repository.InitMirror(ctx, repoPath, sourceURL)
		if err != nil {
			return nil, repository.ErrRepositoryNotExists
		}
		err = h.syncMirror(ctx, repo, repoName, sourceURL)
		if err != nil {
			return nil, fmt.Errorf("failed to sync mirror: %w", err)
		}
	} else {
		sourceURL, isMirror, err := h.mirrorSourceFunc(ctx, repoName)
		if err != nil {
			slog.WarnContext(ctx, "mirrorSourceFunc error", "repo", repoName, "error", err)
			return repo, nil
		}
		if !isMirror {
			return repo, nil
		}
		err = h.syncMirror(ctx, repo, repoName, sourceURL)
		if err != nil {
			return nil, fmt.Errorf("failed to sync mirror: %w", err)
		}
	}
	return repo, nil
}

func removeKeyFromMap(m map[string]string, keys []string) map[string]string {
	if m == nil {
		return nil
	}
	result := make(map[string]string)
	for _, key := range keys {
		result[key] = m[key]
	}
	return result
}

// syncMirror syncs a mirror and fires post-receive hooks for any ref changes.
func (h *Handler) syncMirror(ctx context.Context, repo *repository.Repository, repoName string, sourceURL string) error {
	remoteRefs, err := repo.ListRemoteRefs(ctx, sourceURL)
	if err != nil {
		return fmt.Errorf("failed to list remote refs: %w", err)
	}

	if h.mirrorRefFilterFunc != nil {
		remoteRefs, err = h.mirrorRefFilterFunc(ctx, repoName, remoteRefs)
		if err != nil {
			return fmt.Errorf("failed to filter mirror refs: %w", err)
		}
	}
	if len(remoteRefs) == 0 {
		return nil
	}

	var before map[string]string
	if h.postReceiveHook != nil || h.preReceiveHook != nil {
		before, _ = repo.Refs()
	}

	before = removeKeyFromMap(before, remoteRefs)

	if h.preReceiveHook != nil {
		var updates []receive.RefUpdate
		for _, target := range remoteRefs {
			oldRev, ok := before[target]
			if oldRev == "" || !ok {
				oldRev = receive.ZeroHash
			}
			updates = append(updates, receive.NewRefUpdate(oldRev, receive.BreakHash, target, repo.RepoPath()))
		}
		if err := h.preReceiveHook(ctx, repoName, updates); err != nil {
			return fmt.Errorf("pre-receive hook error: %w", err)
		}
	}

	if err := repo.SyncMirrorRefs(ctx, sourceURL, remoteRefs); err != nil {
		return fmt.Errorf("failed to sync mirror refs: %w", err)
	}

	if h.postReceiveHook != nil {
		after, _ := repo.Refs()
		after = removeKeyFromMap(after, remoteRefs)
		updates := receive.DiffRefs(before, after, repo.RepoPath())
		if len(updates) > 0 {
			if err := h.postReceiveHook(ctx, repoName, updates); err != nil {
				return fmt.Errorf("post-receive hook error: %w", err)
			}
		}
	}
	return nil
}

func responseJSON(w http.ResponseWriter, data any, sc int) {
	header := w.Header()
	if header.Get("Content-Type") == "" {
		header.Set("Content-Type", "application/json; charset=utf-8")
	}

	if sc >= http.StatusBadRequest {
		header.Del("Content-Length")
		header.Set("X-Content-Type-Options", "nosniff")
	}

	if sc != 0 {
		w.WriteHeader(sc)
	}

	if data == nil {
		_, _ = w.Write([]byte("{}"))
		return
	}

	switch t := data.(type) {
	case error:
		var dataErr struct {
			Error string `json:"error"`
		}
		dataErr.Error = t.Error()
		data = dataErr
	case string:
		var dataErr struct {
			Error string `json:"error"`
		}
		dataErr.Error = t
		data = dataErr
	}

	enc := json.NewEncoder(w)
	_ = enc.Encode(data)
}
