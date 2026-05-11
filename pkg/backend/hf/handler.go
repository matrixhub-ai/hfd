package hf

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/matrixhub-ai/hfd/pkg/backend"
	"github.com/matrixhub-ai/hfd/pkg/constants"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// Handler handles HTTP requests for HuggingFace-compatible API endpoints, including repository management and git operations.
type Handler struct {
	storage             *storage.Storage
	root                *mux.Router
	next                http.Handler
	lfsStorage          lfs.Storage
	permissionHookFunc  permission.PermissionHookFunc
	preReceiveHookFunc  receive.PreReceiveHookFunc
	postReceiveHookFunc receive.PostReceiveHookFunc
	mirror              *mirror.Mirror
	routerTable         map[string]backend.RouterEntry
}

// Option defines a functional option for configuring the Handler.
type Option func(*Handler)

// WithStorage sets the storage backend for the handler. This is required.
func WithStorage(storage *storage.Storage) Option {
	return func(h *Handler) {
		h.storage = storage
	}
}

// WithNext sets the next http.Handler to call if the request is not handled by this handler.
func WithNext(next http.Handler) Option {
	return func(h *Handler) {
		h.next = next
	}
}

// WithPermissionHookFunc sets the permission hook for verifying operations.
func WithPermissionHookFunc(fn permission.PermissionHookFunc) Option {
	return func(h *Handler) {
		h.permissionHookFunc = fn
	}
}

// WithPreReceiveHookFunc sets the pre-receive hook called before ref changes are applied.
// If the hook returns an error, the operation is rejected.
func WithPreReceiveHookFunc(fn receive.PreReceiveHookFunc) Option {
	return func(h *Handler) {
		h.preReceiveHookFunc = fn
	}
}

// WithPostReceiveHookFunc sets the post-receive hook called after a git push is processed.
// Errors from this hook are logged but do not affect the push result.
func WithPostReceiveHookFunc(fn receive.PostReceiveHookFunc) Option {
	return func(h *Handler) {
		h.postReceiveHookFunc = fn
	}
}

// WithLFSStorage configures the LFS storage backend.
func WithLFSStorage(storage lfs.Storage) Option {
	return func(h *Handler) {
		h.lfsStorage = storage
	}
}

// WithMirror sets the mirror to use for repository synchronization. If not provided,
// a mirror will be created when mirrorSourceFunc is set.
func WithMirror(m *mirror.Mirror) Option {
	return func(h *Handler) {
		h.mirror = m
	}
}

// NewHandler creates a new Handler with the given repository directory.
func NewHandler(opts ...Option) (*Handler, map[string]string) {
	h := &Handler{
		root: mux.NewRouter(),
	}

	for _, opt := range opts {
		opt(h)
	}

	routerTable := make(map[string]backend.RouterEntry)

	// Auth endpoint - used by huggingface-cli auth commands (login, whoami)
	routerTable["/api/whoami-v2"] = backend.RouterEntry{Operation: constants.OpHfGetWhoami, Method: http.MethodGet, Handler: h.handleWhoami}

	// Repository management endpoints - used by huggingface_hub for repo CRUD
	routerTable["/api/repos/create"] = backend.RouterEntry{Operation: constants.OpHfPostCreateRepo, Method: http.MethodPost, Handler: h.handleCreateRepo}
	routerTable["/api/repos/delete"] = backend.RouterEntry{Operation: constants.OpHfDeleteDeleteRepo, Method: http.MethodDelete, Handler: h.handleDeleteRepo}
	routerTable["/api/repos/move"] = backend.RouterEntry{Operation: constants.OpHfPostMoveRepo, Method: http.MethodPost, Handler: h.handleMoveRepo}

	// YAML validation endpoint - used by huggingface_hub to validate README YAML front matter
	routerTable["/api/validate-yaml"] = backend.RouterEntry{Operation: constants.OpHfPostValidateYaml, Method: http.MethodPost, Handler: h.handleValidateYAML}

	// Repository settings, branch, tag, and refs endpoints
	routerTable["/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/settings"] = backend.RouterEntry{Operation: constants.OpHfPutRepoSettings, Method: http.MethodPut, Handler: h.handleRepoSettings}
	routerTable["/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/branch/{rev}"] = backend.RouterEntry{Operation: constants.OpHfPostCreateBranch, Method: http.MethodPost, Handler: h.handleCreateBranch}
	routerTable["/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/branch/{rev}"] = backend.RouterEntry{Operation: constants.OpHfDeleteDeleteBranch, Method: http.MethodDelete, Handler: h.handleDeleteBranch}
	routerTable["/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/tag/{rev}"] = backend.RouterEntry{Operation: constants.OpHfPostCreateTag, Method: http.MethodPost, Handler: h.handleCreateTag}
	routerTable["/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/tag/{rev}"] = backend.RouterEntry{Operation: constants.OpHfDeleteDeleteTag, Method: http.MethodDelete, Handler: h.handleDeleteTag}
	routerTable["/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/refs"] = backend.RouterEntry{Operation: constants.OpHfGetListRefs, Method: http.MethodGet, Handler: h.handleListRefs}
	routerTable["/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/commits/{rev}"] = backend.RouterEntry{Operation: constants.OpHfGetListCommits, Method: http.MethodGet, Handler: h.handleListCommits}
	routerTable["/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/compare/{compare}"] = backend.RouterEntry{Operation: constants.OpHfGetCompare, Method: http.MethodGet, Handler: h.handleCompare}
	routerTable["/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/super-squash/{rev}"] = backend.RouterEntry{Operation: constants.OpHfPostSuperSquash, Method: http.MethodPost, Handler: h.handleSuperSquash}

	// API endpoints for all repo types (models, datasets, spaces)
	routerTable["/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/preupload/{rev}"] = backend.RouterEntry{Operation: constants.OpHfPostPreupload, Method: http.MethodPost, Handler: h.handlePreupload}
	routerTable["/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/commit/{rev}"] = backend.RouterEntry{Operation: constants.OpHfPostCommit, Method: http.MethodPost, Handler: h.handleCommit}
	routerTable["/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/treesize/{revpath:.*}"] = backend.RouterEntry{Operation: constants.OpHfGetTreeSize, Method: http.MethodGet, Handler: h.handleTreeSize}
	routerTable["/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/tree/{revpath:.*}"] = backend.RouterEntry{Operation: constants.OpHfGetTree, Method: http.MethodGet, Handler: h.handleTree}
	routerTable["/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/revision/{rev}"] = backend.RouterEntry{Operation: constants.OpHfGetInfoRevision, Method: http.MethodGet, Handler: h.handleInfoRevision}
	routerTable["/api/{repoType:models|datasets|spaces}/{namespace}/{repo}"] = backend.RouterEntry{Operation: constants.OpHfGetInfoRevision, Method: http.MethodGet, Handler: h.handleInfoRevision}
	routerTable["/api/{repoType:models|datasets|spaces}"] = backend.RouterEntry{Operation: constants.OpHfGetList, Method: http.MethodGet, Handler: h.handleList}

	// File download endpoints - datasets and spaces use a type prefix, models use the root
	routerTable["/{repoType:datasets|spaces}/{namespace}/{repo}/resolve/{revpath:.*}"] = backend.RouterEntry{Operation: constants.OpHfGetResolve, Method: http.MethodGet, Handler: h.handleResolve}
	routerTable["/{namespace}/{repo}/resolve/{revpath:.*}"] = backend.RouterEntry{Operation: constants.OpHfGetResolve, Method: http.MethodGet, Handler: h.handleResolve}
	routerTable["/api/resolve-cache/{repoType:models|datasets|spaces}/{namespace}/{repo}/{revpath:.*}"] = backend.RouterEntry{Operation: constants.OpHfGetResolve, Method: http.MethodGet, Handler: h.handleResolve}

	h.routerTable = routerTable
	h.register()

	return h, h.routerMap()
}

// ServeHTTP implements the http.Handler interface.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.root.ServeHTTP(w, r)
}

// Router returns the underlying mux.Router for route inspection.
func (h *Handler) Router() *mux.Router {
	return h.root
}

func (h *Handler) routerMap() map[string]string {
	routerMap := make(map[string]string)
	for pattern, r := range h.routerTable {
		routerMap[fmt.Sprintf("%s %s", r.Method, pattern)] = r.Operation
	}

	return routerMap
}

func (h *Handler) register() {
	for pattern, r := range h.routerTable {
		// registryHuggingFace registers the HuggingFace-compatible API endpoints.
		// These endpoints allow using huggingface-cli and huggingface_hub library
		// with HF_ENDPOINT pointing to this server.
		h.root.HandleFunc(pattern, r.Handler).Methods(r.Method)
	}

	h.root.NotFoundHandler = h.next
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

func (h *Handler) openRepo(ctx context.Context, repoPath, repoName, service string) (*repository.Repository, error) {
	if h.mirror == nil || service != repository.GitUploadPack {
		return repository.Open(repoPath)
	}
	return h.mirror.OpenOrSync(ctx, repoPath, repoName)
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
