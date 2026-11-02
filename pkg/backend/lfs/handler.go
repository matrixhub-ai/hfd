package lfs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/backend/internal/httpapi"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// Handler handles HTTP requests for Git LFS API endpoints, including batch operations and object content management.
type Handler struct {
	storage            *storage.Storage
	root               *mux.Router
	next               http.Handler
	locksStorage       *lfs.LockStorage
	preOpenHook        PreOpenHookFunc
	tokenSignValidator authenticate.TokenSignValidator
	mirror             *mirror.Mirror
}

// PreOpenHookFunc admits a batch or lock request; errors answer 403/401/404 for permission/auth/not-found, else 500.
type PreOpenHookFunc func(ctx context.Context, repoName string, write bool) error

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

// WithPreOpenHookFunc sets the hook that admits batch and lock requests.
func WithPreOpenHookFunc(fn PreOpenHookFunc) Option {
	return func(h *Handler) {
		h.preOpenHook = fn
	}
}

// WithTokenSignValidator sets the token signer for signing LFS batch response action headers.
func WithTokenSignValidator(signer authenticate.TokenSignValidator) Option {
	return func(h *Handler) {
		h.tokenSignValidator = signer
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
func NewHandler(opts ...Option) *Handler {
	h := &Handler{
		root:         mux.NewRouter(),
		locksStorage: lfs.NewLock(),
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

func (h *Handler) register() {
	// Git LFS protocol endpoints
	h.registryLFS(h.root)
	h.registryLFSLock(h.root)
	h.root.NotFoundHandler = h.next
}

func (h *Handler) registryLFS(r *mux.Router) {
	r.HandleFunc("/{repo:.+}.git/info/lfs/objects/batch", h.handleBatch).Methods(http.MethodPost).MatcherFunc(metaMatcher)
	r.HandleFunc("/{repo:.+}/info/lfs/objects/batch", h.handleBatch).Methods(http.MethodPost).MatcherFunc(metaMatcher)
	r.HandleFunc("/objects/{oid}", h.handleGetContent).Methods(http.MethodGet, http.MethodHead)
	r.HandleFunc("/objects/{oid}", h.handlePutContent).Methods(http.MethodPut)
	r.HandleFunc("/objects/{oid}/verify", h.handleVerifyObject).Methods(http.MethodPost)
}

func (h *Handler) registryLFSLock(r *mux.Router) {
	// Lock routes hang off the LFS URL like the batch route, so {repo} stays the bare repository name.
	r.HandleFunc("/{repo:.+}.git/info/lfs/locks", h.handleGetLock).Methods(http.MethodGet).MatcherFunc(metaMatcher)
	r.HandleFunc("/{repo:.+}/info/lfs/locks", h.handleGetLock).Methods(http.MethodGet).MatcherFunc(metaMatcher)
	r.HandleFunc("/{repo:.+}.git/info/lfs/locks/verify", h.handleLocksVerify).Methods(http.MethodPost).MatcherFunc(metaMatcher)
	r.HandleFunc("/{repo:.+}/info/lfs/locks/verify", h.handleLocksVerify).Methods(http.MethodPost).MatcherFunc(metaMatcher)
	r.HandleFunc("/{repo:.+}.git/info/lfs/locks", h.handleCreateLock).Methods(http.MethodPost).MatcherFunc(metaMatcher)
	r.HandleFunc("/{repo:.+}/info/lfs/locks", h.handleCreateLock).Methods(http.MethodPost).MatcherFunc(metaMatcher)
	r.HandleFunc("/{repo:.+}.git/info/lfs/locks/{id}/unlock", h.handleDeleteLock).Methods(http.MethodPost).MatcherFunc(metaMatcher)
	r.HandleFunc("/{repo:.+}/info/lfs/locks/{id}/unlock", h.handleDeleteLock).Methods(http.MethodPost).MatcherFunc(metaMatcher)
}

func responseJSON(w http.ResponseWriter, data any, sc int) {
	httpapi.RespondJSON(w, data, sc)
}

// admit runs the pre-open hook, answering a refusal as {key: text}; it reports whether the request may proceed.
func (h *Handler) admit(w http.ResponseWriter, r *http.Request, repoName string, write bool, key string) bool {
	if h.preOpenHook == nil {
		return true
	}
	err := h.preOpenHook(r.Context(), repoName, write)
	if err == nil {
		return true
	}
	sc, text := http.StatusInternalServerError, err.Error()
	switch {
	case errors.Is(err, fs.ErrPermission):
		sc, text = http.StatusForbidden, "permission denied"
	case errors.Is(err, authenticate.ErrUnauthenticated):
		w.Header().Set("WWW-Authenticate", `Basic realm="hfd"`)
		sc, text = http.StatusUnauthorized, "Unauthorized"
	case errors.Is(err, repository.ErrRepositoryNotExists):
		sc, text = http.StatusNotFound, fmt.Sprintf("repository %q not found", repoName)
	default:
		slog.ErrorContext(r.Context(), "pre-open hook error", "repo", repoName, "write", write, "error", err)
	}
	responseJSON(w, map[string]string{key: text}, sc)
	return false
}
