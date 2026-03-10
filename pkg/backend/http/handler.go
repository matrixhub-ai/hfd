package backend

import (
	"io"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/wzshiming/hfd/pkg/mirror"
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
	permissionHookFunc  permission.PermissionHookFunc
	preReceiveHookFunc  receive.PreReceiveHookFunc
	postReceiveHookFunc receive.PostReceiveHookFunc
	mirrorTTL           time.Duration
	mirror              *mirror.Mirror
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

// WithPermissionHookFunc sets the permission hook for verifying operations.
func WithPermissionHookFunc(fn permission.PermissionHookFunc) Option {
	return func(h *Handler) {
		h.permissionHookFunc = fn
	}
}

// WithPreReceiveHookFunc sets the pre-receive hook called before a git push is processed.
// If the hook returns an error, the push is rejected.
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

// WithMirrorTTL sets a minimum duration between successive mirror syncs for the same repository.
// A zero value preserves the existing behavior of syncing on every read.
func WithMirrorTTL(ttl time.Duration) Option {
	return func(h *Handler) {
		h.mirrorTTL = ttl
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

	if h.mirrorSourceFunc != nil {
		h.mirror = mirror.NewMirror(
			mirror.WithMirrorSourceFunc(h.mirrorSourceFunc),
			mirror.WithMirrorRefFilterFunc(h.mirrorRefFilterFunc),
			mirror.WithPermissionHookFunc(h.permissionHookFunc),
			mirror.WithPreReceiveHookFunc(h.preReceiveHookFunc),
			mirror.WithPostReceiveHookFunc(h.postReceiveHookFunc),
			mirror.WithTTL(h.mirrorTTL),
		)
	}

	h.register()
	return h
}

// ServeHTTP implements the http.Handler interface.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.root.ServeHTTP(w, r)
}

func (h *Handler) register() {
	// Git protocol endpoints
	h.registryGit(h.root)

	h.root.NotFoundHandler = h.next
}

func (h *Handler) registryGit(r *mux.Router) {
	r.HandleFunc("/{repo:.+}.git/info/refs", h.handleInfoRefs).Methods(http.MethodGet)
	r.HandleFunc("/{repo:.+}/info/refs", h.handleInfoRefs).Methods(http.MethodGet)
	r.HandleFunc("/{repo:.+}.git/git-upload-pack", h.handleUploadPack).Methods(http.MethodPost)
	r.HandleFunc("/{repo:.+}/git-upload-pack", h.handleUploadPack).Methods(http.MethodPost)
	r.HandleFunc("/{repo:.+}.git/git-receive-pack", h.handleReceivePack).Methods(http.MethodPost)
	r.HandleFunc("/{repo:.+}/git-receive-pack", h.handleReceivePack).Methods(http.MethodPost)
}

func responseText(w http.ResponseWriter, text string, sc int) {
	header := w.Header()
	if header.Get("Content-Type") == "" {
		header.Set("Content-Type", "text/plain; charset=utf-8")
	}

	if sc >= http.StatusBadRequest {
		header.Del("Content-Length")
		header.Set("X-Content-Type-Options", "nosniff")
	}

	if sc != 0 {
		w.WriteHeader(sc)
	}

	if text == "" {
		return
	}

	_, _ = io.WriteString(w, text)
}
