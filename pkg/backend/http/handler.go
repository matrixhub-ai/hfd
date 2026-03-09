package backend

import (
	"io"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/wzshiming/hfd/pkg/permission"
	"github.com/wzshiming/hfd/pkg/receive"
	"github.com/wzshiming/hfd/pkg/repository"
	"github.com/wzshiming/hfd/pkg/storage"
)

// Handler
type Handler struct {
	storage *storage.Storage

	root *mux.Router

	next http.Handler

	proxyManager    *repository.ProxyManager
	permissionHook  permission.PermissionHook
	preReceiveHook  receive.PreReceiveHook
	postReceiveHook receive.PostReceiveHook
}

type Option func(*Handler)

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

// WithProxyManager sets the repository proxy manager for transparent upstream repository fetching.
func WithProxyManager(pm *repository.ProxyManager) Option {
	return func(h *Handler) {
		h.proxyManager = pm
	}
}

// WithPermissionHookFunc sets the permission hook for verifying operations.
func WithPermissionHookFunc(hook permission.PermissionHook) Option {
	return func(h *Handler) {
		h.permissionHook = hook
	}
}

// WithPreReceiveHookFunc sets the pre-receive hook called before a git push is processed.
// If the hook returns an error, the push is rejected.
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

func (h *Handler) register() {
	// Git protocol endpoints
	h.registryGit(h.root)

	h.root.NotFoundHandler = h.next
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
