// Package cas serves the hub-side control plane of the xet CAS data plane:
// the write-token route that mints upload credentials, gated on the update
// operation. Everything else falls through to the next handler.
package cas

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/backend/internal/httpapi"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/permission"
)

// Handler answers the hub-style xet-write-token route with a CAS credential
// minted by the data plane.
type Handler struct {
	root               *mux.Router
	next               http.Handler
	permissionHookFunc permission.PermissionHookFunc
	mirror             *mirror.Mirror
}

// Option defines a functional option for configuring the Handler.
type Option func(*Handler)

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

// WithMirror sets the mirror whose data plane mints the write tokens.
func WithMirror(m *mirror.Mirror) Option {
	return func(h *Handler) {
		h.mirror = m
	}
}

// NewHandler creates a new Handler.
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
	// No .Methods(): non-GET write-token requests belong to the data plane.
	h.root.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/xet-write-token/{rev}", h.handleWriteToken)

	h.root.NotFoundHandler = h.next
}

// typedRepoName renders the route vars as an hf repository name: models
// unprefixed, other types prefixed.
func typedRepoName(vars map[string]string) string {
	name := vars["namespace"] + "/" + vars["repo"]
	if t := vars["repoType"]; t == "datasets" || t == "spaces" {
		return t + "/" + name
	}
	return name
}

func (h *Handler) handleWriteToken(w http.ResponseWriter, r *http.Request) {
	if !h.checkPermission(w, r, permission.OperationUpdateRepo, typedRepoName(mux.Vars(r))) {
		return
	}
	if h.mirror == nil || !h.mirror.CanMintToken() || h.next == nil {
		http.NotFound(w, r)
		return
	}
	// The xet composition mounted as next owns non-GET semantics.
	if r.Method != http.MethodGet {
		h.next.ServeHTTP(w, r)
		return
	}
	casURL, token, expiresAt := h.mirror.MintXETToken(r)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"casUrl":      casURL,
		"accessToken": token,
		"exp":         expiresAt.Unix(),
	})
}

// checkPermission runs the permission hook and writes the failure response.
// It returns true when the operation may proceed.
func (h *Handler) checkPermission(w http.ResponseWriter, r *http.Request, op permission.Operation, repoName string) bool {
	return permission.Guard{
		Hook:    h.permissionHookFunc,
		Respond: func(w http.ResponseWriter, msg string, sc int) { httpapi.RespondJSON(w, msg, sc) },
	}.Allow(w, r, op, repoName, permission.Context{})
}
