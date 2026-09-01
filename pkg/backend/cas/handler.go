// Package cas serves the hub-side control plane of the xet CAS data plane:
// the three token routes that mint CAS credentials. Write-token is gated on
// the update operation, read-token on read, and /xet-token is ungated. The
// xet library ships no server-side write-token, so this is the sole mint
// source for uploads; the read routes shadow the xet hub front end's
// equivalents so they keep working without a pull upstream (which
// Mirror.SetXETLinkHeaders advertises). Responses carry both the X-Xet-*
// headers and the JSON body, serving huggingface_hub >= 1.29 (headers-only)
// and older clients (body) alike. Everything else falls through to the next
// handler.
package cas

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/backend/internal/httpapi"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/permission"
)

// Handler answers the hub-style xet token routes with CAS credentials minted by the data plane.
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

// WithMirror sets the mirror whose data plane mints the tokens.
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
	if h.next == nil {
		h.next = http.NotFoundHandler()
	}
	h.register()
	return h
}

// ServeHTTP implements the http.Handler interface.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.root.ServeHTTP(w, r)
}

func (h *Handler) register() {
	// Match encoded paths so escaped revisions (refs%2Fpr%2F1) stay single segments; cleaning stays upstream.
	h.root.UseEncodedPath()
	h.root.SkipClean(true)

	// No .Methods(): the method check lives inside; non-GET falls through to next.
	h.root.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/xet-write-token/{rev}", h.handleWriteToken)
	h.root.HandleFunc("/api/{repoType:models|datasets|spaces}/{namespace}/{repo}/xet-read-token/{rev}", h.handleReadToken)
	// No gate: no repo context; CAS tokens are global, so a repo-scoped gate is impossible here.
	h.root.HandleFunc("/xet-token", h.serveToken)

	h.root.NotFoundHandler = h.next
}

// typedRepoName renders the route vars as an hf repository name: models
// unprefixed, other types prefixed. Vars arrive encoded (UseEncodedPath); the
// permission identity is the decoded name, and spellings that fail to decode
// or decode to extra path separators report false as no canonical route.
func typedRepoName(vars map[string]string) (string, bool) {
	ns, nsErr := url.PathUnescape(vars["namespace"])
	repo, repoErr := url.PathUnescape(vars["repo"])
	if nsErr != nil || repoErr != nil || strings.Contains(ns, "/") || strings.Contains(repo, "/") {
		return "", false
	}
	name := ns + "/" + repo
	if t := vars["repoType"]; t == "datasets" || t == "spaces" {
		return t + "/" + name, true
	}
	return name, true
}

func (h *Handler) handleWriteToken(w http.ResponseWriter, r *http.Request) {
	h.handleRepoToken(w, r, permission.OperationUpdateRepo)
}

func (h *Handler) handleReadToken(w http.ResponseWriter, r *http.Request) {
	h.handleRepoToken(w, r, permission.OperationReadRepo)
}

func (h *Handler) handleRepoToken(w http.ResponseWriter, r *http.Request, op permission.Operation) {
	repoName, ok := typedRepoName(mux.Vars(r))
	if !ok {
		h.next.ServeHTTP(w, r)
		return
	}
	if !h.checkPermission(w, r, op, repoName) {
		return
	}
	h.serveToken(w, r)
}

func (h *Handler) serveToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || h.mirror == nil || !h.mirror.CanMintToken() {
		h.next.ServeHTTP(w, r)
		return
	}
	casURL, token, expiresAt := h.mirror.MintXETToken(r)
	respondToken(w, casURL, token, expiresAt.Unix())
}

// respondToken writes the dual contract: X-Xet-* headers (hub >= 1.29) plus the JSON body older clients read.
func respondToken(w http.ResponseWriter, casURL, token string, exp int64) {
	w.Header().Set("X-Xet-Cas-Url", casURL)
	w.Header().Set("X-Xet-Access-Token", token)
	w.Header().Set("X-Xet-Token-Expiration", strconv.FormatInt(exp, 10))
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"casUrl":      casURL,
		"accessToken": token,
		"exp":         exp,
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
