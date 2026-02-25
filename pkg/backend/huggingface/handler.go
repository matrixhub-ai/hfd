package huggingface

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/wzshiming/gitd/pkg/storage"
)

// Handler
type Handler struct {
	storage *storage.Storage

	root *mux.Router

	next http.Handler
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
	// HuggingFace-compatible API endpoints
	h.registryHuggingFace(h.root)

	h.root.NotFoundHandler = h.next
}

// registryHuggingFace registers the HuggingFace-compatible API endpoints.
// These endpoints allow using huggingface-cli and huggingface_hub library
// with HF_ENDPOINT pointing to this server.
func (h *Handler) registryHuggingFace(r *mux.Router) {
	// Repository creation endpoint - used by huggingface_hub to create repos
	r.HandleFunc("/api/repos/create", h.handleHFCreateRepo).Methods(http.MethodPost)

	// YAML validation endpoint - used by huggingface_hub to validate README YAML front matter
	r.HandleFunc("/api/validate-yaml", h.handleHFValidateYAML).Methods(http.MethodPost)

	// Pre-upload endpoint - used by huggingface_hub to determine upload modes
	r.HandleFunc("/api/models/{repo:.+}/preupload/{revision:.*}", h.handleHFPreupload).Methods(http.MethodPost)

	// Commit endpoint - used by huggingface_hub to create commits
	r.HandleFunc("/api/models/{repo:.+}/commit/{revision:.*}", h.handleHFCommit).Methods(http.MethodPost)

	// Model info endpoint with revision - used by huggingface_hub for snapshot_download
	r.HandleFunc("/api/models/{repo:.+}/revision/{revision:.*}", h.handleHFModelInfoRevision).Methods(http.MethodGet)

	// Tree endpoint - used by huggingface_hub to list files in the model repository
	r.HandleFunc("/api/models/{repo:.+}/tree/{refpath:.*}", h.handleHFTree).Methods(http.MethodGet)

	// Model info endpoint - used by huggingface_hub to get model metadata
	r.HandleFunc("/api/models/{repo:.+}", h.handleHFModelInfo).Methods(http.MethodGet)

	// File download endpoint - used by huggingface_hub to download files
	r.HandleFunc("/{repo:.+}/resolve/{refpath:.*}", h.handleHFResolve).Methods(http.MethodGet, http.MethodHead)

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
