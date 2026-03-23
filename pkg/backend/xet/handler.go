package xet

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	pkgxet "github.com/matrixhub-ai/hfd/pkg/xet"
)

// Handler handles HTTP requests for xet CAS (Content Addressable Storage) API endpoints.
// It implements the CAS protocol as defined in the xet-core OpenAPI spec:
// https://github.com/huggingface/xet-core/blob/main/openapi/cas.openapi.yaml
type Handler struct {
	storage            *pkgxet.Storage
	root               *mux.Router
	next               http.Handler
	tokenSignValidator authenticate.TokenSignValidator
}

// Option defines a functional option for configuring the Handler.
type Option func(*Handler)

// WithStorage sets the xet CAS storage backend for the handler. This is required.
func WithStorage(storage *pkgxet.Storage) Option {
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

// WithTokenSignValidator sets the token validator for CAS authentication.
func WithTokenSignValidator(signer authenticate.TokenSignValidator) Option {
	return func(h *Handler) {
		h.tokenSignValidator = signer
	}
}

// NewHandler creates a new xet CAS Handler with the given options.
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
	// CAS API endpoints following xet-core OpenAPI specification.

	// Upload xorb - POST /v1/xorbs/{prefix}/{hash}
	h.root.HandleFunc("/v1/xorbs/{prefix}/{hash}", h.handleUploadXorb).Methods(http.MethodPost)

	// Upload shard - POST /v1/shards (and /shards alias)
	h.root.HandleFunc("/v1/shards", h.handleUploadShard).Methods(http.MethodPost)
	h.root.HandleFunc("/shards", h.handleUploadShard).Methods(http.MethodPost)

	// Query chunk deduplication - GET /v1/chunks/{prefix}/{hash}
	h.root.HandleFunc("/v1/chunks/{prefix}/{hash}", h.handleGetChunk).Methods(http.MethodGet)

	// Get file reconstruction - GET /v1/reconstructions/{file_id}
	h.root.HandleFunc("/v1/reconstructions/{file_id}", h.handleGetReconstructionV1).Methods(http.MethodGet)

	// Get file reconstruction V2 - GET /v2/reconstructions/{file_id}
	h.root.HandleFunc("/v2/reconstructions/{file_id}", h.handleGetReconstructionV2).Methods(http.MethodGet)

	// Get xorb content - GET /v1/xorbs/{prefix}/{hash}
	// This serves xorb blobs for reconstruction; used as the URL in fetch_info.
	h.root.HandleFunc("/v1/xorbs/{prefix}/{hash}", h.handleGetXorb).Methods(http.MethodGet)

	h.root.NotFoundHandler = h.next
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
