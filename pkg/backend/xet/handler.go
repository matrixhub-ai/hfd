package xet

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"

	pkgxet "github.com/matrixhub-ai/hfd/pkg/xet"
)

// Handler handles HTTP requests for xet CAS (Content-Addressable Storage) API endpoints.
type Handler struct {
	root       *mux.Router
	next       http.Handler
	xetStorage pkgxet.Storage
}

// Option defines a functional option for configuring the Handler.
type Option func(*Handler)

// WithNext sets the next http.Handler to call if the request is not handled by this handler.
func WithNext(next http.Handler) Option {
	return func(h *Handler) {
		h.next = next
	}
}

// WithXetStorage configures the xet CAS storage backend.
func WithXetStorage(storage pkgxet.Storage) Option {
	return func(h *Handler) {
		h.xetStorage = storage
	}
}

// NewHandler creates a new Handler for xet CAS API endpoints.
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
	// Xet CAS protocol endpoints (per xet-core OpenAPI spec and local_server reference):
	// POST /v1/xorbs/{prefix}/{hash}       - upload a xorb
	// HEAD /v1/xorbs/{prefix}/{hash}       - check if a xorb exists
	// POST /v1/shards                      - upload a shard (OpenAPI spec)
	// POST /shards                         - upload a shard (alias for compatibility)
	// GET  /v1/chunks/{prefix}/{hash}      - global dedup query
	// GET  /v1/reconstructions/{file_id}   - file reconstruction (V1)
	// GET  /v2/reconstructions/{file_id}   - file reconstruction (V2)
	// GET  /v1/reconstructions             - batch reconstruction query
	// GET  /reconstructions                - batch reconstruction query (alias)
	// GET  /v1/fetch_term                  - fetch raw xorb data (V1 and V2 terms)
	// GET  /v1/get_xorb/{prefix}/{hash}/   - direct xorb download with Range support
	// HEAD /v1/files/{file_id}             - get file size
	// GET  /health                         - health check
	h.root.HandleFunc("/v1/xorbs/{prefix}/{hash:[a-f0-9]+}", h.handlePostXorb).Methods(http.MethodPost)
	h.root.HandleFunc("/v1/xorbs/{prefix}/{hash:[a-f0-9]+}", h.handleHeadXorb).Methods(http.MethodHead)
	h.root.HandleFunc("/v1/shards", h.handlePostShard).Methods(http.MethodPost)
	h.root.HandleFunc("/shards", h.handlePostShard).Methods(http.MethodPost)
	h.root.HandleFunc("/v1/chunks/{prefix}/{hash:[a-f0-9]+}", h.handleGetChunk).Methods(http.MethodGet)
	h.root.HandleFunc("/v1/reconstructions/{file_id:[a-f0-9]+}", h.handleGetReconstruction).Methods(http.MethodGet)
	h.root.HandleFunc("/v2/reconstructions/{file_id:[a-f0-9]+}", h.handleGetReconstructionV2).Methods(http.MethodGet)
	h.root.HandleFunc("/v1/reconstructions", h.handleBatchGetReconstruction).Methods(http.MethodGet)
	h.root.HandleFunc("/reconstructions", h.handleBatchGetReconstruction).Methods(http.MethodGet)
	h.root.HandleFunc("/v1/fetch_term", h.handleFetchTerm).Methods(http.MethodGet)
	h.root.HandleFunc("/v1/get_xorb/{prefix}/{hash:[a-f0-9]+}/", h.handleGetXorb).Methods(http.MethodGet)
	h.root.HandleFunc("/v1/files/{file_id:[a-f0-9]+}", h.handleHeadFile).Methods(http.MethodHead)
	h.root.HandleFunc("/health", h.handleHealth).Methods(http.MethodGet)
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
