package xet

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/xet"
)

// Handler handles HTTP requests for XET CAS (Content-Addressable Storage) API endpoints.
// These endpoints implement the xet-core CAS protocol for xorb and shard management.
type Handler struct {
	root    *mux.Router
	next    http.Handler
	storage *xet.Storage
}

// Option defines a functional option for configuring the Handler.
type Option func(*Handler)

// WithNext sets the next http.Handler to call if the request is not handled by this handler.
func WithNext(next http.Handler) Option {
	return func(h *Handler) {
		h.next = next
	}
}

// WithStorage sets the XET CAS storage backend.
func WithStorage(storage *xet.Storage) Option {
	return func(h *Handler) {
		h.storage = storage
	}
}

// NewHandler creates a new XET CAS Handler with the given options.
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
	// XET CAS protocol endpoints
	// Xorb operations - content-addressed blob storage
	h.root.HandleFunc("/api/v1/xorbs/{prefix}/{hash}", h.handlePutXorb).Methods(http.MethodPost, http.MethodPut)
	h.root.HandleFunc("/api/v1/xorbs/{prefix}/{hash}", h.handleGetXorb).Methods(http.MethodGet, http.MethodHead)

	// Shard operations - file-to-xorb mapping metadata
	h.root.HandleFunc("/api/v1/shards", h.handlePutShard).Methods(http.MethodPost)

	// Chunk retrieval - serves individual chunks from xorbs
	h.root.HandleFunc("/api/v1/chunks/{prefix}/{hash}", h.handleGetChunk).Methods(http.MethodGet)

	// Reconstruction - returns instructions for reassembling files from chunks
	h.root.HandleFunc("/api/v1/reconstructions/{file_id:.*}", h.handleGetReconstruction).Methods(http.MethodGet)
	h.root.HandleFunc("/api/v2/reconstructions/{file_id:.*}", h.handleGetReconstructionV2).Methods(http.MethodGet)

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
	enc := json.NewEncoder(w)
	_ = enc.Encode(data)
}
