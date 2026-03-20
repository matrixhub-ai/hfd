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
	h.root.HandleFunc("/xet/cas/objects/{hash:[a-f0-9]+}", h.handleGetObject).Methods(http.MethodGet, http.MethodHead)
	h.root.HandleFunc("/xet/cas/objects/{hash:[a-f0-9]+}", h.handlePutObject).Methods(http.MethodPut)
	h.root.HandleFunc("/xet/cas/objects/has", h.handleHas).Methods(http.MethodPost)
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
