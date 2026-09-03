// Package internalapi serves the internal management endpoints hfd adds in front of xet's.
package internalapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/gc"
)

// Handler serves POST /internal/gc?dry_run=&grace= over a gc.Collector.
// The endpoint is unauthenticated and meant to be mounted behind the operator-only --internal gate.
// The collector's sweep lock is independent of xet's own /internal/gc/sweep handler, so the two must not be run concurrently.
type Handler struct {
	collector *gc.Collector
	gcGrace   time.Duration
	root      *mux.Router
	next      http.Handler
}

// Option defines a functional option for configuring the Handler.
type Option func(*Handler)

// WithCollector sets the collector the GC endpoint runs.
func WithCollector(c *gc.Collector) Option {
	return func(h *Handler) {
		h.collector = c
	}
}

// WithGCGrace sets the grace used when the request omits ?grace; zero = xetstorage.DefaultSweepGrace, negative = disabled.
func WithGCGrace(d time.Duration) Option {
	return func(h *Handler) {
		h.gcGrace = d
	}
}

// WithNext sets the next http.Handler to call if a request does not match any internal route.
func WithNext(next http.Handler) Option {
	return func(h *Handler) {
		h.next = next
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
	h.root.HandleFunc("/internal/gc", h.handleGC).Methods(http.MethodPost)
	h.root.NotFoundHandler = h.next
	return h
}

// ServeHTTP implements the http.Handler interface.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.root.ServeHTTP(w, r)
}

func (h *Handler) handleGC(w http.ResponseWriter, r *http.Request) {
	opts := gc.Options{Grace: h.gcGrace}
	q := r.URL.Query()
	if q.Has("dry_run") {
		dryRun, err := strconv.ParseBool(q.Get("dry_run"))
		if err != nil {
			http.Error(w, "Invalid dry_run value", http.StatusBadRequest)
			return
		}
		opts.DryRun = dryRun
	}
	if q.Has("grace") {
		grace, err := time.ParseDuration(q.Get("grace"))
		if err != nil || grace < 0 {
			http.Error(w, "Invalid grace value", http.StatusBadRequest)
			return
		}
		if grace == 0 {
			grace = -1 // explicit zero disables the window; zero means default in Collect
		}
		opts.Grace = grace
	}
	res, err := h.collector.Collect(r.Context(), opts)
	if err != nil {
		if errors.Is(err, xetstorage.ErrGCBusy) {
			http.Error(w, "GC already running", http.StatusConflict)
			return
		}
		http.Error(w, "GC failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}
