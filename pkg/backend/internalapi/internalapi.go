// Package internalapi serves hfd's internal management endpoints over the xet store.
package internalapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/gc"
)

// Handler is hfd's unauthenticated management API, meant to sit behind the operator-only --internal gate:
// GET /internal/objects and DELETE /internal/objects/{oid} list and unlink stored objects,
// POST /internal/gc runs a repository-aware collect and POST /internal/gc/sweep one sha256-anchored sweep step,
// both taking ?dry_run=&grace=&max=&budget=; all over one gc.Collector, so the store has a single sweeper.
type Handler struct {
	collector *gc.Collector
	gcGrace   time.Duration
	root      *mux.Router
	next      http.Handler
}

// Option defines a functional option for configuring the Handler.
type Option func(*Handler)

// WithCollector sets the collector the GC endpoints run.
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
	h.root.HandleFunc("/internal/objects", h.handleList).Methods(http.MethodGet)
	h.root.HandleFunc("/internal/objects/{oid}", h.handleUnlink).Methods(http.MethodDelete)
	h.root.HandleFunc("/internal/gc", h.handleGC).Methods(http.MethodPost)
	h.root.HandleFunc("/internal/gc/sweep", h.handleSweep).Methods(http.MethodPost)
	h.root.NotFoundHandler = h.next
	return h
}

// ServeHTTP implements the http.Handler interface.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.root.ServeHTTP(w, r)
}

// parseOptions reads ?dry_run=&grace=&max=&budget= for both GC endpoints, answering 400 itself on a bad value.
func (h *Handler) parseOptions(w http.ResponseWriter, r *http.Request) (gc.Options, bool) {
	var opts gc.Options
	// Parse the raw query strictly: r.URL.Query() drops malformed pairs, which could silently turn a dry run destructive.
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		http.Error(w, "Invalid query string", http.StatusBadRequest)
		return opts, false
	}
	if opts.DryRun, err = parseDryRun(q); err != nil {
		http.Error(w, "Invalid dry_run value", http.StatusBadRequest)
		return opts, false
	}
	if opts.Grace, err = parseGrace(q, h.gcGrace); err != nil {
		http.Error(w, "Invalid grace value", http.StatusBadRequest)
		return opts, false
	}
	if q.Has("max") {
		if opts.MaxDeletes, err = strconv.Atoi(q.Get("max")); err != nil || opts.MaxDeletes < 0 {
			http.Error(w, "Invalid max value", http.StatusBadRequest)
			return opts, false
		}
	}
	if q.Has("budget") {
		if opts.Budget, err = time.ParseDuration(q.Get("budget")); err != nil || opts.Budget < 0 {
			http.Error(w, "Invalid budget value", http.StatusBadRequest)
			return opts, false
		}
	}
	return opts, true
}

// parseDryRun reads ?dry_run; a present but unparsable value is an error.
func parseDryRun(q url.Values) (bool, error) {
	if !q.Has("dry_run") {
		return false, nil
	}
	return strconv.ParseBool(q.Get("dry_run"))
}

// parseGrace reads ?grace over def: omitted keeps def, an explicit zero disables the window, negative is an error.
func parseGrace(q url.Values, def time.Duration) (time.Duration, error) {
	if !q.Has("grace") {
		return def, nil
	}
	grace, err := time.ParseDuration(q.Get("grace"))
	if err != nil || grace < 0 {
		return 0, errors.New("invalid grace")
	}
	if grace == 0 {
		grace = -1 // zero means default further down
	}
	return grace, nil
}

func (h *Handler) handleList(w http.ResponseWriter, r *http.Request) {
	objects, err := h.collector.List(r.Context())
	if err != nil {
		http.Error(w, "List failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, objects)
}

func (h *Handler) handleUnlink(w http.ResponseWriter, r *http.Request) {
	removed, err := h.collector.Unlink(r.Context(), mux.Vars(r)["oid"])
	if err != nil {
		if errors.Is(err, gc.ErrInvalidOID) {
			http.Error(w, "Invalid oid", http.StatusBadRequest)
			return
		}
		http.Error(w, "Unlink failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if !removed {
		http.Error(w, "Object not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleGC(w http.ResponseWriter, r *http.Request) {
	opts, ok := h.parseOptions(w, r)
	if !ok {
		return
	}
	res, err := h.collector.Collect(r.Context(), opts)
	if err != nil {
		if errors.Is(err, xetstorage.ErrGCBusy) {
			http.Error(w, "GC already running", http.StatusConflict)
			return
		}
		if res == nil {
			http.Error(w, "GC failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		// Unlinks already happened: report them alongside the failure.
		res.Error = err.Error()
		writeJSON(w, http.StatusInternalServerError, res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *Handler) handleSweep(w http.ResponseWriter, r *http.Request) {
	opts, ok := h.parseOptions(w, r)
	if !ok {
		return
	}
	res, err := h.collector.SweepStep(r.Context(), opts)
	if err != nil {
		if errors.Is(err, xetstorage.ErrGCBusy) {
			http.Error(w, "GC already running", http.StatusConflict)
			return
		}
		http.Error(w, "Sweep failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
