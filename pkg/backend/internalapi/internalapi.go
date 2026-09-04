// Package internalapi serves the internal management endpoints hfd adds in front of xet's.
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

// Handler serves POST /internal/gc?dry_run=&grace= (repository-aware collect) and
// POST /internal/gc/sweep?dry_run=&grace=&max=&budget=&anchor= (xet's sweep contract) over one
// gc.Collector, so both share a single lock and the store has exactly one sweeper; the sweep
// route shadows xet's own when this handler is mounted in front of it. The endpoints are
// unauthenticated and meant to be mounted behind the operator-only --internal gate.
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
	h.root.HandleFunc("/internal/gc", h.handleGC).Methods(http.MethodPost)
	h.root.HandleFunc("/internal/gc/sweep", h.handleSweep).Methods(http.MethodPost)
	h.root.NotFoundHandler = h.next
	return h
}

// ServeHTTP implements the http.Handler interface.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.root.ServeHTTP(w, r)
}

// query parses the raw query strictly: r.URL.Query() drops malformed pairs, which could silently turn a dry run destructive.
func query(w http.ResponseWriter, r *http.Request) (url.Values, bool) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		http.Error(w, "Invalid query string", http.StatusBadRequest)
		return nil, false
	}
	return q, true
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

func (h *Handler) handleGC(w http.ResponseWriter, r *http.Request) {
	q, ok := query(w, r)
	if !ok {
		return
	}
	var opts gc.Options
	var err error
	if opts.DryRun, err = parseDryRun(q); err != nil {
		http.Error(w, "Invalid dry_run value", http.StatusBadRequest)
		return
	}
	if opts.Grace, err = parseGrace(q, h.gcGrace); err != nil {
		http.Error(w, "Invalid grace value", http.StatusBadRequest)
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
	q, ok := query(w, r)
	if !ok {
		return
	}
	var opts xetstorage.SweepOptions
	var err error
	if opts.DryRun, err = parseDryRun(q); err != nil {
		http.Error(w, "Invalid dry_run value", http.StatusBadRequest)
		return
	}
	if opts.Grace, err = parseGrace(q, h.gcGrace); err != nil {
		http.Error(w, "Invalid grace value", http.StatusBadRequest)
		return
	}
	if q.Has("max") {
		if opts.MaxDeletes, err = strconv.Atoi(q.Get("max")); err != nil || opts.MaxDeletes < 0 {
			http.Error(w, "Invalid max value", http.StatusBadRequest)
			return
		}
	}
	if q.Has("budget") {
		if opts.Budget, err = time.ParseDuration(q.Get("budget")); err != nil || opts.Budget < 0 {
			http.Error(w, "Invalid budget value", http.StatusBadRequest)
			return
		}
	}
	if q.Has("anchor") {
		switch q.Get("anchor") {
		case "both":
			opts.Anchor = xetstorage.AnchorBoth
		case "files":
			opts.Anchor = xetstorage.AnchorFiles
		case "sha256":
			opts.Anchor = xetstorage.AnchorSHA256
		default:
			http.Error(w, "Invalid anchor value", http.StatusBadRequest)
			return
		}
	}
	res, err := h.collector.Sweep(r.Context(), opts)
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
