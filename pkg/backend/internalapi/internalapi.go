// Package internalapi serves hfd's internal management endpoints over the repositories and the xet store.
package internalapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	xetmirror "github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/mirror/spool"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/gc"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// Handler is hfd's unauthenticated management API, meant to sit behind the operator-only --internal gate; it assembles the
// usage report and the GC step from the repository GC, the xet store, the spool and the engine:
// GET /internal/objects lists stored objects,
// GET /internal/usage reports repository, shared Git object, xet storage, spool and mirror index usage,
// POST /internal/gc (?dry_run=&grace=&max=&budget=) runs one GC step: Git GC in every repository, then unlinking of the sha256 index
// entries no reachable LFS pointer names (the prune pass, uncharged and unbounded), then the budgeted sweep passes — storage
// (sha256-anchored, max bounds it), idle spools and the engine index — under the same grace and dry run with budget as one
// wall-clock budget spent storage → spool → mirror; the response carries the prune section even when that pass failed and a
// section per sweep pass that ran and succeeded (storage, spools, mirror), lists failed passes in errors with status 200, and
// done once every pass finished — repeat until done; only a GC step already in flight (409) or a bad parameter (400) fails the request.
type Handler struct {
	gc      *gc.GC
	store   xetstorage.Storage
	sweeper *xetstorage.GC
	spool   *spool.Spool
	mirror  *xetmirror.Mirror
	gcGrace time.Duration
	mu      sync.Mutex // one GC step in flight
	root    *mux.Router
	next    http.Handler
}

// Option defines a functional option for configuring the Handler.
type Option func(*Handler)

// WithGC sets the GC that reports repository usage and runs the prune pass.
func WithGC(g *gc.GC) Option {
	return func(h *Handler) {
		h.gc = g
	}
}

// WithStorage sets the xet store the object listing, the usage report and the storage sweep read.
func WithStorage(st xetstorage.Storage) Option {
	return func(h *Handler) {
		h.store = st
	}
}

// WithSpool sets the ingest spool whose idle files the GC step sweeps and whose usage it reports; nil skips it.
func WithSpool(sp *spool.Spool) Option {
	return func(h *Handler) {
		h.spool = sp
	}
}

// WithMirror sets the xet mirror engine whose index the GC step compacts and whose usage it reports; nil skips it.
func WithMirror(m *xetmirror.Mirror) Option {
	return func(h *Handler) {
		h.mirror = m
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
	if h.store != nil {
		h.sweeper = xetstorage.NewGC(h.store)
	}
	if h.next == nil {
		h.next = http.NotFoundHandler()
	}
	h.root.HandleFunc("/internal/objects", h.handleList).Methods(http.MethodGet)
	h.root.HandleFunc("/internal/usage", h.handleUsage).Methods(http.MethodGet)
	h.root.HandleFunc("/internal/gc", h.handleGC).Methods(http.MethodPost)
	h.root.NotFoundHandler = h.next
	return h
}

// ServeHTTP implements the http.Handler interface.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.root.ServeHTTP(w, r)
}

// gcOptions configures one GC step: grace and dryRun drive every pass; maxDeletes bounds the storage pass; budget is one
// wall-clock budget for the sweep passes; dry runs ignore both bounds.
type gcOptions struct {
	grace      time.Duration // zero = xetstorage.DefaultSweepGrace, negative = disabled
	dryRun     bool
	maxDeletes int           // max shard+xorb deletions per step, 0 = unlimited
	budget     time.Duration // wall-clock cap per step across the sweep passes, 0 = unlimited
}

// parseOptions reads ?dry_run=&grace=&max=&budget=, answering 400 itself on a bad value.
func (h *Handler) parseOptions(w http.ResponseWriter, r *http.Request) (gcOptions, bool) {
	var opts gcOptions
	// Parse the raw query strictly: r.URL.Query() drops malformed pairs, which could silently turn a dry run destructive.
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		http.Error(w, "Invalid query string", http.StatusBadRequest)
		return opts, false
	}
	if opts.dryRun, err = parseDryRun(q); err != nil {
		http.Error(w, "Invalid dry_run value", http.StatusBadRequest)
		return opts, false
	}
	if opts.grace, err = parseGrace(q, h.gcGrace); err != nil {
		http.Error(w, "Invalid grace value", http.StatusBadRequest)
		return opts, false
	}
	if q.Has("max") {
		if opts.maxDeletes, err = strconv.Atoi(q.Get("max")); err != nil || opts.maxDeletes < 0 {
			http.Error(w, "Invalid max value", http.StatusBadRequest)
			return opts, false
		}
	}
	if q.Has("budget") {
		if opts.budget, err = time.ParseDuration(q.Get("budget")); err != nil || opts.budget < 0 {
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

// object is one stored LFS object resolvable by OID.
type object struct {
	OID        string `json:"oid"`
	Size       uint64 `json:"size"`
	UniqueSize uint64 `json:"unique_size"`
	SharedSize uint64 `json:"shared_size"`
}

// listObjects lists the stored objects hfd can resolve by OID, largest first.
func listObjects(ctx context.Context, st xetstorage.Storage) ([]object, error) {
	indexed := map[string]struct{}{}
	err := st.WalkSHA256Index(ctx, func(sha256Hex, _ string) error {
		indexed[sha256Hex] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk sha256 index: %w", err)
	}
	entries, err := xetstorage.ListFiles(ctx, st)
	if err != nil {
		return nil, err
	}
	objects := []object{}
	for _, e := range entries {
		if _, ok := indexed[e.SHA256]; ok {
			objects = append(objects, object{OID: e.SHA256, Size: e.OriginalSize, UniqueSize: e.UniqueSize, SharedSize: e.SharedSize})
		}
	}
	return objects, nil
}

func (h *Handler) handleList(w http.ResponseWriter, r *http.Request) {
	objects, err := listObjects(r.Context(), h.store)
	if err != nil {
		http.Error(w, "List failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, objects)
}

// usageResponse is the usage report; unconfigured sections stay, at zero.
type usageResponse struct {
	Repository    repository.Usage
	SharedObjects repository.ObjectUsage // loose objects of the store the repositories share; Repository leaves them out
	Xet           xetstorage.Usage
	Spool         xetstorage.ObjectUsage
	Mirror        xetstorage.ObjectUsage
}

func (h *Handler) handleUsage(w http.ResponseWriter, r *http.Request) {
	usage, err := h.usage(r)
	if err != nil {
		http.Error(w, "Usage failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, usage)
}

// usage composes the report; it is not an atomic snapshot.
func (h *Handler) usage(r *http.Request) (usageResponse, error) {
	ctx := r.Context()
	var resp usageResponse
	if err := ctx.Err(); err != nil {
		return resp, err
	}
	var err error
	if h.store != nil {
		if resp.Xet, err = h.store.Usage(ctx); err != nil {
			return resp, fmt.Errorf("xet usage: %w", err)
		}
	}
	if h.spool != nil {
		if resp.Spool, err = h.spool.Usage(ctx); err != nil {
			return resp, fmt.Errorf("spool usage: %w", err)
		}
	}
	if h.mirror != nil {
		if resp.Mirror, err = h.mirror.Usage(ctx); err != nil {
			return resp, fmt.Errorf("mirror usage: %w", err)
		}
	}
	if resp.Repository, err = h.gc.Usage(ctx); err != nil {
		return resp, err
	}
	if resp.SharedObjects, err = h.gc.SharedObjectsUsage(ctx); err != nil {
		return resp, fmt.Errorf("shared objects usage: %w", err)
	}
	return resp, nil
}

// storageResult reports the storage pass of one step: counts, not hashes.
type storageResult struct {
	DryRun           bool     `json:"dry_run"`
	SweptShards      int      `json:"swept_shards"`
	SweptXorbs       int      `json:"swept_xorbs"`
	ReclaimedBytes   int64    `json:"reclaimed_bytes"`
	SkippedInGrace   int      `json:"skipped_in_grace"`
	Dangling         []string `json:"dangling"`          // OIDs whose data is missing; reported, never deleted
	UnreadableShards []string `json:"unreadable_shards"` // treated live; no xorb deleted that pass
	Done             bool     `json:"done"`              // the storage pass finished
	RemainingShards  int      `json:"remaining_shards"`
	RemainingXorbs   int      `json:"remaining_xorbs"`
}

// gcResponse reports one GC step: the prune pass keeps its section even when it failed, each sweep pass has one when it ran and
// succeeded, Errors lists the failed passes as "<name>: <message>" in pass order, and Done is true once every configured
// pass finished in this step.
type gcResponse struct {
	DryRun  bool                   `json:"dry_run"`
	Done    bool                   `json:"done"`
	Prune   *gc.PruneResult        `json:"prune,omitempty"`
	Storage *storageResult         `json:"storage,omitempty"`
	Spools  *spool.SweepResult     `json:"spools,omitempty"`
	Mirror  *xetmirror.SweepResult `json:"mirror,omitempty"`
	Errors  []string               `json:"errors,omitempty"`
}

func (h *Handler) handleGC(w http.ResponseWriter, r *http.Request) {
	opts, ok := h.parseOptions(w, r)
	if !ok {
		return
	}
	if !h.mu.TryLock() {
		http.Error(w, "GC already running", http.StatusConflict)
		return
	}
	defer h.mu.Unlock()
	ctx := r.Context()

	// A failed prune does not stop the sweep: it only unlinks, and the sweep reclaims nothing an index entry still anchors.
	pr, err := h.gc.Prune(ctx, gc.PruneOptions{Grace: opts.grace, DryRun: opts.dryRun})
	if errors.Is(err, gc.ErrBusy) {
		http.Error(w, "GC already running", http.StatusConflict)
		return
	}
	resp := gcResponse{DryRun: opts.dryRun, Prune: pr}
	done := err == nil
	if err != nil {
		resp.Errors = append(resp.Errors, "prune: "+err.Error())
	}

	var deadline time.Time
	if opts.budget > 0 && !opts.dryRun {
		deadline = time.Now().Add(opts.budget)
	}
	swept, ended := 0, true
	if h.sweeper != nil {
		// hfd sweeps sha256-anchored: once prune drops the OID, the files entry alone keeps nothing alive.
		xr, err := h.sweeper.SweepStep(ctx, xetstorage.SweepOptions{
			Anchor: xetstorage.AnchorSHA256, Grace: opts.grace, DryRun: opts.dryRun, MaxDeletes: opts.maxDeletes, Budget: opts.budget,
		})
		if err != nil {
			resp.Errors = append(resp.Errors, "storage: "+err.Error())
			done = false
		} else {
			resp.Storage = &storageResult{
				DryRun:           opts.dryRun,
				SweptShards:      len(xr.SweptShards),
				SweptXorbs:       len(xr.SweptXorbs),
				ReclaimedBytes:   xr.ReclaimedBytes,
				SkippedInGrace:   xr.SkippedInGrace,
				Dangling:         nonNil(xr.DanglingSHA256Entries),
				UnreadableShards: nonNil(xr.UnreadableShards),
				Done:             xr.Done,
				RemainingShards:  xr.RemainingShards,
				RemainingXorbs:   xr.RemainingXorbs,
			}
			swept = len(xr.SweptShards) + len(xr.SweptXorbs)
			done = done && xr.Done
		}
		// The later passes wait for the storage pass to finish, not for one that failed.
		ended = err != nil || xr.Done
	}
	// handDown returns the budget left for the next pass; false defers it because this step already swept and spent its budget.
	handDown := func() (time.Duration, bool) {
		if deadline.IsZero() {
			return 0, true
		}
		if left := time.Until(deadline); left > 0 {
			return left, true
		}
		if swept > 0 {
			return 0, false
		}
		return time.Nanosecond, true
	}
	if ended && h.spool != nil {
		if b, ok := handDown(); !ok {
			done = false
		} else if spools, err := h.spool.Sweep(ctx, spool.SweepOptions{Grace: opts.grace, DryRun: opts.dryRun, Budget: b}); err != nil {
			resp.Errors = append(resp.Errors, "spools: "+err.Error())
			done = false
		} else {
			resp.Spools = &spools
			swept += spools.SweptSpools
			done = done && spools.Done
		}
	}
	if ended && h.mirror != nil {
		if b, ok := handDown(); !ok {
			done = false
		} else if index, err := h.mirror.Sweep(ctx, xetmirror.SweepOptions{Grace: opts.grace, DryRun: opts.dryRun, Budget: b}); err != nil {
			resp.Errors = append(resp.Errors, "mirror: "+err.Error())
			done = false
		} else {
			resp.Mirror = &index
			done = done && index.Done
		}
	}
	resp.Done = done
	writeJSON(w, http.StatusOK, resp)
}

// nonNil keeps an empty list encoding as [] rather than null.
func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
