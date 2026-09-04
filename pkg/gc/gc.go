// Package gc reclaims xet LFS content no git repository references any more.
package gc

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-billy/v6"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/repository"
)

var zeroSHA256 = strings.Repeat("0", 64)

// ErrInvalidOID reports an OID that is not a non-zero 64-hex sha256 digest.
var ErrInvalidOID = errors.New("invalid oid")

// Collector marks live LFS OIDs across all repositories and sweeps the rest from the xet store.
//
// Liveness is a git pointer in any repository; Unlink takes precedence, so content unlinked that
// way is reclaimed by the next sweep even while a pointer still names it.
// The grace window is keyed on shard mtime, which a dedup hit does not refresh: an OID deleted
// with one repository and re-pushed to another is exposed until the new ref lands, so Collect
// only while pushes are quiescent.
type Collector struct {
	repos billy.Filesystem
	store xetstorage.GCStore
	gc    *xetstorage.GC
	mu    sync.Mutex // serializes Collect and SweepStep; xet's own GC lock underneath is then never contended
}

// NewCollector creates a Collector over the repositories filesystem and the xet store.
func NewCollector(repos billy.Filesystem, store xetstorage.GCStore) *Collector {
	return &Collector{repos: repos, store: store, gc: xetstorage.NewGC(store)}
}

// SweepOptions configures one SweepStep.
type SweepOptions struct {
	Grace      time.Duration // zero = xetstorage.DefaultSweepGrace, negative = disabled
	DryRun     bool
	MaxDeletes int           // max shard+xorb deletions per step, 0 = unlimited
	Budget     time.Duration // wall-clock cap per step, 0 = unlimited
}

// SweepResult reports one sweep step.
type SweepResult struct {
	DryRun           bool     `json:"dry_run"`
	SweptShards      int      `json:"swept_shards"`
	SweptXorbs       int      `json:"swept_xorbs"`
	ReclaimedBytes   int64    `json:"reclaimed_bytes"`
	SkippedInGrace   int      `json:"skipped_in_grace"`
	Dangling         []string `json:"dangling"`          // OIDs whose data is missing; reported, never deleted
	UnreadableShards []string `json:"unreadable_shards"` // treated live; no xorb deleted that pass
	Done             bool     `json:"done"`
	RemainingShards  int      `json:"remaining_shards"`
	RemainingXorbs   int      `json:"remaining_xorbs"`
}

// SweepStep runs one bounded sha256-anchored sweep step under the same lock as Collect, so the store has a single sweeper.
func (c *Collector) SweepStep(ctx context.Context, opts SweepOptions) (*SweepResult, error) {
	if !c.mu.TryLock() {
		return nil, xetstorage.ErrGCBusy
	}
	defer c.mu.Unlock()
	return c.sweep(ctx, opts)
}

// sweep is the only place hfd builds xet's sweep options, always sha256-anchored; the caller holds c.mu.
func (c *Collector) sweep(ctx context.Context, opts SweepOptions) (*SweepResult, error) {
	xr, err := c.gc.SweepStep(ctx, xetstorage.SweepOptions{
		Anchor: xetstorage.AnchorSHA256, Grace: opts.Grace, DryRun: opts.DryRun, MaxDeletes: opts.MaxDeletes, Budget: opts.Budget,
	})
	if err != nil {
		return nil, err
	}
	return &SweepResult{
		DryRun:           xr.DryRun,
		SweptShards:      len(xr.SweptShards),
		SweptXorbs:       len(xr.SweptXorbs),
		ReclaimedBytes:   xr.ReclaimedBytes,
		SkippedInGrace:   xr.SkippedInGrace,
		Dangling:         xr.DanglingSHA256Entries,
		UnreadableShards: xr.UnreadableShards,
		Done:             xr.Done,
		RemainingShards:  xr.RemainingShards,
		RemainingXorbs:   xr.RemainingXorbs,
	}, nil
}

// Options configures one Collect run.
type Options struct {
	Grace  time.Duration // zero = xetstorage.DefaultSweepGrace, negative = disabled
	DryRun bool
}

// Result reports one Collect run.
type Result struct {
	DryRun         bool         `json:"dry_run"`
	Repositories   int          `json:"repositories"`
	LiveObjects    int          `json:"live_objects"`
	Unlinked       []string     `json:"unlinked"` // sorted sha256 hex; dry run: what would be unlinked
	SkippedInGrace int          `json:"skipped_in_grace"`
	Sweep          *SweepResult `json:"sweep,omitempty"` // nil on dry run
	Error          string       `json:"error,omitempty"` // set by the HTTP layer when the run failed after Unlinked was applied
}

// Collect unlinks unreferenced OIDs past the grace window, then runs one sha256-anchored sweep; busy runs fail with xetstorage.ErrGCBusy.
// An error after unlinking began comes with the partial Result, whose Unlinked lists the entries already removed.
func (c *Collector) Collect(ctx context.Context, opts Options) (*Result, error) {
	if !c.mu.TryLock() {
		return nil, xetstorage.ErrGCBusy
	}
	defer c.mu.Unlock()

	res := &Result{DryRun: opts.DryRun, Unlinked: []string{}}
	live := map[string]struct{}{}
	repos, err := c.mark(ctx, "/", live)
	if err != nil {
		return nil, err
	}
	res.Repositories, res.LiveObjects = repos, len(live)

	grace := opts.Grace
	if grace == 0 {
		grace = xetstorage.DefaultSweepGrace
	}
	// Same rule as xet's sweep: second-truncated cutoff, so S3's coarse mtimes only widen the shield.
	cutoff := time.Now().Add(-grace).Truncate(time.Second)
	shards := map[string]time.Time{}
	err = c.store.WalkShards(ctx, func(shardHash string, _ int64, modTime time.Time) error {
		shards[shardHash] = modTime
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk shards: %w", err)
	}
	candidates := []string{}
	err = c.store.WalkSHA256Index(ctx, func(sha256Hex, shardHash string) error {
		if sha256Hex == zeroSHA256 {
			return nil
		}
		if _, ok := live[sha256Hex]; ok {
			return nil
		}
		if grace > 0 {
			if modTime, ok := shards[shardHash]; !ok || modTime.IsZero() || !modTime.Before(cutoff) {
				res.SkippedInGrace++
				return nil
			}
		}
		candidates = append(candidates, sha256Hex)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk sha256 index: %w", err)
	}
	slices.Sort(candidates)

	if opts.DryRun {
		res.Unlinked = candidates
	} else {
		for _, h := range candidates {
			digest, err := parseOID(h)
			if err != nil {
				return nil, fmt.Errorf("invalid sha256 index entry %q: %w", h, err)
			}
			removed, err := c.gc.UnlinkSHA256(ctx, digest)
			if err != nil {
				return res, c.failed(ctx, res, fmt.Errorf("unlink sha256 %s: %w", h, err))
			}
			if removed {
				res.Unlinked = append(res.Unlinked, h)
			}
		}
		res.Sweep, err = c.sweep(ctx, SweepOptions{Grace: opts.Grace})
		if err != nil {
			return res, c.failed(ctx, res, fmt.Errorf("sweep: %w", err))
		}
	}

	var sweptShards, sweptXorbs int
	var reclaimed int64
	if res.Sweep != nil {
		sweptShards, sweptXorbs, reclaimed = res.Sweep.SweptShards, res.Sweep.SweptXorbs, res.Sweep.ReclaimedBytes
	}
	slog.InfoContext(ctx, "gc collect", "repositories", res.Repositories, "live", res.LiveObjects,
		"unlinked", len(res.Unlinked), "skipped_in_grace", res.SkippedInGrace, "swept_shards", sweptShards,
		"swept_xorbs", sweptXorbs, "reclaimed_bytes", reclaimed, "dry_run", res.DryRun)
	return res, nil
}

// failed logs a run that aborted after unlinking began and returns err.
func (c *Collector) failed(ctx context.Context, res *Result, err error) error {
	slog.ErrorContext(ctx, "gc collect failed after unlinking", "unlinked", len(res.Unlinked), "err", err)
	return err
}

// parseOID decodes a 64-hex sha256 digest, rejecting the all-zero one (the shared empty-file marker).
func parseOID(oid string) ([32]byte, error) {
	raw, err := hex.DecodeString(oid)
	if err != nil {
		return [32]byte{}, fmt.Errorf("%w: %v", ErrInvalidOID, err)
	}
	if len(raw) != 32 {
		return [32]byte{}, fmt.Errorf("%w: %d bytes", ErrInvalidOID, len(raw))
	}
	digest := [32]byte(raw)
	if digest == [32]byte{} {
		return digest, fmt.Errorf("%w: all-zero digest", ErrInvalidOID)
	}
	return digest, nil
}

// Unlink removes the OID's sha256 index entry, reporting whether it existed; data is reclaimed by the next sweep.
func (c *Collector) Unlink(ctx context.Context, oid string) (bool, error) {
	digest, err := parseOID(oid)
	if err != nil {
		return false, err
	}
	return c.gc.UnlinkSHA256(ctx, digest)
}

// Object is one stored LFS object resolvable by OID.
type Object struct {
	OID        string `json:"oid"`
	Size       uint64 `json:"size"`
	UniqueSize uint64 `json:"unique_size"`
	SharedSize uint64 `json:"shared_size"`
}

// List lists the stored objects hfd can resolve by OID, largest first.
func (c *Collector) List(ctx context.Context) ([]Object, error) {
	indexed := map[string]struct{}{}
	err := c.store.WalkSHA256Index(ctx, func(sha256Hex, _ string) error {
		indexed[sha256Hex] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk sha256 index: %w", err)
	}
	entries, err := xetstorage.ListFiles(ctx, c.store)
	if err != nil {
		return nil, err
	}
	objects := []Object{}
	for _, e := range entries {
		if _, ok := indexed[e.SHA256]; ok {
			objects = append(objects, Object{OID: e.SHA256, Size: e.OriginalSize, UniqueSize: e.UniqueSize, SharedSize: e.SharedSize})
		}
	}
	return objects, nil
}

// mark walks dir for repositories at any depth, adding their LFS OIDs to live and returning the
// repository count. A .git-suffixed directory without a valid HEAD is a namespace, unless it
// holds git internals: then it is a damaged repository and the run aborts rather than miss it.
func (c *Collector) mark(ctx context.Context, dir string, live map[string]struct{}) (int, error) {
	entries, err := c.repos.ReadDir(dir)
	if err != nil {
		if dir == "/" && errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("read %s: %w", dir, err)
	}
	repos := 0
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if !e.IsDir() {
			continue
		}
		path := filepath.Join(dir, e.Name())
		if !strings.HasSuffix(e.Name(), ".git") || !repository.IsRepository(c.repos, path) {
			if strings.HasSuffix(e.Name(), ".git") {
				if _, err := c.repos.Stat(filepath.Join(path, "objects")); err == nil {
					return 0, fmt.Errorf("damaged repository %s: git internals without a valid HEAD", path)
				}
			}
			n, err := c.mark(ctx, path, live)
			if err != nil {
				return 0, err
			}
			repos += n
			continue
		}
		repo, err := repository.Open(c.repos, path)
		if err != nil {
			return 0, fmt.Errorf("open %s: %w", path, err)
		}
		ptrs, err := repo.ScanLFSPointers()
		if err != nil {
			return 0, fmt.Errorf("scan %s: %w", path, err)
		}
		for _, ptr := range ptrs {
			live[ptr.OID()] = struct{}{}
		}
		repos++
	}
	return repos, nil
}
