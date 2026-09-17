// Package gc reclaims xet LFS content and shared git objects no git repository references any more.
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
	"github.com/go-git/go-git/v6/plumbing"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/repository"
)

var zeroSHA256 = strings.Repeat("0", 64)

// ErrInvalidOID reports an OID that is not a non-zero 64-hex sha256 digest.
var ErrInvalidOID = errors.New("invalid oid")

// now is the sweep budget clock; tests replace it.
var now = time.Now

// Collector prunes xet sha256 index entries no repository LFS pointer names; SweepStep reclaims the data afterwards,
// then the shared git objects no repository reaches.
//
// Liveness is a git pointer in any repository.
// The grace window is keyed on shard mtime, which a dedup hit does not refresh: an OID deleted
// with one repository and re-pushed to another is exposed until the new ref lands, so Prune
// only while pushes are quiescent.
type Collector struct {
	repos billy.Filesystem
	store xetstorage.GCStore
	gc    *xetstorage.GC
	mu    sync.Mutex // serializes Prune and SweepStep
}

// NewCollector creates a Collector over the repositories filesystem and the xet store.
func NewCollector(repos billy.Filesystem, store xetstorage.GCStore) *Collector {
	return &Collector{repos: repos, store: store, gc: xetstorage.NewGC(store)}
}

// Options configures one sweep step; MaxDeletes and Budget bound the sweep, while prune is uncharged and unbounded.
type Options struct {
	Grace      time.Duration // zero = xetstorage.DefaultSweepGrace, negative = disabled
	DryRun     bool
	MaxDeletes int           // max shard+xorb+git object deletions per sweep, 0 = unlimited
	Budget     time.Duration // wall-clock cap per sweep, 0 = unlimited
}

// SweepResult reports one sweep step over both stores.
type SweepResult struct {
	DryRun              bool     `json:"dry_run"`
	SweptShards         int      `json:"swept_shards"`
	SweptXorbs          int      `json:"swept_xorbs"`
	SweptGitObjects     int      `json:"swept_git_objects"`
	ReclaimedBytes      int64    `json:"reclaimed_bytes"`
	SkippedInGrace      int      `json:"skipped_in_grace"`
	Dangling            []string `json:"dangling"`          // OIDs whose data is missing; reported, never deleted
	UnreadableShards    []string `json:"unreadable_shards"` // treated live; no xorb deleted that pass
	Done                bool     `json:"done"`
	RemainingShards     int      `json:"remaining_shards"`
	RemainingXorbs      int      `json:"remaining_xorbs"`
	RemainingGitObjects int      `json:"remaining_git_objects"`
}

// SweepStep runs one bounded sweep step under the same lock as Prune, so the stores have a single sweeper:
// xet's sha256-anchored pass first, then, once that pass is done, the shared git objects no repository reaches.
func (c *Collector) SweepStep(ctx context.Context, opts Options) (*SweepResult, error) {
	if !c.mu.TryLock() {
		return nil, xetstorage.ErrGCBusy
	}
	defer c.mu.Unlock()
	return c.sweep(ctx, opts)
}

type gitObject struct {
	hash plumbing.Hash
	size int64
}

// sweep is the only place hfd builds xet's sweep options, always sha256-anchored; the caller holds c.mu.
// MaxDeletes and Budget span both stores: the whole xet pass is charged, git marking is not.
func (c *Collector) sweep(ctx context.Context, opts Options) (*SweepResult, error) {
	start := now()
	xr, err := c.gc.SweepStep(ctx, xetstorage.SweepOptions{
		Anchor: xetstorage.AnchorSHA256, Grace: opts.Grace, DryRun: opts.DryRun, MaxDeletes: opts.MaxDeletes, Budget: opts.Budget,
	})
	if err != nil {
		return nil, err
	}
	res := &SweepResult{
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
	}
	if !xr.Done {
		return res, nil
	}

	markStart := now()
	// Keyed by hex so the ObjectID format field cannot split equal digests.
	live := map[string]struct{}{}
	_, err = c.forEachRepository(ctx, func(repo *repository.Repository) error {
		return repo.WalkObjects(ctx, func(hash plumbing.Hash, _ plumbing.ObjectType) error {
			live[hash.String()] = struct{}{}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	grace := opts.Grace
	if grace == 0 {
		grace = xetstorage.DefaultSweepGrace
	}
	cutoff := start.Add(-grace).Truncate(time.Second)
	var dead []gitObject
	err = repository.WalkSharedObjects(c.repos, func(hash plumbing.Hash, info fs.FileInfo) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, ok := live[hash.String()]; ok {
			return nil
		}
		if grace > 0 && (info.ModTime().IsZero() || !info.ModTime().Before(cutoff)) {
			res.SkippedInGrace++
			return nil
		}
		dead = append(dead, gitObject{hash: hash, size: info.Size()})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk shared objects: %w", err)
	}
	slices.SortFunc(dead, func(a, b gitObject) int { return a.hash.Compare(b.hash.Bytes()) })
	start = start.Add(now().Sub(markStart))

	swept := res.SweptShards + res.SweptXorbs
	exhausted := func() bool {
		return !opts.DryRun && ((opts.MaxDeletes > 0 && swept >= opts.MaxDeletes) ||
			(opts.Budget > 0 && swept > 0 && now().Sub(start) >= opts.Budget))
	}
	for i, obj := range dead {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if exhausted() {
			res.Done, res.RemainingGitObjects = false, len(dead)-i
			return res, nil
		}
		if !opts.DryRun {
			err := repository.RemoveSharedObject(c.repos, obj.hash)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("remove shared object %s: %w", obj.hash, err)
			}
			swept++
		}
		res.SweptGitObjects++
		res.ReclaimedBytes += obj.size
	}
	return res, nil
}

// PruneOptions configures one prune run.
type PruneOptions struct {
	Grace  time.Duration // zero = xetstorage.DefaultSweepGrace, negative = disabled
	DryRun bool
}

// PruneResult reports one prune run.
type PruneResult struct {
	DryRun         bool     `json:"dry_run"`
	Repositories   int      `json:"repositories"`
	LiveObjects    int      `json:"live_objects"`
	Unlinked       []string `json:"unlinked"` // sorted sha256 hex; dry run: what would be unlinked
	SkippedInGrace int      `json:"skipped_in_grace"`
	Error          string   `json:"error,omitempty"` // set by the HTTP layer when the run failed after Unlinked was applied
}

// Prune unlinks unreferenced OIDs past the grace window; the data stays until SweepStep reclaims it.
// Busy runs fail with xetstorage.ErrGCBusy; an error after unlinking began comes with the partial result, whose Unlinked lists the entries already removed.
func (c *Collector) Prune(ctx context.Context, opts PruneOptions) (*PruneResult, error) {
	if !c.mu.TryLock() {
		return nil, xetstorage.ErrGCBusy
	}
	defer c.mu.Unlock()

	res := &PruneResult{DryRun: opts.DryRun, Unlinked: []string{}}
	live := map[string]struct{}{}
	repos, err := c.mark(ctx, live)
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
				err = fmt.Errorf("unlink sha256 %s: %w", h, err)
				slog.ErrorContext(ctx, "gc prune failed after unlinking", "unlinked", len(res.Unlinked), "err", err)
				return res, err
			}
			if removed {
				res.Unlinked = append(res.Unlinked, h)
			}
		}
	}

	slog.InfoContext(ctx, "gc prune", "repositories", res.Repositories, "live", res.LiveObjects,
		"unlinked", len(res.Unlinked), "skipped_in_grace", res.SkippedInGrace, "dry_run", res.DryRun)
	return res, nil
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

// mark walks the repositories for LFS pointers, adding their OIDs to live and returning the repository count.
func (c *Collector) mark(ctx context.Context, live map[string]struct{}) (int, error) {
	return c.forEachRepository(ctx, func(repo *repository.Repository) error {
		ptrs, err := repo.ScanLFSPointers(ctx)
		if err != nil {
			return err
		}
		for _, ptr := range ptrs {
			live[ptr.OID()] = struct{}{}
		}
		return nil
	})
}

// forEachRepository opens every repository under the root for fn and returns how many there were; any error aborts.
func (c *Collector) forEachRepository(ctx context.Context, fn func(*repository.Repository) error) (int, error) {
	if _, err := c.repos.Stat("/"); errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	repos := 0
	err := repository.Walk(ctx, c.repos, "/", func(path string) error {
		if fi, err := c.repos.Stat(filepath.Join(path, "objects")); err == nil && !fi.IsDir() {
			return fmt.Errorf("damaged repository %s: objects is not a directory", path)
		}
		repo, err := repository.Open(c.repos, path)
		if err != nil {
			return fmt.Errorf("open %s: %w", path, err)
		}
		if err := fn(repo); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		repos++
		return nil
	})
	if err != nil {
		return 0, err
	}
	return repos, nil
}
