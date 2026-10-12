// Package gc runs Git GC in every repository and unlinks from the LFS object store the OIDs no surviving pointer names;
// reclaiming their data is the store's sweep, which the internal API runs after a prune.
package gc

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/go-git/go-billy/v6"

	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// ErrBusy reports a Prune already running on this Collector.
var ErrBusy = errors.New("gc already running")

// DefaultGrace is the grace window a zero PruneOptions.Grace stands for, the same as the store sweep's default.
const DefaultGrace = time.Hour

// LFSStore is the object store the prune pass unlinks LFS OIDs from; see pkg/gc/xetstore for the xet one.
type LFSStore interface {
	// Walk calls fn for every stored OID with when it was stored; a zero time keeps the object inside any grace window.
	Walk(ctx context.Context, fn func(oid string, storedAt time.Time) error) error
	// Unlink drops oid from the store, reporting whether it was present; the data stays until a sweep reclaims it.
	Unlink(ctx context.Context, oid string) (bool, error)
}

// Collector covers the repository-aware side of GC over the repositories and the LFS object store: the prune pass (Git GC
// in every repository, then unlinking of the OIDs no surviving LFS pointer names) and the repositories' usage.
//
// Liveness is a git pointer in any repository.
// The grace window is keyed on the stored time, which a re-push of the same content may not refresh: an OID deleted
// with one repository and re-pushed to another is exposed until the new ref lands, so Prune
// only while pushes are quiescent.
type Collector struct {
	repos billy.Filesystem
	store LFSStore
	mu    sync.Mutex // serializes Prune
}

// NewCollector creates a Collector over the repositories filesystem and the store; without a store only Usage works.
func NewCollector(repos billy.Filesystem, store LFSStore) *Collector {
	return &Collector{repos: repos, store: store}
}

// PruneOptions configures one prune run.
type PruneOptions struct {
	Grace  time.Duration // shields Git garbage and stored objects younger than this; zero = DefaultGrace, negative = disabled
	DryRun bool          // previews Git GC read-only and unlinks nothing
}

// PruneResult reports one prune run; the Git fields sum the repositories whose GC succeeded, as a preview in a dry run.
type PruneResult struct {
	DryRun            bool              `json:"dry_run"`
	Repositories      int               `json:"repositories"` // repositories scanned, GC failures included
	DeletedGitObjects int               `json:"deleted_git_objects"`
	DeletedGitBytes   int64             `json:"deleted_git_bytes"` // payload of the deleted objects
	ReclaimedBytes    int64             `json:"reclaimed_bytes"`   // shrink of the objects directories on disk; 0 in a dry run, which repacks nothing
	Failed            map[string]string `json:"failed,omitempty"`  // repository path -> Git GC error; all its pointers stay live
	LiveObjects       int               `json:"live_objects"`
	Unlinked          []string          `json:"unlinked"` // sorted OIDs; dry run: what would be unlinked
	SkippedInGrace    int               `json:"skipped_in_grace"`
}

// Prune runs Git GC in every repository, then unlinks the unreferenced OIDs past the grace window;
// the data stays until a sweep reclaims it. Busy runs fail with ErrBusy; any other error comes with the partial result,
// whose Git fields and Unlinked list what already happened.
func (c *Collector) Prune(ctx context.Context, opts PruneOptions) (*PruneResult, error) {
	if !c.mu.TryLock() {
		return nil, ErrBusy
	}
	defer c.mu.Unlock()
	return c.prune(ctx, opts)
}

// prune is the prune pass; the caller holds c.mu.
func (c *Collector) prune(ctx context.Context, opts PruneOptions) (*PruneResult, error) {
	res := &PruneResult{DryRun: opts.DryRun, Unlinked: []string{}}
	if c.store == nil {
		return res, errors.New("gc: no store configured")
	}
	grace := opts.Grace
	if grace == 0 {
		grace = DefaultGrace
	}
	// Same rule as the sweep: second-truncated cutoff, so coarse stored times only widen the shield; zero disables it for Git too.
	var cutoff time.Time
	if grace > 0 {
		cutoff = time.Now().Add(-grace).Truncate(time.Second)
	}
	live := map[string]struct{}{}
	if err := c.mark(ctx, res, cutoff, live); err != nil {
		return res, err
	}
	res.LiveObjects = len(live)

	candidates := []string{}
	err := c.store.Walk(ctx, func(oid string, storedAt time.Time) error {
		if _, ok := live[oid]; ok {
			return nil
		}
		if grace > 0 && (storedAt.IsZero() || !storedAt.Before(cutoff)) {
			res.SkippedInGrace++
			return nil
		}
		candidates = append(candidates, oid)
		return nil
	})
	if err != nil {
		return res, fmt.Errorf("walk objects: %w", err)
	}
	slices.Sort(candidates)

	if opts.DryRun {
		res.Unlinked = candidates
	} else {
		for _, oid := range candidates {
			removed, err := c.store.Unlink(ctx, oid)
			if err != nil {
				err = fmt.Errorf("unlink %s: %w", oid, err)
				slog.ErrorContext(ctx, "gc prune failed after unlinking", "unlinked", len(res.Unlinked), "err", err)
				return res, err
			}
			if removed {
				res.Unlinked = append(res.Unlinked, oid)
			}
		}
	}

	if len(res.Failed) > 0 {
		return res, fmt.Errorf("git gc failed in %d repositories", len(res.Failed))
	}
	return res, nil
}

// mark runs Git GC in every repository, adding its stats to res, then collects the OIDs the surviving LFS pointers name into live.
func (c *Collector) mark(ctx context.Context, res *PruneResult, cutoff time.Time, live map[string]struct{}) error {
	if _, err := c.repos.Stat("/"); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return repository.Walk(ctx, c.repos, "/", func(path string) error {
		repo, err := repository.Open(c.repos, path)
		if err != nil {
			return fmt.Errorf("open %s: %w", path, err)
		}
		gc, err := repo.GC(ctx, cutoff, res.DryRun)
		if err != nil && errors.Is(err, ctx.Err()) {
			return err
		}
		if err != nil {
			if res.Failed == nil {
				res.Failed = map[string]string{}
			}
			res.Failed[path] = err.Error()
		}
		res.DeletedGitObjects += len(gc.DeletedObjects)
		res.DeletedGitBytes += gc.DeletedBytes
		res.ReclaimedBytes += gc.ReclaimedBytes
		// In a dry run the deleted objects are still stored, so they are skipped to mark the post-GC pointer set; a failed GC deleted none.
		ptrs, err := repo.ScanLFSPointersExcept(ctx, gc.DeletedObjects)
		if err != nil {
			return fmt.Errorf("scan %s: %w", path, err)
		}
		for _, ptr := range ptrs {
			live[ptr.OID()] = struct{}{}
		}
		res.Repositories++
		return nil
	})
}
