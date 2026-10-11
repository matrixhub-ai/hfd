// Package gc runs Git GC in every repository and prunes the xet sha256 index entries no surviving LFS pointer names;
// reclaiming the data is the xet storage GC's sweep, which the internal API runs after a prune.
package gc

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
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

// Collector covers the repository-aware side of GC over the repositories and the xet store: the prune pass (Git GC in every
// repository, then unlinking of the sha256 index entries no surviving LFS pointer names), the object listing and the
// repositories' usage.
//
// Liveness is a git pointer in any repository.
// The grace window is keyed on shard mtime, which a dedup hit does not refresh: an OID deleted
// with one repository and re-pushed to another is exposed until the new ref lands, so Prune
// only while pushes are quiescent.
type Collector struct {
	repos billy.Filesystem
	store xetstorage.Storage
	gc    *xetstorage.GC
	mu    sync.Mutex // serializes Prune
}

// NewCollector creates a Collector over the repositories filesystem and the xet store.
func NewCollector(repos billy.Filesystem, store xetstorage.Storage) *Collector {
	return &Collector{repos: repos, store: store, gc: xetstorage.NewGC(store)}
}

// PruneOptions configures one prune run.
type PruneOptions struct {
	Grace  time.Duration // shields Git garbage and shards younger than this; zero = xetstorage.DefaultSweepGrace, negative = disabled
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
	Unlinked          []string          `json:"unlinked"` // sorted sha256 hex; dry run: what would be unlinked
	SkippedInGrace    int               `json:"skipped_in_grace"`
}

// Prune runs Git GC in every repository, then unlinks the unreferenced OIDs past the grace window;
// the data stays until a sweep reclaims it. Busy runs fail with xetstorage.ErrGCBusy; any other error comes with the partial result,
// whose Git fields and Unlinked list what already happened.
func (c *Collector) Prune(ctx context.Context, opts PruneOptions) (*PruneResult, error) {
	if !c.mu.TryLock() {
		return nil, xetstorage.ErrGCBusy
	}
	defer c.mu.Unlock()
	return c.prune(ctx, opts)
}

// prune is the prune pass; the caller holds c.mu.
func (c *Collector) prune(ctx context.Context, opts PruneOptions) (*PruneResult, error) {
	res := &PruneResult{DryRun: opts.DryRun, Unlinked: []string{}}
	grace := opts.Grace
	if grace == 0 {
		grace = xetstorage.DefaultSweepGrace
	}
	// Same rule as xet's sweep: second-truncated cutoff, so S3's coarse mtimes only widen the shield; zero disables it for Git too.
	var cutoff time.Time
	if grace > 0 {
		cutoff = time.Now().Add(-grace).Truncate(time.Second)
	}
	live := map[string]struct{}{}
	if err := c.mark(ctx, res, cutoff, live); err != nil {
		return res, err
	}
	res.LiveObjects = len(live)

	shards := map[string]time.Time{}
	err := c.store.WalkShards(ctx, func(shardHash string, _ int64, modTime time.Time) error {
		shards[shardHash] = modTime
		return nil
	})
	if err != nil {
		return res, fmt.Errorf("walk shards: %w", err)
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
		return res, fmt.Errorf("walk sha256 index: %w", err)
	}
	slices.Sort(candidates)

	if opts.DryRun {
		res.Unlinked = candidates
	} else {
		for _, h := range candidates {
			digest, err := parseOID(h)
			if err != nil {
				return res, fmt.Errorf("invalid sha256 index entry %q: %w", h, err)
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

	if len(res.Failed) > 0 {
		return res, fmt.Errorf("git gc failed in %d repositories", len(res.Failed))
	}
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
			slog.ErrorContext(ctx, "git gc failed", "repository", path, "err", err)
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
