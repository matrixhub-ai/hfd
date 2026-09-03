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

// maxDepth bounds the repository walk: repo.git, ns/repo.git, {datasets,spaces}/ns/repo.git.
const maxDepth = 3

var zeroSHA256 = strings.Repeat("0", 64)

// Collector marks live LFS OIDs across all repositories and sweeps the rest from the xet store.
type Collector struct {
	repos billy.Filesystem
	store xetstorage.GCStore
	gc    *xetstorage.GC
	mu    sync.Mutex
}

// NewCollector creates a Collector over the repositories filesystem and the xet store.
func NewCollector(repos billy.Filesystem, store xetstorage.GCStore) *Collector {
	return &Collector{repos: repos, store: store, gc: xetstorage.NewGC(store)}
}

// Options configures one Collect run.
type Options struct {
	Grace  time.Duration // zero = xetstorage.DefaultSweepGrace, negative = disabled
	DryRun bool
}

// Result reports one Collect run.
type Result struct {
	DryRun         bool                    `json:"dry_run"`
	Repositories   int                     `json:"repositories"`
	LiveObjects    int                     `json:"live_objects"`
	Unlinked       []string                `json:"unlinked"` // sorted sha256 hex; dry run: what would be unlinked
	SkippedInGrace int                     `json:"skipped_in_grace"`
	Sweep          *xetstorage.SweepResult `json:"sweep,omitempty"` // nil on dry run
}

// Collect unlinks unreferenced OIDs past the grace window, then runs one sha256-anchored sweep; busy runs fail with xetstorage.ErrGCBusy.
func (c *Collector) Collect(ctx context.Context, opts Options) (*Result, error) {
	if !c.mu.TryLock() {
		return nil, xetstorage.ErrGCBusy
	}
	defer c.mu.Unlock()

	res := &Result{DryRun: opts.DryRun, Unlinked: []string{}}
	live := map[string]struct{}{}
	repos, err := c.mark(ctx, "/", 0, live)
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
			raw, err := hex.DecodeString(h)
			if err != nil {
				return nil, fmt.Errorf("invalid sha256 index entry %q: %w", h, err)
			}
			if len(raw) != 32 {
				return nil, fmt.Errorf("invalid sha256 index entry %q: %d bytes", h, len(raw))
			}
			removed, err := c.gc.UnlinkSHA256(ctx, [32]byte(raw))
			if err != nil {
				return nil, fmt.Errorf("unlink sha256 %s: %w", h, err)
			}
			if removed {
				res.Unlinked = append(res.Unlinked, h)
			}
		}
		res.Sweep, err = c.gc.SweepStep(ctx, xetstorage.SweepOptions{Anchor: xetstorage.AnchorSHA256, Grace: opts.Grace})
		if err != nil {
			return nil, fmt.Errorf("sweep: %w", err)
		}
	}

	var sweptShards, sweptXorbs int
	var reclaimed int64
	if res.Sweep != nil {
		sweptShards, sweptXorbs, reclaimed = len(res.Sweep.SweptShards), len(res.Sweep.SweptXorbs), res.Sweep.ReclaimedBytes
	}
	slog.InfoContext(ctx, "gc collect", "repositories", res.Repositories, "live", res.LiveObjects,
		"unlinked", len(res.Unlinked), "skipped_in_grace", res.SkippedInGrace, "swept_shards", sweptShards,
		"swept_xorbs", sweptXorbs, "reclaimed_bytes", reclaimed, "dry_run", res.DryRun)
	return res, nil
}

// mark walks dir for repositories, adding their LFS OIDs to live and returning the repository count.
func (c *Collector) mark(ctx context.Context, dir string, depth int, live map[string]struct{}) (int, error) {
	entries, err := c.repos.ReadDir(dir)
	if err != nil {
		if depth == 0 && errors.Is(err, fs.ErrNotExist) {
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
		if !strings.HasSuffix(e.Name(), ".git") {
			if depth+1 < maxDepth {
				n, err := c.mark(ctx, path, depth+1, live)
				if err != nil {
					return 0, err
				}
				repos += n
			}
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
