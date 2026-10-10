package gc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	xetmirror "github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/mirror/spool"
	xetstorage "github.com/wzshiming/xet/storage"
)

// newSpool builds a spool over the fixture's xet store in a directory of its own.
func (f *fixture) newSpool(t *testing.T) (*spool.Spool, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "spool")
	sp, err := spool.NewSpool(dir, f.xs)
	if err != nil {
		t.Fatalf("new spool: %v", err)
	}
	return sp, dir
}

// newEngine builds a xet mirror engine over the fixture's store sharing sp, returning it with its index directory.
func (f *fixture) newEngine(t *testing.T, sp *spool.Spool) (*xetmirror.Mirror, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "mirror")
	engine, err := xetmirror.NewMirror(xetmirror.WithStorage(f.xs), xetmirror.WithCacheDir(dir), xetmirror.WithSpool(sp))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return engine, filepath.Join(dir, "index")
}

// plant writes content to dir/name, creating dir.
func plant(t *testing.T, dir, name, content string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// plantIndexTemp leaves the temp file of an interrupted manifest write in the engine index.
func plantIndexTemp(t *testing.T, indexDir string) string {
	t.Helper()
	return plant(t, filepath.Join(indexDir, "repo", "commits"), "."+strings.Repeat("c", 40)+".json.tmp", "{}")
}

func fileExists(t *testing.T, p string) bool {
	t.Helper()
	_, err := os.Stat(p)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stat %s: %v", p, err)
	}
	return err == nil
}

func mustSweep(t *testing.T, c *Collector, opts Options) *SweepResult {
	t.Helper()
	res, err := c.SweepStep(context.Background(), opts)
	if err != nil {
		t.Fatalf("sweep step %+v: %v", opts, err)
	}
	return res
}

func TestSweepSpools(t *testing.T) {
	f := newFixture(t)
	sp, dir := f.newSpool(t)
	const content = "spooled bytes"
	idle := plant(t, dir, "idle.spool", content)
	other := plant(t, dir, "notaspool.txt", content)
	ageFiles(t, dir, 2*time.Hour)
	c := NewCollector(f.st.RepositoriesFS(), f.xs, WithSpool(sp))

	// The spool is past the default 1h window, so only a forwarded 24h grace spares it.
	res := mustSweep(t, c, Options{Grace: 24 * time.Hour})
	if !res.Done || res.Spools == nil || *res.Spools != (spool.SweepResult{}) || res.Mirror != nil || !fileExists(t, idle) {
		t.Fatalf("sweep within grace: %+v spools=%+v, want nothing swept", res, res.Spools)
	}
	for _, dry := range []bool{true, false} {
		res := mustSweep(t, c, Options{Grace: -1, DryRun: dry})
		want := spool.SweepResult{DryRun: dry, SweptSpools: 1, ReclaimedBytes: int64(len(content))}
		if !res.Done || res.Spools == nil || *res.Spools != want || res.Mirror != nil {
			t.Fatalf("sweep(dry_run=%t): %+v spools=%+v, want %+v", dry, res, res.Spools, want)
		}
		if fileExists(t, idle) != dry {
			t.Fatalf("sweep(dry_run=%t): idle spool present = %t", dry, !dry)
		}
	}
	if res := mustSweep(t, c, Options{Grace: -1}); res.Spools == nil || *res.Spools != (spool.SweepResult{}) {
		t.Fatalf("sweep after reclaim: spools=%+v, want none swept", res.Spools)
	}
	if !fileExists(t, other) {
		t.Fatal("sweep removed a file without the .spool suffix")
	}
}

func TestSweepSkipsSpoolsAndIndexUntilDone(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	sp, spoolDir := f.newSpool(t)
	engine, indexDir := f.newEngine(t, sp)
	idle := plant(t, spoolDir, "idle.spool", "idle")
	tmp := plantIndexTemp(t, indexDir)
	c := NewCollector(f.st.RepositoriesFS(), f.xs, WithSpool(sp), WithMirror(engine))
	f.put(t, "dead-s ")
	f.put(t, "dead-t ")
	if res, err := c.Prune(ctx, PruneOptions{Grace: -1}); err != nil || len(res.Unlinked) != 2 {
		t.Fatalf("prune: result=%+v err=%v", res, err)
	}

	// The cap stops the storage pass after one shard, so neither follow-up pass may run yet.
	res := mustSweep(t, c, Options{Grace: -1, MaxDeletes: 1})
	t.Logf("bounded step: %+v", *res)
	if res.Done || res.SweptShards != 1 || res.Spools != nil || res.Mirror != nil || !fileExists(t, idle) || !fileExists(t, tmp) {
		t.Fatalf("bounded step: %+v, want an unfinished storage pass with the spool and index untouched", res)
	}
	res = mustSweep(t, c, Options{Grace: -1})
	t.Logf("finishing step: %+v", *res)
	if !res.Done || fileExists(t, idle) || fileExists(t, tmp) {
		t.Fatalf("finishing step: %+v, want the idle spool and the index temp file removed", res)
	}
	body, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"spools":{"dry_run":false,"swept_spools":1,"reclaimed_bytes":4}`,
		`"mirror":{"dry_run":false,"dropped_entries":0,"removed_manifests":0,"removed_temp_files":1}`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("finishing step body %s: missing %s", body, want)
		}
	}
}

func TestSweepWithoutSpoolOrMirror(t *testing.T) {
	f := newFixture(t)
	for name, c := range map[string]*Collector{
		"unset": f.collector(),
		"nil":   NewCollector(f.st.RepositoriesFS(), f.xs, WithSpool(nil), WithMirror(nil)),
	} {
		res := mustSweep(t, c, Options{Grace: -1})
		body, err := json.Marshal(res)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Done || res.Spools != nil || res.Mirror != nil || strings.Contains(string(body), `"spools"`) || strings.Contains(string(body), `"mirror"`) {
			t.Errorf("%s: sweep %s, want a finished step without spools or mirror", name, body)
		}
	}
}

func TestSweepIndex(t *testing.T) {
	f := newFixture(t)
	sp, _ := f.newSpool(t)
	engine, indexDir := f.newEngine(t, sp)
	tmp := plantIndexTemp(t, indexDir)
	ageFiles(t, indexDir, 2*time.Hour)
	c := NewCollector(f.st.RepositoriesFS(), f.xs, WithMirror(engine))

	// The temp file is past the default 1h window, so only a forwarded 24h grace spares it.
	res := mustSweep(t, c, Options{Grace: 24 * time.Hour})
	if !res.Done || res.Mirror == nil || *res.Mirror != (xetmirror.SweepResult{}) || res.Spools != nil || !fileExists(t, tmp) {
		t.Fatalf("sweep within grace: %+v index=%+v, want nothing removed", res, res.Mirror)
	}
	for _, dry := range []bool{true, false} {
		res := mustSweep(t, c, Options{Grace: -1, DryRun: dry})
		want := xetmirror.SweepResult{DryRun: dry, RemovedTempFiles: 1}
		if !res.Done || res.Mirror == nil || *res.Mirror != want || fileExists(t, tmp) != dry {
			t.Fatalf("sweep(dry_run=%t): %+v index=%+v, want %+v", dry, res, res.Mirror, want)
		}
	}

	// An engine-less deployment still sweeps its spool.
	res = mustSweep(t, NewCollector(f.st.RepositoriesFS(), f.xs, WithSpool(sp), WithMirror(nil)), Options{Grace: -1})
	if !res.Done || res.Spools == nil || res.Mirror != nil {
		t.Fatalf("engine-less sweep: %+v spools=%+v", res, res.Spools)
	}
}

func TestUsageSpoolAndIndex(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	sp, spoolDir := f.newSpool(t)
	engine, indexDir := f.newEngine(t, sp)
	const content = "spooled usage"
	plant(t, spoolDir, "usage.spool", content)
	plant(t, spoolDir, "notaspool.txt", content)
	plantIndexTemp(t, indexDir)
	index, err := engine.Usage(ctx)
	if err != nil || index.Index.Count != 1 {
		t.Fatalf("engine usage = %+v, %v; want the one index file", index, err)
	}

	// The fixture's empty store and missing repositories root add nothing.
	want := Usage{Spool: xetstorage.ObjectUsage{Count: 1, Bytes: int64(len(content))}, Index: index.Index}
	got, err := NewCollector(f.st.RepositoriesFS(), f.xs, WithSpool(sp), WithMirror(engine)).Usage(ctx)
	if err != nil || got != want {
		t.Fatalf("Usage = %+v, %v; want %+v, nil", got, err, want)
	}
	body, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{fmt.Sprintf(`"Spool":{"Count":1,"Bytes":%d}`, len(content)), fmt.Sprintf(`"Index":{"Count":1,"Bytes":%d}`, index.Index.Bytes)} {
		if !strings.Contains(string(body), key) {
			t.Errorf("usage body %s: missing %s", body, key)
		}
	}

	got, err = f.collector().Usage(ctx)
	if err != nil || got != (Usage{}) {
		t.Fatalf("unconfigured Usage = %+v, %v; want zero, nil", got, err)
	}
	if body, err := json.Marshal(got); err != nil || strings.Contains(string(body), `"Spool"`) || strings.Contains(string(body), `"Index"`) {
		t.Fatalf("unconfigured usage body %s, %v; want no Spool or Index", body, err)
	}
}

// A spool or index directory the walk cannot read fails the sweep step and Usage without a partial result.
func TestSpoolOrIndexWalkError(t *testing.T) {
	ctx := context.Background()
	for _, broken := range []string{"spool", "index"} {
		t.Run(broken, func(t *testing.T) {
			f := newFixture(t)
			sp, spoolDir := f.newSpool(t)
			engine, indexDir := f.newEngine(t, sp)
			dir := map[string]string{"spool": spoolDir, "index": indexDir}[broken]
			// A regular file where the directory belongs fails ReadDir with an error other than ErrNotExist.
			if err := os.Remove(dir); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(dir, []byte("not a directory"), 0o644); err != nil {
				t.Fatal(err)
			}
			c := NewCollector(f.st.RepositoriesFS(), f.xs, WithSpool(sp), WithMirror(engine))
			// The sweep reports the index under the engine's name, Usage under its own.
			sweepErr := map[string]string{"spool": "spool sweep", "index": "mirror sweep"}[broken]
			if res, err := c.SweepStep(ctx, Options{Grace: -1}); err == nil || res != nil || !strings.Contains(err.Error(), sweepErr) {
				t.Errorf("SweepStep = %+v, %v; want no result and the %s error", res, err, sweepErr)
			}
			if got, err := c.Usage(ctx); err == nil || got != (Usage{}) || !strings.Contains(err.Error(), broken+" usage") {
				t.Errorf("Usage = %+v, %v; want zero and the %s usage error", got, err, broken)
			}
		})
	}
}
