package gc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6/util"
	xetclient "github.com/wzshiming/xet/client"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

const objectSize = 64 * 1024

type fixture struct {
	st *storage.Storage
	xs *xetstorage.FileStorage
	m  *mirror.Mirror
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "xet")
	client, err := xetclient.NewClient(xetclient.WithCacheDir(filepath.Join(dataDir, "chunks")))
	if err != nil {
		t.Fatalf("new xet client: %v", err)
	}
	xs, err := xetstorage.NewFileStorage(xetstorage.WithBasePath(filepath.Join(dataDir, "storage")))
	if err != nil {
		t.Fatalf("new xet storage: %v", err)
	}
	m, err := mirror.NewMirror(mirror.WithXETStorage(xs), mirror.WithXETClient(client), mirror.WithDataDir(dataDir))
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}
	t.Cleanup(m.Wait)
	return &fixture{st: storage.NewStorage(storage.WithRootDir(t.TempDir())), xs: xs, m: m}
}

func (f *fixture) collector() *Collector {
	return NewCollector(f.st.RepositoriesFS(), f.xs)
}

func (f *fixture) put(t *testing.T, seed string) string {
	t.Helper()
	data := bytes.Repeat([]byte(seed), objectSize/len(seed)+1)[:objectSize]
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])
	if err := f.m.PutObject(context.Background(), oid, bytes.NewReader(data), objectSize); err != nil {
		t.Fatalf("put object %s: %v", seed, err)
	}
	return oid
}

func (f *fixture) commitPointer(t *testing.T, name, oid string) {
	t.Helper()
	ctx := context.Background()
	repo, err := repository.Init(ctx, f.st.RepositoriesFS(), repository.ResolvePath(name), "main")
	if err != nil {
		t.Fatalf("init %s: %v", name, err)
	}
	ptr := fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, objectSize)
	_, err = repo.CreateCommit(ctx, "main", "add model", "Test", "test@test.com",
		[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: "model.bin", Content: []byte(ptr)}}, "")
	if err != nil {
		t.Fatalf("commit %s: %v", name, err)
	}
}

func (f *fixture) stored(t *testing.T, oid string) bool {
	t.Helper()
	raw, err := hex.DecodeString(oid)
	if err != nil {
		t.Fatalf("decode oid: %v", err)
	}
	_, err = f.xs.GetFileHashBySHA256(context.Background(), "default", [32]byte(raw))
	return err == nil
}

func TestCollect(t *testing.T) {
	ctx := context.Background()
	t.Run("UnlinksUnreferenced", func(t *testing.T) {
		f := newFixture(t)
		live, dead := f.put(t, "live-a "), f.put(t, "dead-a ")
		f.commitPointer(t, "org/repo", live)
		res, err := f.collector().Collect(ctx, Options{Grace: -1})
		if err != nil {
			t.Fatalf("collect: %v", err)
		}
		if res.Repositories != 1 || res.LiveObjects != 1 || !slices.Equal(res.Unlinked, []string{dead}) || res.SkippedInGrace != 0 {
			t.Fatalf("unexpected result: %+v", res)
		}
		if res.Sweep == nil || !res.Sweep.Done || len(res.Sweep.SweptShards) == 0 {
			t.Fatalf("unexpected sweep: %+v", res.Sweep)
		}
		if !f.stored(t, live) || f.stored(t, dead) {
			t.Fatalf("live stored=%v dead stored=%v", f.stored(t, live), f.stored(t, dead))
		}
	})
	t.Run("GraceShieldsFresh", func(t *testing.T) {
		f := newFixture(t)
		live, dead := f.put(t, "live-b "), f.put(t, "dead-b ")
		f.commitPointer(t, "org/repo", live)
		res, err := f.collector().Collect(ctx, Options{})
		if err != nil {
			t.Fatalf("collect: %v", err)
		}
		if len(res.Unlinked) != 0 || res.SkippedInGrace != 1 || !f.stored(t, dead) {
			t.Fatalf("unexpected result: %+v stored=%v", res, f.stored(t, dead))
		}
	})
	t.Run("DryRunDeletesNothing", func(t *testing.T) {
		f := newFixture(t)
		live, dead := f.put(t, "live-c "), f.put(t, "dead-c ")
		f.commitPointer(t, "org/repo", live)
		res, err := f.collector().Collect(ctx, Options{Grace: -1, DryRun: true})
		if err != nil {
			t.Fatalf("collect: %v", err)
		}
		if !res.DryRun || !slices.Equal(res.Unlinked, []string{dead}) || res.Sweep != nil || !f.stored(t, dead) {
			t.Fatalf("unexpected result: %+v stored=%v", res, f.stored(t, dead))
		}
	})
	t.Run("DatasetsLayout", func(t *testing.T) {
		f := newFixture(t)
		live := f.put(t, "live-d ")
		f.commitPointer(t, "datasets/org/ds", live)
		res, err := f.collector().Collect(ctx, Options{Grace: -1})
		if err != nil {
			t.Fatalf("collect: %v", err)
		}
		if res.Repositories != 1 || res.LiveObjects != 1 || !f.stored(t, live) {
			t.Fatalf("unexpected result: %+v stored=%v", res, f.stored(t, live))
		}
	})
	t.Run("BrokenRepoAborts", func(t *testing.T) {
		f := newFixture(t)
		live, dead := f.put(t, "live-e "), f.put(t, "dead-e ")
		f.commitPointer(t, "org/repo", live)
		// go-git opens a HEAD-only dir as an empty repo; a file where objects/ belongs makes the scan fail.
		for _, file := range []string{"HEAD", "objects"} {
			if err := util.WriteFile(f.st.RepositoriesFS(), "/org/broken.git/"+file, []byte("ref: refs/heads/main\n"), 0o644); err != nil {
				t.Fatalf("write %s: %v", file, err)
			}
		}
		if _, err := f.collector().Collect(ctx, Options{Grace: -1}); err == nil || !strings.Contains(err.Error(), "/org/broken.git") {
			t.Fatalf("collect: expected error naming the broken repository, got %v", err)
		}
		if !f.stored(t, dead) {
			t.Fatal("dead object unlinked despite aborted collect")
		}
	})
	t.Run("MissingRoot", func(t *testing.T) {
		f := newFixture(t)
		res, err := f.collector().Collect(ctx, Options{Grace: -1})
		if err != nil {
			t.Fatalf("collect: %v", err)
		}
		if res.Repositories != 0 || res.LiveObjects != 0 || len(res.Unlinked) != 0 {
			t.Fatalf("unexpected result: %+v", res)
		}
	})
}
