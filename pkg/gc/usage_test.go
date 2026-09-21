package gc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/util"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/repository"
)

type readDirFailure struct {
	billy.Filesystem
	path string
	err  error
}

func (storage readDirFailure) ReadDir(name string) ([]fs.DirEntry, error) {
	if name == storage.path {
		return nil, storage.err
	}
	return storage.Filesystem.ReadDir(name)
}

func objectsBytes(t *testing.T, fs billy.Filesystem, repoPath string) int64 {
	t.Helper()
	var total int64
	err := util.Walk(fs, repoPath+"/objects", func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s/objects: %v", repoPath, err)
	}
	return total
}

// gitFiles sums the on-disk usage of the given repositories; Objects.Count is left to the caller.
func gitFiles(t *testing.T, fs billy.Filesystem, repoPaths ...string) repository.Usage {
	t.Helper()
	var want repository.Usage
	for _, repoPath := range repoPaths {
		want.Objects.Bytes += objectsBytes(t, fs, repoPath)
		err := util.Walk(fs, repoPath, func(name string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() && !strings.HasPrefix(name, repoPath+"/objects/") {
				want.Other.Count++
				want.Other.Bytes += info.Size()
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", repoPath, err)
		}
	}
	return want
}

func usageSnapshot(t *testing.T, fs billy.Filesystem) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := util.Walk(fs, "/", func(name string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		state := fmt.Sprintf("%v %d %d", info.Mode(), info.Size(), info.ModTime().UnixNano())
		if info.Mode().IsRegular() {
			content, err := util.ReadFile(fs, name)
			if err != nil {
				return err
			}
			sum := sha256.Sum256(content)
			state += " " + hex.EncodeToString(sum[:])
		}
		files[name] = state
		return nil
	})
	if err != nil {
		t.Fatalf("snapshot repositories: %v", err)
	}
	return files
}

func (f *fixture) xetUsage(t *testing.T) xetstorage.Usage {
	t.Helper()
	usage, err := f.xs.Usage(context.Background())
	if err != nil {
		t.Fatalf("xet usage: %v", err)
	}
	if usage.Xorbs.Count == 0 || usage.Shards.Count == 0 || usage.SHA256Index.Count == 0 {
		t.Fatalf("xet usage = %+v, want stored xorbs, shards and sha256 entries", usage)
	}
	return usage
}

type usageStub struct {
	xetstorage.Storage
	usage func(context.Context) (xetstorage.Usage, error)
}

func (stub usageStub) Usage(ctx context.Context) (xetstorage.Usage, error) {
	return stub.usage(ctx)
}

func TestUsage(t *testing.T) {
	ctx := context.Background()
	t.Run("MissingRoot", func(t *testing.T) {
		fixture := newFixture(t)
		for name, collector := range map[string]*Collector{"store": fixture.collector(), "nil store": NewCollector(fixture.st.RepositoriesFS(), nil)} {
			if got, err := collector.Usage(ctx); err != nil || got != (Usage{}) {
				t.Errorf("%s: Usage = %+v, %v; want zero, nil", name, got, err)
			}
		}
	})
	t.Run("MissingRootReportsXet", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.put(t, "usage-x ")
		want := Usage{Xet: fixture.xetUsage(t)}
		if got, err := fixture.collector().Usage(ctx); err != nil || got != want {
			t.Fatalf("Usage = %+v, %v; want %+v, nil", got, err, want)
		}
	})
	t.Run("EmptyRepository", func(t *testing.T) {
		fixture := newFixture(t)
		repos := fixture.st.RepositoriesFS()
		if _, err := repository.Init(ctx, repos, "/org/empty.git", "main"); err != nil {
			t.Fatalf("init: %v", err)
		}
		want := Usage{Usage: gitFiles(t, repos, "/org/empty.git")}
		if got, err := fixture.collector().Usage(ctx); err != nil || want.Other.Count == 0 || got != want {
			t.Fatalf("Usage = %+v, %v; want %+v with metadata only, nil", got, err, want)
		}
	})
	t.Run("SumsRepositories", func(t *testing.T) {
		fixture := newFixture(t)
		oid := fixture.put(t, "usage-a ")
		fixture.commitPointer(t, "org/a", oid)
		fixture.commitPointer(t, "datasets/org/b", oid)
		repos := fixture.st.RepositoriesFS()
		want := Usage{Usage: gitFiles(t, repos, "/org/a.git", "/datasets/org/b.git")}
		want.Objects.Count = 6
		if want.Objects.Bytes <= 0 || want.Objects.Bytes >= objectSize || want.Other.Count == 0 {
			t.Fatalf("repositories hold %+v, want objects between 0 and the %d-byte LFS payload plus metadata", want.Usage, objectSize)
		}
		collector := NewCollector(repos, nil)
		if got, err := collector.Usage(ctx); err != nil || got != want {
			t.Fatalf("Usage = %+v, %v; want %+v, nil", got, err, want)
		}
		fixture.put(t, "usage-b ")
		for name, content := range map[string]string{"/org/a.git/hooks/objects/nested": "hooked\n", "/datasets/org/b.git/note": "root\n"} {
			if err := util.WriteFile(repos, name, []byte(content), 0o644); err != nil {
				t.Fatalf("write %s: %v", name, err)
			}
			want.Other.Count++
			want.Other.Bytes += int64(len(content))
		}
		if err := util.WriteFile(repos, "/org/README", []byte("no repository\n"), 0o644); err != nil {
			t.Fatalf("write sibling: %v", err)
		}
		if got, err := collector.Usage(ctx); err != nil || got != want {
			t.Fatalf("Usage after xet-only upload and metadata = %+v, %v; want %+v, nil", got, err, want)
		}
	})
	t.Run("XetStore", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.commitPointer(t, "org/a", fixture.put(t, "usage-x "))
		repos := fixture.st.RepositoriesFS()
		want := Usage{Usage: gitFiles(t, repos, "/org/a.git"), Xet: fixture.xetUsage(t)}
		want.Objects.Count = 3
		before := usageSnapshot(t, repos)
		collector := fixture.collector()
		got, err := collector.Usage(ctx)
		if err != nil || got != want {
			t.Fatalf("Usage = %+v, %v; want %+v, nil", got, err, want)
		}
		if !maps.Equal(before, usageSnapshot(t, repos)) {
			t.Fatal("Usage changed repository files")
		}
		fixture.put(t, "usage-y ")
		want.Xet = fixture.xetUsage(t)
		if want.Xet == got.Xet {
			t.Fatalf("second upload left xet usage at %+v", want.Xet)
		}
		if got, err := collector.Usage(ctx); err != nil || got != want {
			t.Fatalf("Usage after xet-only upload = %+v, %v; want %+v, nil", got, err, want)
		}
	})
	t.Run("ToleratesGarbage", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.commitPointer(t, "org/a", fixture.put(t, "usage-i "))
		repos := fixture.st.RepositoriesFS()
		for name, content := range map[string]string{
			"/org/a.git/objects/aa/bb": "short",
			"/org/a.git/objects/pack/pack-" + strings.Repeat("f", 40) + ".pack": "no index",
			"/org/a.git/objects/pack/tmp_pack_garbage":                          "partial",
		} {
			if err := util.WriteFile(repos, name, []byte(content), 0o644); err != nil {
				t.Fatalf("write %s: %v", name, err)
			}
		}
		want := Usage{Usage: gitFiles(t, repos, "/org/a.git"), Xet: fixture.xetUsage(t)}
		want.Objects.Count = 3
		if got, err := fixture.collector().Usage(ctx); err != nil || got != want {
			t.Fatalf("Usage = %+v, %v; want %+v, nil", got, err, want)
		}
	})
	t.Run("Canceled", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		fixture := newFixture(t)
		if got, err := fixture.collector().Usage(canceled); !errors.Is(err, context.Canceled) || got != (Usage{}) {
			t.Errorf("missing root: Usage = %+v, %v; want zero, context.Canceled", got, err)
		}
		fixture.commitPointer(t, "org/a", fixture.put(t, "usage-c "))
		if got, err := fixture.collector().Usage(canceled); !errors.Is(err, context.Canceled) || got != (Usage{}) {
			t.Errorf("populated: Usage = %+v, %v; want zero, context.Canceled", got, err)
		}
	})
	t.Run("StoreCancelsContext", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.put(t, "usage-f ")
		for _, populated := range []bool{false, true} {
			if populated {
				fixture.commitPointer(t, "org/a", fixture.put(t, "usage-g "))
			}
			canceling, cancel := context.WithCancel(ctx)
			t.Cleanup(cancel)
			store := usageStub{Storage: fixture.xs, usage: func(context.Context) (xetstorage.Usage, error) {
				cancel()
				return fixture.xs.Usage(ctx)
			}}
			if got, err := NewCollector(fixture.st.RepositoriesFS(), store).Usage(canceling); !errors.Is(err, context.Canceled) || got != (Usage{}) {
				t.Errorf("populated=%t: Usage = %+v, %v; want zero, context.Canceled", populated, got, err)
			}
		}
	})
	t.Run("StoreErrorAborts", func(t *testing.T) {
		fixture := newFixture(t)
		injected := errors.New("injected xet usage failure")
		store := usageStub{Storage: fixture.xs, usage: func(context.Context) (xetstorage.Usage, error) { return xetstorage.Usage{}, injected }}
		for _, populated := range []bool{false, true} {
			if populated {
				fixture.commitPointer(t, "org/a", fixture.put(t, "usage-h "))
			}
			if got, err := NewCollector(fixture.st.RepositoriesFS(), store).Usage(ctx); !errors.Is(err, injected) || got != (Usage{}) {
				t.Errorf("populated=%t: Usage = %+v, %v; want zero, injected error", populated, got, err)
			}
		}
	})
	t.Run("ReadDirErrorAborts", func(t *testing.T) {
		fixture := newFixture(t)
		oid := fixture.put(t, "usage-d ")
		fixture.commitPointer(t, "datasets/org/b", oid)
		fixture.commitPointer(t, "org/a", oid)
		injected := errors.New("injected readdir failure")
		for _, failing := range []string{"/", "/org"} {
			collector := NewCollector(readDirFailure{Filesystem: fixture.st.RepositoriesFS(), path: failing, err: injected}, fixture.xs)
			if got, err := collector.Usage(ctx); !errors.Is(err, injected) || got != (Usage{}) {
				t.Errorf("ReadDir %s failing: Usage = %+v, %v; want zero, injected error", failing, got, err)
			}
		}
	})
	t.Run("BrokenRepoAborts", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.commitPointer(t, "org/a", fixture.put(t, "usage-e "))
		for _, file := range []string{"HEAD", "objects"} {
			if err := util.WriteFile(fixture.st.RepositoriesFS(), "/org/broken.git/"+file, []byte("ref: refs/heads/main\n"), 0o644); err != nil {
				t.Fatalf("write %s: %v", file, err)
			}
		}
		if got, err := fixture.collector().Usage(ctx); err == nil || !strings.Contains(err.Error(), "/org/broken.git") || got != (Usage{}) {
			t.Fatalf("Usage = %+v, %v; want zero and an error naming the broken repository", got, err)
		}
	})
}
