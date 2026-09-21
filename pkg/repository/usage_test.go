package repository

// Tests for Repository.Usage: every stored object counts once, bytes split
// between objects/ and the rest, and reading never writes.

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6/plumbing"
)

// usageBlob returns n incompressible bytes, so packing cannot shrink them away.
func usageBlob(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

type usageFixture struct {
	bare string
	work string
	repo *Repository
}

func buildUsageFixture(t *testing.T) *usageFixture {
	t.Helper()
	root := t.TempDir()
	bare, work := filepath.Join(root, "repo.git"), filepath.Join(root, "work")
	runGit(t, "", "init", "--bare", "--initial-branch=main", bare)
	// Every push lands as its own pack and receive-pack must not gc behind the test.
	runGit(t, bare, "config", "receive.unpackLimit", "1")
	runGit(t, bare, "config", "gc.auto", "0")
	initParityWork(t, work)
	runGit(t, work, "remote", "add", "origin", bare)
	commitFile(t, work, "file.txt", "one\n", "c1")
	runGit(t, work, "push", "-q", "origin", "main")
	runGit(t, work, "tag", "-a", "v1", "-m", "v1", "main")
	runGit(t, work, "push", "-q", "origin", "v1")
	// A pushed then deleted branch leaves its pack as garbage.
	runGit(t, work, "checkout", "-q", "-b", "tmp", "main")
	commitFile(t, work, "tmp.bin", string(usageBlob(64<<10)), "tmp")
	runGit(t, work, "push", "-q", "origin", "tmp")
	runGit(t, bare, "update-ref", "-d", "refs/heads/tmp")
	repo, err := Open(osfs.Default, bare)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	// hfd writes loose objects; rolling main back orphans them.
	main := strings.TrimSpace(gitOut(t, bare, "rev-parse", "refs/heads/main"))
	_, err = repo.CreateCommit(t.Context(), "main", "orphan", "Test", "test@example.com",
		[]CommitOperation{{Type: CommitOperationAdd, Path: "orphan.bin", Content: usageBlob(64 << 10)}}, main)
	if err != nil {
		t.Fatalf("create orphan commit: %v", err)
	}
	runGit(t, bare, "update-ref", "refs/heads/main", main)
	return &usageFixture{bare: bare, work: work, repo: repo}
}

// memfsUsageRepo builds a repository off the OS filesystem with one live commit on main and one orphaned commit.
func memfsUsageRepo(t *testing.T, fs billy.Filesystem) *Repository {
	t.Helper()
	ctx := t.Context()
	repo, err := Init(ctx, fs, "/repo.git", "main")
	if err != nil {
		t.Fatalf("init repository: %v", err)
	}
	live, err := repo.CreateCommit(ctx, "main", "live", "Test", "test@example.com",
		[]CommitOperation{{Type: CommitOperationAdd, Path: "file.txt", Content: []byte("one\n")}}, "")
	if err != nil {
		t.Fatalf("create live commit: %v", err)
	}
	_, err = repo.CreateCommit(ctx, "main", "orphan", "Test", "test@example.com",
		[]CommitOperation{{Type: CommitOperationAdd, Path: "orphan.bin", Content: usageBlob(64 << 10)}}, live)
	if err != nil {
		t.Fatalf("create orphan commit: %v", err)
	}
	if err := repo.repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), plumbing.NewHash(live))); err != nil {
		t.Fatal(err)
	}
	return repo
}

// gitObjectIDs lists every object native git finds at bare, loose or packed, each once.
func gitObjectIDs(t *testing.T, bare string) map[string]bool {
	t.Helper()
	ids := map[string]bool{}
	for _, id := range strings.Fields(gitOut(t, bare, "cat-file", "--batch-all-objects", "--batch-check=%(objectname)")) {
		ids[id] = true
	}
	return ids
}

// usageFile pins a file's metadata and content for byte-for-byte comparisons.
type usageFile struct {
	mode     fs.FileMode
	size     int64
	modified int64
	digest   [sha256.Size]byte
}

// usageSnapshot records every file and directory under path on fs.
func usageSnapshot(t *testing.T, fs billy.Filesystem, path string) map[string]usageFile {
	t.Helper()
	files := map[string]usageFile{}
	err := util.Walk(fs, path, func(name string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		state := usageFile{mode: info.Mode(), size: info.Size(), modified: info.ModTime().UnixNano()}
		if info.Mode().IsRegular() {
			content, err := util.ReadFile(fs, name)
			if err != nil {
				return err
			}
			state.digest = sha256.Sum256(content)
		}
		files[name] = state
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// usageOf is the Usage a snapshot of the repository at path implies, with count stored objects.
func usageOf(files map[string]usageFile, path string, count int64) Usage {
	objects := filepath.Join(path, "objects") + string(filepath.Separator)
	usage := Usage{Objects: ObjectUsage{Count: count}}
	for name, f := range files {
		switch {
		case f.mode.IsDir():
		case strings.HasPrefix(name, objects):
			usage.Objects.Bytes += f.size
		default:
			usage.Other.Count++
			usage.Other.Bytes += f.size
		}
	}
	return usage
}

func TestUsage(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		f := buildUsageFixture(t)
		requireGitMode(t, f.repo, native)
		ids := gitObjectIDs(t, f.bare)
		if reachable := strings.Count(gitOut(t, f.bare, "rev-list", "--objects", "--all"), "\n"); reachable >= len(ids) {
			t.Fatalf("fixture stores %d objects, want unreachable ones beyond the %d reachable", len(ids), reachable)
		}
		before := usageSnapshot(t, osfs.Default, f.bare)
		got, err := f.repo.Usage(t.Context())
		if err != nil {
			t.Fatalf("Usage: %v", err)
		}
		if want := usageOf(before, f.bare, int64(len(ids))); got != want {
			t.Errorf("Usage = %+v, want %+v", got, want)
		}
		if !maps.Equal(before, usageSnapshot(t, osfs.Default, f.bare)) {
			t.Fatal("Usage changed repository files")
		}
	})

	t.Run("memfs", func(t *testing.T) {
		fs := memfs.New()
		repo := memfsUsageRepo(t, fs)
		want := usageOf(usageSnapshot(t, fs, "/repo.git"), "/repo.git", 6)
		if want.Objects.Bytes == 0 || want.Other.Count == 0 {
			t.Fatalf("fixture holds %+v, want loose objects and metadata", want)
		}
		if got, err := repo.Usage(t.Context()); err != nil || got != want {
			t.Fatalf("Usage = %+v, %v; want %+v, nil", got, err, want)
		}
		// Only the top-level objects/ tree counts as objects.
		for name, content := range map[string]string{"/repo.git/note": "root\n", "/repo.git/hooks/objects/nested": "hooked\n", "/repo.git/objects-extra/file": "not objects\n"} {
			if err := util.WriteFile(fs, name, []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
			want.Other.Count++
			want.Other.Bytes += int64(len(content))
		}
		if got, err := repo.Usage(t.Context()); err != nil || got != want {
			t.Errorf("Usage with added metadata = %+v, %v; want %+v, nil", got, err, want)
		}
	})

	t.Run("Empty", func(t *testing.T) {
		fs := memfs.New()
		repo, err := Init(t.Context(), fs, "/empty.git", "main")
		if err != nil {
			t.Fatal(err)
		}
		want := usageOf(usageSnapshot(t, fs, "/empty.git"), "/empty.git", 0)
		if got, err := repo.Usage(t.Context()); err != nil || want.Objects != (ObjectUsage{}) || want.Other.Count == 0 || got != want {
			t.Fatalf("Usage = %+v, %v; want %+v with metadata only, nil", got, err, want)
		}
	})

	t.Run("MissingObjects", func(t *testing.T) {
		filesystem := memfs.New()
		repo, err := Init(t.Context(), filesystem, "/empty.git", "main")
		if err != nil {
			t.Fatal(err)
		}
		if err := util.RemoveAll(filesystem, "/empty.git/objects"); err != nil {
			t.Fatal(err)
		}
		if got, err := repo.Usage(t.Context()); !errors.Is(err, os.ErrNotExist) || got != (Usage{}) {
			t.Fatalf("Usage = %+v, %v; want zero, os.ErrNotExist", got, err)
		}
	})

	t.Run("DanglingObjects", func(t *testing.T) {
		filesystem := osfs.New(t.TempDir())
		repo, err := Init(t.Context(), filesystem, "/empty.git", "main")
		if err != nil {
			t.Fatal(err)
		}
		if err := util.RemoveAll(filesystem, "/empty.git/objects"); err != nil {
			t.Fatal(err)
		}
		if err := filesystem.Symlink("missing", "/empty.git/objects"); err != nil {
			t.Fatal(err)
		}
		if got, err := repo.Usage(t.Context()); !errors.Is(err, os.ErrNotExist) || got != (Usage{}) {
			t.Fatalf("Usage = %+v, %v; want zero, os.ErrNotExist", got, err)
		}
	})

	t.Run("ListingError", func(t *testing.T) {
		fs := memfs.New()
		repo, err := Init(t.Context(), fs, "/empty.git", "main")
		if err != nil {
			t.Fatal(err)
		}
		errListing := errors.New("listing failed")
		for _, failing := range []string{"/empty.git/refs", "/empty.git/objects/pack"} {
			repo.fs = readDirHook{Filesystem: fs, hook: func(path string) error {
				if path == failing {
					return errListing
				}
				return nil
			}}
			if got, err := repo.Usage(t.Context()); !errors.Is(err, errListing) || got != (Usage{}) {
				t.Errorf("ReadDir %s failing: Usage = %+v, %v; want zero, %v", failing, got, err, errListing)
			}
		}
	})

	t.Run("CorruptIndex", func(t *testing.T) {
		fs := memfs.New()
		repo, err := Init(t.Context(), fs, "/empty.git", "main")
		if err != nil {
			t.Fatal(err)
		}
		for _, ext := range []string{".idx", ".pack"} {
			if err := util.WriteFile(fs, "/empty.git/objects/pack/pack-"+strings.Repeat("a", 40)+ext, []byte("garbage"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if got, err := repo.Usage(t.Context()); err == nil || errors.Is(err, os.ErrNotExist) || got != (Usage{}) {
			t.Fatalf("Usage = %+v, %v; want zero and a corrupt index error", got, err)
		}
	})

	t.Run("SymlinkObjects", func(t *testing.T) {
		for name, outside := range map[string]bool{"inside": false, "outside": true} {
			t.Run(name, func(t *testing.T) {
				bare := filepath.Join(t.TempDir(), "repo.git")
				repo, err := Init(t.Context(), osfs.Default, bare, "main")
				if err != nil {
					t.Fatal(err)
				}
				target := filepath.Join(bare, "store")
				if outside {
					target = filepath.Join(t.TempDir(), "store")
				}
				objects := filepath.Join(bare, "objects")
				rel, err := filepath.Rel(bare, target)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(objects, target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(rel, objects); err != nil {
					t.Fatal(err)
				}
				if got, err := repo.Usage(t.Context()); err == nil || errors.Is(err, os.ErrNotExist) || got != (Usage{}) {
					t.Fatalf("Usage = %+v, %v; want zero and a non-directory error", got, err)
				}
			})
		}
	})
}

func TestUsageCancelled(t *testing.T) {
	populated := memfsUsageRepo(t, memfs.New())
	empty, err := Init(t.Context(), memfs.New(), "/empty.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for name, repo := range map[string]*Repository{"populated": populated, "empty": empty} {
		if got, err := repo.Usage(ctx); !errors.Is(err, context.Canceled) || got != (Usage{}) {
			t.Errorf("%s Usage = %+v, %v; want zero, context.Canceled", name, got, err)
		}
	}

	t.Run("DuringWalk", func(t *testing.T) {
		fs := memfs.New()
		repo, err := Init(t.Context(), fs, "/empty.git", "main")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		repo.fs = readDirHook{Filesystem: fs, hook: func(string) error { cancel(); return nil }}
		if got, err := repo.Usage(ctx); !errors.Is(err, context.Canceled) || got != (Usage{}) {
			t.Errorf("Usage = %+v, %v; want zero, context.Canceled", got, err)
		}
	})

	t.Run("DuringMetadataWalk", func(t *testing.T) {
		fs := memfs.New()
		repo, err := Init(t.Context(), fs, "/empty.git", "main")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		listed := 0
		repo.fs = readDirHook{Filesystem: fs, hook: func(path string) error {
			if ctx.Err() != nil {
				listed++
			}
			if path == "/empty.git" {
				cancel()
			}
			return nil
		}}
		if got, err := repo.Usage(ctx); !errors.Is(err, context.Canceled) || got != (Usage{}) {
			t.Errorf("Usage = %+v, %v; want zero, context.Canceled", got, err)
		}
		if listed != 0 {
			t.Errorf("Usage listed %d directories after the cancellation", listed)
		}
	})

	t.Run("VanishingLastEntry", func(t *testing.T) {
		filesystem := memfs.New()
		repo := memfsUsageRepo(t, filesystem)
		if err := filesystem.MkdirAll("/repo.git/zzz", 0o755); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		repo.fs = readDirHook{Filesystem: filesystem, hook: func(name string) error {
			if name == "/repo.git/zzz" {
				cancel()
				return os.ErrNotExist
			}
			return nil
		}}
		if got, err := repo.Usage(ctx); !errors.Is(err, context.Canceled) || got != (Usage{}) {
			t.Fatalf("Usage = %+v, %v; want zero, context.Canceled", got, err)
		}
	})
}

type payloadGuard struct {
	billy.Filesystem
	objects string
}

func (g payloadGuard) Open(name string) (billy.File, error) {
	return g.OpenFile(name, os.O_RDONLY, 0)
}

func (g payloadGuard) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	if strings.HasPrefix(name, g.objects+string(filepath.Separator)) && !strings.HasSuffix(name, ".idx") {
		return nil, fmt.Errorf("opened object payload %s", name)
	}
	return g.Filesystem.OpenFile(name, flag, perm)
}

func sameFirstByteBlob(id string) []byte {
	for i := 0; ; i++ {
		content := fmt.Appendf(nil, "sibling %d\n", i)
		sum := sha1.Sum(append(fmt.Appendf(nil, "blob %d\x00", len(content)), content...))
		if hex.EncodeToString(sum[:1]) == id[:2] {
			return content
		}
	}
}

func TestUsageMetadataOnly(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		f := buildUsageFixture(t)
		requireGitMode(t, f.repo, native)
		ctx := t.Context()
		packed := strings.TrimSpace(gitOut(t, f.bare, "rev-parse", "main:file.txt"))
		main := strings.TrimSpace(gitOut(t, f.bare, "rev-parse", "refs/heads/main"))
		_, err := f.repo.CreateCommit(ctx, "main", "live", "Test", "test@example.com",
			[]CommitOperation{{Type: CommitOperationAdd, Path: "sibling.txt", Content: sameFirstByteBlob(packed)}}, main)
		if err != nil {
			t.Fatalf("create live commit: %v", err)
		}
		runGit(t, f.work, "fetch", "-q", "origin")
		runGit(t, f.work, "checkout", "-q", "-B", "main", "origin/main")
		blob := usageBlob(64 << 10)
		commitFile(t, f.work, "delta.bin", string(blob), "d1")
		commitFile(t, f.work, "delta.bin", string(append(blob, usageBlob(1<<10)...)), "d2")
		runGit(t, f.work, "push", "-q", "origin", "main")
		runGit(t, f.bare, "repack", "-a", "-q")
		idxs, err := filepath.Glob(filepath.Join(f.bare, "objects", "pack", "*.idx"))
		if err != nil || len(idxs) < 3 {
			t.Fatalf("pack indexes = %v, %v; want at least three", idxs, err)
		}
		var verified strings.Builder
		for _, idx := range idxs {
			verified.WriteString(gitOut(t, f.bare, "verify-pack", "-v", idx))
		}
		if !strings.Contains(verified.String(), "chain length") {
			t.Fatal("fixture packs hold no deltas")
		}
		ids := gitObjectIDs(t, f.bare)
		want := usageOf(usageSnapshot(t, osfs.Default, f.bare), f.bare, int64(len(ids)))
		f.repo.fs = payloadGuard{Filesystem: f.repo.fs, objects: filepath.Join(f.bare, "objects")}
		if got, err := f.repo.Usage(ctx); err != nil || got != want {
			t.Fatalf("Usage = %+v, %v; want %+v, nil", got, err, want)
		}
	})
}

func TestUsageGarbage(t *testing.T) {
	f := buildUsageFixture(t)
	objects := filepath.Join(f.bare, "objects")
	idxs, err := filepath.Glob(filepath.Join(objects, "pack", "*.idx"))
	if err != nil || len(idxs) == 0 {
		t.Fatalf("pack indexes = %v, %v; want some", idxs, err)
	}
	ids := gitObjectIDs(t, f.bare)
	if err := os.Rename(strings.TrimSuffix(idxs[0], ".idx")+".pack", filepath.Join(objects, "pack", "pack-"+strings.Repeat("f", 40)+".pack")); err != nil {
		t.Fatal(err)
	}
	if remaining := gitObjectIDs(t, f.bare); len(remaining) >= len(ids) {
		t.Fatalf("git still lists %d of %d objects with the pack unpaired", len(remaining), len(ids))
	} else {
		ids = remaining
	}
	for name, content := range map[string]string{
		"pack/tmp_pack_garbage":               "partial",
		"aa/bb":                               "short",
		"zz/" + strings.Repeat("z", 38):       "not hex",
		"ff/" + strings.Repeat("f", 38):       "not zlib",
		"dd/" + strings.Repeat("d", 62):       "wrong object format",
		"ee/" + strings.Repeat("e", 38) + "x": "too long",
	} {
		if err := os.MkdirAll(filepath.Join(objects, filepath.Dir(name)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(objects, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want := usageOf(usageSnapshot(t, osfs.Default, f.bare), f.bare, int64(len(ids))+1)
	f.repo.fs = payloadGuard{Filesystem: f.repo.fs, objects: objects}
	if got, err := f.repo.Usage(t.Context()); err != nil || got != want {
		t.Fatalf("Usage = %+v, %v; want %+v, nil", got, err, want)
	}
}

type vanishHook struct {
	billy.Filesystem
	paths map[string][]string
}

func (h vanishHook) vanish(op, name string) error {
	if slices.Contains(h.paths[name], op) {
		return util.RemoveAll(h.Filesystem, name)
	}
	return nil
}

func (h vanishHook) Lstat(name string) (os.FileInfo, error) {
	if err := h.vanish("Lstat", name); err != nil {
		return nil, err
	}
	return h.Filesystem.Lstat(name)
}

func (h vanishHook) Stat(name string) (os.FileInfo, error) {
	if err := h.vanish("Stat", name); err != nil {
		return nil, err
	}
	return h.Filesystem.Stat(name)
}

func (h vanishHook) Open(name string) (billy.File, error) {
	if err := h.vanish("Open", name); err != nil {
		return nil, err
	}
	return h.Filesystem.Open(name)
}

func TestUsageVanishing(t *testing.T) {
	f := buildUsageFixture(t)
	objects := filepath.Join(f.bare, "objects")
	before := usageSnapshot(t, osfs.Default, f.bare)
	var loose []string
	for name, file := range before {
		if file.mode.IsRegular() && filepath.Dir(filepath.Dir(name)) == objects && len(filepath.Base(filepath.Dir(name))) == 2 {
			loose = append(loose, name)
		}
	}
	slices.Sort(loose)
	idxs, err := filepath.Glob(filepath.Join(objects, "pack", "*.idx"))
	if len(loose) < 2 || err != nil || len(idxs) < 2 {
		t.Fatalf("fixture has %d loose objects and %d pack indexes (%v), want at least two of each", len(loose), len(idxs), err)
	}
	// Directory iteration order differs across filesystems.
	vanishing := vanishHook{Filesystem: osfs.Default, paths: map[string][]string{
		loose[0]: {"Lstat"}, filepath.Join(objects, "info"): {"Lstat"}, idxs[0]: {"Open"}, strings.TrimSuffix(idxs[1], ".idx") + ".pack": {"Stat", "Lstat"},
		filepath.Join(f.bare, "description"): {"Lstat"}, filepath.Join(f.bare, "refs", "tags"): {"Lstat"},
	}}
	listed := filepath.Dir(loose[len(loose)-1])
	f.repo.fs = readDirHook{Filesystem: vanishing, hook: func(path string) error {
		if path == listed {
			return util.RemoveAll(osfs.Default, path)
		}
		return nil
	}}
	got, err := f.repo.Usage(t.Context())
	if err != nil {
		t.Fatalf("Usage: %v", err)
	}
	after := usageSnapshot(t, osfs.Default, f.bare)
	for _, path := range append(slices.Collect(maps.Keys(vanishing.paths)), listed) {
		if _, ok := after[path]; ok || len(after) >= len(before) {
			t.Fatalf("%s survived the walk", path)
		}
	}
	// The index was measured before it vanished at Open; its IDs were not.
	want := usageOf(after, f.bare, int64(len(gitObjectIDs(t, f.bare))))
	want.Objects.Bytes += before[idxs[0]].size
	if got != want {
		t.Errorf("Usage = %+v, want %+v from what remains", got, want)
	}
}

// readDirHook runs hook before every directory listing so a test can fail or cancel mid-walk.
type readDirHook struct {
	billy.Filesystem
	hook func(path string) error
}

func (h readDirHook) ReadDir(path string) ([]fs.DirEntry, error) {
	if err := h.hook(path); err != nil {
		return nil, err
	}
	return h.Filesystem.ReadDir(path)
}

// Usage counts an object once however many packs store it, and follows native maintenance through a fresh view of the packs.
func TestUsageRedundantPacks(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		f := buildUsageFixture(t)
		requireGitMode(t, f.repo, native)
		ctx := t.Context()
		ids := gitObjectIDs(t, f.bare)
		want := usageOf(usageSnapshot(t, osfs.Default, f.bare), f.bare, int64(len(ids)))
		if got, err := f.repo.Usage(ctx); err != nil || got != want {
			t.Fatalf("Usage = %+v, %v; want %+v, nil", got, err, want)
		}
		// Without -d the old packs stay, so every reachable object is stored twice.
		runGit(t, f.bare, "repack", "-a", "-q")
		redundant := usageOf(usageSnapshot(t, osfs.Default, f.bare), f.bare, int64(len(ids)))
		if !maps.Equal(ids, gitObjectIDs(t, f.bare)) || redundant.Objects.Bytes <= want.Objects.Bytes {
			t.Fatalf("repack left %+v, want the same %d objects in more than %d bytes", redundant.Objects, len(ids), want.Objects.Bytes)
		}
		if got, err := f.repo.Usage(ctx); err != nil || got != redundant {
			t.Fatalf("Usage after repack = %+v, %v; want %+v, nil", got, err, redundant)
		}
		runGit(t, f.bare, "gc", "-q", "--prune=now")
		ids = gitObjectIDs(t, f.bare)
		collected := usageOf(usageSnapshot(t, osfs.Default, f.bare), f.bare, int64(len(ids)))
		if collected.Objects.Count >= redundant.Objects.Count || collected.Objects.Bytes >= redundant.Objects.Bytes {
			t.Fatalf("gc left %+v, want fewer than %d objects in fewer than %d bytes", collected.Objects, redundant.Objects.Count, redundant.Objects.Bytes)
		}
		if got, err := f.repo.Usage(ctx); err != nil || got != collected {
			t.Fatalf("Usage after gc = %+v, %v; want %+v, nil", got, err, collected)
		}
	})
}
