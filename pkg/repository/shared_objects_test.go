package repository

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/filesystem"
	"github.com/go-git/go-git/v6/storage/filesystem/dotgit"
	"github.com/go-git/go-git/v6/storage/memory"
)

// sharedTestPool is the pool's objects directory on the data filesystem under the production wiring.
const sharedTestPool = "/git/sha1/objects"

// sharedTestLine is the alternates line of a repository two levels below the repositories directory.
const sharedTestLine = "../../../../git/sha1/objects\n"

// sharedTestOnlyAlternates is all a repository's objects directory holds once every object went to the pool.
var sharedTestOnlyAlternates = []string{"info/alternates"}

var sharedTestLoose = regexp.MustCompile(`^[0-9a-f]{2}/[0-9a-f]{38}$`)

// sharedTestBind binds dataFS the way storage.NewStorage wires it.
func sharedTestBind(dataFS billy.Filesystem) billy.Filesystem {
	return BindSharedObjects(dataFS, "/repositories", "/git/sha1")
}

func sharedTestInit(t *testing.T, fs billy.Filesystem, repoPath string) *Repository {
	t.Helper()
	repo, err := Init(t.Context(), fs, repoPath, "main")
	if err != nil {
		t.Fatalf("init %s: %v", repoPath, err)
	}
	return repo
}

// sharedTestReopen drops the cached handle so Open runs again.
func sharedTestReopen(t *testing.T, fs billy.Filesystem, repoPath string) *Repository {
	t.Helper()
	lruCache.Remove(cacheKey{fs, repoPath})
	repo, err := Open(fs, repoPath)
	if err != nil {
		t.Fatalf("reopen %s: %v", repoPath, err)
	}
	return repo
}

func sharedTestBlobHash(content []byte) string {
	h := plumbing.NewHasher(formatcfg.SHA1, plumbing.BlobObject, int64(len(content)))
	_, _ = h.Write(content)
	return h.Sum().String()
}

func sharedTestObjectPath(hex string) string {
	return path.Join(sharedTestPool, hex[:2], hex[2:])
}

func sharedTestAlternates(t *testing.T, dataFS billy.Filesystem, repoPath string) string {
	t.Helper()
	data, err := util.ReadFile(dataFS, path.Join("/repositories", repoPath, "objects", "info", "alternates"))
	if err != nil {
		t.Fatalf("read alternates of %s: %v", repoPath, err)
	}
	return string(data)
}

// sharedTestTree lists every path below dir on fs, directories with a trailing slash; a missing dir lists nothing.
func sharedTestTree(t *testing.T, fs billy.Filesystem, dir string) []string {
	t.Helper()
	var entries []string
	err := util.Walk(fs, dir, func(name string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel := strings.TrimPrefix(strings.TrimPrefix(filepath.ToSlash(name), dir), "/")
		if rel == "" {
			return nil
		}
		if info.IsDir() {
			rel += "/"
		}
		entries = append(entries, rel)
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("walk %s: %v", dir, err)
	}
	slices.Sort(entries)
	return entries
}

func sharedTestFiles(t *testing.T, fs billy.Filesystem, dir string) []string {
	t.Helper()
	return slices.DeleteFunc(sharedTestTree(t, fs, dir), func(p string) bool { return strings.HasSuffix(p, "/") })
}

func sharedTestLocalFiles(t *testing.T, dataFS billy.Filesystem, repoPath string) []string {
	t.Helper()
	return sharedTestFiles(t, dataFS, path.Join("/repositories", repoPath, "objects"))
}

func sharedTestPoolObjects(t *testing.T, dataFS billy.Filesystem) []string {
	t.Helper()
	var hashes []string
	for _, f := range sharedTestFiles(t, dataFS, sharedTestPool) {
		if sharedTestLoose.MatchString(f) {
			hashes = append(hashes, strings.Replace(f, "/", "", 1))
		}
	}
	return hashes
}

func sharedTestImplements[T any](v any) bool {
	_, ok := v.(T)
	return ok
}

// sharedTestGitEnv runs git without system or user configuration.
func sharedTestGitEnv(t *testing.T) []string {
	t.Helper()
	home := t.TempDir()
	env := slices.DeleteFunc(os.Environ(), func(kv string) bool {
		return strings.HasPrefix(kv, "GIT_") || strings.HasPrefix(kv, "HOME=") || strings.HasPrefix(kv, "XDG_CONFIG_HOME=")
	})
	return append(env, "HOME="+home, "XDG_CONFIG_HOME="+home, "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=safe.bareRepository", "GIT_CONFIG_VALUE_0=all")
}

func sharedTestGit(t *testing.T, env []string, dir string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir = dir
	cmd.Env = env
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, stderr.String())
	}
	return string(out)
}

// sharedTestPack encodes blobs holding contents into a pack, deltifying similar blobs.
func sharedTestPack(t *testing.T, contents ...string) ([]byte, []plumbing.Hash) {
	t.Helper()
	source := memory.NewStorage()
	var hashes []plumbing.Hash
	for _, c := range contents {
		hashes = append(hashes, storeBlob(t, source, c))
	}
	var pack bytes.Buffer
	if _, err := packfile.NewEncoder(&pack, source, false).Encode(hashes, 10); err != nil {
		t.Fatalf("encode pack: %v", err)
	}
	return pack.Bytes(), hashes
}

// sharedTestPackBases returns the objects pack stores whole; every other object in it is a delta.
func sharedTestPackBases(t *testing.T, pack []byte) []string {
	t.Helper()
	scanner := packfile.NewScanner(bytes.NewReader(pack))
	var bases []string
	for scanner.Scan() {
		data := scanner.Data()
		if data.Section != packfile.ObjectSection {
			continue
		}
		if oh := data.Value().(packfile.ObjectHeader); !oh.Type.IsDelta() {
			bases = append(bases, oh.Hash.String())
		}
	}
	if err := scanner.Error(); err != nil {
		t.Fatalf("scan pack: %v", err)
	}
	return bases
}

func sharedTestPackfileWriter(t *testing.T, repo *Repository) storer.PackfileWriter {
	t.Helper()
	pw, ok := repo.repo.Storer.(storer.PackfileWriter)
	if !ok {
		t.Fatalf("storer %T does not implement storer.PackfileWriter", repo.repo.Storer)
	}
	return pw
}

// sharedTestWritePack streams pack through pw and returns the first error.
func sharedTestWritePack(pw storer.PackfileWriter, pack []byte) error {
	w, err := pw.PackfileWriter()
	if err != nil {
		return err
	}
	_, err = w.Write(pack)
	return errors.Join(err, w.Close())
}

func sharedTestDirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func sharedTestReadBlob(t *testing.T, repo *Repository, h plumbing.Hash) string {
	t.Helper()
	blob, err := repo.repo.BlobObject(h)
	if err != nil {
		t.Fatalf("blob %s: %v", h, err)
	}
	r, err := blob.Reader()
	if err != nil {
		t.Fatalf("blob %s reader: %v", h, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read blob %s: %v", h, err)
	}
	return string(data)
}

// sharedTestReconfigure replaces the config at host with a format-1 one declaring extension and drops the cached handle.
func sharedTestReconfigure(t *testing.T, fs billy.Filesystem, repoPath, host, extension string) {
	t.Helper()
	config := "[core]\n\trepositoryformatversion = 1\n\tbare = true\n[extensions]\n\t" + extension + "\n"
	if err := os.WriteFile(filepath.Join(host, "config"), []byte(config), 0o644); err != nil {
		t.Fatal(err)
	}
	lruCache.Remove(cacheKey{fs, repoPath})
}

func TestSharedObjectsAlternates(t *testing.T) {
	root := t.TempDir()
	dataFS := osfs.New(root)
	bound := sharedTestBind(dataFS)

	t.Run("LinePerDepth", func(t *testing.T) {
		for _, c := range []struct{ name, repoPath, line string }{
			{"TopLevel", "/repo.git", "../../../git/sha1/objects\n"},
			{"Namespaced", "/org/repo.git", sharedTestLine},
			{"Typed", "/datasets/org/ds.git", "../../../../../git/sha1/objects\n"},
		} {
			t.Run(c.name, func(t *testing.T) {
				sharedTestInit(t, bound, c.repoPath)
				if got := sharedTestAlternates(t, dataFS, c.repoPath); got != c.line {
					t.Errorf("alternates of %s = %q, want %q", c.repoPath, got, c.line)
				}
			})
		}
	})

	t.Run("CreatesPoolPackDirectory", func(t *testing.T) {
		sharedTestInit(t, bound, "/org/pack.git")
		if fi, err := dataFS.Stat(path.Join(sharedTestPool, "pack")); err != nil || !fi.IsDir() {
			t.Fatalf("stat %s/pack: %v, want a directory", sharedTestPool, err)
		}
	})

	t.Run("ReopenKeepsUpToDateFile", func(t *testing.T) {
		sharedTestInit(t, bound, "/org/keep.git")
		alt := filepath.Join(root, "repositories", "org", "keep.git", "objects", "info", "alternates")
		old := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
		if err := os.Chtimes(alt, old, old); err != nil {
			t.Fatal(err)
		}
		sharedTestReopen(t, bound, "/org/keep.git")
		fi, err := os.Stat(alt)
		if err != nil {
			t.Fatal(err)
		}
		if !fi.ModTime().Equal(old) {
			t.Errorf("Open rewrote an up-to-date alternates file: mtime %v, want %v", fi.ModTime(), old)
		}
		if got := sharedTestAlternates(t, dataFS, "/org/keep.git"); got != sharedTestLine {
			t.Errorf("alternates = %q, want %q", got, sharedTestLine)
		}
	})

	t.Run("ReopenRepairsFile", func(t *testing.T) {
		for _, c := range []struct {
			name   string
			damage func(alt string) error
		}{
			{"Stale", func(alt string) error { return os.WriteFile(alt, []byte("/elsewhere/objects\n"), 0o644) }},
			{"Missing", os.Remove},
		} {
			t.Run(c.name, func(t *testing.T) {
				repoPath := "/org/" + strings.ToLower(c.name) + ".git"
				sharedTestInit(t, bound, repoPath)
				if err := c.damage(filepath.Join(root, "repositories", repoPath, "objects", "info", "alternates")); err != nil {
					t.Fatal(err)
				}
				sharedTestReopen(t, bound, repoPath)
				if got := sharedTestAlternates(t, dataFS, repoPath); got != sharedTestLine {
					t.Errorf("alternates after reopen = %q, want %q", got, sharedTestLine)
				}
			})
		}
	})

	t.Run("OpenMissingRepositoryWritesNothing", func(t *testing.T) {
		emptyRoot := t.TempDir()
		emptyFS := osfs.New(emptyRoot)
		emptyBound := sharedTestBind(emptyFS)
		before := sharedTestTree(t, emptyFS, "/")
		if _, err := Open(emptyBound, "/org/missing.git"); !errors.Is(err, ErrRepositoryNotExists) {
			t.Fatalf("Open = %v, want ErrRepositoryNotExists", err)
		}
		if after := sharedTestTree(t, emptyFS, "/"); !slices.Equal(after, before) {
			t.Errorf("Open of a missing repository changed the data filesystem from %q to %q", before, after)
		}
		if _, err := os.Stat(filepath.Join(emptyRoot, "git")); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("stat <root>/git: %v, want not exist", err)
		}
	})
}

func TestSharedObjectsGoGitWrites(t *testing.T) {
	setGitBinary(t, "")
	dataFS := osfs.New(t.TempDir())
	bound := sharedTestBind(dataFS)
	const repoPath = "/org/repo.git"
	repo := sharedTestInit(t, bound, repoPath)
	content := []byte("pooled blob\n")
	commit := mustCommit(t, repo, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: content})
	commitObject, err := repo.repo.CommitObject(plumbing.NewHash(commit))
	if err != nil {
		t.Fatalf("read commit: %v", err)
	}

	t.Run("ObjectsLandInPool", func(t *testing.T) {
		for kind, hex := range map[string]string{"blob": sharedTestBlobHash(content), "tree": commitObject.TreeHash.String(), "commit": commit} {
			if _, err := dataFS.Stat(sharedTestObjectPath(hex)); err != nil {
				t.Errorf("%s %s not in the pool: %v", kind, hex, err)
			}
		}
	})

	t.Run("RepositoryHoldsOnlyAlternates", func(t *testing.T) {
		if got := sharedTestLocalFiles(t, dataFS, repoPath); !slices.Equal(got, sharedTestOnlyAlternates) {
			t.Errorf("repository objects = %q, want %q", got, sharedTestOnlyAlternates)
		}
	})

	t.Run("RawObjectWriterWritesPool", func(t *testing.T) {
		raw := []byte("raw object writer\n")
		w, err := repo.repo.Storer.RawObjectWriter(plumbing.BlobObject, int64(len(raw)))
		if err != nil {
			t.Fatalf("RawObjectWriter: %v", err)
		}
		if _, err := w.Write(raw); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		if _, err := dataFS.Stat(sharedTestObjectPath(sharedTestBlobHash(raw))); err != nil {
			t.Errorf("raw blob not in the pool: %v", err)
		}
		if got := sharedTestLocalFiles(t, dataFS, repoPath); !slices.Equal(got, sharedTestOnlyAlternates) {
			t.Errorf("repository objects = %q, want %q", got, sharedTestOnlyAlternates)
		}
	})

	t.Run("ReopenedRepositoryReadsBlob", func(t *testing.T) {
		if got := readBlob(t, sharedTestReopen(t, bound, repoPath), "main", "a.txt"); got != string(content) {
			t.Errorf("a.txt = %q, want %q", got, content)
		}
	})

	t.Run("FullHashResolvesPoolCommit", func(t *testing.T) {
		got, err := sharedTestReopen(t, bound, repoPath).ResolveRevision(commit)
		if err != nil || got != commit {
			t.Errorf("ResolveRevision(%s) = %q, %v; want %s", commit, got, err, commit)
		}
	})
}

func TestSharedObjectsDedup(t *testing.T) {
	setGitBinary(t, "")
	dataFS := osfs.New(t.TempDir())
	bound := sharedTestBind(dataFS)
	content := []byte("identical content\n")
	for _, repoPath := range []string{"/org/first.git", "/datasets/org/second.git"} {
		mustCommit(t, sharedTestInit(t, bound, repoPath), "main", "", CommitOperation{Type: CommitOperationAdd, Path: "file.txt", Content: content})
		if got := sharedTestLocalFiles(t, dataFS, repoPath); !slices.Equal(got, sharedTestOnlyAlternates) {
			t.Errorf("%s objects = %q, want %q", repoPath, got, sharedTestOnlyAlternates)
		}
	}
	if hex := sharedTestBlobHash(content); !slices.Contains(sharedTestPoolObjects(t, dataFS), hex) {
		t.Errorf("pool lacks shared blob %s", hex)
	}
	if packs := sharedTestFiles(t, dataFS, path.Join(sharedTestPool, "pack")); len(packs) != 0 {
		t.Errorf("pool packs %q may hold another copy, want none", packs)
	}
}

func TestSharedObjectsConcurrentWriters(t *testing.T) {
	setGitBinary(t, "")
	dataFS := osfs.New(t.TempDir())
	bound := sharedTestBind(dataFS)
	repos := make([]*Repository, 8)
	for i := range repos {
		repos[i] = sharedTestInit(t, bound, fmt.Sprintf("/org/repo-%d.git", i))
	}
	shared := []byte("same bytes from every writer\n")
	errs := make([]error, len(repos))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, repo := range repos {
		wg.Go(func() {
			<-start
			for range 20 {
				if _, err := repo.storeBlob(shared); err != nil {
					errs[i] = err
					return
				}
			}
			_, errs[i] = repo.CreateCommit(t.Context(), "main", "add shared", "Test", "test@example.com",
				[]CommitOperation{{Type: CommitOperationAdd, Path: "shared.txt", Content: shared}}, "")
		})
	}
	close(start)
	wg.Wait()

	t.Run("NoWriterFailed", func(t *testing.T) {
		for i, err := range errs {
			if err != nil {
				t.Errorf("writer %d: %v", i, err)
			}
		}
	})

	t.Run("PoolHoldsBlobOnce", func(t *testing.T) {
		if hex := sharedTestBlobHash(shared); !slices.Contains(sharedTestPoolObjects(t, dataFS), hex) {
			t.Errorf("pool lacks shared blob %s", hex)
		}
		for i := range repos {
			if got := sharedTestLocalFiles(t, dataFS, fmt.Sprintf("/org/repo-%d.git", i)); !slices.Equal(got, sharedTestOnlyAlternates) {
				t.Errorf("repo %d objects = %q, want %q", i, got, sharedTestOnlyAlternates)
			}
		}
	})

	t.Run("NoLeftoverFiles", func(t *testing.T) {
		for _, f := range sharedTestFiles(t, dataFS, sharedTestPool) {
			if !sharedTestLoose.MatchString(f) {
				t.Errorf("pool holds non-object file %s after the writers finished", f)
			}
		}
	})

	t.Run("EveryRepositoryReadsBlob", func(t *testing.T) {
		for i, repo := range repos {
			if got := readBlob(t, repo, "main", "shared.txt"); got != string(shared) {
				t.Errorf("repo %d shared.txt = %q, want %q", i, got, shared)
			}
		}
	})
}

func TestSharedObjectsMirrorPull(t *testing.T) {
	setGitBinary(t, "")
	sourceHost := filepath.Join(t.TempDir(), "source.git")
	source := sharedTestInit(t, osfs.Default, sourceHost)
	mustCommit(t, source, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: []byte("first\n")},
		CommitOperation{Type: CommitOperationAdd, Path: "b.txt", Content: []byte("second\n")})
	head := mustCommit(t, source, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: []byte("updated\n")},
		CommitOperation{Type: CommitOperationAdd, Path: "c.txt", Content: []byte("third\n")})
	if err := source.repo.RepackObjects(&git.RepackConfig{}); err != nil {
		t.Fatalf("repack upstream: %v", err)
	}
	sourceURL := (&url.URL{Scheme: "file", Path: sourceHost}).String()

	root := t.TempDir()
	dataFS := osfs.New(root)
	const repoPath = "/org/mirror.git"
	repo, err := InitMirror(t.Context(), sharedTestBind(dataFS), repoPath, sourceURL)
	if err != nil {
		t.Fatalf("InitMirror: %v", err)
	}
	if err := repo.PullMirrorRefs(t.Context(), sourceURL, []string{"refs/heads/main"}, nil); err != nil {
		t.Fatalf("PullMirrorRefs: %v", err)
	}

	t.Run("FetchedObjectsLandLooseInPool", func(t *testing.T) {
		pool := sharedTestPoolObjects(t, dataFS)
		for _, content := range []string{"first\n", "second\n", "updated\n", "third\n"} {
			if hex := sharedTestBlobHash([]byte(content)); !slices.Contains(pool, hex) {
				t.Errorf("blob %q (%s) not loose in the pool", content, hex)
			}
		}
		if !slices.Contains(pool, head) {
			t.Errorf("commit %s not loose in the pool", head)
		}
	})

	t.Run("RepositoryHoldsNoPack", func(t *testing.T) {
		if got := sharedTestLocalFiles(t, dataFS, repoPath); !slices.Equal(got, sharedTestOnlyAlternates) {
			t.Errorf("mirror objects = %q, want %q", got, sharedTestOnlyAlternates)
		}
	})

	t.Run("MirrorReadsFetchedBlob", func(t *testing.T) {
		if got := readBlob(t, repo, "main", "a.txt"); got != "updated\n" {
			t.Errorf("a.txt = %q, want %q", got, "updated\n")
		}
	})

	t.Run("NativeFsck", func(t *testing.T) {
		if _, err := exec.LookPath("git"); err != nil {
			t.Skip("git not in PATH")
		}
		sharedTestGit(t, sharedTestGitEnv(t), filepath.Join(root, "repositories", "org", "mirror.git"), "fsck", "--full", "--strict")
	})
}

func TestSharedObjectsStorerInterfaces(t *testing.T) {
	setGitBinary(t, "")
	dataFS := osfs.New(t.TempDir())
	repo := sharedTestInit(t, sharedTestBind(dataFS), "/org/repo.git")
	st := repo.repo.Storer
	type reindexer interface{ Reindex() error }
	type prefixSearcher interface {
		HashesWithPrefix(prefix []byte) ([]plumbing.Hash, error)
	}
	for _, c := range []struct {
		name      string
		got, want bool
	}{
		{"PackfileWriter", sharedTestImplements[storer.PackfileWriter](st), true},
		{"LooseObjectStorer", sharedTestImplements[storer.LooseObjectStorer](st), true},
		{"PackedObjectStorer", sharedTestImplements[storer.PackedObjectStorer](st), true},
		{"Reindex", sharedTestImplements[reindexer](st), true},
		{"HashesWithPrefix", sharedTestImplements[prefixSearcher](st), true},
		{"LowMemoryCapable", sharedTestImplements[packfile.LowMemoryCapable](st), true},
		{"DeltaObjectStorer", sharedTestImplements[storer.DeltaObjectStorer](st), false},
		{"Closer", sharedTestImplements[io.Closer](st), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Errorf("bound storer %T implements %s = %t, want %t", st, c.name, c.got, c.want)
			}
		})
	}

	t.Run("LowMemoryModeOn", func(t *testing.T) {
		if lm, ok := st.(packfile.LowMemoryCapable); !ok || !lm.LowMemoryMode() {
			t.Errorf("bound storer %T does not ask for low-memory pack parsing", st)
		}
	})

	t.Run("LooseObjectsStayLocal", func(t *testing.T) {
		loose, ok := st.(storer.LooseObjectStorer)
		if !ok {
			t.Fatalf("bound storer %T is not a storer.LooseObjectStorer", st)
		}
		content := []byte("only in the pool\n")
		mustCommit(t, repo, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: content})
		var listed []string
		if err := loose.ForEachObjectHash(func(h plumbing.Hash) error {
			listed = append(listed, h.String())
			return nil
		}); err != nil {
			t.Fatalf("ForEachObjectHash: %v", err)
		}
		if len(listed) != 0 {
			t.Errorf("ForEachObjectHash listed %q, want only the repository's own loose objects (none)", listed)
		}
		hex := sharedTestBlobHash(content)
		_ = loose.DeleteLooseObject(plumbing.NewHash(hex))
		if _, err := dataFS.Stat(sharedTestObjectPath(hex)); err != nil {
			t.Errorf("DeleteLooseObject removed pool object %s: %v", hex, err)
		}
	})

	t.Run("UnboundStorerIsFilesystemStorage", func(t *testing.T) {
		plain := sharedTestInit(t, osfs.New(t.TempDir()), "/org/repo.git")
		if _, ok := plain.repo.Storer.(*filesystem.Storage); !ok {
			t.Errorf("unbound storer = %T, want *filesystem.Storage", plain.repo.Storer)
		}
	})
}

func TestSharedObjectsExtensions(t *testing.T) {
	root := t.TempDir()
	bound := sharedTestBind(osfs.New(root))
	reopenWith := func(t *testing.T, fs billy.Filesystem, repoPath, host, extension string) error {
		t.Helper()
		sharedTestInit(t, fs, repoPath)
		sharedTestReconfigure(t, fs, repoPath, host, extension)
		_, err := Open(fs, repoPath)
		return err
	}

	t.Run("SHA256Refused", func(t *testing.T) {
		host := filepath.Join(root, "repositories", "org", "sha256.git")
		if err := reopenWith(t, bound, "/org/sha256.git", host, "objectformat = sha256"); err == nil {
			t.Error("Open succeeded on a sha256 repository bound to the sha1 pool")
		}
	})

	t.Run("SHA256OpensUnbound", func(t *testing.T) {
		plainRoot := t.TempDir()
		host := filepath.Join(plainRoot, "org", "sha256.git")
		if err := reopenWith(t, osfs.New(plainRoot), "/org/sha256.git", host, "objectformat = sha256"); err != nil {
			t.Errorf("Open of an unbound sha256 repository: %v", err)
		}
	})

	t.Run("SHA1Allowed", func(t *testing.T) {
		host := filepath.Join(root, "repositories", "org", "sha1.git")
		if err := reopenWith(t, bound, "/org/sha1.git", host, "objectformat = sha1"); err != nil {
			t.Errorf("Open with objectformat = sha1: %v", err)
		}
	})

	t.Run("WorktreeConfigAllowed", func(t *testing.T) {
		host := filepath.Join(root, "repositories", "org", "wt.git")
		if err := reopenWith(t, bound, "/org/wt.git", host, "worktreeConfig = true"); err != nil {
			t.Errorf("Open with worktreeConfig = true: %v", err)
		}
	})
}

func TestSharedObjectsPackfileWriter(t *testing.T) {
	setGitBinary(t, "")
	common := strings.Repeat("a line both blobs share so the second deltifies against the first\n", 128)
	contents := []string{common + "first tail\n", common + "second tail\n"}
	pack, hashes := sharedTestPack(t, contents...)
	bases := sharedTestPackBases(t, pack)
	if len(bases) != 1 {
		t.Fatalf("fixture pack stores %d objects whole, want one base and one delta", len(bases))
	}
	base, delta := hashes[0].String(), hashes[1].String()
	if base != bases[0] {
		base, delta = delta, base
	}
	emptyPack, _ := sharedTestPack(t)
	unpackRoot, emptyRoot, baseRoot, deltaRoot := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
	spool := t.TempDir()
	// Every temp dir exists before TMPDIR moves, so spool only ever holds the pack writer's files.
	t.Setenv("TMPDIR", spool)

	t.Run("UnpacksIntoPool", func(t *testing.T) {
		dataFS := osfs.New(unpackRoot)
		bound := sharedTestBind(dataFS)
		const repoPath = "/org/repo.git"
		w, err := sharedTestPackfileWriter(t, sharedTestInit(t, bound, repoPath)).PackfileWriter()
		if err != nil {
			t.Fatalf("PackfileWriter: %v", err)
		}
		if _, err := w.Write(pack); err != nil {
			t.Fatalf("write pack: %v", err)
		}
		if len(sharedTestDirNames(t, spool)) == 0 {
			t.Error("pack is not spooled under os.TempDir() while being written")
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close pack writer: %v", err)
		}
		if names := sharedTestDirNames(t, spool); len(names) != 0 {
			t.Errorf("spool left %q in os.TempDir()", names)
		}
		if got := sharedTestLocalFiles(t, dataFS, repoPath); !slices.Equal(got, sharedTestOnlyAlternates) {
			t.Errorf("repository objects = %q, want %q", got, sharedTestOnlyAlternates)
		}
		reopened := sharedTestReopen(t, bound, repoPath)
		for i, h := range hashes {
			if _, err := dataFS.Stat(sharedTestObjectPath(h.String())); err != nil {
				t.Errorf("object %s not loose in the pool: %v", h, err)
				continue
			}
			if got := sharedTestReadBlob(t, reopened, h); got != contents[i] {
				t.Errorf("object %s reads back %d bytes, want %d", h, len(got), len(contents[i]))
			}
		}
	})

	t.Run("EmptyPackSucceeds", func(t *testing.T) {
		repo := sharedTestInit(t, sharedTestBind(osfs.New(emptyRoot)), "/org/repo.git")
		for _, c := range []struct {
			name string
			pack []byte
		}{
			{"ZeroBytes", nil},
			{"NoObjects", emptyPack},
		} {
			t.Run(c.name, func(t *testing.T) {
				if err := sharedTestWritePack(sharedTestPackfileWriter(t, repo), c.pack); err != nil {
					t.Errorf("empty pack: %v, want success", err)
				}
				if names := sharedTestDirNames(t, spool); len(names) != 0 {
					t.Errorf("spool left %q in os.TempDir()", names)
				}
			})
		}
	})

	t.Run("ObjectWriteFailureSurfaces", func(t *testing.T) {
		for _, c := range []struct{ name, root, hex string }{
			{"Base", baseRoot, base},
			{"Delta", deltaRoot, delta},
		} {
			t.Run(c.name, func(t *testing.T) {
				dataFS := osfs.New(c.root)
				repo := sharedTestInit(t, sharedTestBind(dataFS), "/org/repo.git")
				// A file where the object's fan-out directory belongs makes moving that loose object into place fail.
				if err := util.WriteFile(dataFS, path.Join(sharedTestPool, c.hex[:2]), nil, 0o644); err != nil {
					t.Fatal(err)
				}
				if err := sharedTestWritePack(sharedTestPackfileWriter(t, repo), pack); err == nil {
					t.Errorf("pack writer reported success although storing %s in the pool failed", c.hex)
				}
				if names := sharedTestDirNames(t, spool); len(names) != 0 {
					t.Errorf("spool left %q in os.TempDir()", names)
				}
			})
		}
	})
}

func TestSharedObjectsMove(t *testing.T) {
	setGitBinary(t, "")
	dataFS := osfs.New(t.TempDir())
	bound := sharedTestBind(dataFS)
	repo := sharedTestInit(t, bound, "/org/a.git")
	mustCommit(t, repo, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: []byte("moved\n")})
	const moved = "/datasets/org/a.git"
	if err := repo.Move(moved); err != nil {
		t.Fatalf("Move: %v", err)
	}

	t.Run("AlternatesFollowNewDepth", func(t *testing.T) {
		if got, want := sharedTestAlternates(t, dataFS, moved), "../../../../../git/sha1/objects\n"; got != want {
			t.Errorf("alternates after Move = %q, want %q", got, want)
		}
	})

	t.Run("ReopenedRepositoryReadsBlob", func(t *testing.T) {
		reopened, err := Open(bound, moved)
		if err != nil {
			t.Fatalf("Open moved repository: %v", err)
		}
		if got := readBlob(t, reopened, "main", "a.txt"); got != "moved\n" {
			t.Errorf("a.txt = %q, want %q", got, "moved\n")
		}
	})
}

func TestSharedObjectsOnMemfs(t *testing.T) {
	setGitBinary(t, "")
	dataFS := memfs.New()
	bound := sharedTestBind(dataFS)
	const repoPath = "/org/repo.git"
	content := []byte("kept in memory\n")
	commit := mustCommit(t, sharedTestInit(t, bound, repoPath), "main", "", CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: content})
	hex := sharedTestBlobHash(content)

	t.Run("AlternatesLine", func(t *testing.T) {
		if got := sharedTestAlternates(t, dataFS, repoPath); got != sharedTestLine {
			t.Errorf("alternates = %q, want %q", got, sharedTestLine)
		}
	})

	t.Run("ObjectsLandInPool", func(t *testing.T) {
		if _, err := dataFS.Stat(sharedTestObjectPath(hex)); err != nil {
			t.Errorf("blob %s not in the pool: %v", hex, err)
		}
		if got := sharedTestLocalFiles(t, dataFS, repoPath); !slices.Equal(got, sharedTestOnlyAlternates) {
			t.Errorf("repository objects = %q, want %q", got, sharedTestOnlyAlternates)
		}
	})

	t.Run("ReopenedRepositoryReadsPool", func(t *testing.T) {
		reopened := sharedTestReopen(t, bound, repoPath)
		if got := readBlob(t, reopened, "main", "a.txt"); got != string(content) {
			t.Errorf("a.txt = %q, want %q", got, content)
		}
		if got, err := reopened.ResolveRevision(commit); err != nil || got != commit {
			t.Errorf("ResolveRevision(%s) = %q, %v; want %s", commit, got, err, commit)
		}
	})

	t.Run("SecondRepositorySharesBlob", func(t *testing.T) {
		const other = "/datasets/org/other.git"
		mustCommit(t, sharedTestInit(t, bound, other), "main", "", CommitOperation{Type: CommitOperationAdd, Path: "copy.txt", Content: content})
		if got := sharedTestLocalFiles(t, dataFS, other); !slices.Equal(got, sharedTestOnlyAlternates) {
			t.Errorf("%s objects = %q, want %q", other, got, sharedTestOnlyAlternates)
		}
		if !slices.Contains(sharedTestPoolObjects(t, dataFS), hex) {
			t.Errorf("pool lacks shared blob %s", hex)
		}
	})
}

func TestSharedObjectsHostPaths(t *testing.T) {
	root := t.TempDir()
	dataFS := osfs.New(root)
	bound := sharedTestBind(dataFS)
	hostPath := func(t *testing.T, elem ...string) string {
		t.Helper()
		abs, err := filepath.Abs(filepath.Join(append([]string{root}, elem...)...))
		if err != nil {
			t.Fatal(err)
		}
		return abs
	}

	t.Run("BoundViewOfRepositoriesDir", func(t *testing.T) {
		if got := bound.Root(); got != "/repositories" {
			t.Errorf("Root() = %q, want /repositories", got)
		}
		if u, ok := bound.(interface{ Underlying() billy.Filesystem }); !ok || u.Underlying() != dataFS {
			t.Errorf("%T does not expose the data filesystem through Underlying()", bound)
		}
		if kind := reflect.TypeOf(bound).Kind(); kind != reflect.Pointer {
			t.Errorf("bound filesystem is a %s, want a pointer usable as a cache key", kind)
		}
	})

	t.Run("LocalDirMapsIntoDataFS", func(t *testing.T) {
		for _, name := range []string{"/repo.git", "/org/repo.git", "/datasets/org/ds.git"} {
			if got, want := localDir(bound, name), hostPath(t, "repositories", name); got != want {
				t.Errorf("localDir(bound, %s) = %q, want %q", name, got, want)
			}
			if got, want := localDir(bound, name), localDir(dataFS, path.Join("/repositories", name)); got != want {
				t.Errorf("localDir(bound, %s) = %q, localDir(dataFS) = %q", name, got, want)
			}
		}
		if got := localDir(sharedTestBind(memfs.New()), "/org/repo.git"); got != "" {
			t.Errorf("localDir on a memfs binding = %q, want empty", got)
		}
	})

	t.Run("SharedObjectsDir", func(t *testing.T) {
		if got, want := sharedTestInit(t, bound, "/org/repo.git").sharedObjectsDir(), hostPath(t, "git", "sha1", "objects"); got != want {
			t.Errorf("sharedObjectsDir() on osfs = %q, want %q", got, want)
		}
		if got := sharedTestInit(t, sharedTestBind(memfs.New()), "/org/repo.git").sharedObjectsDir(); got != "" {
			t.Errorf("sharedObjectsDir() on memfs = %q, want empty", got)
		}
		if got := sharedTestInit(t, osfs.New(t.TempDir()), "/org/repo.git").sharedObjectsDir(); got != "" {
			t.Errorf("sharedObjectsDir() unbound = %q, want empty", got)
		}
	})
}

func TestSharedObjectsNativeGit(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not in PATH")
	}
	setGitBinary(t, "")
	root := t.TempDir()
	dataFS := osfs.New(root)
	bound := sharedTestBind(dataFS)
	const repoPath = "/org/repo.git"
	host := filepath.Join(root, "repositories", "org", "repo.git")
	repo := sharedTestInit(t, bound, repoPath)
	first := mustCommit(t, repo, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: []byte("alpha\n")})
	second := mustCommit(t, repo, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "b.txt", Content: []byte("beta\n")})
	blob := sharedTestBlobHash([]byte("alpha\n"))
	if got := sharedTestLocalFiles(t, dataFS, repoPath); !slices.Equal(got, sharedTestOnlyAlternates) {
		t.Fatalf("objects are not pool-only before native git runs: %q", got)
	}
	setGitBinary(t, gitPath)
	requireGitMode(t, repo, true)
	env := sharedTestGitEnv(t)
	run := func(t *testing.T, dir string, args ...string) string {
		t.Helper()
		return sharedTestGit(t, env, dir, args...)
	}

	t.Run("CatFile", func(t *testing.T) {
		if got := run(t, host, "cat-file", "-p", blob); got != "alpha\n" {
			t.Errorf("cat-file -p %s = %q, want %q", blob, got, "alpha\n")
		}
		if got := run(t, host, "cat-file", "-t", second); got != "commit\n" {
			t.Errorf("cat-file -t %s = %q, want commit", second, got)
		}
	})

	t.Run("RevListObjects", func(t *testing.T) {
		tree := strings.TrimSpace(run(t, host, "rev-parse", second+"^{tree}"))
		objects := run(t, host, "rev-list", "--objects", "--all")
		for _, want := range []string{first, second, tree, blob} {
			if !strings.Contains(objects, want) {
				t.Errorf("rev-list --objects --all lacks %s:\n%s", want, objects)
			}
		}
	})

	t.Run("Fsck", func(t *testing.T) {
		run(t, host, "fsck", "--full", "--strict")
	})

	t.Run("CloneNoLocal", func(t *testing.T) {
		clone := filepath.Join(t.TempDir(), "clone")
		run(t, "", "clone", "--quiet", "--no-local", host, clone)
		if got := run(t, clone, "log", "--format=%H"); got != second+"\n"+first+"\n" {
			t.Errorf("clone log = %q, want %s then %s", got, second, first)
		}
	})

	t.Run("GCKeepsPool", func(t *testing.T) {
		before := sharedTestFiles(t, dataFS, sharedTestPool)
		run(t, host, "gc", "--quiet", "--prune=now")
		if after := sharedTestFiles(t, dataFS, sharedTestPool); !slices.Equal(after, before) {
			t.Errorf("git gc changed the pool from %q to %q", before, after)
		}
		for _, h := range []string{first, second, blob} {
			run(t, host, "cat-file", "-e", h)
		}
	})

	t.Run("PushIntoBareRepository", func(t *testing.T) {
		work := filepath.Join(t.TempDir(), "work")
		run(t, "", "clone", "--quiet", "--no-local", host, work)
		if err := os.WriteFile(filepath.Join(work, "c.txt"), []byte("gamma\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		run(t, work, "add", "c.txt")
		run(t, work, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--quiet", "-m", "pushed")
		run(t, work, "push", "--quiet", host, "HEAD:refs/heads/x")
		reopened := sharedTestReopen(t, bound, repoPath)
		for name, want := range map[string]string{"a.txt": "alpha\n", "c.txt": "gamma\n"} {
			if got := readBlob(t, reopened, "x", name); got != want {
				t.Errorf("%s on x = %q, want %q", name, got, want)
			}
		}
	})
}

func TestWalkSharedObjects(t *testing.T) {
	setGitBinary(t, "")
	never := func(t *testing.T) func(plumbing.Hash, os.FileInfo) error {
		return func(h plumbing.Hash, _ os.FileInfo) error {
			t.Errorf("fn called with %s", h)
			return nil
		}
	}

	t.Run("UnboundFilesystemYieldsNothing", func(t *testing.T) {
		plain := osfs.New(t.TempDir())
		// A pool-shaped tree on an unbound filesystem is still not a pool.
		if err := util.WriteFile(plain, sharedTestObjectPath(strings.Repeat("ab", 20)), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := WalkSharedObjects(plain, never(t)); err != nil {
			t.Errorf("WalkSharedObjects(unbound) = %v, want nil", err)
		}
	})

	t.Run("MissingPoolYieldsNothing", func(t *testing.T) {
		if err := WalkSharedObjects(sharedTestBind(osfs.New(t.TempDir())), never(t)); err != nil {
			t.Errorf("WalkSharedObjects(no pool yet) = %v, want nil", err)
		}
	})

	root := t.TempDir()
	dataFS := osfs.New(root)
	bound := sharedTestBind(dataFS)
	mustCommit(t, sharedTestInit(t, bound, "/org/repo.git"), "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: []byte("walked\n")},
		CommitOperation{Type: CommitOperationAdd, Path: "b.txt", Content: []byte("walked too\n")})
	want := map[string]int64{}
	for _, hex := range sharedTestPoolObjects(t, dataFS) {
		fi, err := dataFS.Stat(sharedTestObjectPath(hex))
		if err != nil {
			t.Fatal(err)
		}
		want[hex] = fi.Size()
	}
	if len(want) != 4 {
		t.Fatalf("fixture pool holds %d loose objects, want 4 (two blobs, a tree, a commit)", len(want))
	}
	for _, name := range []string{"pack/tmp_x", "tmp_obj_1", "info/packs", "ab/short",
		"ab/" + strings.Repeat("B", 38), "ab/" + strings.Repeat("g", 38), "zz/" + strings.Repeat("d", 38)} {
		if err := util.WriteFile(dataFS, path.Join(sharedTestPool, name), []byte("noise"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := dataFS.MkdirAll(path.Join(sharedTestPool, "cd", strings.Repeat("e", 38)), 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "git", "sha1", "objects", "ef", strings.Repeat("f", 38))
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, filepath.FromSlash(sharedTestObjectPath(slices.Sorted(maps.Keys(want))[0])))
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	t.Run("YieldsLooseObjectsOnly", func(t *testing.T) {
		got := map[string]int64{}
		err := WalkSharedObjects(bound, func(h plumbing.Hash, info os.FileInfo) error {
			got[h.String()] = info.Size()
			return nil
		})
		if err != nil {
			t.Fatalf("WalkSharedObjects: %v", err)
		}
		if !maps.Equal(got, want) {
			t.Errorf("WalkSharedObjects yielded %v, want %v", got, want)
		}
	})

	t.Run("ReturnsCallbackError", func(t *testing.T) {
		errStop := errors.New("stop")
		calls := 0
		err := WalkSharedObjects(bound, func(plumbing.Hash, os.FileInfo) error {
			calls++
			return errStop
		})
		if err != errStop || calls != 1 {
			t.Errorf("WalkSharedObjects = %v after %d calls, want errStop after 1", err, calls)
		}
	})
}

func TestSharedObjectsUnboundUnchanged(t *testing.T) {
	setGitBinary(t, "")
	for _, c := range []struct {
		name string
		fs   billy.Filesystem
	}{
		{"osfs", osfs.New(t.TempDir())},
		{"memfs", memfs.New()},
	} {
		t.Run(c.name, func(t *testing.T) {
			const repoPath = "/org/repo.git"
			content := []byte("stays local\n")
			mustCommit(t, sharedTestInit(t, c.fs, repoPath), "main", "", CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: content})
			hex := sharedTestBlobHash(content)
			if _, err := c.fs.Stat(path.Join(repoPath, "objects", hex[:2], hex[2:])); err != nil {
				t.Errorf("blob not stored in the repository: %v", err)
			}
			if _, err := c.fs.Stat(path.Join(repoPath, "objects", "info", "alternates")); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("stat alternates: %v, want not exist", err)
			}
			if _, err := c.fs.Stat("/git"); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("stat /git: %v, want not exist", err)
			}
		})
	}
}

// sharedTestCountingFS counts listings of pool directories on the filesystem it wraps.
type sharedTestCountingFS struct {
	billy.Filesystem
	reads atomic.Int64
}

func (fs *sharedTestCountingFS) ReadDir(name string) ([]os.DirEntry, error) {
	if p := path.Join("/", filepath.ToSlash(name)); p == sharedTestPool || strings.HasPrefix(p, sharedTestPool+"/") {
		fs.reads.Add(1)
	}
	return fs.Filesystem.ReadDir(name)
}

// Chroot wraps instead of delegating, so the views go-git derives for alternates stay counted.
func (fs *sharedTestCountingFS) Chroot(dir string) (billy.Filesystem, error) {
	return chroot.New(fs, dir), nil
}

// sharedTestLooseObjects lists the loose objects repoPath holds itself.
func sharedTestLooseObjects(t *testing.T, dataFS billy.Filesystem, repoPath string) []string {
	t.Helper()
	var hashes []string
	for _, f := range sharedTestLocalFiles(t, dataFS, repoPath) {
		if sharedTestLoose.MatchString(f) {
			hashes = append(hashes, strings.Replace(f, "/", "", 1))
		}
	}
	return hashes
}

func TestSharedObjectsPrefixLookupIsLocal(t *testing.T) {
	setGitBinary(t, "")
	root := t.TempDir()
	dataFS := osfs.New(root)
	counting := &sharedTestCountingFS{Filesystem: dataFS}
	bound := sharedTestBind(counting)
	const legacyPath = "/org/legacy.git"

	// An unbound view keeps the objects of a repository that predates the pool in its own objects directory.
	legacyFS := chroot.New(dataFS, "/repositories")
	unbound, err := Init(t.Context(), legacyFS, legacyPath, "main")
	if err != nil {
		t.Fatalf("init %s unbound: %v", legacyPath, err)
	}
	baseCommit := mustCommit(t, unbound, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "base.txt", Content: []byte("legacy base blob\n")})
	lruCache.Remove(cacheKey{legacyFS, legacyPath})
	var packed []string
	if _, err := exec.LookPath("git"); err == nil {
		packed = sharedTestLooseObjects(t, dataFS, legacyPath)
		sharedTestGit(t, sharedTestGitEnv(t), filepath.Join(root, "repositories", "org", "legacy.git"), "repack", "-a", "-d", "-q")
	} else {
		pack, hashes := sharedTestPack(t, "legacy packed blob\n")
		objects := filesystem.NewObjectStorage(dotgit.New(chroot.New(dataFS, path.Join("/repositories", legacyPath))), cache.NewObjectLRUDefault())
		if err := sharedTestWritePack(objects, pack); err != nil {
			t.Fatalf("write local pack: %v", err)
		}
		for _, h := range hashes {
			packed = append(packed, h.String())
		}
	}
	if unbound, err = Open(legacyFS, legacyPath); err != nil {
		t.Fatalf("reopen %s unbound: %v", legacyPath, err)
	}
	looseCommit := mustCommit(t, unbound, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "loose.txt", Content: []byte("legacy loose blob\n")})
	lruCache.Remove(cacheKey{legacyFS, legacyPath})
	legacy, err := Open(bound, legacyPath)
	if err != nil {
		t.Fatalf("open %s bound: %v", legacyPath, err)
	}
	loose := sharedTestLooseObjects(t, dataFS, legacyPath)
	if len(packed) == 0 || slices.ContainsFunc(packed, func(h string) bool { return slices.Contains(loose, h) }) {
		t.Fatalf("fixture packed %q, want objects held only in a local pack, none of them loose in %q", packed, loose)
	}
	local := slices.Concat(loose, packed)

	other := sharedTestInit(t, bound, "/org/other.git")
	otherCommit := mustCommit(t, other, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "other.txt", Content: []byte("other pooled blob\n")})
	pooled := sharedTestPoolObjects(t, dataFS)
	if len(pooled) != 3 || !slices.Contains(pooled, otherCommit) {
		t.Fatalf("fixture pool holds %q, want only the blob, tree and commit of the other repository", pooled)
	}

	type prefixSearcher interface {
		HashesWithPrefix(prefix []byte) ([]plumbing.Hash, error)
	}
	lookup := func(t *testing.T, repo *Repository, prefix []byte) []string {
		t.Helper()
		ps, ok := repo.repo.Storer.(prefixSearcher)
		if !ok {
			t.Fatalf("storer %T does not implement HashesWithPrefix", repo.repo.Storer)
		}
		hashes, err := ps.HashesWithPrefix(prefix)
		if err != nil {
			t.Fatalf("HashesWithPrefix(%x): %v", prefix, err)
		}
		var hexes []string
		for _, h := range hashes {
			hexes = append(hexes, h.String())
		}
		return hexes
	}
	prefixLens := []int{1, 4, 20}

	for _, c := range []struct {
		name, where string
		hexes       []string
	}{
		{"LooseObjectsMatch", "loose", loose},
		{"PackedObjectsMatch", "in its own pack", packed},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, hex := range c.hexes {
				for _, n := range prefixLens {
					if got := lookup(t, legacy, plumbing.NewHash(hex).Bytes()[:n]); !slices.Contains(got, hex) {
						t.Errorf("HashesWithPrefix(%s) = %q, lacks object %s the repository holds %s", hex[:2*n], got, hex, c.where)
					}
				}
			}
		})
	}

	t.Run("EmptyPrefixListsOnlyLocalObjects", func(t *testing.T) {
		got := lookup(t, legacy, []byte{})
		slices.Sort(got)
		if want := slices.Sorted(slices.Values(local)); !slices.Equal(got, want) {
			t.Errorf("HashesWithPrefix(empty) = %q, want exactly the repository's own objects %q", got, want)
		}
	})

	t.Run("OtherRepositoryPoolObjectsNeverMatch", func(t *testing.T) {
		for _, hex := range pooled {
			for _, n := range prefixLens {
				if got := lookup(t, legacy, plumbing.NewHash(hex).Bytes()[:n]); slices.Contains(got, hex) {
					t.Errorf("HashesWithPrefix(%s) returned %s, an object another repository put in the pool", hex[:2*n], hex)
				}
			}
		}
	})

	t.Run("OwnPoolObjectsNeverMatch", func(t *testing.T) {
		if got := lookup(t, other, []byte{}); len(got) != 0 {
			t.Errorf("HashesWithPrefix(empty) = %q on a repository holding no objects itself, want none", got)
		}
		for _, hex := range pooled {
			for _, n := range prefixLens {
				if got := lookup(t, other, plumbing.NewHash(hex).Bytes()[:n]); slices.Contains(got, hex) {
					t.Errorf("HashesWithPrefix(%s) returned %s from the pool, want only objects the repository holds itself", hex[:2*n], hex)
				}
			}
		}
	})

	t.Run("ListsNoPoolDirectory", func(t *testing.T) {
		counting.reads.Store(0)
		for _, repo := range []*Repository{legacy, other} {
			lookup(t, repo, []byte{})
			for _, hex := range slices.Concat(local, pooled) {
				for _, n := range prefixLens {
					lookup(t, repo, plumbing.NewHash(hex).Bytes()[:n])
				}
			}
		}
		if n := counting.reads.Load(); n != 0 {
			t.Errorf("HashesWithPrefix listed pool directories %d times, want none", n)
		}
	})

	t.Run("ShortHashOfOtherRepositoryCommitFails", func(t *testing.T) {
		if got, err := legacy.ResolveRevision(otherCommit[:7]); err == nil {
			t.Errorf("ResolveRevision(%s) = %s, want an error: only another repository's commit in the pool matches", otherCommit[:7], got)
		}
	})

	t.Run("ShortHashOfLocalCommitResolves", func(t *testing.T) {
		for _, commit := range []string{baseCommit, looseCommit} {
			if got, err := legacy.ResolveRevision(commit[:7]); err != nil || got != commit {
				t.Errorf("ResolveRevision(%s) = %q, %v; want local commit %s", commit[:7], got, err, commit)
			}
		}
	})

	t.Run("FullHashResolvesPoolCommit", func(t *testing.T) {
		if got, err := other.ResolveRevision(otherCommit); err != nil || got != otherCommit {
			t.Errorf("ResolveRevision(%s) = %q, %v; want the pooled commit itself", otherCommit, got, err)
		}
	})
}

func TestSharedObjectsAlternatesOwnership(t *testing.T) {
	root := t.TempDir()
	dataFS := osfs.New(root)
	bound := sharedTestBind(dataFS)
	writeAlternates := func(t *testing.T, repoPath, content string) {
		t.Helper()
		if err := util.WriteFile(dataFS, path.Join("/repositories", repoPath, "objects", "info", "alternates"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	const twoLines = sharedTestLine + "/elsewhere/objects\n"

	t.Run("Empty", func(t *testing.T) {
		for _, c := range []struct{ name, content string }{
			{"ZeroBytes", ""},
			{"BlankLines", "\n \n"},
		} {
			t.Run(c.name, func(t *testing.T) {
				repoPath := "/org/" + strings.ToLower(c.name) + ".git"
				sharedTestInit(t, bound, repoPath)
				writeAlternates(t, repoPath, c.content)
				sharedTestReopen(t, bound, repoPath)
				if got := sharedTestAlternates(t, dataFS, repoPath); got != sharedTestLine {
					t.Errorf("alternates %q after reopen = %q, want %q", c.content, got, sharedTestLine)
				}
			})
		}
	})

	t.Run("StaleDepth", func(t *testing.T) {
		const repoPath = "/org/depth.git"
		sharedTestInit(t, bound, repoPath)
		writeAlternates(t, repoPath, "../../../git/sha1/objects\n")
		sharedTestReopen(t, bound, repoPath)
		if got := sharedTestAlternates(t, dataFS, repoPath); got != sharedTestLine {
			t.Errorf("alternates after reopen = %q, want the stale line replaced by %q", got, sharedTestLine)
		}
	})

	t.Run("TwoLines", func(t *testing.T) {
		const repoPath = "/org/multi.git"
		sharedTestInit(t, bound, repoPath)
		writeAlternates(t, repoPath, twoLines)
		lruCache.Remove(cacheKey{bound, repoPath})
		_, err := Open(bound, repoPath)
		if got := sharedTestAlternates(t, dataFS, repoPath); got != twoLines {
			t.Errorf("Open rewrote alternates holding two lines to %q, want %q untouched", got, twoLines)
		}
		if err == nil {
			t.Fatal("Open succeeded on a repository whose alternates holds two lines, want an error")
		}
		if want := filepath.Join(repoPath, "objects", "info", "alternates"); !strings.Contains(err.Error(), want) {
			t.Errorf("Open error %q does not name %s", err, want)
		}
		if !strings.Contains(err.Error(), "2") {
			t.Errorf("Open error %q does not state the 2 lines it found", err)
		}
	})

	t.Run("TwoLinesMove", func(t *testing.T) {
		const from, to = "/org/mv.git", "/org/moved.git"
		repo := sharedTestInit(t, bound, from)
		writeAlternates(t, from, twoLines)
		err := repo.Move(to)
		if _, serr := dataFS.Stat(path.Join("/repositories", to)); serr != nil {
			t.Fatalf("stat %s after Move: %v, want the directory renamed before alternates are checked", to, serr)
		}
		if got := sharedTestAlternates(t, dataFS, to); got != twoLines {
			t.Errorf("Move rewrote alternates holding two lines to %q, want %q untouched", got, twoLines)
		}
		if err == nil {
			t.Fatal("Move succeeded although the moved repository's alternates holds two lines, want an error")
		}
		if want := filepath.Join(to, "objects", "info", "alternates"); !strings.Contains(err.Error(), want) {
			t.Errorf("Move error %q does not name %s", err, want)
		}
	})
}

// A short hash names a pooled commit the repository's refs reach, and nothing in another repository sharing the pool.
func TestSharedObjectsShortHashResolvesReachableCommit(t *testing.T) {
	bound := sharedTestBind(osfs.New(t.TempDir()))
	const repoPath = "/org/repo.git"
	commit := mustCommit(t, sharedTestInit(t, bound, repoPath), "main", "", CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: []byte("pooled\n")})
	other := sharedTestInit(t, bound, "/org/other.git")
	mustCommit(t, other, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "b.txt", Content: []byte("other\n")})

	t.Run("OwnRepository", func(t *testing.T) {
		if got, err := sharedTestReopen(t, bound, repoPath).ResolveRevision(commit[:7]); err != nil || got != commit {
			t.Errorf("ResolveRevision(%s) = %q, %v; want pooled commit %s", commit[:7], got, err, commit)
		}
	})

	t.Run("OtherRepository", func(t *testing.T) {
		if got, err := other.ResolveRevision(commit[:7]); !errors.Is(err, ErrRevisionNotFound) {
			t.Errorf("other repository ResolveRevision(%s) = %q, %v; want ErrRevisionNotFound", commit[:7], got, err)
		}
	})
}
