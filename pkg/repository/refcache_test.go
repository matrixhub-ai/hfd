package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"maps"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/filesystem/dotgit"

	"github.com/matrixhub-ai/hfd/pkg/receive"
)

// rcAgeStamps sets the mtime of HEAD, packed-refs and every directory under refs/ to at.
func rcAgeStamps(tb testing.TB, dir string, at time.Time) {
	tb.Helper()
	paths := []string{filepath.Join(dir, "HEAD"), filepath.Join(dir, "packed-refs")}
	err := filepath.WalkDir(filepath.Join(dir, "refs"), func(path string, d iofs.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			paths = append(paths, path)
		}
		return err
	})
	if err != nil {
		tb.Fatalf("walk refs: %v", err)
	}
	for _, path := range paths {
		if err := os.Chtimes(path, at, at); err != nil && !os.IsNotExist(err) {
			tb.Fatalf("chtimes %s: %v", path, err)
		}
	}
}

// rcRefValue renders a reference as its hash, or "ref: <target>" when symbolic.
func rcRefValue(ref *plumbing.Reference) string {
	if ref.Type() == plumbing.SymbolicReference {
		return "ref: " + ref.Target().String()
	}
	return ref.Hash().String()
}

// rcStorerRefs lists st as name to rcRefValue.
func rcStorerRefs(t *testing.T, st storer.ReferenceStorer) map[string]string {
	t.Helper()
	iter, err := st.IterReferences()
	if err != nil {
		t.Fatalf("IterReferences: %v", err)
	}
	refs := map[string]string{}
	if err := iter.ForEach(func(ref *plumbing.Reference) error {
		refs[ref.Name().String()] = rcRefValue(ref)
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	return refs
}

// rcCountingFS counts the read calls made through it by operation name.
type rcCountingFS struct {
	billy.Filesystem
	mu  sync.Mutex
	ops map[string]int
}

func (fs *rcCountingFS) count(op string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.ops == nil {
		fs.ops = map[string]int{}
	}
	fs.ops[op]++
}

// take returns the calls counted so far and starts counting afresh.
func (fs *rcCountingFS) take() map[string]int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	ops := fs.ops
	fs.ops = nil
	return ops
}

func (fs *rcCountingFS) Open(name string) (billy.File, error) {
	fs.count("Open")
	return fs.Filesystem.Open(name)
}

func (fs *rcCountingFS) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	fs.count("OpenFile")
	return fs.Filesystem.OpenFile(name, flag, perm)
}

func (fs *rcCountingFS) ReadDir(name string) ([]os.DirEntry, error) {
	fs.count("ReadDir")
	return fs.Filesystem.ReadDir(name)
}

func (fs *rcCountingFS) Readlink(name string) (string, error) {
	fs.count("Readlink")
	return fs.Filesystem.Readlink(name)
}

func (fs *rcCountingFS) Stat(name string) (os.FileInfo, error) {
	fs.count("Stat")
	return fs.Filesystem.Stat(name)
}

func (fs *rcCountingFS) Lstat(name string) (os.FileInfo, error) {
	fs.count("Lstat")
	return fs.Filesystem.Lstat(name)
}

// rcOnlyStats reports whether ops holds nothing but Stat calls; billy's chroot turns each Stat into Lstats of the path's components.
func rcOnlyStats(ops map[string]int) bool {
	for op := range ops {
		if op != "Stat" && op != "Lstat" {
			return false
		}
	}
	return true
}

// Served from a built snapshot, a listing or a lookup only stats; it neither lists a directory nor opens a file.
func TestRefCacheHotPathZeroIO(t *testing.T) {
	bare, _ := buildParityUpstream(t, t.TempDir())
	rcAgeStamps(t, bare, time.Now().Add(-time.Hour))
	cfs := &rcCountingFS{Filesystem: osfs.Default}
	repo, err := Open(cfs, bare)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	st := repo.repo.Storer
	want := rcStorerRefs(t, st)
	cfs.take()
	if got := rcStorerRefs(t, st); !maps.Equal(got, want) {
		t.Fatalf("second listing = %v, want %v", got, want)
	}
	if ops := cfs.take(); !rcOnlyStats(ops) {
		t.Errorf("second IterReferences did %v, want only Stat", ops)
	}
	for _, name := range []string{"refs/heads/main", "refs/heads/topic/nested", "refs/heads/missing"} {
		ref, err := st.Reference(plumbing.ReferenceName(name))
		if value, ok := want[name]; ok {
			if err != nil || rcRefValue(ref) != value {
				t.Errorf("Reference(%s) = %v, %v; want %s", name, ref, err, value)
			}
		} else if !errors.Is(err, plumbing.ErrReferenceNotFound) {
			t.Errorf("Reference(%s) = %v, %v; want ErrReferenceNotFound", name, ref, err)
		}
		if ops := cfs.take(); !rcOnlyStats(ops) {
			t.Errorf("Reference(%s) after a listing did %v, want only Stat", name, ops)
		}
	}
}

// rcPackedRefsRepo returns a repository whose n branches live only in packed-refs, its stamps an hour old.
func rcPackedRefsRepo(tb testing.TB, n int) *Repository {
	tb.Helper()
	dir := tb.TempDir()
	repo, err := Init(context.Background(), osfs.Default, dir, "main")
	if err != nil {
		tb.Fatalf("init repo: %v", err)
	}
	var packed strings.Builder
	packed.WriteString("# pack-refs with: peeled fully-peeled sorted \n")
	for i := range n {
		fmt.Fprintf(&packed, "%040x refs/heads/b%05d\n", i+1, i)
	}
	if err := os.WriteFile(filepath.Join(dir, "packed-refs"), []byte(packed.String()), 0o644); err != nil {
		tb.Fatalf("write packed-refs: %v", err)
	}
	rcAgeStamps(tb, dir, time.Now().Add(-time.Hour))
	return repo
}

// rcIterate returns one listing of st walked to its end.
func rcIterate(tb testing.TB, st storer.ReferenceStorer) func() {
	return func() {
		iter, err := st.IterReferences()
		if err != nil {
			tb.Fatalf("IterReferences: %v", err)
		}
		if err := iter.ForEach(func(*plumbing.Reference) error { return nil }); err != nil {
			tb.Fatalf("ForEach: %v", err)
		}
	}
}

// rcBytesPerRun is testing.AllocsPerRun for the bytes f allocates.
func rcBytesPerRun(runs int, f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		f()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(runs)
}

// rcHotCost returns the allocations and bytes of a hot listing of n packed refs, the least of a few rounds since noise only adds.
func rcHotCost(t *testing.T, n int) (allocs float64, perOp uint64) {
	t.Helper()
	const rounds, runs = 3, 20
	iterate := rcIterate(t, rcPackedRefsRepo(t, n).repo.Storer)
	iterate()
	allocs, perOp = math.Inf(1), math.MaxUint64
	for range rounds {
		allocs = min(allocs, testing.AllocsPerRun(runs, iterate))
		perOp = min(perOp, rcBytesPerRun(runs, iterate))
	}
	return allocs, perOp
}

// A hot listing shares the snapshot instead of allocating per ref: ten times the refs cost no more allocations.
func TestRefCacheHotIterAllocs(t *testing.T) {
	small, _ := rcHotCost(t, 2000)
	allocs, perOp := rcHotCost(t, 20000)
	if allocs > 1024 || allocs > small+16 {
		t.Errorf("hot IterReferences+ForEach = %.0f allocs over 20000 packed refs and %.0f over 2000, want <= 1024 and at most 16 more", allocs, small)
	}
	if perOp >= 64<<10 {
		t.Errorf("hot IterReferences+ForEach over 20000 packed refs = %d B/op, want < 64 KiB", perOp)
	}
}

func BenchmarkRefCacheIterReferences(b *testing.B) {
	iterate := rcIterate(b, rcPackedRefsRepo(b, 20000).repo.Storer)
	iterate()
	b.ReportAllocs()
	for b.Loop() {
		iterate()
	}
	if perOp := rcBytesPerRun(20, iterate); perOp >= 64<<10 {
		b.Errorf("hot IterReferences+ForEach over 20000 packed refs = %d B/op, want < 64 KiB", perOp)
	}
}

// rcWarm builds repo's ref snapshot and requires the next listing to be served from it, so a stale snapshot would show.
func rcWarm(t *testing.T, repo *Repository) {
	t.Helper()
	rcStorerRefs(t, repo.repo.Storer)
	built := repo.refs.rebuildCount()
	rcStorerRefs(t, repo.repo.Storer)
	if n := repo.refs.rebuildCount(); n != built {
		t.Fatalf("a listing right after a build rebuilt the snapshot again (%d -> %d), so the fixture cannot show a stale one", built, n)
	}
}

// rcDiskRefs returns git's refs of dir plus HEAD's content, as rcStorerRefs renders them.
func rcDiskRefs(t *testing.T, dir string) map[string]string {
	t.Helper()
	refs := gitLocalRefs(t, dir)
	head, err := os.ReadFile(filepath.Join(dir, "HEAD"))
	if err != nil {
		t.Fatalf("read HEAD: %v", err)
	}
	refs["HEAD"] = strings.TrimSpace(string(head))
	return refs
}

// rcRequireRefs requires lookups of every ref, then a listing, of st to match git's view of dir, and the gone names to be missing.
func rcRequireRefs(t *testing.T, st storer.ReferenceStorer, dir string, gone ...string) {
	t.Helper()
	want := rcDiskRefs(t, dir)
	for name, value := range want {
		if ref, err := st.Reference(plumbing.ReferenceName(name)); err != nil || rcRefValue(ref) != value {
			t.Errorf("Reference(%s) = %v, %v; want %s", name, ref, err, value)
		}
	}
	for _, name := range gone {
		if ref, err := st.Reference(plumbing.ReferenceName(name)); !errors.Is(err, plumbing.ErrReferenceNotFound) {
			t.Errorf("Reference(%s) = %v, %v; want ErrReferenceNotFound", name, ref, err)
		}
	}
	if got := rcStorerRefs(t, st); !maps.Equal(got, want) {
		t.Errorf("IterReferences = %v, want %v", got, want)
	}
}

// rcRequireView requires RefHash, then Refs, Branches, Tags and ls-remote of repo to match git's view of dir, and the gone names to be missing.
func rcRequireView(t *testing.T, repo *Repository, dir string, gone ...string) {
	t.Helper()
	want := gitLocalRefs(t, dir)
	for name, hash := range want {
		if got, err := repo.RefHash(plumbing.ReferenceName(name)); err != nil || got != hash {
			t.Errorf("RefHash(%s) = %q, %v; want %s", name, got, err, hash)
		}
	}
	for _, name := range gone {
		if got, err := repo.RefHash(plumbing.ReferenceName(name)); !errors.Is(err, plumbing.ErrReferenceNotFound) {
			t.Errorf("RefHash(%s) = %q, %v; want ErrReferenceNotFound", name, got, err)
		}
	}
	if got, err := repo.Refs(); err != nil || !maps.Equal(got, want) {
		t.Errorf("Refs() = %v, %v; want %v", got, err, want)
	}
	var branches, tags []string
	for name := range want {
		if short, ok := strings.CutPrefix(name, "refs/heads/"); ok {
			branches = append(branches, short)
		}
		if short, ok := strings.CutPrefix(name, "refs/tags/"); ok {
			tags = append(tags, short)
		}
	}
	for _, list := range []struct {
		name string
		get  func() ([]string, error)
		want []string
	}{{"Branches", repo.Branches, branches}, {"Tags", repo.Tags, tags}} {
		got, err := list.get()
		slices.Sort(got)
		slices.Sort(list.want)
		if err != nil || !slices.Equal(got, list.want) {
			t.Errorf("%s() = %q, %v; want %q", list.name, got, err, list.want)
		}
	}
	var adv bytes.Buffer
	if err := repo.AdvertiseRefs(t.Context(), &adv, GitUploadPack, ""); err != nil {
		t.Fatalf("AdvertiseRefs: %v", err)
	}
	lsRemote := map[string]string{}
	for name, hash := range advertisedRefs(t, readPktLines(t, adv.Bytes())[2:]) {
		if strings.HasPrefix(name, "refs/") && !strings.HasSuffix(name, "^{}") {
			lsRemote[name] = hash
		}
	}
	if !maps.Equal(lsRemote, want) {
		t.Errorf("ls-remote = %v, want %v", lsRemote, want)
	}
}

// rcEmptyPack returns a pack without objects, enough to push objects the repository already holds.
func rcEmptyPack(t *testing.T, dir string) []byte {
	t.Helper()
	pack, err := exec.CommandContext(t.Context(), "git", "-C", dir, "pack-objects", "--stdout").Output()
	if err != nil {
		t.Fatalf("pack-objects: %v", err)
	}
	return pack
}

// rcPush sends "<old> <new> <ref>" commands to repo's stateless receive-pack and requires every one to apply.
func rcPush(t *testing.T, repo *Repository, pack []byte, cmds ...string) {
	t.Helper()
	zero := plumbing.ZeroHash.String()
	lines := make([]string, 0, len(cmds)+1)
	want := []string{"unpack ok"}
	deletesOnly := true
	for i, cmd := range cmds {
		fields := strings.Fields(cmd)
		want = append(want, "ok "+fields[2])
		deletesOnly = deletesOnly && fields[1] == zero
		if i == 0 {
			cmd += "\x00report-status"
		}
		lines = append(lines, cmd+"\n")
	}
	body := pktLines(t, append(lines, "")...)
	if !deletesOnly {
		body = append(body, pack...)
	}
	var out bytes.Buffer
	if err := repo.Stateless(t.Context(), &out, bytes.NewReader(body), GitReceivePack, "", ReceivePackHooks{}); err != nil {
		t.Fatalf("Stateless receive-pack: %v", err)
	}
	if got := reportLines(t, &out, false); !slices.Equal(got, want) {
		t.Fatalf("report = %q, want %q", got, want)
	}
}

// rcCopyRepo copies the repository at src to a fresh path, so a case starts from the fixture with a handle of its own.
func rcCopyRepo(t *testing.T, src string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "repo.git")
	if err := os.CopyFS(dst, os.DirFS(src)); err != nil {
		t.Fatalf("copy repository: %v", err)
	}
	return dst
}

// Every in-process write reaches the next lookup and listing at once, even when the filesystem's stamps do not move.
func TestRefCacheInProcessWrites(t *testing.T) {
	fixture, _ := buildParityUpstream(t, t.TempDir())
	refs := gitLocalRefs(t, fixture)
	main, topic, v1 := refs["refs/heads/main"], refs["refs/heads/topic/nested"], refs["refs/tags/v1"]
	zero := plumbing.ZeroHash.String()
	pack := rcEmptyPack(t, fixture)
	old := time.Now().Add(-time.Hour)
	noErr := func(t *testing.T, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	forEachGitMode(t, func(t *testing.T, native bool) {
		// The stamps are put back after the write, so only its own invalidation can reveal it.
		check := func(t *testing.T, repo *Repository, dir string, write func(t *testing.T), gone ...string) {
			t.Helper()
			requireGitMode(t, repo, native)
			rcAgeStamps(t, dir, old)
			rcWarm(t, repo)
			write(t)
			rcAgeStamps(t, dir, old)
			rcRequireView(t, repo, dir, gone...)
		}
		for _, step := range []struct {
			name  string
			write func(t *testing.T, repo *Repository, dir string)
			gone  []string
		}{
			{"PushCreate", func(t *testing.T, repo *Repository, _ string) {
				rcPush(t, repo, pack, zero+" "+v1+" refs/heads/pushed")
			}, nil},
			{"PushFastForward", func(t *testing.T, repo *Repository, _ string) {
				rcPush(t, repo, pack, main+" "+topic+" refs/heads/main")
			}, nil},
			{"ForcePush", func(t *testing.T, repo *Repository, _ string) {
				rcPush(t, repo, pack, main+" "+v1+" refs/heads/main")
			}, nil},
			{"PushDelete", func(t *testing.T, repo *Repository, _ string) {
				rcPush(t, repo, pack, v1+" "+zero+" refs/tags/v1")
			}, []string{"refs/tags/v1"}},
			{"CreateBranch", func(t *testing.T, repo *Repository, _ string) {
				noErr(t, repo.CreateBranch("api", "main"))
			}, nil},
			{"CreateTag", func(t *testing.T, repo *Repository, _ string) {
				noErr(t, repo.CreateTag("api", "main"))
			}, nil},
			{"DeleteBranch", func(t *testing.T, repo *Repository, _ string) {
				noErr(t, repo.DeleteBranch("topic/nested"))
			}, []string{"refs/heads/topic/nested"}},
			{"DeleteTag", func(t *testing.T, repo *Repository, _ string) {
				noErr(t, repo.DeleteTag("v1"))
			}, []string{"refs/tags/v1"}},
			{"CreateCommit", func(t *testing.T, repo *Repository, _ string) {
				mustCommit(t, repo, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "new.txt", Content: []byte("new\n")})
			}, nil},
			{"SuperSquash", func(t *testing.T, repo *Repository, _ string) {
				_, err := repo.SuperSquash(t.Context(), "main", "squash", "Test", "test@test.com")
				noErr(t, err)
			}, nil},
			{"StorerSetReference", func(t *testing.T, repo *Repository, _ string) {
				noErr(t, repo.repo.Storer.SetReference(plumbing.NewHashReference("refs/heads/topic/nested", plumbing.NewHash(v1))))
			}, nil},
			{"NativeWriteThenReindex", func(t *testing.T, repo *Repository, dir string) {
				runGit(t, dir, "update-ref", "refs/heads/main", topic)
				noErr(t, repo.reindex())
			}, nil},
		} {
			t.Run(step.name, func(t *testing.T) {
				dir := rcCopyRepo(t, fixture)
				repo, err := Open(osfs.Default, dir)
				if err != nil {
					t.Fatalf("open repository: %v", err)
				}
				check(t, repo, dir, func(t *testing.T) { step.write(t, repo, dir) }, step.gone...)
			})
		}

		// Native fetches reach the snapshot through reindex, which NativeWriteThenReindex pins.
		if !native {
			t.Run("PullMirrorRefs", func(t *testing.T) {
				ctx := t.Context()
				upstream := rcCopyRepo(t, fixture)
				mirrorPath := filepath.Join(t.TempDir(), "mirror.git")
				mirror, err := InitMirror(ctx, osfs.Default, mirrorPath, upstream)
				if err != nil {
					t.Fatalf("init mirror: %v", err)
				}
				if err := mirror.PullMirrorRefs(ctx, upstream, []string{"refs/heads/main", "refs/tags/v1"}, nil); err != nil {
					t.Fatalf("first pull: %v", err)
				}
				runGit(t, upstream, "update-ref", "refs/heads/main", topic)
				// main moves in place and v1 is pruned.
				check(t, mirror, mirrorPath, func(t *testing.T) {
					if err := mirror.PullMirrorRefs(ctx, upstream, []string{"refs/heads/main"}, nil); err != nil {
						t.Fatalf("PullMirrorRefs: %v", err)
					}
				}, "refs/tags/v1")
			})
		}
	})
}

// External git writes reach the next lookup and listing through the stamps they move.
func TestRefCacheExternalWrites(t *testing.T) {
	fixture, _ := buildParityUpstream(t, t.TempDir())
	v1 := gitLocalRefs(t, fixture)["refs/tags/v1"]
	for _, step := range []struct {
		name         string
		setup, write [][]string
		gone         []string
	}{
		{"UpdateRefCreate", nil, [][]string{{"update-ref", "refs/heads/external", v1}}, nil},
		{"UpdateRefMove", nil, [][]string{{"update-ref", "refs/heads/main", v1}}, nil},
		{"UpdateRefDelete", nil, [][]string{{"update-ref", "-d", "refs/heads/topic/nested"}}, []string{"refs/heads/topic/nested"}},
		{"SymbolicRefHEAD", nil, [][]string{{"symbolic-ref", "HEAD", "refs/heads/topic/nested"}}, nil},
		{"PackRefs", nil, [][]string{{"update-ref", "refs/heads/main", v1}, {"pack-refs", "--all", "--prune"}}, nil},
		{"PackedRefDelete", [][]string{{"pack-refs", "--all", "--prune"}}, [][]string{{"update-ref", "-d", "refs/tags/v1"}}, []string{"refs/tags/v1"}},
	} {
		t.Run(step.name, func(t *testing.T) {
			dir := rcCopyRepo(t, fixture)
			for _, args := range step.setup {
				runGit(t, dir, args...)
			}
			repo, err := Open(osfs.Default, dir)
			if err != nil {
				t.Fatalf("open repository: %v", err)
			}
			// Hour-old stamps let the snapshot settle, so only the write's own stamps can reveal it.
			rcAgeStamps(t, dir, time.Now().Add(-time.Hour))
			rcWarm(t, repo)
			for _, args := range step.write {
				runGit(t, dir, args...)
			}
			rcRequireRefs(t, repo.repo.Storer, dir, step.gone...)
		})
	}
}

// rcStillLockDir puts a new lock's directory mtime back to at, as memfs and s3fs never move it, so the lock alone leaves the snapshot trusted.
type rcStillLockDir struct {
	billy.Filesystem
	at time.Time
}

func (fs *rcStillLockDir) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	f, err := fs.Filesystem.OpenFile(name, flag, perm)
	if err != nil || flag&os.O_EXCL == 0 || !strings.HasSuffix(name, ".lock") {
		return f, err
	}
	if err := os.Chtimes(filepath.Dir(name), fs.at, fs.at); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return f, nil
}

// receive-pack checks a command's old value against the ref it reads under the lock, never against a snapshot gone stale.
func TestRefCacheReceivePackReadsOldValueLive(t *testing.T) {
	fixture, _ := buildParityUpstream(t, t.TempDir())
	refs := gitLocalRefs(t, fixture)
	main, topic, v1 := refs["refs/heads/main"], refs["refs/heads/topic/nested"], refs["refs/tags/v1"]
	pack := rcEmptyPack(t, fixture)
	hourAgo := time.Now().Add(-time.Hour)
	for _, tc := range []struct {
		name, old, status, after, why string
	}{
		{"OldOnDisk", topic, "ok refs/heads/main", v1, "main's on-disk value as the old value"},
		{"OldInSnapshot", main, "ng refs/heads/main incorrect old value provided", topic, "the snapshot's stale main as the old value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := rcCopyRepo(t, fixture)
			// hfd does not take the wrapper for an OS filesystem, so go-git serves the push even with git installed.
			repo, err := Open(&rcStillLockDir{Filesystem: osfs.Default, at: hourAgo}, dir)
			if err != nil {
				t.Fatalf("open repository: %v", err)
			}
			requireGitMode(t, repo, false)
			rcAgeStamps(t, dir, hourAgo)
			rcWarm(t, repo)
			// A writer bypassing invalidation moves main in place and puts the stamps back.
			if err := os.WriteFile(filepath.Join(dir, "refs", "heads", "main"), []byte(topic+"\n"), 0o644); err != nil {
				t.Fatalf("write main: %v", err)
			}
			rcAgeStamps(t, dir, hourAgo)
			if ref, err := repo.repo.Storer.Reference("refs/heads/main"); err != nil || ref.Hash().String() != main {
				t.Fatalf("Reference(refs/heads/main) = %v, %v; want the snapshot's stale %s, without which a snapshot read cannot show", ref, err, main)
			}
			body := append(pktLines(t, tc.old+" "+v1+" refs/heads/main\x00report-status\n", ""), pack...)
			var out bytes.Buffer
			if err := repo.Stateless(t.Context(), &out, bytes.NewReader(body), GitReceivePack, "", ReceivePackHooks{}); err != nil {
				t.Fatalf("Stateless receive-pack: %v", err)
			}
			if got, want := reportLines(t, &out, false), []string{"unpack ok", tc.status}; !slices.Equal(got, want) {
				t.Errorf("push with %s: report = %q, want %q", tc.why, got, want)
			}
			if got := gitLocalRefs(t, dir)["refs/heads/main"]; got != tc.after {
				t.Errorf("push with %s left main at %s, want %s", tc.why, got, tc.after)
			}
		})
	}
}

// hfd learns what native git applied by diffing ref reads before and after it runs.
func TestRefCacheNativePostReceiveReadsOldValueLive(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skipf("git binary required: %v", err)
	}
	setGitBinary(t, gitPath)
	dir, _ := buildParityUpstream(t, t.TempDir())
	refs := gitLocalRefs(t, dir)
	main, topic := refs["refs/heads/main"], refs["refs/heads/topic/nested"]
	pack := rcEmptyPack(t, dir)
	repo, err := Open(osfs.Default, dir)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	requireGitMode(t, repo, true)
	hourAgo := time.Now().Add(-time.Hour)
	rcAgeStamps(t, dir, hourAgo)
	rcWarm(t, repo)
	// A writer bypassing invalidation moves main in place and puts the stamps back.
	if err := os.WriteFile(filepath.Join(dir, "refs", "heads", "main"), []byte(topic+"\n"), 0o644); err != nil {
		t.Fatalf("write main: %v", err)
	}
	rcAgeStamps(t, dir, hourAgo)
	if ref, err := repo.repo.Storer.Reference("refs/heads/main"); err != nil || ref.Hash().String() != main {
		t.Fatalf("Reference(refs/heads/main) = %v, %v; want the snapshot's stale %s, without which a snapshot read cannot show", ref, err, main)
	}
	var post []string
	hooks := ReceivePackHooks{PostReceive: func(_ context.Context, updates []receive.RefUpdate) {
		post = append(post, updateStrings(updates)...)
	}}
	// main goes back to the snapshot's value, so a before-read of the snapshot sees no move.
	update := topic + " " + main + " refs/heads/main"
	body := append(pktLines(t, update+"\x00report-status\n", ""), pack...)
	var out bytes.Buffer
	if err := repo.Stateless(t.Context(), &out, bytes.NewReader(body), GitReceivePack, "", hooks); err != nil {
		t.Fatalf("Stateless receive-pack: %v", err)
	}
	if got, want := reportLines(t, &out, false), []string{"unpack ok", "ok refs/heads/main"}; !slices.Equal(got, want) {
		t.Fatalf("report = %q, want %q", got, want)
	}
	if got := gitLocalRefs(t, dir)["refs/heads/main"]; got != main {
		t.Fatalf("push left main at %s, want %s", got, main)
	}
	if want := []string{update}; !slices.Equal(post, want) {
		t.Errorf("PostReceive got %q, want %q: the update from main's old value on disk, not the snapshot's", post, want)
	}
}

func rcImplements[T any](v any) bool {
	_, ok := v.(T)
	return ok
}

// The decorator keeps every optional interface hfd and go-git assert on the storer.
func TestRefCacheOptionalInterfaces(t *testing.T) {
	st := initTestRepo(t).repo.Storer
	if _, ok := st.(*refCache); !ok {
		t.Fatalf("Storer is %T, want the *refCache decorator", st)
	}
	for _, iface := range []struct {
		name string
		ok   bool
	}{
		{"Reindex", rcImplements[interface{ Reindex() error }](st)},
		{"storer.PackedObjectStorer", rcImplements[storer.PackedObjectStorer](st)},
		{"storer.LooseObjectStorer", rcImplements[storer.LooseObjectStorer](st)},
		{"storer.PackfileWriter", rcImplements[storer.PackfileWriter](st)},
		{"Filesystem", rcImplements[interface{ Filesystem() billy.Filesystem }](st)},
		{"SupportsExtension", rcImplements[interface{ SupportsExtension(string, string) bool }](st)},
		{"LowMemoryMode", rcImplements[interface{ LowMemoryMode() bool }](st)},
		{"io.Closer", rcImplements[io.Closer](st)},
	} {
		if !iface.ok {
			t.Errorf("Storer %T does not implement %s", st, iface.name)
		}
	}
}

// A burst of writes costs at most one rebuild: writes drop the snapshot and lookups never build one.
func TestRefCacheRebuildEconomy(t *testing.T) {
	setGitBinary(t, "")
	old := time.Now().Add(-time.Hour)
	zero := plumbing.ZeroHash.String()

	t.Run("PullMirrorRefs", func(t *testing.T) {
		ctx := t.Context()
		root := t.TempDir()
		bare, _ := buildParityUpstream(t, root)
		main := gitLocalRefs(t, bare)["refs/heads/main"]
		names := make([]string, 200)
		want := make(map[string]string, len(names))
		var packed strings.Builder
		packed.WriteString("# pack-refs with: peeled fully-peeled sorted \n")
		for i := range names {
			names[i] = fmt.Sprintf("refs/heads/r%03d", i)
			want[names[i]] = main
			packed.WriteString(main + " " + names[i] + "\n")
		}
		if err := os.WriteFile(filepath.Join(bare, "packed-refs"), []byte(packed.String()), 0o644); err != nil {
			t.Fatalf("write packed-refs: %v", err)
		}
		mirrorPath := filepath.Join(root, "mirror.git")
		mirror, err := InitMirror(ctx, osfs.Default, mirrorPath, bare)
		if err != nil {
			t.Fatalf("init mirror: %v", err)
		}
		requireGitMode(t, mirror, false)
		rcAgeStamps(t, mirrorPath, old)
		rcWarm(t, mirror)
		built := mirror.refs.rebuildCount()
		if err := mirror.PullMirrorRefs(ctx, bare, names, nil); err != nil {
			t.Fatalf("PullMirrorRefs: %v", err)
		}
		if n := mirror.refs.rebuildCount() - built; n > 1 {
			t.Errorf("PullMirrorRefs of %d refs rebuilt the ref snapshot %d times, want at most 1", len(names), n)
		}
		requireSameRefs(t, "mirror refs after the pull", gitLocalRefs(t, mirrorPath), want)
	})

	t.Run("StatelessPush", func(t *testing.T) {
		bare, _ := buildParityUpstream(t, t.TempDir())
		want := gitLocalRefs(t, bare)
		main, v1 := want["refs/heads/main"], want["refs/tags/v1"]
		var cmds []string
		for i := range 15 {
			for _, name := range []string{fmt.Sprintf("refs/heads/u%02d", i), fmt.Sprintf("refs/heads/d%02d", i)} {
				if err := os.WriteFile(filepath.Join(bare, filepath.FromSlash(name)), []byte(v1+"\n"), 0o644); err != nil {
					t.Fatalf("write %s: %v", name, err)
				}
			}
			cmds = append(cmds, fmt.Sprintf("%s %s refs/heads/u%02d", v1, main, i), fmt.Sprintf("%s %s refs/heads/d%02d", v1, zero, i))
			want[fmt.Sprintf("refs/heads/u%02d", i)] = main
		}
		for i := range 20 {
			cmds = append(cmds, fmt.Sprintf("%s %s refs/heads/n%02d", zero, main, i))
			want[fmt.Sprintf("refs/heads/n%02d", i)] = main
		}
		repo, err := Open(osfs.Default, bare)
		if err != nil {
			t.Fatalf("open repository: %v", err)
		}
		requireGitMode(t, repo, false)
		pack := rcEmptyPack(t, bare)
		rcAgeStamps(t, bare, old)
		rcWarm(t, repo)
		built := repo.refs.rebuildCount()
		rcPush(t, repo, pack, cmds...)
		if n := repo.refs.rebuildCount() - built; n > 1 {
			t.Errorf("a stateless push of %d commands rebuilt the ref snapshot %d times, want at most 1", len(cmds), n)
		}
		requireSameRefs(t, "refs after the push", gitLocalRefs(t, bare), want)
	})
}

// A snapshot built within refSettleWindow of its newest stamp is never trusted; a settled one is served until refSnapshotMaxAge.
func TestRefCacheTTLAndSettle(t *testing.T) {
	fixture, _ := buildParityUpstream(t, t.TempDir())
	refs := gitLocalRefs(t, fixture)
	main, v1 := refs["refs/heads/main"], refs["refs/tags/v1"]
	for _, tc := range []struct {
		name     string
		stampAge time.Duration // age of the stamps when the snapshot is built
		later    time.Duration // clock advance between the build and the reads
		served   bool
		why      string
	}{
		{"StampsAtNow", 0, 0, false, "a snapshot built at its stamps' mtime is unsettled"},
		{"HourOldStamps", time.Hour, refSnapshotMaxAge - time.Second, true, "a settled snapshot younger than refSnapshotMaxAge"},
		{"PastMaxAge", time.Hour, refSnapshotMaxAge + time.Second, false, "a snapshot older than refSnapshotMaxAge"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := rcCopyRepo(t, fixture)
			repo, err := Open(osfs.Default, dir)
			if err != nil {
				t.Fatalf("open repository: %v", err)
			}
			clock := time.Now().Truncate(time.Second)
			repo.refs.now = func() time.Time { return clock }
			stamp := clock.Add(-tc.stampAge)
			rcAgeStamps(t, dir, stamp)
			st := repo.repo.Storer
			rcStorerRefs(t, st)
			// main moves in place, so no stamp the snapshot records moves.
			if err := os.WriteFile(filepath.Join(dir, "refs", "heads", "main"), []byte(v1+"\n"), 0o644); err != nil {
				t.Fatalf("write main: %v", err)
			}
			rcAgeStamps(t, dir, stamp)
			clock = clock.Add(tc.later)
			built := repo.refs.rebuildCount()

			want, how, rebuilds := v1, "a direct read of", uint64(1)
			if tc.served {
				want, how, rebuilds = main, "the snapshot's", 0
			}
			if ref, err := st.Reference("refs/heads/main"); err != nil || ref.Hash().String() != want || repo.refs.rebuildCount() != built {
				t.Errorf("%s: lookup = %v, %v after %d rebuilds; want %s %s and no rebuild", tc.why, ref, err, repo.refs.rebuildCount()-built, how, want)
			}
			built = repo.refs.rebuildCount()
			if got, n := rcStorerRefs(t, st)["refs/heads/main"], repo.refs.rebuildCount()-built; got != want || n != rebuilds {
				t.Errorf("%s: listing has main = %s after %d rebuilds; want %s after %d", tc.why, got, n, want, rebuilds)
			}
		})
	}
}

// A malformed packed-refs fails both reads with ErrPackedRefsBadFormat and caches nothing: repaired behind identical stamps, both reads succeed.
func TestRefCacheCorruptPackedRefs(t *testing.T) {
	repo := initTestRepo(t)
	first := mustCommit(t, repo, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "file.txt", Content: []byte("v1\n")})
	second := mustCommit(t, repo, "main", first, CommitOperation{Type: CommitOperationAdd, Path: "file.txt", Content: []byte("v2\n")})
	dir := repo.repoPath
	packed := filepath.Join(dir, "packed-refs")
	writePacked := func(t *testing.T, hash, sep string, mtime time.Time) {
		t.Helper()
		if err := os.WriteFile(packed, []byte("# pack-refs with: peeled fully-peeled sorted \n"+hash+sep+"refs/heads/main\n"), 0o644); err != nil {
			t.Fatalf("write packed-refs: %v", err)
		}
		if err := os.Chtimes(packed, mtime, mtime); err != nil {
			t.Fatalf("chtimes packed-refs: %v", err)
		}
	}
	// main lives only in packed-refs, so every read of it parses the file.
	if err := os.Remove(filepath.Join(dir, "refs", "heads", "main")); err != nil {
		t.Fatalf("remove loose main: %v", err)
	}
	old := time.Now().Add(-time.Hour).Truncate(time.Second)
	writePacked(t, first, " ", old)
	rcAgeStamps(t, dir, old)
	rcWarm(t, repo)
	st := repo.repo.Storer

	broken := old.Add(time.Second)
	writePacked(t, first, "_", broken)
	for range 2 {
		if ref, err := st.Reference("refs/heads/main"); !errors.Is(err, dotgit.ErrPackedRefsBadFormat) {
			t.Errorf("Reference with a malformed packed-refs = %v, %v; want ErrPackedRefsBadFormat", ref, err)
		}
		if _, err := st.IterReferences(); !errors.Is(err, dotgit.ErrPackedRefsBadFormat) {
			t.Errorf("IterReferences with a malformed packed-refs = %v, want ErrPackedRefsBadFormat", err)
		}
	}

	// The repair keeps the malformed file's size and mtime: only a cache holding nothing reads it.
	writePacked(t, second, " ", broken)
	if ref, err := st.Reference("refs/heads/main"); err != nil || ref.Hash().String() != second {
		t.Errorf("Reference after the repair = %v, %v; want %s", ref, err, second)
	}
	if got := rcStorerRefs(t, st)["refs/heads/main"]; got != second {
		t.Errorf("listing after the repair has main = %q, want %s", got, second)
	}
}
