//go:build unix

package e2e_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// fixtureCheckSpec is small enough to build per test yet has every kind of extra ref.
var fixtureCheckSpec = fixtureSpec{commits: 50, files: 100, filesPerCommit: 3, refs: 500}

// TestBenchFixtureSpec checks each shape's spec and what the scale grows, and the scales parseBenchScale accepts.
func TestBenchFixtureSpec(t *testing.T) {
	for _, tc := range []struct {
		shape benchShape
		scale int
		want  fixtureSpec
	}{
		{shapeTiny, 1, fixtureSpec{commits: 3, files: 3, filesPerCommit: 1, refs: 2}},
		{shapeTiny, 2, fixtureSpec{commits: 3, files: 3, filesPerCommit: 1, refs: 2}},
		{shapeManyObjects, 1, fixtureSpec{commits: 5000, files: 5000, filesPerCommit: 3, refs: 2}},
		{shapeManyObjects, 2, fixtureSpec{commits: 10000, files: 5000, filesPerCommit: 3, refs: 2}},
		{shapeLargeBlobs, 1, fixtureSpec{commits: 1, blobs: 4, blobSize: 128 << 20}},
		{shapeLargeBlobs, 2, fixtureSpec{commits: 1, blobs: 8, blobSize: 128 << 20}},
		{shapeManyRefs, 1, fixtureSpec{commits: 200, files: 50, filesPerCommit: 2, refs: 20000}},
		{shapeManyRefs, 2, fixtureSpec{commits: 200, files: 50, filesPerCommit: 2, refs: 40000}},
	} {
		if got := shapeSpec(tc.shape, tc.scale); got != tc.want {
			t.Errorf("shapeSpec(%q, %d) = %+v, want %+v", tc.shape, tc.scale, got, tc.want)
		}
	}

	for _, tc := range []struct {
		in   string
		want int
	}{{"", 1}, {"1", 1}, {"3", 3}, {" 2 ", 2}} {
		if got, err := parseBenchScale(tc.in); got != tc.want || err != nil {
			t.Errorf("parseBenchScale(%q) = %d, %v; want %d, nil", tc.in, got, err, tc.want)
		}
	}
	for _, in := range []string{"0", "-1", "x", "1.5"} {
		if got, err := parseBenchScale(in); err == nil {
			t.Errorf("parseBenchScale(%q) = %d, nil; want an error", in, got)
		}
	}
}

// TestBenchFixtureScaleEnv checks benchScale takes the scale from HFD_BENCH_SCALE.
func TestBenchFixtureScaleEnv(t *testing.T) {
	t.Setenv("HFD_BENCH_SCALE", "2")
	if got := benchScale(t); got != 2 {
		t.Fatalf("benchScale with HFD_BENCH_SCALE=2 = %d, want 2", got)
	}
}

// TestBenchFixtureBuild checks buildFixture's counts agree with git's and its repository is packed without bitmaps, varied and reproducible.
func TestBenchFixtureBuild(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a.git")
	f := buildFixture(t, dir, fixtureCheckSpec)
	if f.dir != dir {
		t.Errorf("buildFixture dir = %q, want %q", f.dir, dir)
	}

	commits, err := strconv.Atoi(fixtureCheckGit(t, dir, "rev-list", "--all", "--count"))
	if err != nil {
		t.Fatalf("parse rev-list --all --count: %v", err)
	}
	if f.commits != 50 || f.commits != commits {
		t.Errorf("buildFixture commits = %d, rev-list --all --count = %d; want both spec.commits = 50", f.commits, commits)
	}

	refs := strings.Fields(fixtureCheckGit(t, dir, "for-each-ref", "--format=%(refname)"))
	if f.refs != 501 || f.refs != len(refs) {
		t.Errorf("buildFixture refs = %d, for-each-ref lists %d; want both 501: refs/heads/main and spec.refs = 500 more", f.refs, len(refs))
	}

	counts := map[string]int{}
	for line := range strings.Lines(fixtureCheckGit(t, dir, "count-objects", "-v")) {
		key, value, _ := strings.Cut(strings.TrimSpace(line), ": ")
		counts[key], _ = strconv.Atoi(value)
	}
	if counts["count"] != 0 || counts["in-pack"] != f.objects || f.objects <= 0 {
		t.Errorf("buildFixture objects = %d, count-objects -v count = %d, in-pack = %d; want no loose objects and objects = in-pack > 0", f.objects, counts["count"], counts["in-pack"])
	}

	packs, err := filepath.Glob(filepath.Join(dir, "objects", "pack", "*.pack"))
	if err != nil {
		t.Fatalf("glob packs: %v", err)
	}
	if len(packs) != 1 {
		t.Errorf("buildFixture left %d packs %q, want repack -a -d to leave one", len(packs), packs)
	}
	var packBytes int64
	for _, pack := range packs {
		info, err := os.Stat(pack)
		if err != nil {
			t.Fatalf("stat %s: %v", pack, err)
		}
		packBytes += info.Size()
	}
	if f.packBytes <= 0 || f.packBytes != packBytes {
		t.Errorf("buildFixture packBytes = %d, objects/pack/*.pack total %d bytes; want them equal and > 0", f.packBytes, packBytes)
	}
	bitmaps, err := filepath.Glob(filepath.Join(dir, "objects", "pack", "*.bitmap"))
	if err != nil {
		t.Fatalf("glob bitmaps: %v", err)
	}
	if len(bitmaps) > 0 {
		t.Errorf("buildFixture wrote pack bitmaps %q, want none", bitmaps)
	}

	if _, stderr, err := gitCmd(t, "", nil, "--git-dir="+dir, "fsck", "--connectivity-only"); err != nil {
		t.Errorf("git fsck --connectivity-only on buildFixture's repository: %v\n%s", err, stderr)
	}
	if head := fixtureCheckGit(t, dir, "symbolic-ref", "HEAD"); head != "refs/heads/main" {
		t.Errorf("buildFixture HEAD -> %q, want refs/heads/main", head)
	}

	if _, err := os.Stat(filepath.Join(dir, "packed-refs")); err != nil {
		t.Errorf("buildFixture left no packed-refs: %v", err)
	}
	var loose []string
	err = filepath.WalkDir(filepath.Join(dir, "refs", "tags"), func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			loose = append(loose, path)
		}
		return err
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("walk refs/tags: %v", err)
	}
	if len(loose) > 0 {
		t.Errorf("buildFixture left loose tags %q, want pack-refs --all to pack them", loose)
	}

	if types := strings.Fields(fixtureCheckGit(t, dir, "for-each-ref", "--format=%(objecttype)", "refs/tags")); !slices.Contains(types, "tag") {
		t.Errorf("buildFixture made no annotated tag: refs/tags point at %q", types)
	}
	for _, prefix := range []string{"refs/tags/", "refs/heads/b", "refs/pr/"} {
		if !slices.ContainsFunc(refs, func(ref string) bool { return strings.HasPrefix(ref, prefix) }) {
			t.Errorf("buildFixture made no ref under %s", prefix)
		}
	}

	const format = "--format=%(refname) %(objectname)"
	again := filepath.Join(filepath.Dir(dir), "b.git")
	buildFixture(t, again, fixtureCheckSpec)
	a := strings.Split(fixtureCheckGit(t, dir, "for-each-ref", format), "\n")
	b := strings.Split(fixtureCheckGit(t, again, "for-each-ref", format), "\n")
	if !slices.Equal(a, b) {
		i := 0
		for i < min(len(a), len(b))-1 && a[i] == b[i] {
			i++
		}
		t.Errorf("buildFixture built different refs from one spec (%d vs %d refs); first difference: %q vs %q", len(a), len(b), a[i], b[i])
	}
}

// TestBenchFixtureLargeBlobs checks a blob spec builds one commit holding spec.blobs blobs of spec.blobSize bytes.
func TestBenchFixtureLargeBlobs(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "blobs.git")
	f := buildFixture(t, dir, fixtureSpec{commits: 1, blobs: 2, blobSize: 1 << 20})
	if f.objects != 4 || f.refs != 1 || f.commits != 1 {
		t.Errorf("buildFixture objects, refs, commits = %d, %d, %d; want 4, 1, 1: one commit on refs/heads/main, its tree and two blobs", f.objects, f.refs, f.commits)
	}

	blobs := 0
	for line := range strings.Lines(fixtureCheckGit(t, dir, "rev-list", "--objects", "--all")) {
		oid, _, _ := strings.Cut(strings.TrimSpace(line), " ")
		if fixtureCheckGit(t, dir, "cat-file", "-t", oid) != "blob" {
			continue
		}
		blobs++
		if size := fixtureCheckGit(t, dir, "cat-file", "-s", oid); size != "1048576" {
			t.Errorf("blob %s is %s bytes, want spec.blobSize = 1048576", oid, size)
		}
	}
	if blobs != 2 {
		t.Errorf("rev-list --objects --all reaches %d blobs, want spec.blobs = 2", blobs)
	}
	if _, stderr, err := gitCmd(t, "", nil, "--git-dir="+dir, "fsck", "--connectivity-only"); err != nil {
		t.Errorf("git fsck --connectivity-only on buildFixture's repository: %v\n%s", err, stderr)
	}
}

// TestBenchFixtureSeed checks seed copies the fixture with every ref to a new path, where a server can serve it.
func TestBenchFixtureSeed(t *testing.T) {
	f := buildFixture(t, filepath.Join(t.TempDir(), "fixture.git"), fixtureCheckSpec)

	seeded := filepath.Join(t.TempDir(), "x", "y.git")
	f.seed(t, seeded)
	if _, err := os.Stat(filepath.Join(seeded, "HEAD")); err != nil {
		t.Fatalf("seed(%q) made no repository there: %v", seeded, err)
	}
	if n := fixtureCheckRemoteRefs(t, seeded, nil); n != f.refs {
		t.Errorf("ls-remote of the seeded copy lists %d refs, want f.refs = %d", n, f.refs)
	}
	if _, stderr, err := gitCmd(t, "", nil, "--git-dir="+seeded, "fsck", "--connectivity-only"); err != nil {
		t.Errorf("git fsck --connectivity-only on the seeded copy: %v\n%s", err, stderr)
	}
	if head, want := fixtureCheckGit(t, seeded, "rev-parse", "HEAD"), fixtureCheckGit(t, f.dir, "rev-parse", "refs/heads/main"); head != want {
		t.Errorf("seeded copy HEAD = %s, want the fixture's refs/heads/main %s", head, want)
	}

	if testS3Client != nil {
		return // the S3 pass keeps a server's repositories in the bucket, out of seed's reach
	}
	const repoID = "bench-org/seeded"
	s := newE2EServer(t)
	f.seed(t, s.repoDir(repoID))
	remote, env := s.httpRemote(repoID)
	runGit(t, "", env, "clone", remote, filepath.Join(t.TempDir(), "clone"))
	if n := fixtureCheckRemoteRefs(t, remote, env); n != f.refs {
		t.Errorf("ls-remote of %s, served from the seeded directory, lists %d refs, want f.refs = %d", remote, n, f.refs)
	}
}

// TestBenchFixtureCache checks benchFixture builds a shape once per pass and keeps it past the test that built it.
func TestBenchFixtureCache(t *testing.T) {
	var first *fixture
	if !t.Run("Build", func(t *testing.T) { first = benchFixture(t, shapeTiny) }) {
		t.FailNow()
	}
	f := benchFixture(t, shapeTiny)
	if f != first {
		t.Errorf("benchFixture(shapeTiny) returned %p (%s), then %p (%s); want the cached fixture again", first, first.dir, f, f.dir)
	}
	spec := shapeSpec(shapeTiny, benchScale(t))
	if f.commits != spec.commits || f.refs != spec.refs+1 {
		t.Errorf("benchFixture(shapeTiny) commits, refs = %d, %d; want %d, %d from shapeSpec(shapeTiny, benchScale(t))", f.commits, f.refs, spec.commits, spec.refs+1)
	}
	if _, err := os.Stat(filepath.Join(f.dir, "HEAD")); err != nil {
		t.Errorf("benchFixture(shapeTiny) dir %s has no HEAD once the test that built it ended: %v", f.dir, err)
	}
}

// fixtureCheckGit runs git on the bare repository gitDir and returns its trimmed stdout, failing tb on error.
func fixtureCheckGit(tb testing.TB, gitDir string, args ...string) string {
	tb.Helper()
	stdout, stderr, err := gitCmd(tb, "", nil, append([]string{"--git-dir=" + gitDir}, args...)...)
	if err != nil {
		tb.Fatalf("git --git-dir=%s %s: %v\n%s", gitDir, strings.Join(args, " "), err, stderr)
	}
	return strings.TrimSpace(stdout)
}

// fixtureCheckRemoteRefs counts the refs git ls-remote lists for remote, leaving out HEAD and peeled tags.
func fixtureCheckRemoteRefs(tb testing.TB, remote string, env []string) int {
	tb.Helper()
	stdout, stderr, err := gitCmd(tb, "", env, "ls-remote", remote)
	if err != nil {
		tb.Fatalf("git ls-remote %s: %v\n%s", remote, err, stderr)
	}
	n := 0
	for line := range strings.Lines(stdout) {
		_, ref, _ := strings.Cut(strings.TrimSpace(line), "\t")
		if ref != "HEAD" && !strings.HasSuffix(ref, "^{}") {
			n++
		}
	}
	return n
}
