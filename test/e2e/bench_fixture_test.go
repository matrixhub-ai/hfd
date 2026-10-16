//go:build unix

package e2e_test

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type benchShape string

const (
	shapeTiny        benchShape = "tiny"
	shapeManyObjects benchShape = "many-objects"
	shapeLargeBlobs  benchShape = "large-blobs"
	shapeManyRefs    benchShape = "many-refs"
)

// refs counts refs besides refs/heads/main.
type fixtureSpec struct {
	commits, files, filesPerCommit int
	blobs                          int
	blobSize                       int64
	refs                           int
}

// shapeSpec panics on an unknown shape, having no tb to fail.
func shapeSpec(shape benchShape, scale int) fixtureSpec {
	switch shape {
	case shapeTiny:
		return fixtureSpec{commits: 3, files: 3, filesPerCommit: 1, refs: 2}
	case shapeManyObjects:
		return fixtureSpec{commits: 5000 * scale, files: 5000, filesPerCommit: 3, refs: 2}
	case shapeLargeBlobs:
		return fixtureSpec{commits: 1, blobs: 4 * scale, blobSize: 128 << 20, refs: 0}
	case shapeManyRefs:
		return fixtureSpec{commits: 200, files: 50, filesPerCommit: 2, refs: 20000 * scale}
	}
	panic(fmt.Sprintf("unknown bench shape %q", shape))
}

// parseBenchScale parses HFD_BENCH_SCALE, where empty means 1.
func parseBenchScale(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 1, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("bench scale %q is not a positive integer", s)
	}
	return n, nil
}

func benchScale(tb testing.TB) int {
	tb.Helper()
	n, err := parseBenchScale(os.Getenv("HFD_BENCH_SCALE"))
	if err != nil {
		tb.Fatalf("HFD_BENCH_SCALE: %v", err)
	}
	return n
}

type fixture struct {
	dir       string
	commits   int   // rev-list --all --count
	refs      int   // for-each-ref lines, refs/heads/main included
	objects   int   // count-objects -v in-pack
	packBytes int64 // objects/pack/*.pack sizes summed
}

const (
	fixtureIdent = "HFD Bench <bench@hfd.test>"
	// Fixed identities and times keep every build's object IDs identical.
	fixtureEpoch = 1700000000
)

// buildFixture creates spec's history in a new bare repository at dir, whose parent must exist.
func buildFixture(tb testing.TB, dir string, spec fixtureSpec) *fixture {
	tb.Helper()
	runGit(tb, "", nil, "init", "--bare", "-b", "main", dir)
	pr, pw := io.Pipe()
	streamed := make(chan error, 1)
	go func() {
		err := writeFixtureStream(pw, spec)
		pw.CloseWithError(err)
		streamed <- err
	}()
	cmd := exec.CommandContext(tb.Context(), "git", "fast-import", "--quiet", "--done")
	cmd.Dir = dir
	cmd.Env = testEnv()
	cmd.Stdin = pr
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	pr.Close() // unblocks the stream writer when fast-import quit early
	if werr := <-streamed; err == nil {
		err = werr
	}
	if err != nil {
		tail := stderr.Bytes()
		tb.Fatalf("git fast-import into %s: %v\n%s", dir, err, tail[max(0, len(tail)-4096):])
	}
	// One pack and packed refs, as after gc, give both engines the same layout to serve.
	// Only native git reads bitmaps, which let it reuse the pack whole; random large blobs never delta.
	runGit(tb, dir, nil, "-c", "repack.writeBitmaps=false", "-c", "core.bigFileThreshold=16m", "repack", "-a", "-d", "-q")
	runGit(tb, dir, nil, "pack-refs", "--all")

	f := &fixture{dir: dir}
	if f.commits, err = strconv.Atoi(strings.TrimSpace(fixtureGit(tb, dir, "rev-list", "--all", "--count"))); err != nil {
		tb.Fatalf("count commits in %s: %v", dir, err)
	}
	f.refs = strings.Count(fixtureGit(tb, dir, "for-each-ref"), "\n")
	for line := range strings.Lines(fixtureGit(tb, dir, "count-objects", "-v")) {
		if n, ok := strings.CutPrefix(strings.TrimSpace(line), "in-pack: "); ok {
			f.objects, _ = strconv.Atoi(n)
		}
	}
	packs, _ := filepath.Glob(filepath.Join(dir, "objects", "pack", "*.pack"))
	for _, pack := range packs {
		info, err := os.Stat(pack)
		if err != nil {
			tb.Fatalf("stat %s: %v", pack, err)
		}
		f.packBytes += info.Size()
	}
	return f
}

// writeFixtureStream writes spec's history as a git fast-import stream ending in done.
func writeFixtureStream(w io.Writer, spec fixtureSpec) error {
	bw := bufio.NewWriterSize(w, 1<<20)
	sum := sha256.Sum256(fmt.Appendf(nil, "%+v", spec))
	rng := rand.New(rand.NewPCG(binary.LittleEndian.Uint64(sum[:8]), binary.LittleEndian.Uint64(sum[8:16])))
	order := rng.Perm(spec.files)
	edits := 0
	var body bytes.Buffer
	for c := range spec.commits {
		when := fixtureEpoch + c
		msg := fmt.Sprintf("commit %d\n", c)
		fmt.Fprintf(bw, "commit refs/heads/main\nmark :%d\nauthor %s %d +0000\ncommitter %s %d +0000\ndata %d\n%s\n",
			c+1, fixtureIdent, when, fixtureIdent, when, len(msg), msg)
		if c > 0 {
			fmt.Fprintf(bw, "from :%d\n", c)
		} else {
			for i := range spec.blobs {
				fmt.Fprintf(bw, "M 100644 inline blob%d.bin\ndata %d\n", i, spec.blobSize)
				seed := sha256.Sum256(fmt.Appendf(nil, "%+v blob%d", spec, i))
				if _, err := io.CopyN(bw, rand.NewChaCha8(seed), spec.blobSize); err != nil {
					return err
				}
				bw.WriteByte('\n')
			}
		}
		for range min(spec.filesPerCommit, spec.files) {
			file := order[edits%spec.files]
			edits++
			body.Reset()
			fmt.Fprintf(&body, "file %d at commit %d\n", file, c)
			for range 4 + rng.IntN(12) {
				fmt.Fprintf(&body, "%016x %016x\n", rng.Uint64(), rng.Uint64())
			}
			fmt.Fprintf(bw, "M 100644 inline d%02d/f%04d.txt\ndata %d\n%s\n", file/100, file, body.Len(), body.Bytes())
		}
		bw.WriteByte('\n')
	}
	for i := range spec.refs {
		mark := 1 + rng.IntN(spec.commits)
		switch i % 10 {
		case 0:
			msg := fmt.Sprintf("tag t%d\n", i)
			fmt.Fprintf(bw, "tag t%d\nfrom :%d\ntagger %s %d +0000\ndata %d\n%s\n",
				i, mark, fixtureIdent, fixtureEpoch+mark-1, len(msg), msg)
		case 1, 2, 3, 4, 5:
			fmt.Fprintf(bw, "reset refs/tags/t%d\nfrom :%d\n\n", i, mark)
		case 6, 7:
			fmt.Fprintf(bw, "reset refs/heads/b%d\nfrom :%d\n\n", i, mark)
		default:
			fmt.Fprintf(bw, "reset refs/pr/%d/head\nfrom :%d\n\n", i, mark)
		}
	}
	bw.WriteString("done\n")
	return bw.Flush()
}

var (
	benchFixturesMu sync.Mutex
	benchFixtures   map[benchShape]*fixture
)

// benchFixture builds shape once per pass for the pass's benchmarks to share; callers must not modify it.
func benchFixture(tb testing.TB, shape benchShape) *fixture {
	tb.Helper()
	benchFixturesMu.Lock()
	defer benchFixturesMu.Unlock()
	if f := benchFixtures[shape]; f != nil {
		return f
	}
	spec := shapeSpec(shape, benchScale(tb))
	if benchFixtures == nil {
		benchFixtures = map[benchShape]*fixture{}
		registerProcessCleanup(func() {
			benchFixturesMu.Lock()
			defer benchFixturesMu.Unlock()
			for _, f := range benchFixtures {
				os.RemoveAll(filepath.Dir(f.dir))
			}
			benchFixtures = nil
		})
	}
	tmp, err := os.MkdirTemp("", "hfd-bench-fixture-"+string(shape)+"-")
	if err != nil {
		tb.Fatalf("create fixture dir: %v", err)
	}
	defer func() {
		if benchFixtures[shape] == nil {
			os.RemoveAll(tmp)
		}
	}()
	f := buildFixture(tb, filepath.Join(tmp, "repo.git"), spec)
	benchFixtures[shape] = f
	return f
}

// seed copies f into a new bare repository at dir, such as a server's repoDir.
func (f *fixture) seed(tb testing.TB, dir string) {
	tb.Helper()
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		tb.Fatalf("create %s: %v", filepath.Dir(dir), err)
	}
	// A local clone hardlinks the fixture's pack, so seeding copies no object data.
	runGit(tb, "", nil, "clone", "--mirror", "--quiet", f.dir, dir)
	refs := 0
	for line := range strings.Lines(fixtureGit(tb, dir, "ls-remote", dir)) {
		_, ref, _ := strings.Cut(strings.TrimSuffix(line, "\n"), "\t")
		if ref != "HEAD" && !strings.HasSuffix(ref, "^{}") {
			refs++
		}
	}
	if refs != f.refs {
		tb.Fatalf("seeded %s advertises %d refs, want %d", dir, refs, f.refs)
	}
}

// fixtureGit returns git's stdout alone, which runGit would mix with stderr.
func fixtureGit(tb testing.TB, dir string, args ...string) string {
	tb.Helper()
	out, stderr, err := gitCmd(tb, dir, nil, args...)
	if err != nil {
		tb.Fatalf("git %s in %s: %v\n%s", strings.Join(args, " "), dir, err, stderr)
	}
	return out
}
