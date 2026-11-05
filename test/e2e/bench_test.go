//go:build unix

package e2e_test

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Every row runs against a native-git and a go-git server seeded from the same
// fixture. The fixtures carry no pack bitmaps, which only native git reads, so
// both engines enumerate the objects they pack. Row names lead with engine=
// for benchstat to compare the engines.

type benchEngine struct {
	name   string
	native bool
}

var benchEngines = []benchEngine{{"native", true}, {"go-git", false}}

// benchProtocols are the transport and wire protocol pairs of the read rows.
var benchProtocols = []struct {
	transport string
	proto     int
}{{"http", 0}, {"http", 1}, {"http", 2}, {"ssh", 0}, {"ssh", 2}}

func benchName(e benchEngine, transport string, proto int, row string) string {
	return fmt.Sprintf("engine=%s/transport=%s/proto=v%d/%s", e.name, transport, proto, row)
}

// startBench skips the S3 pass, where both servers would run go-git, and needs a git client.
func startBench(b *testing.B) {
	b.Helper()
	skipUnlessLocalPass(b)
	if _, err := exec.LookPath("git"); err != nil {
		b.Fatalf("git client not found: %v", err)
	}
	// Info logs, such as one per HF API delete, would split the result lines benchstat parses.
	prev := slog.SetLogLoggerLevel(slog.LevelWarn)
	b.Cleanup(func() { slog.SetLogLoggerLevel(prev) })
}

// benchServer starts e's server with SSH and the transport counters benchMeter reads.
func benchServer(b *testing.B, e benchEngine, opts ...e2eOption) *e2eServer {
	b.Helper()
	opts = append([]e2eOption{withSSH(), withTransportStats()}, opts...)
	if !e.native {
		opts = append(opts, withGoGit())
	}
	return newE2EServer(b, opts...)
}

func benchRepoID(shape benchShape) string { return "bench/" + string(shape) }

// seedBench serves shape as repoID on s, which must not have opened repoID yet.
func seedBench(b *testing.B, s *e2eServer, e benchEngine, shape benchShape, repoID string) {
	b.Helper()
	benchFixture(b, shape).seed(b, s.repoDir(repoID))
	url, _ := s.httpRemote(repoID)
	requireEngine(b, url, e.native)
}

// seededServers starts a server per engine, each serving every shape as benchRepoID(shape).
func seededServers(b *testing.B, shapes ...benchShape) []*e2eServer {
	b.Helper()
	servers := make([]*e2eServer, len(benchEngines))
	for i, e := range benchEngines {
		servers[i] = benchServer(b, e)
		for _, shape := range shapes {
			seedBench(b, servers[i], e, shape, benchRepoID(shape))
		}
	}
	return servers
}

func remoteFor(s *e2eServer, transport, repoID string) (string, []string) {
	if transport == "ssh" {
		return s.sshRemote(repoID)
	}
	return s.httpRemote(repoID)
}

// protoArgs selects the wire protocol, which git sends as Git-Protocol over HTTP and GIT_PROTOCOL over SSH.
func protoArgs(proto int, args ...string) []string {
	return append([]string{
		"-c", fmt.Sprintf("protocol.version=%d", proto),
		// A client's detached auto-maintenance is no part of a row and races the removal of the client's directory.
		"-c", "maintenance.auto=false", "-c", "gc.auto=0",
	}, args...)
}

// workClone clones url with a committer identity, for clients that commit.
func workClone(b *testing.B, url string, env []string) string {
	b.Helper()
	dir := filepath.Join(b.TempDir(), "work")
	runGit(b, "", env, "clone", "--quiet", url, dir)
	runGit(b, dir, nil, "config", "user.name", "HFD Bench")
	runGit(b, dir, nil, "config", "user.email", "bench@hfd.test")
	return dir
}

// scratch hands out fresh paths under one temporary directory; clear removes
// those handed out so far, so only one iteration's output stays on disk.
type scratch struct {
	b     *testing.B
	root  string
	n     int
	paths []string
}

func newScratch(b *testing.B) *scratch { return &scratch{b: b, root: b.TempDir()} }

func (d *scratch) next() string {
	d.n++
	path := filepath.Join(d.root, strconv.Itoa(d.n))
	d.paths = append(d.paths, path)
	return path
}

func (d *scratch) clear() {
	for _, path := range d.paths {
		removeAll(d.b, path)
	}
	d.paths = d.paths[:0]
}

func removeAll(b *testing.B, path string) {
	b.Helper()
	if err := os.RemoveAll(path); err != nil {
		b.Fatalf("remove %s: %v", path, err)
	}
}

func serverBytes(s *e2eServer) int64 {
	in, out := s.transportBytes()
	return in + out
}

// BenchmarkLsRemote times listing a tiny repository and one with 20000 refs.
func BenchmarkLsRemote(b *testing.B) {
	startBench(b)
	shapes := []benchShape{shapeTiny, shapeManyRefs}
	servers := seededServers(b, shapes...)
	for _, shape := range shapes {
		for _, p := range benchProtocols {
			for i, e := range benchEngines {
				s := servers[i]
				url, env := remoteFor(s, p.transport, benchRepoID(shape))
				b.Run(benchName(e, p.transport, p.proto, "shape="+string(shape)), func(b *testing.B) {
					m := newBenchMeter(b, s)
					for range b.N {
						m.git("", env, protoArgs(p.proto, "ls-remote", url)...)
					}
					m.finish()
				})
			}
		}
	}
}

// BenchmarkClone times cloning 40000 small objects and, over protocol v2
// only, four 128 MiB blobs.
func BenchmarkClone(b *testing.B) {
	startBench(b)
	shapes := []benchShape{shapeManyObjects, shapeLargeBlobs}
	servers := seededServers(b, shapes...)
	for _, shape := range shapes {
		for _, p := range benchProtocols {
			var timeout time.Duration
			if shape == shapeLargeBlobs {
				// go-git takes minutes per large-blob clone, so these rows keep to v2 and allow 15 minutes.
				if p.proto != 2 {
					continue
				}
				timeout = 15 * time.Minute
			}
			for i, e := range benchEngines {
				s := servers[i]
				url, env := remoteFor(s, p.transport, benchRepoID(shape))
				b.Run(benchName(e, p.transport, p.proto, "shape="+string(shape)), func(b *testing.B) {
					benchClones(b, s, timeout, env, p.proto, url)
				})
			}
		}
	}
}

// benchClones times cloning url into a new directory per iteration, with opts
// after clone; a nonzero timeout replaces benchMeter's client limit.
func benchClones(b *testing.B, s *e2eServer, timeout time.Duration, env []string, proto int, url string, opts ...string) {
	dirs := newScratch(b)
	m := newBenchMeter(b, s)
	m.timeout = timeout
	for range b.N {
		m.stopTimer()
		dirs.clear()
		args := slices.Concat(protoArgs(proto, "clone", "--quiet"), opts, []string{url, dirs.next()})
		m.startTimer()
		m.git("", env, args...)
	}
	m.finish()
}

// BenchmarkFetch times fetching the one commit the server has beyond its
// clients, once with haves the server lacks enough to gzip the negotiation.
func BenchmarkFetch(b *testing.B) {
	startBench(b)
	repoID := benchRepoID(shapeManyObjects)
	servers := seededServers(b, shapeManyObjects)
	type pristines struct{ plain, gzip string }
	clients := make([]pristines, len(servers))
	heads := make([]string, len(servers))
	for i, s := range servers {
		url, env := s.httpRemote(repoID)
		plain := filepath.Join(b.TempDir(), "pristine")
		runGit(b, "", env, "clone", "--quiet", "--no-checkout", url, plain)
		// 96 haves unknown to the server take a negotiation round past the 1 KiB from which git gzips it.
		gzip := workClone(b, url, env)
		runGit(b, gzip, nil, "checkout", "--quiet", "-b", "local-only")
		for n := range 96 {
			runGit(b, gzip, nil, "commit", "--quiet", "--allow-empty", "-m", fmt.Sprintf("local %d", n))
		}
		runGit(b, gzip, nil, "checkout", "--quiet", "main")
		work := workClone(b, url, env)
		runGit(b, work, nil, "commit", "--quiet", "--allow-empty", "-m", "ahead of the pristine clients")
		runGit(b, work, env, "push", "--quiet", "origin", "HEAD:refs/heads/main")
		clients[i] = pristines{plain, gzip}
		heads[i] = strings.TrimSpace(runGit(b, work, nil, "rev-parse", "HEAD"))
	}

	for _, row := range []struct{ transport, variant string }{
		{"http", "incremental"}, {"ssh", "incremental"}, {"http", "gzip-negotiation"},
	} {
		for i, e := range benchEngines {
			s, head := servers[i], heads[i]
			pristine := clients[i].plain
			if row.variant == "gzip-negotiation" {
				pristine = clients[i].gzip
			}
			url, env := remoteFor(s, row.transport, repoID)
			b.Run(benchName(e, row.transport, 2, "variant="+row.variant), func(b *testing.B) {
				dirs := newScratch(b)
				m := newBenchMeter(b, s)
				for range b.N {
					m.stopTimer()
					dirs.clear()
					// A fresh client per fetch: fetch-pack skips the transfer once the objects are local.
					dir := dirs.next()
					runGit(b, "", nil, "clone", "--quiet", "--no-checkout", pristine, dir)
					runGit(b, dir, nil, "remote", "set-url", "origin", url)
					before := strings.TrimSpace(runGit(b, dir, nil, "rev-parse", "refs/remotes/origin/main"))
					m.startTimer()
					m.git(dir, env, protoArgs(2, "fetch", "--quiet", "origin")...)
					m.stopTimer()
					// A fetch that skips the transfer still lists the refs, so check origin/main really advanced.
					after := strings.TrimSpace(runGit(b, dir, nil, "rev-parse", "refs/remotes/origin/main"))
					if after == before || after != head {
						b.Fatalf("fetch from %s moved origin/main from %s to %s, want it advanced to the server's head %s", url, before, after, head)
					}
				}
				m.finish()
			})
		}
	}
}

// benchRepoSeq numbers the repositories rows create per iteration: the server
// caches a handle per path, which a recreated directory would leave stale.
var benchRepoSeq atomic.Int64

// BenchmarkPush times pushing one commit, mirroring four 128 MiB blobs or
// 2000 refs into an empty repository, and deleting 2000 refs.
func BenchmarkPush(b *testing.B) {
	startBench(b)
	repoID := benchRepoID(shapeManyObjects)
	servers := seededServers(b, shapeManyObjects)
	works := make([]string, len(servers))
	for i, s := range servers {
		url, env := s.httpRemote(repoID)
		works[i] = workClone(b, url, env)
	}
	for _, transport := range []string{"http", "ssh"} {
		for i, e := range benchEngines {
			s, work := servers[i], works[i]
			url, env := remoteFor(s, transport, repoID)
			b.Run(benchName(e, transport, 2, "variant=small"), func(b *testing.B) {
				runGit(b, work, nil, "remote", "set-url", "origin", url)
				m := newBenchMeter(b, s)
				benchCommitPushes(b, m, work, env)
				m.finish()
			})
		}
	}

	initBare := func(b *testing.B, dir string) {
		runGit(b, "", nil, "init", "--quiet", "--bare", "-b", "main", dir)
	}
	for i, e := range benchEngines {
		s := servers[i]
		b.Run(benchName(e, "http", 2, "variant=large"), func(b *testing.B) {
			f := benchFixture(b, shapeLargeBlobs)
			benchMirrorPushes(b, s, f.dir, func(dir string) { initBare(b, dir) })
		})
	}
	// 2000 refs at any HFD_BENCH_SCALE for the many-refs and delete rows, whose time grows with the refs squared.
	pushRefs := buildFixture(b, filepath.Join(b.TempDir(), "push-refs.git"), fixtureSpec{commits: 200, files: 50, filesPerCommit: 2, refs: 2000})
	for i, e := range benchEngines {
		s := servers[i]
		b.Run(benchName(e, "http", 2, "variant=many-refs"), func(b *testing.B) {
			benchMirrorPushes(b, s, pushRefs.dir, func(dir string) { initBare(b, dir) })
		})
	}
	for i, e := range benchEngines {
		s := servers[i]
		b.Run(benchName(e, "http", 2, "variant=delete"), func(b *testing.B) {
			// Mirroring an empty repository deletes every remote ref without naming 2000 of them in argv.
			empty := filepath.Join(b.TempDir(), "empty.git")
			initBare(b, empty)
			benchMirrorPushes(b, s, empty, func(dir string) {
				pushRefs.seed(b, dir)
				// Both engines otherwise refuse to delete the branch HEAD names.
				runGit(b, dir, nil, "config", "receive.denyDeleteCurrent", "ignore")
			})
		})
	}
}

// benchCommitPushes times pushing one new empty commit from work per iteration.
func benchCommitPushes(b *testing.B, m *benchMeter, work string, env []string) {
	for range b.N {
		m.stopTimer()
		runGit(b, work, nil, "commit", "--quiet", "--allow-empty", "-m", "bench push")
		m.startTimer()
		m.git(work, env, protoArgs(2, "push", "--quiet", "origin", "HEAD:refs/heads/main")...)
	}
}

// benchMirrorPushes times push --mirror from src into a new repository per
// iteration, which prepare creates at the directory it is given.
func benchMirrorPushes(b *testing.B, s *e2eServer, src string, prepare func(dir string)) {
	var dir string
	m := newBenchMeter(b, s)
	for range b.N {
		m.stopTimer()
		if dir != "" {
			removeAll(b, dir)
		}
		repoID := fmt.Sprintf("bench/push-%d", benchRepoSeq.Add(1))
		dir = s.repoDir(repoID)
		prepare(dir)
		url, env := s.httpRemote(repoID)
		m.startTimer()
		m.git(src, env, protoArgs(2, "push", "--quiet", "--mirror", url)...)
	}
	m.finish()
	removeAll(b, dir)
}

// BenchmarkShallow times a depth-1 clone and deepening one by 100 commits.
func BenchmarkShallow(b *testing.B) {
	startBench(b)
	repoID := benchRepoID(shapeManyObjects)
	servers := seededServers(b, shapeManyObjects)
	for _, transport := range []string{"http", "ssh"} {
		for i, e := range benchEngines {
			s := servers[i]
			url, env := remoteFor(s, transport, repoID)
			b.Run(benchName(e, transport, 2, "variant=depth1"), func(b *testing.B) {
				benchClones(b, s, 0, env, 2, url, "--depth", "1")
			})
		}
	}
	for _, transport := range []string{"http", "ssh"} {
		for i, e := range benchEngines {
			s := servers[i]
			url, env := remoteFor(s, transport, repoID)
			pristine := filepath.Join(b.TempDir(), "pristine")
			runGit(b, "", env, "clone", "--quiet", "--depth", "1", "--no-checkout", url, pristine)
			b.Run(benchName(e, transport, 2, "variant=deepen"), func(b *testing.B) {
				dirs := newScratch(b)
				m := newBenchMeter(b, s)
				for range b.N {
					m.stopTimer()
					dirs.clear()
					dir := dirs.next()
					// A shallow repository cannot be cloned with hardlinks, so the pristine client is copied.
					if err := os.CopyFS(dir, os.DirFS(pristine)); err != nil {
						b.Fatalf("copy %s: %v", pristine, err)
					}
					m.startTimer()
					m.git(dir, env, protoArgs(2, "fetch", "--quiet", "--deepen=100", "origin")...)
				}
				m.finish()
			})
		}
	}
}

// BenchmarkMirror times a pull-through mirror pulling a repository whole and
// syncing it unchanged, and a push that is mirrored on before it returns.
// Upstream and destination run in this process and stay native, so their CPU
// is in server-cpu-ms/op while their traffic is upstream-MB/op.
func BenchmarkMirror(b *testing.B) {
	startBench(b)
	const org, name = "bench", "mirror"
	repoID := org + "/" + name
	type mirrorSetup struct {
		upstream, mirror, dest, src *e2eServer
		work                        string
	}
	setups := make([]mirrorSetup, len(benchEngines))
	for i, e := range benchEngines {
		upstream := newE2EServer(b, withTransportStats())
		benchFixture(b, shapeManyObjects).seed(b, upstream.repoDir(repoID))
		mirror := benchServer(b, e, withMirrorSource(upstream.httpURL))
		mirrorURL, _ := mirror.httpRemote(repoID)
		// Opening repoID for the first time pulls it in.
		requireEngine(b, mirrorURL, e.native)

		dest := newE2EServer(b, withTransportStats())
		dest.createRepo(b, org, name)
		src := benchServer(b, e, withMirrorDestination(dest.httpURL))
		seedBench(b, src, e, shapeManyObjects, repoID)
		srcURL, srcEnv := src.httpRemote(repoID)
		work := workClone(b, srcURL, srcEnv)
		runGit(b, work, nil, "commit", "--quiet", "--allow-empty", "-m", "sync the destination")
		runGit(b, work, srcEnv, "push", "--quiet", "origin", "HEAD:refs/heads/main")
		setups[i] = mirrorSetup{upstream, mirror, dest, src, work}
	}

	for _, variant := range []string{"cold-pull", "warm-sync"} {
		for i, e := range benchEngines {
			ms := setups[i]
			url, env := ms.mirror.httpRemote(repoID)
			b.Run(benchName(e, "http", 2, "variant="+variant), func(b *testing.B) {
				m := newBenchMeter(b, ms.mirror)
				upstream := serverBytes(ms.upstream)
				for range b.N {
					if variant == "cold-pull" {
						m.stopTimer()
						ms.mirror.deleteRepo(b, org, name)
						m.startTimer()
					}
					m.git("", env, protoArgs(2, "ls-remote", url)...)
				}
				reportUpstream(b, ms.upstream, upstream)
				m.finish()
			})
		}
	}
	for i, e := range benchEngines {
		ms := setups[i]
		_, env := ms.src.httpRemote(repoID)
		b.Run(benchName(e, "http", 2, "variant=push-mirror"), func(b *testing.B) {
			m := newBenchMeter(b, ms.src)
			dest := serverBytes(ms.dest)
			benchCommitPushes(b, m, ms.work, env)
			reportUpstream(b, ms.dest, dest)
			m.finish()
		})
	}
}

// reportUpstream reports per op the bytes s, an upstream or destination, moved beyond since.
func reportUpstream(b *testing.B, s *e2eServer, since int64) {
	b.ReportMetric(float64(serverBytes(s)-since)/float64(b.N)/1e6, "upstream-MB/op")
}

// BenchmarkParallelClone times HFD_BENCH_PARALLEL clones at once, 4 by default.
func BenchmarkParallelClone(b *testing.B) {
	startBench(b)
	parallel := 4
	if v := strings.TrimSpace(os.Getenv("HFD_BENCH_PARALLEL")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			b.Fatalf("HFD_BENCH_PARALLEL=%q is not a positive integer", v)
		}
		parallel = n
	}
	repoID := benchRepoID(shapeManyObjects)
	servers := seededServers(b, shapeManyObjects)
	for i, e := range benchEngines {
		s := servers[i]
		url, env := s.httpRemote(repoID)
		b.Run(benchName(e, "http", 2, fmt.Sprintf("shape=%s/parallel=%d", shapeManyObjects, parallel)), func(b *testing.B) {
			dirs := newScratch(b)
			targets := make([]string, parallel)
			errs := make([]error, parallel)
			m := newBenchMeter(b, s)
			for range b.N {
				m.stopTimer()
				dirs.clear()
				for j := range targets {
					targets[j] = dirs.next()
				}
				m.startTimer()
				var wg sync.WaitGroup
				for j, dir := range targets {
					wg.Go(func() {
						if out, err := m.gitErr("", env, protoArgs(2, "clone", "--quiet", url, dir)...); err != nil {
							errs[j] = fmt.Errorf("clone into %s: %w\n%s", dir, err, out)
						}
					})
				}
				wg.Wait()
				if err := errors.Join(errs...); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(parallel*b.N)/b.Elapsed().Seconds(), "clones/s")
			m.finish()
		})
	}
}
