//go:build unix

package e2e_test

import (
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestBenchMeterCPUAttribution checks server-cpu-ms/op counts the CPU of the
// test process and of the processes it reaps in running windows, less the
// meter's own git clients, and nothing from stopped windows.
func TestBenchMeterCPUAttribution(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "bench-meter-*")
	if err != nil {
		t.Fatalf("create the hashed file: %v", err)
	}
	if _, err := io.CopyN(f, rand.NewChaCha8([32]byte{'h', 'f', 'd'}), 128<<20); err != nil {
		t.Fatalf("write the hashed file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close the hashed file: %v", err)
	}
	file := f.Name()
	s := newE2EServer(t, withTransportStats())

	var viaMeter, direct float64
	t.Run("ViaMeter", func(t *testing.T) {
		r := runMeterBench(t, func(b *testing.B) {
			m := newBenchMeter(b, s)
			for range b.N {
				m.git("", nil, "hash-object", file)
			}
			m.finish()
		})
		viaMeter = meterServerCPU(t, r)
		t.Logf("hash-object through meter.git: %.1f server-cpu-ms/op over N=%d", viaMeter, r.N)
		if viaMeter >= 50 {
			t.Errorf("server-cpu-ms/op = %.1f for hash-object run through meter.git, want < 50: the meter's own git clients are not server CPU", viaMeter)
		}
	})
	t.Run("Direct", func(t *testing.T) {
		var childMS float64
		r := runMeterBench(t, func(b *testing.B) {
			m := newBenchMeter(b, s)
			var child time.Duration
			for range b.N {
				child += hashObjectUnmetered(t, b, file)
			}
			m.finish()
			childMS = float64(child) / float64(time.Millisecond) / float64(b.N)
		})
		direct = meterServerCPU(t, r)
		t.Logf("hash-object run directly: %.1f server-cpu-ms/op over N=%d, its processes using %.1f ms/op", direct, r.N, childMS)
		if direct < 50 {
			t.Errorf("server-cpu-ms/op = %.1f for hash-object run outside the meter, want >= 50: reaped child processes are server CPU", direct)
		}
		if direct < childMS-1 || direct >= childMS+50 {
			t.Errorf("server-cpu-ms/op = %.1f for hash-object run outside the meter, want in [%.1f, %.1f): the %.1f ms/op its processes used, counted once and divided by N, plus under 50 of the test process's own", direct, childMS-1, childMS+50, childMS)
		}
	})
	t.Run("InProcess", func(t *testing.T) {
		const spin = 100 * time.Millisecond
		r := runMeterBench(t, func(b *testing.B) {
			m := newBenchMeter(b, s)
			for range b.N {
				spinSelfCPU(t, b, spin)
			}
			m.finish()
		})
		got := meterServerCPU(t, r)
		t.Logf("spinning %v of in-process CPU per op: %.1f server-cpu-ms/op over N=%d", spin, got, r.N)
		if want := float64(spin)/float64(time.Millisecond) - 1; got < want {
			t.Errorf("server-cpu-ms/op = %.1f for spinning %v of the test process's own CPU per op, want >= %.1f: CPU the test process uses in a running window, as go-git serving in-process does, is server CPU", got, spin, want)
		}
	})
	t.Run("StoppedClient", func(t *testing.T) {
		var childMS float64
		r := runMeterBench(t, func(b *testing.B) {
			m := newBenchMeter(b, s)
			var child time.Duration
			for range b.N {
				m.stopTimer()
				m.git("", nil, "hash-object", file)
				m.startTimer()
				child += hashObjectUnmetered(t, b, file)
			}
			m.finish()
			childMS = float64(child) / float64(time.Millisecond) / float64(b.N)
		})
		got := meterServerCPU(t, r)
		t.Logf("hash-object through meter.git while stopped, then directly: %.1f server-cpu-ms/op over N=%d, the direct processes using %.1f ms/op", got, r.N, childMS)
		if got < childMS-1 {
			t.Errorf("server-cpu-ms/op = %.1f for hash-object run through meter.git while stopped and then directly, want >= %.1f: a meter client run while stopped is not subtracted", got, childMS-1)
		}
	})
	t.Run("Stopped", func(t *testing.T) {
		r := runMeterBench(t, func(b *testing.B) {
			m := newBenchMeter(b, s)
			for range b.N {
				m.stopTimer()
				hashObjectUnmetered(t, b, file)
				m.startTimer()
				// Timed but CPU-free, keeping b.N small.
				time.Sleep(500 * time.Millisecond)
			}
			m.finish()
		})
		stopped := meterServerCPU(t, r)
		t.Logf("hash-object run directly while stopped: %.1f server-cpu-ms/op over N=%d", stopped, r.N)
		if stopped >= 50 {
			t.Errorf("server-cpu-ms/op = %.1f for hash-object run directly between stopTimer and startTimer, want < 50: stopped windows are not measured", stopped)
		}
	})
	if t.Failed() {
		return
	}
	if direct < 4*viaMeter {
		t.Errorf("server-cpu-ms/op = %.1f for hash-object run directly, want >= 4 x %.1f, its value through meter.git", direct, viaMeter)
	}
}

// TestBenchMeterBytes checks SetBytes reports the running windows' transport
// bytes per op, leaving out the traffic of stopped windows.
func TestBenchMeterBytes(t *testing.T) {
	s := newE2EServer(t, withTransportStats())
	s.createRepo(t, "bench-org", "meter")
	repoURL, _ := s.httpRemote("bench-org/meter")
	traffic := func() int64 {
		in, out := s.transportBytes()
		return in + out
	}
	// The server counts a write once it returns, which can be after the client already has the bytes.
	settled := func() int64 {
		for last := traffic(); ; {
			time.Sleep(100 * time.Millisecond)
			now := traffic()
			if now == last {
				return now
			}
			last = now
		}
	}
	settled()

	var want, stoppedBytes int64
	r := runMeterBench(t, func(b *testing.B) {
		m := newBenchMeter(b, s)
		var running, stopped int64
		for range b.N {
			m.stopTimer()
			dir := filepath.Join(b.TempDir(), "c")
			before := traffic()
			resp, err := http.Get(repoURL + "/info/refs?service=git-upload-pack")
			if err != nil {
				t.Errorf("get the advertisement while stopped: %v", err)
				b.FailNow()
			}
			_, err = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Errorf("read the advertisement while stopped: %v", err)
				b.FailNow()
			}
			stopped += settled() - before
			m.startTimer()

			before = traffic()
			m.git("", nil, "clone", "--quiet", repoURL, dir)
			running += settled() - before
		}
		m.finish()
		want, stoppedBytes = running/int64(b.N), stopped
	})
	t.Logf("SetBytes = %d bytes/op over N=%d; running windows %d bytes/op, stopped windows %d bytes in all", r.Bytes, r.N, want, stoppedBytes)
	if stoppedBytes == 0 {
		t.Fatalf("the advertisements fetched while stopped moved no bytes, so stopped and running windows cannot be told apart")
	}
	if r.Bytes <= 0 || r.Bytes != want {
		t.Errorf("SetBytes = %d bytes/op over N=%d, want %d > 0: the running windows' transport bytes divided by N, leaving out the %d bytes moved while stopped", r.Bytes, r.N, want, stoppedBytes)
	}
	meterServerCPU(t, r)
}

// TestBenchMeterRequiresTransportStats checks newBenchMeter fails the
// benchmark on a server without withTransportStats.
func TestBenchMeterRequiresTransportStats(t *testing.T) {
	s := newE2EServer(t)
	var failed, returned bool
	testing.Benchmark(func(b *testing.B) {
		defer func() { failed = b.Failed() }()
		newBenchMeter(b, s)
		returned = true
	})
	if !failed {
		t.Fatalf("newBenchMeter on a server without withTransportStats left the benchmark passing, want b.Fatalf")
	}
	if returned {
		t.Errorf("newBenchMeter on a server without withTransportStats returned, want b.Fatalf to stop the benchmark")
	}
}

// TestBenchMeterConcurrentGit checks gitErr can be called from several
// goroutines at once; run it under -race.
func TestBenchMeterConcurrentGit(t *testing.T) {
	file := filepath.Join(t.TempDir(), "small.txt")
	if err := os.WriteFile(file, []byte("bench meter\n"), 0o644); err != nil {
		t.Fatalf("write %s: %v", file, err)
	}
	s := newE2EServer(t, withTransportStats())

	var reportErr sync.Once
	r := runMeterBench(t, func(b *testing.B) {
		m := newBenchMeter(b, s)
		for range b.N {
			var wg sync.WaitGroup
			for range 4 {
				wg.Go(func() {
					if out, err := m.gitErr("", nil, "hash-object", file); err != nil {
						reportErr.Do(func() { t.Errorf("meter.gitErr(hash-object) from 4 goroutines: %v\n%s", err, out) })
					}
				})
			}
			wg.Wait()
		}
		m.finish()
	})
	meterServerCPU(t, r)
}

// TestBenchMeterTimeout checks gitErr's watchdog applies the meter's timeout,
// 3 minutes when it is zero.
func TestBenchMeterTimeout(t *testing.T) {
	file := filepath.Join(t.TempDir(), "small.txt")
	if err := os.WriteFile(file, []byte("bench meter\n"), 0o644); err != nil {
		t.Fatalf("write %s: %v", file, err)
	}
	s := newE2EServer(t, withTransportStats())

	var shortErr, defaultErr error
	var defaultOut string
	runMeterBench(t, func(b *testing.B) {
		m := newBenchMeter(b, s)
		m.timeout = time.Nanosecond
		_, shortErr = m.gitErr("", nil, "hash-object", file)
		m.timeout = 0
		defaultOut, defaultErr = m.gitErr("", nil, "hash-object", file)
		m.finish()
	})
	if shortErr == nil {
		t.Errorf("meter.gitErr(hash-object) with timeout = 1ns succeeded, want its watchdog to stop the client")
	}
	if defaultErr != nil {
		t.Errorf("meter.gitErr(hash-object) with timeout = 0: %v\n%s\nwant it to finish under the 3-minute default", defaultErr, defaultOut)
	}
}

// runMeterBench runs fn under testing.Benchmark, failing t if the benchmark
// failed; testing.Benchmark drops the benchmark's own log.
func runMeterBench(t *testing.T, fn func(b *testing.B)) testing.BenchmarkResult {
	t.Helper()
	var failed bool
	r := testing.Benchmark(func(b *testing.B) {
		defer func() { failed = b.Failed() }()
		fn(b)
	})
	if failed {
		t.Fatalf("the benchmark failed; testing.Benchmark drops its log, so a b.Fatal in newBenchMeter or meter.git shows only as this")
	}
	return r
}

// meterServerCPU returns r's server-cpu-ms/op, failing t when it is missing.
func meterServerCPU(t *testing.T, r testing.BenchmarkResult) float64 {
	t.Helper()
	v, ok := r.Extra["server-cpu-ms/op"]
	if !ok {
		t.Fatalf("benchmark reported no server-cpu-ms/op (extra metrics %v): benchMeter.finish must report it", r.Extra)
	}
	return v
}

// hashObjectUnmetered runs git hash-object on file outside the meter and
// returns the CPU its process used.
func hashObjectUnmetered(t *testing.T, b *testing.B, file string) time.Duration {
	t.Helper()
	cmd := exec.Command("git", "hash-object", file)
	cmd.Env = testEnv()
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("git hash-object %s: %v\n%s", file, err, out)
		b.FailNow()
	}
	return cmd.ProcessState.UserTime() + cmd.ProcessState.SystemTime()
}

// spinSelfCPU spins until the test process has used d more CPU time.
func spinSelfCPU(t *testing.T, b *testing.B, d time.Duration) {
	t.Helper()
	used := func() time.Duration {
		var ru syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
			t.Errorf("getrusage(RUSAGE_SELF): %v", err)
			b.FailNow()
		}
		return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
	}
	for start := used(); used()-start < d; {
	}
}
