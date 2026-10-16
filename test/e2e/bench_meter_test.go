//go:build unix

package e2e_test

import (
	"cmp"
	"context"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// benchMeter charges a benchmark's server with what its timed windows cost:
// the CPU time of this process and of the git processes it reaps, less that of
// the git clients run through gitErr, and the bytes on the server's
// connections. stopTimer and startTimer stand in for b's.
type benchMeter struct {
	b *testing.B
	s *e2eServer
	// timeout is gitErr's watchdog on each git client, 3 minutes when zero; rows with slow clients raise it.
	timeout time.Duration

	mu      sync.Mutex
	running bool
	// openCPU and openBytes are the readings taken when the running window opened.
	openCPU   time.Duration
	openBytes int64
	cpu       time.Duration // over the closed windows
	bytes     int64         // over the closed windows
	clientCPU time.Duration // of the clients that exited while a window ran
}

// newBenchMeter resets b's timer and opens the first running window once the
// benchmark's setup is done; s must be started withTransportStats.
func newBenchMeter(b *testing.B, s *e2eServer) *benchMeter {
	b.Helper()
	if s.traffic == nil {
		b.Fatalf("newBenchMeter: the server was started without withTransportStats")
	}
	m := &benchMeter{b: b, s: s}
	b.ResetTimer()
	m.startTimer()
	return m
}

// gitErr runs git like runGit but returns its error; a client exiting while a
// window runs has its CPU kept off the server's. Safe for concurrent use.
func (m *benchMeter) gitErr(dir string, env []string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cmp.Or(m.timeout, 3*time.Minute))
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = append(testEnv(), env...)
	cmd.WaitDelay = 10 * time.Second
	output, err := cmd.CombinedOutput()
	if cmd.ProcessState != nil {
		cpu := rusageCPU(cmd.ProcessState.SysUsage().(*syscall.Rusage))
		m.mu.Lock()
		// A window toggled between the client's exit and this check misattributes its CPU; accepted.
		if m.running {
			m.clientCPU += cpu
		}
		m.mu.Unlock()
	}
	return string(output), err
}

// git is gitErr failing b on error; call it from the benchmark goroutine only.
func (m *benchMeter) git(dir string, env []string, args ...string) string {
	m.b.Helper()
	output, err := m.gitErr(dir, env, args...)
	if err != nil {
		m.b.Fatalf("git %s failed: %v\noutput: %s", strings.Join(args, " "), err, output)
	}
	return output
}

// stopTimer closes the running window and stops b's timer.
func (m *benchMeter) stopTimer() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.running {
		return
	}
	in, out := m.s.transportBytes()
	m.bytes += in + out - m.openBytes
	m.cpu += m.processCPU() - m.openCPU
	m.running = false
	m.b.StopTimer()
}

// startTimer starts b's timer and opens a running window.
func (m *benchMeter) startTimer() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		return
	}
	m.b.StartTimer()
	in, out := m.s.transportBytes()
	m.openBytes = in + out
	m.openCPU = m.processCPU()
	m.running = true
}

// finish closes the running window and reports the server's bytes per op,
// through b.SetBytes, and its server-cpu-ms/op; call it once after the loop.
func (m *benchMeter) finish() {
	m.stopTimer()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.b.SetBytes(m.bytes / int64(m.b.N))
	// The git clients land in RUSAGE_CHILDREN beside the native engine's git processes.
	server := m.cpu - m.clientCPU
	// A client reaped while stopped but tallied after a restart can take an idle server below zero.
	server = max(server, 0)
	m.b.ReportMetric(float64(server)/float64(time.Millisecond)/float64(m.b.N), "server-cpu-ms/op")
}

// processCPU is the user+sys time of this process and of its reaped children.
func (m *benchMeter) processCPU() time.Duration {
	var total time.Duration
	for _, who := range []int{syscall.RUSAGE_SELF, syscall.RUSAGE_CHILDREN} {
		var ru syscall.Rusage
		if err := syscall.Getrusage(who, &ru); err != nil {
			m.b.Fatalf("getrusage(%d): %v", who, err)
		}
		total += rusageCPU(&ru)
	}
	return total
}

func rusageCPU(ru *syscall.Rusage) time.Duration {
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
