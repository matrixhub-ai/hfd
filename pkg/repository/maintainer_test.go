package repository

// Tests for Maintainer: debounced, bounded, waitable and closable repacks in the background.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/osfs"
)

// mtTimeout bounds every wait for what the Maintainer must let happen; generous so a loaded -race run never flakes.
const mtTimeout = 30 * time.Second

// mtImmediate is the delay under which the fake clock fires at once, as a real timer does for Reset(0).
const mtImmediate = time.Second

// mtCond is a mutex whose waiters wake on every broadcast.
type mtCond struct {
	mu      sync.Mutex
	changed chan struct{}
}

// broadcast wakes every waiter; mu is held.
func (c *mtCond) broadcast() {
	if c.changed != nil {
		close(c.changed)
	}
	c.changed = make(chan struct{})
}

// await reports whether ok, called with mu held, holds within mtTimeout.
func (c *mtCond) await(ok func() bool) bool {
	deadline := time.After(mtTimeout)
	for {
		c.mu.Lock()
		if c.changed == nil {
			c.changed = make(chan struct{})
		}
		done, changed := ok(), c.changed
		c.mu.Unlock()
		if done {
			return true
		}
		select {
		case <-changed:
		case <-deadline:
			return false
		}
	}
}

// mtClock is the afterFunc seam: a timer fires when the test calls fire, or at once when armed under mtImmediate.
type mtClock struct {
	mtCond
	seq      int // arms so far
	timers   []*mtTimer
	inflight int // callbacks started and not yet returned
}

// mtTimer is a timer of an mtClock.
type mtTimer struct {
	c      *mtClock
	f      func()
	delay  time.Duration   // of the latest arm
	armed  int             // that arm's sequence number while pending, else 0
	resets []time.Duration // the delay of every Reset
	stops  int             // Stop calls
}

func (c *mtClock) afterFunc(d time.Duration, f func()) maintenanceTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	tm := &mtTimer{c: c, f: f}
	c.timers = append(c.timers, tm)
	tm.arm(d)
	return tm
}

func (tm *mtTimer) Stop() bool {
	tm.c.mu.Lock()
	defer tm.c.mu.Unlock()
	pending := tm.armed != 0
	tm.armed = 0
	tm.stops++
	return pending
}

func (tm *mtTimer) Reset(d time.Duration) bool {
	tm.c.mu.Lock()
	defer tm.c.mu.Unlock()
	pending := tm.armed != 0
	tm.resets = append(tm.resets, d)
	tm.arm(d)
	return pending
}

// arm schedules tm after d; c.mu is held.
func (tm *mtTimer) arm(d time.Duration) {
	c := tm.c
	tm.delay, tm.armed = d, 0
	if d < mtImmediate {
		c.start(tm.f)
		return
	}
	c.seq++
	tm.armed = c.seq
	c.broadcast()
}

// state returns tm's latest arming delay and the delay of every Reset so far.
func (tm *mtTimer) state() (time.Duration, []time.Duration) {
	tm.c.mu.Lock()
	defer tm.c.mu.Unlock()
	return tm.delay, slices.Clone(tm.resets)
}

// stopped returns how often tm was stopped.
func (tm *mtTimer) stopped() int {
	tm.c.mu.Lock()
	defer tm.c.mu.Unlock()
	return tm.stops
}

// fire runs tm's callback now, pending or not, as a firing that raced a Stop or Reset does.
func (tm *mtTimer) fire() {
	tm.c.mu.Lock()
	defer tm.c.mu.Unlock()
	tm.armed = 0
	tm.c.start(tm.f)
}

// start runs f on its own goroutine, as time.AfterFunc does; c.mu is held.
func (c *mtClock) start(f func()) {
	c.inflight++
	go func() {
		f()
		c.mu.Lock()
		defer c.mu.Unlock()
		c.inflight--
		c.broadcast()
	}()
}

// mark returns the arms so far, so waitArmed can ask for a later one.
func (c *mtClock) mark() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seq
}

// fire runs every pending timer as if its delay had elapsed.
func (c *mtClock) fire() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, tm := range c.timers {
		if tm.armed != 0 {
			tm.armed = 0
			c.start(tm.f)
		}
	}
}

// pending returns the timer still pending from the latest arm after mark, or nil; c.mu is held.
func (c *mtClock) pending(mark int) *mtTimer {
	var latest *mtTimer
	for _, tm := range c.timers {
		if tm.armed > mark && (latest == nil || tm.armed > latest.armed) {
			latest = tm
		}
	}
	return latest
}

// armedSince returns the timer still pending from the latest arm after mark, or nil.
func (c *mtClock) armedSince(mark int) *mtTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pending(mark)
}

// waitArmed waits for a timer armed after mark, failing the test with what if none is.
func (c *mtClock) waitArmed(t *testing.T, mark int, what string) *mtTimer {
	t.Helper()
	var tm *mtTimer
	if !c.await(func() bool { tm = c.pending(mark); return tm != nil }) {
		t.Fatalf("%s armed no timer within %v", what, mtTimeout)
	}
	return tm
}

// waitPending waits until n timers are pending, failing the test with what if they are not.
func (c *mtClock) waitPending(t *testing.T, n int, what string) {
	t.Helper()
	var got int
	if !c.await(func() bool {
		got = 0
		for _, tm := range c.timers {
			if tm.armed != 0 {
				got++
			}
		}
		return got == n
	}) {
		t.Fatalf("%s left %d timers pending after %v, want %d", what, got, mtTimeout, n)
	}
}

// settle waits until every callback the clock started has returned.
func (c *mtClock) settle(t *testing.T) {
	t.Helper()
	if !c.await(func() bool { return c.inflight == 0 }) {
		t.Fatalf("timer callbacks still running after %v", mtTimeout)
	}
}

// mtKey is a repository as Notify names it.
type mtKey struct {
	fs   billy.Filesystem
	path string
}

// mtSeam is a repack seam recording its calls; with a gate each call waits for a token or its context.
type mtSeam struct {
	gate    chan struct{}
	result  func(k mtKey, call int) (busy bool, err error) // nil reports success
	entered chan mtKey                                     // each call as it starts

	mu       sync.Mutex
	calls    map[mtKey]int
	running  map[mtKey]int
	inFlight int
	peak     int // most calls in flight at once
	selfPeak int // most calls of one key in flight at once
}

func newMTSeam(gate chan struct{}) *mtSeam {
	return &mtSeam{gate: gate, entered: make(chan mtKey, 64), calls: map[mtKey]int{}, running: map[mtKey]int{}}
}

func (s *mtSeam) repack(ctx context.Context, fs billy.Filesystem, path string) (bool, error) {
	k := mtKey{fs, path}
	s.mu.Lock()
	s.calls[k]++
	call := s.calls[k]
	s.running[k]++
	s.inFlight++
	s.peak = max(s.peak, s.inFlight)
	s.selfPeak = max(s.selfPeak, s.running[k])
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.running[k]--
		s.inFlight--
		s.mu.Unlock()
	}()
	s.entered <- k
	if s.gate != nil {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	if s.result == nil {
		return false, nil
	}
	return s.result(k, call)
}

// count returns how often the seam ran for k.
func (s *mtSeam) count(k mtKey) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[k]
}

// total returns how often the seam ran for any key.
func (s *mtSeam) total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		n += c
	}
	return n
}

// peaks returns the most calls in flight at once, overall and for one key.
func (s *mtSeam) peaks() (all, self int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak, s.selfPeak
}

// waitEntered waits for the next call to start, failing the test with what if none does.
func (s *mtSeam) waitEntered(t *testing.T, what string) mtKey {
	t.Helper()
	select {
	case k := <-s.entered:
		return k
	case <-time.After(mtTimeout):
		t.Fatalf("%s: the repack seam did not run within %v", what, mtTimeout)
		return mtKey{}
	}
}

// mtMaintainer returns a Maintainer running seam on clock's timers, closed when the test ends.
func mtMaintainer(t *testing.T, o MaintainerOptions, seam *mtSeam) (*Maintainer, *mtClock) {
	t.Helper()
	m := NewMaintainer(o)
	clock := &mtClock{}
	m.afterFunc = clock.afterFunc
	m.repack = seam.repack
	t.Cleanup(m.Close)
	return m, clock
}

// mtReturns requires fn to return within mtTimeout.
func mtReturns(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(mtTimeout):
		t.Fatalf("%s did not return within %v", what, mtTimeout)
	}
}

// mtFlush requires Flush to return nil within mtTimeout.
func mtFlush(t *testing.T, m *Maintainer, what string) {
	t.Helper()
	var err error
	mtReturns(t, "Flush "+what, func() { err = m.Flush(t.Context()) })
	if err != nil {
		t.Fatalf("Flush %s = %v, want nil", what, err)
	}
}

// mtLog collects slog's default output for one test, readable while the Maintainer logs.
type mtLog struct {
	mtCond
	buf bytes.Buffer
}

func (l *mtLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.broadcast()
	return l.buf.Write(p)
}

func (l *mtLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// match returns the logged lines holding all of subs; l.mu is held.
func (l *mtLog) match(subs []string) []string {
	var lines []string
next:
	for line := range strings.SplitSeq(l.buf.String(), "\n") {
		for _, s := range subs {
			if !strings.Contains(line, s) {
				continue next
			}
		}
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// lines returns the lines logged so far holding all of subs.
func (l *mtLog) lines(subs ...string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.match(subs)
}

// wait waits for a logged line holding all of subs and returns every such line.
func (l *mtLog) wait(t *testing.T, what string, subs ...string) []string {
	t.Helper()
	var lines []string
	if !l.await(func() bool { lines = l.match(subs); return len(lines) > 0 }) {
		t.Fatalf("log = %q, want %s within %v", l.String(), what, mtTimeout)
	}
	return lines
}

// mtLogs routes slog's default logger, and the log package behind it, into a fresh mtLog until the test ends; call it before mtMaintainer so Close runs first.
func mtLogs(t *testing.T) *mtLog {
	l := &mtLog{}
	previous, out, flags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewTextHandler(l, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previous)
		// SetDefault pointed the log package at l, and restoring a default handler leaves it there.
		log.SetOutput(out)
		log.SetFlags(flags)
	})
	return l
}

// Notifications for one repository coalesce into one run; another path or another filesystem is another repository.
func TestMaintainerCoalesces(t *testing.T) {
	seam := newMTSeam(nil)
	m, _ := mtMaintainer(t, MaintainerOptions{Debounce: time.Hour, Concurrency: 2}, seam)
	fs, other := memfs.New(), memfs.New()
	notified := map[mtKey]int{{fs, "/a.git"}: 5, {fs, "/b.git"}: 3, {other, "/a.git"}: 2}
	for k, n := range notified {
		for range n {
			m.Notify(k.fs, k.path)
		}
	}
	mtFlush(t, m, "after the notifications")
	for k, n := range notified {
		if got := seam.count(k); got != 1 {
			t.Errorf("%d Notify calls for %s on filesystem %p ran the seam %d times, want once", n, k.path, k.fs, got)
		}
	}
}

// The debounce is trailing: a Notify while the window runs stops the pending timer and arms a new one for the full window.
func TestMaintainerDebounceIsTrailing(t *testing.T) {
	const window = 42 * time.Minute
	seam := newMTSeam(nil)
	m, clock := mtMaintainer(t, MaintainerOptions{Debounce: window, Concurrency: 1}, seam)
	k := mtKey{memfs.New(), "/a.git"}
	m.Notify(k.fs, k.path)
	first := clock.waitArmed(t, 0, "Notify")
	if delay, _ := first.state(); delay != window {
		t.Fatalf("Notify armed a timer for %v, want the %v debounce", delay, window)
	}
	mark := clock.mark()
	m.Notify(k.fs, k.path)
	again := clock.waitArmed(t, mark, "a second Notify within the window")
	if _, resets := first.state(); len(resets) != 0 {
		t.Fatalf("a second Notify within the window Reset the pending timer to %v, want it stopped and a new timer armed", resets)
	}
	if first.stopped() == 0 {
		t.Fatal("a second Notify within the window left the pending timer unstopped")
	}
	if delay, _ := again.state(); delay != window {
		t.Fatalf("a second Notify within the window armed a timer for %v, want the full %v window", delay, window)
	}
	if n := seam.count(k); n != 0 {
		t.Fatalf("the seam ran %d times before the window elapsed, want 0", n)
	}
}

// A timer a later Notify replaced starts no run even when its callback runs late, so the key stays armed and Flush runs it once.
func TestMaintainerDebounceIgnoresStaleCallback(t *testing.T) {
	seam := newMTSeam(nil)
	m, clock := mtMaintainer(t, MaintainerOptions{Debounce: time.Hour, Concurrency: 1}, seam)
	k := mtKey{memfs.New(), "/a.git"}
	m.Notify(k.fs, k.path)
	first := clock.waitArmed(t, 0, "Notify")
	m.Notify(k.fs, k.path)
	first.fire()
	clock.settle(t)
	// A run the late callback started would make this Notify a second run.
	m.Notify(k.fs, k.path)
	mtFlush(t, m, "after the replaced timer's late callback")
	if n := seam.total(); n != 1 {
		t.Errorf("the seam ran %d times, want once: the late callback of the timer a second Notify replaced started a run", n)
	}
}

// A Notify while the key runs marks it dirty: once the run ends the key is re-armed for a full window and runs again, never alongside itself.
func TestMaintainerDirtyRerun(t *testing.T) {
	const window = time.Hour
	seam := newMTSeam(make(chan struct{}, 2))
	m, clock := mtMaintainer(t, MaintainerOptions{Debounce: window, Concurrency: 2}, seam)
	k := mtKey{memfs.New(), "/a.git"}
	m.Notify(k.fs, k.path)
	clock.waitArmed(t, 0, "Notify")
	clock.fire()
	seam.waitEntered(t, "the first run")
	mark := clock.mark()
	mtReturns(t, "Notify while the key runs", func() { m.Notify(k.fs, k.path) })
	seam.gate <- struct{}{}
	rearmed := clock.waitArmed(t, mark, "the end of a run a Notify interrupted")
	if delay, _ := rearmed.state(); delay != window {
		t.Errorf("the dirty key was re-armed for %v, want the full %v window", delay, window)
	}
	if n := seam.count(k); n != 1 {
		t.Fatalf("the dirty key ran %d times before its window elapsed, want once", n)
	}
	seam.gate <- struct{}{}
	mtFlush(t, m, "with the key re-armed")
	if n := seam.count(k); n != 2 {
		t.Errorf("the dirty key ran %d times, want a second run after the first", n)
	}
	if _, self := seam.peaks(); self != 1 {
		t.Errorf("the key ran %d times at once, want never alongside itself", self)
	}
}

// A busy run re-arms its key for a full window and runs again once that elapses; busy is no failure.
func TestMaintainerBusyRearms(t *testing.T) {
	const window = time.Hour
	logs := mtLogs(t)
	seam := newMTSeam(nil)
	seam.result = func(_ mtKey, call int) (bool, error) { return call == 1, nil }
	m, clock := mtMaintainer(t, MaintainerOptions{Debounce: window, Concurrency: 1}, seam)
	k := mtKey{memfs.New(), "/a.git"}
	m.Notify(k.fs, k.path)
	clock.waitArmed(t, 0, "Notify")
	mark := clock.mark()
	clock.fire()
	seam.waitEntered(t, "the busy run")
	rearmed := clock.waitArmed(t, mark, "a busy run")
	if delay, _ := rearmed.state(); delay != window {
		t.Errorf("the busy key was re-armed for %v, want the full %v window", delay, window)
	}
	if n := seam.count(k); n != 1 {
		t.Fatalf("the busy key ran %d times before its window elapsed, want once", n)
	}
	clock.fire()
	seam.waitEntered(t, "the run once the busy window elapsed")
	mtFlush(t, m, "after the busy key ran again")
	if n := seam.count(k); n != 2 {
		t.Errorf("the busy key ran %d times, want twice", n)
	}
	if lines := append(logs.lines("level=WARN"), logs.lines("level=ERROR")...); len(lines) != 0 {
		t.Errorf("a busy run logged %q, want no warning or error", lines)
	}
}

// Concurrency bounds the runs in flight across repositories; a Concurrency of 0 runs one at a time.
func TestMaintainerConcurrency(t *testing.T) {
	for _, tc := range []struct {
		name        string
		concurrency int
		keys, want  int
	}{{"Two", 2, 5, 2}, {"ZeroMeansOne", 0, 3, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			seam := newMTSeam(make(chan struct{}, tc.keys))
			m, clock := mtMaintainer(t, MaintainerOptions{Debounce: time.Hour, Concurrency: tc.concurrency}, seam)
			fs := memfs.New()
			for i := range tc.keys {
				m.Notify(fs, fmt.Sprintf("/%d.git", i))
			}
			clock.waitPending(t, tc.keys, "a Notify for each repository")
			clock.fire()
			for i := range tc.want {
				seam.waitEntered(t, fmt.Sprintf("run %d of the %d allowed at once", i+1, tc.want))
			}
			// Each run let finish frees one slot for the next repository.
			for i := tc.want; i < tc.keys; i++ {
				seam.gate <- struct{}{}
				seam.waitEntered(t, fmt.Sprintf("run %d once a slot freed", i+1))
			}
			for range tc.want {
				seam.gate <- struct{}{}
			}
			mtFlush(t, m, "once every run was let finish")
			if all, _ := seam.peaks(); all != tc.want {
				t.Errorf("Concurrency %d let %d runs be in flight at once, want %d", tc.concurrency, all, tc.want)
			}
			for i := range tc.keys {
				if n := seam.count(mtKey{fs, fmt.Sprintf("/%d.git", i)}); n != 1 {
					t.Errorf("/%d.git ran %d times, want once", i, n)
				}
			}
		})
	}
}

// A Debounce of 0 runs a notified repository at once, with no Flush.
func TestMaintainerZeroDebounce(t *testing.T) {
	seam := newMTSeam(nil)
	m := NewMaintainer(MaintainerOptions{Debounce: 0, Concurrency: 1})
	m.repack = seam.repack
	t.Cleanup(m.Close)
	k := mtKey{memfs.New(), "/a.git"}
	m.Notify(k.fs, k.path)
	if got := seam.waitEntered(t, "Notify with a zero debounce"); got != k {
		t.Errorf("the seam ran for %s, want %s", got.path, k.path)
	}
}

// Flush fires what is armed and returns once nothing is pending, running or dirty, never holding Notify up; a context that times out ends it.
func TestMaintainerFlush(t *testing.T) {
	t.Run("WaitsForRunningAndDirty", func(t *testing.T) {
		seam := newMTSeam(make(chan struct{}, 2))
		m, _ := mtMaintainer(t, MaintainerOptions{Debounce: time.Hour, Concurrency: 1}, seam)
		k := mtKey{memfs.New(), "/a.git"}
		m.Notify(k.fs, k.path)
		flushed := make(chan error, 1)
		go func() { flushed <- m.Flush(t.Context()) }()
		seam.waitEntered(t, "the armed key Flush fires")
		mtReturns(t, "Notify during Flush", func() { m.Notify(k.fs, k.path) })
		select {
		case err := <-flushed:
			t.Fatalf("Flush returned %v while the key ran, want it to wait", err)
		default:
		}
		seam.gate <- struct{}{}
		seam.waitEntered(t, "the dirty re-run Flush waits for")
		select {
		case err := <-flushed:
			t.Fatalf("Flush returned %v while the dirty re-run ran, want it to wait", err)
		default:
		}
		seam.gate <- struct{}{}
		select {
		case err := <-flushed:
			if err != nil {
				t.Fatalf("Flush = %v, want nil", err)
			}
		case <-time.After(mtTimeout):
			t.Fatalf("Flush did not return within %v of the dirty re-run", mtTimeout)
		}
		if n := seam.count(k); n != 2 {
			t.Errorf("the key ran %d times under Flush, want the run and its dirty re-run", n)
		}
	})

	t.Run("ContextTimeout", func(t *testing.T) {
		seam := newMTSeam(make(chan struct{}, 1))
		m, clock := mtMaintainer(t, MaintainerOptions{Debounce: time.Hour, Concurrency: 1}, seam)
		k := mtKey{memfs.New(), "/a.git"}
		m.Notify(k.fs, k.path)
		clock.waitArmed(t, 0, "Notify")
		clock.fire()
		seam.waitEntered(t, "the run")
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		var err error
		mtReturns(t, "Flush with a context that times out", func() { err = m.Flush(ctx) })
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Flush with a context that timed out during a run = %v, want %v", err, context.DeadlineExceeded)
		}
		seam.gate <- struct{}{}
	})
}

// Close drops armed timers, ignores later Notify calls, cancels and awaits running seams, and may be repeated; a nil Maintainer does nothing.
func TestMaintainerClose(t *testing.T) {
	t.Run("DropsArmedAndLaterNotify", func(t *testing.T) {
		seam := newMTSeam(nil)
		m, clock := mtMaintainer(t, MaintainerOptions{Debounce: time.Hour, Concurrency: 1}, seam)
		fs := memfs.New()
		m.Notify(fs, "/armed.git")
		clock.waitArmed(t, 0, "Notify before Close")
		mtReturns(t, "Close", m.Close)
		mark := clock.mark()
		mtReturns(t, "Notify after Close", func() { m.Notify(fs, "/late.git") })
		if clock.armedSince(mark) != nil {
			t.Error("Notify after Close armed a timer, want it ignored")
		}
		// The dropped window elapses anyway.
		clock.fire()
		clock.settle(t)
		if n := seam.total(); n != 0 {
			t.Errorf("the seam ran %d times after Close, want armed timers dropped and later Notify ignored", n)
		}
		mtReturns(t, "a second Close", m.Close)
	})

	t.Run("CancelsAndAwaitsRunningSeam", func(t *testing.T) {
		m := NewMaintainer(MaintainerOptions{Debounce: time.Hour, Concurrency: 1})
		clock := &mtClock{}
		m.afterFunc = clock.afterFunc
		entered, cancelled, finish := make(chan struct{}, 1), make(chan error, 1), make(chan struct{})
		release := sync.OnceFunc(func() { close(finish) })
		m.repack = func(ctx context.Context, _ billy.Filesystem, _ string) (bool, error) {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-ctx.Done():
			case <-finish:
			}
			select {
			case cancelled <- ctx.Err():
			default:
			}
			<-finish
			return false, ctx.Err()
		}
		t.Cleanup(m.Close)
		t.Cleanup(release)
		m.Notify(memfs.New(), "/a.git")
		clock.waitArmed(t, 0, "Notify")
		clock.fire()
		select {
		case <-entered:
		case <-time.After(mtTimeout):
			t.Fatalf("the repack seam did not run within %v", mtTimeout)
		}
		closed := make(chan struct{})
		go func() {
			m.Close()
			close(closed)
		}()
		select {
		case err := <-cancelled:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("the running seam's context ended with %v, want %v from Close", err, context.Canceled)
			}
		case <-time.After(mtTimeout):
			t.Fatalf("Close did not cancel the running seam's context within %v", mtTimeout)
		}
		select {
		case <-closed:
			t.Fatal("Close returned while the cancelled seam still ran, want it to await running seams")
		default:
		}
		release()
		select {
		case <-closed:
		case <-time.After(mtTimeout):
			t.Fatalf("Close did not return within %v of the cancelled seam returning", mtTimeout)
		}
	})

	t.Run("NilIsNoop", func(t *testing.T) {
		var m *Maintainer
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("a nil Maintainer panicked: %v", r)
			}
		}()
		m.Notify(memfs.New(), "/a.git")
		if err := m.Flush(t.Context()); err != nil {
			t.Errorf("Flush on a nil Maintainer = %v, want nil", err)
		}
		m.Close()
	})
}

// A failing run is logged as a warning and never returned; a panicking run is recovered and logged with its stack, and the scheduler keeps running.
func TestMaintainerFailures(t *testing.T) {
	t.Run("ErrorIsLogged", func(t *testing.T) {
		logs := mtLogs(t)
		seam := newMTSeam(nil)
		seam.result = func(mtKey, int) (bool, error) { return false, errors.New("mt-broken") }
		m, _ := mtMaintainer(t, MaintainerOptions{Debounce: time.Hour, Concurrency: 1}, seam)
		k := mtKey{memfs.New(), "/broken.git"}
		m.Notify(k.fs, k.path)
		mtFlush(t, m, "after a failing run")
		if n := seam.count(k); n != 1 {
			t.Fatalf("the failing key ran %d times, want once", n)
		}
		if lines := logs.wait(t, "a warning naming the repository and the error",
			"level=WARN", `msg="Repository maintenance failed"`, "repo=/broken.git", "error=mt-broken"); len(lines) != 1 {
			t.Errorf("log warned %q, want one warning", lines)
		}
	})

	t.Run("PanicIsRecovered", func(t *testing.T) {
		logs := mtLogs(t)
		seam := newMTSeam(nil)
		fs := memfs.New()
		panicky, next := mtKey{fs, "/panic.git"}, mtKey{fs, "/next.git"}
		seam.result = func(k mtKey, _ int) (bool, error) {
			if k == panicky {
				panic("mt-panic")
			}
			return false, nil
		}
		m, clock := mtMaintainer(t, MaintainerOptions{Debounce: time.Hour, Concurrency: 1}, seam)
		m.Notify(panicky.fs, panicky.path)
		clock.waitArmed(t, 0, "Notify")
		clock.fire()
		seam.waitEntered(t, "the panicking run")
		m.Notify(next.fs, next.path)
		mtFlush(t, m, "after a panicking run")
		if n := seam.count(next); n != 1 {
			t.Fatalf("the next key ran %d times after a panic took the only slot, want once", n)
		}
		m.Notify(panicky.fs, panicky.path)
		mtFlush(t, m, "with the panicked key notified again")
		if n := seam.count(panicky); n != 2 {
			t.Errorf("the panicked key ran %d times, want it to run again once notified", n)
		}
		// The stack names the frame that panicked, in this file.
		logs.wait(t, "an error with the panic and its stack", "level=ERROR", "mt-panic", "maintainer_test.go")
	})
}

// The default seam lays a go-git repository out with a reusable pack, leaves a native-engine one as it is and skips a missing one silently.
func TestMaintainerDefaultRepack(t *testing.T) {
	maintain := func(t *testing.T, keys ...mtKey) {
		t.Helper()
		m := NewMaintainer(MaintainerOptions{Debounce: time.Hour, Concurrency: 1})
		t.Cleanup(m.Close)
		for _, k := range keys {
			m.Notify(k.fs, k.path)
		}
		mtFlush(t, m, "with the default seam")
		mtReturns(t, "Close", m.Close)
	}

	t.Run("GoGit", func(t *testing.T) {
		f := rpMixedRepo(t, memfs.New(), "/repo.git", false)
		rpNeedsRepack(t, f.repo, true, "before maintenance")
		maintain(t, mtKey{f.fs, f.path})
		rp, err := f.repo.ReusablePack(t.Context())
		rpRequireReusable(t, f, rp, err)
		rpNeedsRepack(t, f.repo, false, "after maintenance")
	})

	t.Run("Native", func(t *testing.T) {
		gitPath, err := exec.LookPath("git")
		if err != nil {
			t.Skipf("git binary required: %v", err)
		}
		setGitBinary(t, "")
		native := rpMixedRepo(t, osfs.Default, filepath.Join(t.TempDir(), "repo.git"), false)
		rpNeedsRepack(t, native.repo, true, "under go-git")
		control := rpMixedRepo(t, memfs.New(), "/control.git", false)
		setGitBinary(t, gitPath)
		requireGitMode(t, native.repo, true)
		before := snapshotFiles(t, native.fs, native.path)
		maintain(t, mtKey{native.fs, native.path}, mtKey{control.fs, control.path})
		// The go-git control proves maintenance ran.
		rpReadManifest(t, control)
		if changed := rpChanged(before, snapshotFiles(t, native.fs, native.path)); len(changed) > 0 {
			t.Errorf("maintenance changed %v in a native-engine repository, want it untouched", changed)
		}
	})

	t.Run("MissingIsSilent", func(t *testing.T) {
		logs := mtLogs(t)
		fs := memfs.New()
		control := rpMixedRepo(t, fs, "/control.git", false)
		maintain(t, mtKey{fs, "/missing.git"}, mtKey{fs, control.path})
		rpReadManifest(t, control)
		if lines := append(logs.lines("level=WARN"), logs.lines("level=ERROR")...); len(lines) != 0 {
			t.Errorf("maintenance of a missing repository logged %q, want no warning or error", lines)
		}
	})
}
