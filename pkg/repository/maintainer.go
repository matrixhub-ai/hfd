package repository

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/go-git/go-billy/v6"
)

// maintRetry paces Flush's polling, and busy retries when there is no debounce window.
const maintRetry = 10 * time.Millisecond

// MaintainerOptions configures NewMaintainer.
type MaintainerOptions struct {
	Debounce    time.Duration // quiet period after a repository's latest Notify; ≤ 0 maintains at once
	Concurrency int           // repositories maintained at the same time; ≤ 0 means 1
}

// Maintainer brings go-git repositories back to the reusable-pack layout in the background once their notifications settle.
type Maintainer struct {
	debounce time.Duration
	slots    chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	runs     sync.WaitGroup

	repack    func(ctx context.Context, fs billy.Filesystem, path string) (busy bool, err error)
	afterFunc func(time.Duration, func()) maintenanceTimer

	mu     sync.Mutex
	keys   map[cacheKey]*maintState
	closed bool
}

// maintenanceTimer is the part of *time.Timer a Maintainer uses.
type maintenanceTimer interface {
	Stop() bool
	Reset(time.Duration) bool
}

// maintState is a scheduled repository: armed while timer is set, otherwise running; dirty once notified during the run.
type maintState struct {
	timer   maintenanceTimer
	seq     uint64 // arming count, so a stale timer callback is ignored
	running bool
	dirty   bool
}

// NewMaintainer returns a Maintainer whose background work runs until Close.
func NewMaintainer(o MaintainerOptions) *Maintainer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Maintainer{
		debounce:  o.Debounce,
		slots:     make(chan struct{}, max(o.Concurrency, 1)),
		ctx:       ctx,
		cancel:    cancel,
		repack:    maintRepack,
		afterFunc: func(d time.Duration, f func()) maintenanceTimer { return time.AfterFunc(d, f) },
		keys:      map[cacheKey]*maintState{},
	}
}

// maintRepack opens the repository and repacks it when it needs it, reporting busy while it is served, repacked or collected.
func maintRepack(ctx context.Context, fs billy.Filesystem, repoPath string) (bool, error) {
	repo, err := Open(fs, repoPath)
	if err != nil {
		return false, err
	}
	return repo.maintain(ctx)
}

// Notify schedules the repository at repoPath on fs for maintenance one debounce window after its latest notification.
func (m *Maintainer) Notify(fs billy.Filesystem, repoPath string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	key := cacheKey{fs, repoPath}
	s, ok := m.keys[key]
	switch {
	case !ok:
		s = &maintState{}
		m.keys[key] = s
		m.armLocked(key, s, m.debounce)
	case s.running:
		s.dirty = true
	default:
		// Reset cannot recall a callback already fired and waiting on mu; re-arming makes it stale.
		s.timer.Stop()
		m.armLocked(key, s, m.debounce)
	}
}

// Flush maintains every scheduled repository now and waits until none is pending or running, re-runs included.
func (m *Maintainer) Flush(ctx context.Context) error {
	if m == nil {
		return nil
	}
	poll := time.NewTicker(maintRetry)
	defer poll.Stop()
	for {
		m.mu.Lock()
		for key, s := range m.keys {
			if s.timer != nil {
				s.timer.Stop()
				m.startLocked(key, s)
			}
		}
		idle := len(m.keys) == 0
		m.mu.Unlock()
		if idle {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
		}
	}
}

// Close drops scheduled repositories, cancels running ones and waits for them to return; later Notify calls do nothing.
func (m *Maintainer) Close() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.closed = true
	for key, s := range m.keys {
		if s.timer != nil {
			s.timer.Stop()
			s.timer = nil
			delete(m.keys, key)
		}
	}
	m.mu.Unlock()
	m.cancel()
	m.runs.Wait()
}

func (m *Maintainer) armLocked(key cacheKey, s *maintState, d time.Duration) {
	s.seq++
	seq := s.seq
	s.timer = m.afterFunc(d, func() { m.fire(key, s, seq) })
}

func (m *Maintainer) fire(key cacheKey, s *maintState, seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s.timer != nil && s.seq == seq {
		m.startLocked(key, s)
	}
}

func (m *Maintainer) startLocked(key cacheKey, s *maintState) {
	s.timer = nil
	s.running = true
	m.runs.Go(func() {
		busy := m.run(key)
		m.finish(key, s, busy)
	})
}

// run maintains key once a slot frees up, reporting whether the repository was busy.
func (m *Maintainer) run(key cacheKey) (busy bool) {
	select {
	case m.slots <- struct{}{}:
	case <-m.ctx.Done():
		return false
	}
	defer func() { <-m.slots }()
	defer func() {
		if p := recover(); p != nil {
			slog.Error("Repository maintenance panicked", "repo", key.path, "panic", p, "stack", string(debug.Stack()))
		}
	}()
	start := time.Now()
	busy, err := m.repack(m.ctx, key.fs, key.path)
	switch {
	case busy:
	case errors.Is(err, ErrRepositoryNotExists):
	case err != nil:
		slog.Warn("Repository maintenance failed", "repo", key.path, "error", err)
	default:
		slog.Info("Repository maintenance finished", "repo", key.path, "duration", time.Since(start))
	}
	return busy
}

// finish re-arms key after a busy or dirty run and forgets it otherwise.
func (m *Maintainer) finish(key cacheKey, s *maintState, busy bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s.running = false
	switch {
	case m.closed:
		delete(m.keys, key)
	case busy:
		s.dirty = false
		d := m.debounce
		if d <= 0 {
			d = maintRetry
		}
		m.armLocked(key, s, d)
	case s.dirty:
		s.dirty = false
		m.armLocked(key, s, m.debounce)
	default:
		delete(m.keys, key)
	}
}
