package e2e_test

import (
	"context"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type Conn = net.Conn

type Wrapper interface {
	// Wrap wraps the given connection and returns a new connection that applies the chaos effects.
	Wrap(conn Conn) Conn
}

// Bandwidth
type Bandwidth struct {
	// Rate  is the bandwidth rate in bytes per second.
	Rate int64
}

func (b *Bandwidth) Wrap(conn Conn) Conn {
	return &bandwidthConn{
		Conn: conn,
		Rate: b.Rate,
	}
}

type bandwidthConn struct {
	net.Conn
	Rate int64

	initReadLimiterOnce  sync.Once
	initWriteLimiterOnce sync.Once
	readLimiter          *rate.Limiter
	writeLimiter         *rate.Limiter
}

func (c *bandwidthConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.wait(c.getReadLimiter(), n)
	return n, err
}

func (c *bandwidthConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.wait(c.getWriteLimiter(), n)
	return n, err
}

func (c *bandwidthConn) getReadLimiter() *rate.Limiter {
	c.initReadLimiterOnce.Do(func() {
		c.readLimiter = c.newLimiter()
	})
	return c.readLimiter
}

func (c *bandwidthConn) getWriteLimiter() *rate.Limiter {
	c.initWriteLimiterOnce.Do(func() {
		c.writeLimiter = c.newLimiter()
	})
	return c.writeLimiter
}

func (c *bandwidthConn) newLimiter() *rate.Limiter {
	if c.Rate <= 0 {
		return nil
	}
	// Use burst=1 so writes/reads are shaped in per-byte cadence instead of
	// allowing an initial large burst.
	return rate.NewLimiter(rate.Limit(c.Rate), 1)
}

func (c *bandwidthConn) wait(limiter *rate.Limiter, n int) {
	if limiter == nil || n <= 0 {
		return
	}
	ctx := context.Background()
	for i := 0; i < n; i++ {
		if err := limiter.WaitN(ctx, 1); err != nil {
			return
		}
	}
}

// Latency
type Latency struct {
	// Delay  is the latency delay in seconds.
	Delay  int64
	Jitter int64
}

func (l *Latency) Wrap(conn Conn) Conn {
	return &latencyConn{
		Conn:   conn,
		delay:  time.Duration(l.Delay) * time.Second,
		jitter: time.Duration(l.Jitter) * time.Second,
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

type latencyConn struct {
	net.Conn

	mu     sync.Mutex
	delay  time.Duration
	jitter time.Duration
	rand   *rand.Rand
}

func (c *latencyConn) Read(p []byte) (int, error) {
	c.wait()
	return c.Conn.Read(p)
}

func (c *latencyConn) Write(p []byte) (int, error) {
	c.wait()
	return c.Conn.Write(p)
}

func (c *latencyConn) wait() {
	if c.delay <= 0 && c.jitter <= 0 {
		return
	}

	wait := c.delay
	if c.jitter > 0 {
		c.mu.Lock()
		jitter := c.rand.Int63n(int64(c.jitter)*2+1) - int64(c.jitter)
		c.mu.Unlock()
		wait += time.Duration(jitter)
	}
	if wait > 0 {
		time.Sleep(wait)
	}
}

// Limit
type Limit struct {
	// Limit is the maximum amount of data in bytes that can be transmitted.
	Limit int64
}

func (l *Limit) Wrap(conn Conn) Conn {
	return &limitConn{
		Conn:      conn,
		remaining: l.Limit,
	}
}

type limitConn struct {
	net.Conn

	mu        sync.Mutex
	remaining int64
}

func (c *limitConn) Read(p []byte) (int, error) {
	buf, exhausted := c.readBuffer(p)
	if exhausted {
		return 0, io.EOF
	}

	n, err := c.Conn.Read(buf)
	c.consume(int64(n))
	return n, err
}

func (c *limitConn) Write(p []byte) (int, error) {
	buf, exhausted := c.readBuffer(p)
	if exhausted {
		return 0, io.ErrShortWrite
	}

	n, err := c.Conn.Write(buf)
	remaining := c.consume(int64(n))
	if err == nil && n < len(p) && remaining <= 0 {
		return n, io.ErrShortWrite
	}
	return n, err
}

func (c *limitConn) readBuffer(p []byte) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.remaining <= 0 {
		return nil, true
	}
	if int64(len(p)) > c.remaining {
		return p[:c.remaining], false
	}
	return p, false
}

func (c *limitConn) consume(n int64) int64 {
	if n <= 0 {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.remaining
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.remaining -= n
	if c.remaining < 0 {
		c.remaining = 0
	}
	return c.remaining
}
