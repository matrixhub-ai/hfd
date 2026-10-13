// Package seekable adapts reopenable forward streams of known size to io.ReadSeekCloser.
package seekable

import (
	"errors"
	"io"
	"io/fs"
	"sync"
)

var _ io.ReadSeekCloser = (*Reader)(nil)

// Reader is an io.ReadSeekCloser over the first size bytes of a reopenable stream; only Close may run concurrently with Read or Seek.
type Reader struct {
	mu     sync.Mutex
	size   int64
	pos    int64
	cur    int64
	rc     io.ReadCloser
	open   func() (io.ReadCloser, error)
	closed bool
}

// New opens the stream once and returns a Reader over its first size bytes.
func New(size int64, open func() (io.ReadCloser, error)) (*Reader, error) {
	if size < 0 {
		return nil, errors.New("seekable: negative size")
	}
	rc, err := open()
	if err != nil {
		return nil, err
	}
	return &Reader{size: size, rc: rc, open: open}, nil
}

// Read reads at the current position, first moving the stream there by seeking, discarding or reopening it.
func (r *Reader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, fs.ErrClosed
	}
	if r.pos >= r.size {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.align(); err != nil {
		return 0, err
	}
	if rest := r.size - r.pos; int64(len(p)) > rest {
		p = p[:rest]
	}
	n, err := r.rc.Read(p)
	r.pos += int64(n)
	r.cur += int64(n)
	if err == io.EOF {
		err = nil
		if r.pos < r.size {
			err = io.ErrUnexpectedEOF
		}
	}
	return n, err
}

// Seek sets the position of the next Read without touching the stream.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, fs.ErrClosed
	}
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += r.pos
	case io.SeekEnd:
		offset += r.size
	default:
		return 0, errors.New("seekable: invalid whence")
	}
	if offset < 0 {
		return 0, errors.New("seekable: negative position")
	}
	r.pos = offset
	return offset, nil
}

// Close closes the current stream; later calls return nil.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	if r.rc == nil {
		return nil
	}
	return r.rc.Close()
}

func (r *Reader) align() error {
	if r.rc != nil {
		if r.cur == r.pos {
			return nil
		}
		if s, ok := r.rc.(io.Seeker); ok {
			var err error
			r.cur, err = s.Seek(r.pos, io.SeekStart)
			return err
		}
		if r.pos > r.cur {
			return r.discard(r.pos - r.cur)
		}
		_ = r.rc.Close()
		r.rc = nil
	}
	rc, err := r.open()
	if err != nil {
		return err
	}
	r.rc, r.cur = rc, 0
	return r.discard(r.pos)
}

func (r *Reader) discard(n int64) error {
	m, err := io.CopyN(io.Discard, r.rc, n)
	r.cur += m
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
