package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
)

var errNotOurRef = errors.New("not our ref")

// wantAllowed admits the commits and tag objects the repository's refs reach, resolved once on first use.
func (r *Repository) wantAllowed(ctx context.Context) func(plumbing.Hash) (bool, error) {
	var (
		once sync.Once
		set  *reachSet
		err  error
	)
	return func(h plumbing.Hash) (bool, error) {
		once.Do(func() { set, err = r.reachable(ctx) })
		if err != nil {
			return false, err
		}
		return set.contains(h), nil
	}
}

// newWantFilter forwards input pkt-line by pkt-line and refuses the request at the first want allowed rejects.
func newWantFilter(input io.Reader, output io.Writer, allowed func(plumbing.Hash) (bool, error)) io.Reader {
	return &wantFilter{input: input, output: output, allowed: allowed}
}

type wantFilter struct {
	input   io.Reader
	output  io.Writer
	allowed func(plumbing.Hash) (bool, error)
	line    [pktline.MaxSize]byte
	pending []byte // the unread rest of line
	err     error  // sticky
}

func (f *wantFilter) Read(p []byte) (int, error) {
	if len(f.pending) == 0 && f.err == nil {
		f.err = f.next()
	}
	if len(f.pending) == 0 {
		return 0, f.err
	}
	n := copy(p, f.pending)
	f.pending = f.pending[n:]
	return n, nil
}

// next queues the next pkt-line verbatim unless it is a refused want.
func (f *wantFilter) next() error {
	n, err := pktline.Read(f.input, f.line[:])
	if err != nil {
		return err
	}
	// flush-pkt, delim-pkt and response-end-pkt are a bare length.
	n = max(n, pktline.LenSize)
	if err := f.check(f.line[pktline.LenSize:n]); err != nil {
		return err
	}
	f.pending = f.line[:n]
	return nil
}

func (f *wantFilter) check(payload []byte) error {
	// go-git's v2 fetch decoder trims a line before it matches "want ".
	oid, ok := bytes.CutPrefix(bytes.TrimSpace(payload), []byte("want "))
	if !ok {
		return nil
	}
	if i := bytes.IndexAny(oid, " \n"); i >= 0 {
		oid = oid[:i]
	}
	if s := string(oid); plumbing.IsHash(s) {
		h, _ := plumbing.FromHex(s)
		ours, err := f.allowed(h)
		if err != nil || ours {
			return err
		}
	}
	_, _ = pktline.WriteError(f.output, fmt.Errorf("upload-pack: not our ref %s", oid))
	return fmt.Errorf("%w: %s", errNotOurRef, oid)
}
