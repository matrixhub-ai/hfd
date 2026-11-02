package repository

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"io"
	"slices"
	"strings"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/idxfile"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/protocol"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp/sideband"
	"github.com/go-git/go-git/v6/plumbing/revlist"
	"github.com/go-git/go-git/v6/storage"
)

// uploadPackReuse is one go-git upload-pack decision: answered from the reusable pack, or handed to go-git for Reason.
type uploadPackReuse struct {
	Reused       bool
	Reason       string // a reuse* constant unless Reused
	PackObjects  uint32 // objects sent verbatim from the reusable pack
	FreshObjects int    // objects encoded after them
}

// uploadPackReuseHook, when set, observes every decision.
var uploadPackReuseHook func(uploadPackReuse)

// Why a request is handed to go-git.
const (
	reuseNoPack       = "no-pack"        // never repacked, alternates, or a manifest out of step with the disk
	reuseNegotiation  = "negotiation"    // haves, or no done
	reuseShape        = "shape"          // shallow, deepen, filter, no want, an unknown line or a malformed request
	reuseCapabilities = "capabilities"   // no ofs-delta, or side-band without side-band-64k
	reuseWantMissing  = "want-missing"   // a want the repository lacks
	reuseTipNotWanted = "tip-not-wanted" // the pack holds a tip the wants do not reach
	reuseBudget       = "budget"         // reuseAncestorBudget ran out before every tip was reached
	reusePrepareError = "prepare-error"  // reading the pack, its index or an object failed before the first byte
)

// reuseAncestorBudget caps the commits walked looking for unwanted tips among the ancestors of the wants.
var reuseAncestorBudget = 1000

// reuseIndex is the decoded index of the reusable pack whose trailer is hash.
type reuseIndex struct {
	hash plumbing.Hash
	idx  *idxfile.MemoryIndex
}

// reuseServe is a request the reusable pack answers, prepared up to the first byte.
type reuseServe struct {
	v        protocol.Version
	sideband bool
	st       storage.Storer
	pack     *ReusablePack
	file     billy.File // the pack, read past header
	header   [12]byte
	fresh    []*packfile.ObjectToPack // R, deltas chosen
	window   uint
}

// serveUploadPackReuse answers a full clone with pack A then R, what the wants reach beyond A; served=false leaves w untouched, an error once served means the response broke off.
func (r *Repository) serveUploadPackReuse(ctx context.Context, w io.Writer, body []byte, v protocol.Version) (served bool, err error) {
	s, reason := r.reusePrepare(ctx, body, v)
	d := uploadPackReuse{Reused: s != nil, Reason: reason}
	if s != nil {
		defer func() { _ = s.file.Close() }()
		d.PackObjects, d.FreshObjects = s.pack.Objects, len(s.fresh)
	}
	if hook := uploadPackReuseHook; hook != nil {
		hook(d)
	}
	if s == nil {
		return false, nil
	}
	return true, s.write(w)
}

// reusePrepare does everything that can fail before the first byte; a nil serve comes with the reason.
func (r *Repository) reusePrepare(ctx context.Context, body []byte, v protocol.Version) (*reuseServe, string) {
	parse := reuseParseV0
	if v == protocol.V2 {
		parse = reuseParseV2
	}
	wants, sideband, reason := parse(body)
	if reason != "" {
		return nil, reason
	}
	plumbing.HashesSort(wants)
	if wants = slices.Compact(wants); len(wants) == 0 {
		return nil, reuseShape
	}
	p, err := r.ReusablePack(ctx)
	switch {
	case err != nil:
		return nil, reusePrepareError
	case p == nil:
		return nil, reuseNoPack
	case p.Hash.Size() != sha1.Size:
		// The trailer of a spliced pack is SHA-1.
		return nil, reusePrepareError
	}
	idx, err := r.reuseIndexOf(p)
	if err != nil {
		return nil, reusePrepareError
	}
	st := r.repo.Storer
	var beyond []plumbing.Hash
	for _, h := range wants {
		if ok, _ := idx.Contains(h); ok {
			continue
		}
		if st.HasEncodedObject(h) != nil {
			return nil, reuseWantMissing
		}
		beyond = append(beyond, h)
	}
	if reason = reuseCovered(st, p.Tips, wants); reason != "" {
		return nil, reason
	}
	cfg, err := st.Config()
	if err != nil {
		return nil, reusePrepareError
	}
	s := &reuseServe{v: v, sideband: sideband, st: st, pack: p, window: cfg.Pack.Window}
	if len(beyond) > 0 {
		objs, err := revlist.Objects(st, beyond, p.Tips)
		if err != nil {
			return nil, reusePrepareError
		}
		objs = slices.DeleteFunc(objs, func(h plumbing.Hash) bool {
			ok, _ := idx.Contains(h)
			return ok
		})
		if s.fresh, err = packfile.NewDeltaSelector(st).ObjectsToPack(objs, s.window); err != nil {
			return nil, reusePrepareError
		}
	}
	if s.file, err = p.Open(); err != nil {
		return nil, reusePrepareError
	}
	h := s.header[:]
	if _, err := io.ReadFull(s.file, h); err != nil || string(h[:4]) != "PACK" || binary.BigEndian.Uint32(h[4:8]) != 2 || binary.BigEndian.Uint32(h[8:]) != p.Objects {
		_ = s.file.Close()
		return nil, reusePrepareError
	}
	return s, ""
}

// reuseParseV0 accepts a v0/v1 request with done and ofs-delta but without haves, shallows or deepen.
func reuseParseV0(body []byte) (wants []plumbing.Hash, sideband bool, reason string) {
	rd := bytes.NewReader(body)
	var req packp.UploadRequest
	var haves packp.UploadHaves
	// A filter line fails the decoding here as it does in go-git.
	if req.Decode(rd) != nil || haves.Decode(rd) != nil {
		return nil, false, reuseShape
	}
	caps := &req.Capabilities
	switch {
	case !haves.Done || len(haves.Haves) > 0:
		return nil, false, reuseNegotiation
	case len(req.Shallows) > 0 || !req.Depth.IsZero():
		return nil, false, reuseShape
	case !caps.Supports(capability.OFSDelta), caps.Supports(capability.Sideband) && !caps.Supports(capability.Sideband64k):
		return nil, false, reuseCapabilities
	}
	return req.Wants, caps.Supports(capability.Sideband64k), ""
}

// reuseParseV2 accepts a fetch whose arguments are wants, ofs-delta and lines that change nothing go-git sends a clone.
func reuseParseV2(body []byte) (wants []plumbing.Hash, sideband bool, reason string) {
	s := pktline.NewScanner(bytes.NewReader(body))
	if !s.Scan() || strings.TrimSuffix(string(s.Bytes()), "\n") != "command=fetch" {
		return nil, false, reuseShape
	}
	// Capabilities such as agent and object-format change nothing go-git sends.
	for s.Scan() && s.Len() >= pktline.LenSize {
	}
	if s.Len() != pktline.Delim {
		return nil, false, reuseShape
	}
	ofsDelta := false
	for s.Scan() && s.Len() >= pktline.LenSize {
		switch line := strings.TrimSuffix(string(s.Bytes()), "\n"); {
		case strings.HasPrefix(line, "want "):
			h, ok := packHashParse(strings.TrimPrefix(line, "want "))
			if !ok {
				return nil, false, reuseShape
			}
			wants = append(wants, h)
		case line == "ofs-delta":
			ofsDelta = true
		case line == "done", line == "thin-pack", line == "include-tag", line == "no-progress":
		case strings.HasPrefix(line, "have "):
			return nil, false, reuseNegotiation
		default:
			return nil, false, reuseShape
		}
	}
	if s.Len() != pktline.Flush || s.Scan() || s.Err() != nil {
		return nil, false, reuseShape
	}
	if !ofsDelta {
		return nil, false, reuseCapabilities
	}
	return wants, true, ""
}

// reuseIndexOf returns the index of p, decoding it only when p is not the pack decoded last.
func (r *Repository) reuseIndexOf(p *ReusablePack) (*idxfile.MemoryIndex, error) {
	if c := r.reuseIndex.Load(); c != nil && c.hash == p.Hash {
		return c.idx, nil
	}
	f, err := p.OpenIndex()
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	idx := idxfile.NewMemoryIndex(p.Hash.Size())
	if err := idxfile.NewDecoder(f, sha1.New()).Decode(idx); err != nil {
		return nil, err
	}
	if idx.PackfileChecksum != p.Hash {
		return nil, idxfile.ErrMalformedIdxFile
	}
	r.reuseIndex.Store(&reuseIndex{hash: p.Hash, idx: idx})
	return idx, nil
}

// reuseCovered reports why the wants do not reach every tip, or "": a tip must be wanted, or be a commit an ancestor of the wanted commits.
func reuseCovered(st storage.Storer, tips, wants []plumbing.Hash) string {
	wanted := make(map[plumbing.Hash]bool, len(wants))
	for _, h := range wants {
		wanted[h] = true
	}
	pending := map[plumbing.Hash]bool{}
	for _, tip := range tips {
		if wanted[tip] {
			continue
		}
		o, err := st.EncodedObject(plumbing.AnyObject, tip)
		if err != nil {
			return reusePrepareError
		}
		if o.Type() != plumbing.CommitObject {
			return reuseTipNotWanted
		}
		pending[tip] = true
	}
	if len(pending) == 0 {
		return ""
	}
	var stack []plumbing.Hash
	for _, h := range wants {
		o, err := st.EncodedObject(plumbing.AnyObject, h)
		for err == nil && o.Type() == plumbing.TagObject {
			var tag *object.Tag
			if tag, err = object.DecodeTag(st, o); err == nil {
				o, err = st.EncodedObject(plumbing.AnyObject, tag.Target)
			}
		}
		if err != nil {
			return reusePrepareError
		}
		if o.Type() == plumbing.CommitObject {
			stack = append(stack, o.Hash())
		}
	}
	seen := map[plumbing.Hash]bool{}
	for len(stack) > 0 {
		h := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[h] {
			continue
		}
		seen[h] = true
		delete(pending, h)
		if len(pending) == 0 {
			return ""
		}
		if len(seen) > reuseAncestorBudget {
			return reuseBudget
		}
		c, err := object.GetCommit(st, h)
		if err != nil {
			return reusePrepareError
		}
		stack = append(stack, c.ParentHashes...)
	}
	return reuseTipNotWanted
}

// write sends what go-git sends ahead of a pack, the pack, and the flush ending a side-band stream.
func (s *reuseServe) write(w io.Writer) error {
	var err error
	if s.v == protocol.V2 {
		err = (&packp.FetchOutput{Packfile: true}).Encode(w)
	} else {
		err = (&packp.ServerResponse{}).Encode(w)
	}
	if err != nil {
		return err
	}
	f := &reuseFramer{w: w, raw: !s.sideband}
	if err := s.writePack(f); err != nil {
		return err
	}
	if err := f.flush(); err != nil {
		return err
	}
	if f.raw {
		return nil
	}
	return pktline.WriteFlush(w)
}

// writePack copies A whole when R is empty; otherwise A's header counting R too, A's entries, R's entries and a trailer over all of them.
func (s *reuseServe) writePack(f *reuseFramer) error {
	hdr := s.header
	n := s.pack.Size - int64(len(hdr))
	if len(s.fresh) > 0 {
		binary.BigEndian.PutUint32(hdr[8:], s.pack.Objects+uint32(len(s.fresh)))
		n -= sha1.Size
		f.sum = sha1.New()
	}
	if _, err := f.Write(hdr[:]); err != nil {
		return err
	}
	if k, err := f.ReadFrom(io.LimitReader(s.file, n)); err != nil || k != n {
		return cmp.Or(err, io.ErrUnexpectedEOF)
	}
	if f.sum == nil {
		return nil
	}
	// R's deltas are offsets within R, and A's entries keep their offsets after the same 12-byte header.
	enc := packfile.NewEncoder(&reuseTrim{w: f, skip: len(hdr)}, s.st, false, packfile.WithObjectSelector(reuseSelected(s.fresh)))
	if _, err := enc.Encode(nil, s.window); err != nil {
		return err
	}
	sum := f.sum.Sum(nil)
	f.sum = nil
	_, err := f.Write(sum)
	return err
}

// reuseSelected hands the encoder the objects whose deltas were chosen before the first byte.
type reuseSelected []*packfile.ObjectToPack

func (s reuseSelected) ObjectsToPack([]plumbing.Hash, uint) ([]*packfile.ObjectToPack, error) {
	return s, nil
}

// reuseTrim passes on an encoded pack without its header and trailer, leaving its entries.
type reuseTrim struct {
	w    io.Writer
	skip int             // header bytes still to drop
	tail [sha1.Size]byte // the latest bytes, withheld as they may end the pack
	n    int
}

func (t *reuseTrim) Write(p []byte) (int, error) {
	total := len(p)
	k := min(t.skip, len(p))
	t.skip -= k
	p = p[k:]
	if len(p) >= len(t.tail) {
		if _, err := t.w.Write(t.tail[:t.n]); err != nil {
			return 0, err
		}
		if _, err := t.w.Write(p[:len(p)-len(t.tail)]); err != nil {
			return 0, err
		}
		t.n = copy(t.tail[:], p[len(p)-len(t.tail):])
		return total, nil
	}
	if over := t.n + len(p) - len(t.tail); over > 0 {
		if _, err := t.w.Write(t.tail[:over]); err != nil {
			return 0, err
		}
		t.n = copy(t.tail[:], t.tail[over:t.n])
	}
	t.n += copy(t.tail[t.n:], p)
	return total, nil
}

// reuseFrameHead is the pkt-line length and band byte opening a frame.
const reuseFrameHead = pktline.LenSize + 1

// reuseFramer sends bytes in side-band-64k pack-data frames, or unframed when raw, through one buffer; sum, when set, sees every byte.
type reuseFramer struct {
	w   io.Writer
	raw bool
	sum hash.Hash
	n   int // payload bytes buffered after the frame head
	buf [pktline.MaxSize]byte
}

func (f *reuseFramer) Write(p []byte) (int, error) {
	for done := 0; done < len(p); {
		k := copy(f.buf[reuseFrameHead+f.n:], p[done:])
		done += k
		if err := f.add(k); err != nil {
			return done, err
		}
	}
	return len(p), nil
}

// ReadFrom reads straight into the frame buffer, so copying A takes no second buffer.
func (f *reuseFramer) ReadFrom(src io.Reader) (int64, error) {
	var total int64
	for {
		k, err := src.Read(f.buf[reuseFrameHead+f.n:])
		total += int64(k)
		if ferr := f.add(k); ferr != nil {
			return total, ferr
		}
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
}

// add takes in the k bytes just placed after the buffered payload, sending the frame once full.
func (f *reuseFramer) add(k int) error {
	if f.sum != nil {
		f.sum.Write(f.buf[reuseFrameHead+f.n : reuseFrameHead+f.n+k])
	}
	f.n += k
	if reuseFrameHead+f.n < len(f.buf) {
		return nil
	}
	return f.flush()
}

// flush sends the buffered payload.
func (f *reuseFramer) flush() error {
	if f.n == 0 {
		return nil
	}
	frame := f.buf[:reuseFrameHead+f.n]
	f.n = 0
	if f.raw {
		frame = frame[reuseFrameHead:]
	} else {
		hex.Encode(frame[:pktline.LenSize], []byte{byte(len(frame) >> 8), byte(len(frame))})
		frame[pktline.LenSize] = byte(sideband.PackData)
	}
	_, err := f.w.Write(frame)
	return err
}
