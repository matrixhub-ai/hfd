package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/revlist"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage"
)

// packManifestPath is where, under a repository, a repack records the layout it published.
const packManifestPath = "objects/info/hfd-packs"

// ReusablePack is the pack closing over Tips, servable byte for byte to a full clone.
type ReusablePack struct {
	Hash    plumbing.Hash   // pack trailer, naming objects/pack/pack-<Hash>.pack
	Size    int64           // .pack bytes, header and trailer included
	Objects uint32          // object count of the pack header
	Tips    []plumbing.Hash // targets of branches, tags and HEAD the pack closes over, ascending

	fs   billy.Filesystem
	base string // objects/pack/pack-<Hash> under the repository
}

// Open opens the pack read-only.
func (p *ReusablePack) Open() (billy.File, error) {
	return p.fs.Open(p.base + ".pack")
}

// OpenIndex opens the pack's index read-only.
func (p *ReusablePack) OpenIndex() (billy.File, error) {
	return p.fs.Open(p.base + ".idx")
}

// ReusablePack returns the pack the manifest names as reusable, or nil when r borrows objects or the manifest is missing, malformed or out of step with the packs on disk.
func (r *Repository) ReusablePack(ctx context.Context) (*ReusablePack, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ok, err := r.packSelfContained(); err != nil || !ok {
		return nil, err
	}
	m, err := r.manifestLoad()
	if err != nil || m == nil || m.reusable == nil {
		return nil, err
	}
	return &ReusablePack{
		Hash:    m.reusable.hash,
		Size:    m.reusable.size,
		Objects: m.reusable.objects,
		Tips:    m.tips,
		fs:      r.fs,
		base:    packFileBase(r.repoPath, m.reusable.hash),
	}, nil
}

// NeedsRepack reports whether Repack would change the layout: a hash ref exists and the manifest is invalid or its refs have moved.
func (r *Repository) NeedsRepack(ctx context.Context) (bool, error) {
	r.rlock()
	defer r.runlock()
	return r.needsRepackLocked(ctx)
}

// needsRepackLocked is NeedsRepack for a caller holding serveMu.
func (r *Repository) needsRepackLocked(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	// An in-place ref rewrite moves no stamp the snapshot checks.
	r.invalidateRefs()
	if ok, err := r.packScoped(); err != nil || !ok {
		return false, err
	}
	t, err := packTargetsOf(r.repo.Storer)
	if err != nil || !t.hashRef {
		return false, err
	}
	m, err := r.manifestLoad()
	if err != nil {
		return false, err
	}
	return m == nil || !slices.Equal(m.tips, t.tips) || m.others != t.othersDigest(), nil
}

// Repack lays r out as the reusable pack plus the rest; unlike GC, a zero cutoff keeps all garbage.
func (r *Repository) Repack(ctx context.Context, cutoff time.Time) error {
	r.lock()
	defer r.unlock()
	return r.repackLocked(ctx, cutoff)
}

// repackLocked is Repack for a caller holding serveMu exclusively.
func (r *Repository) repackLocked(ctx context.Context, cutoff time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// An in-place ref rewrite moves no stamp the snapshot checks.
	r.invalidateRefs()
	if ok, err := r.packScoped(); err != nil || !ok {
		return err
	}
	err := r.repack(ctx, func(modTime time.Time) bool { return !cutoff.IsZero() && modTime.Before(cutoff) })
	err = errors.Join(err, r.reindex())
	r.objectCache.Clear()
	return err
}

// maintain repacks r when it needs it, reporting busy instead of waiting while r is served by go-git upload-pack, repacked or collected.
func (r *Repository) maintain(ctx context.Context) (busy bool, err error) {
	if !r.tryLock() {
		return true, nil
	}
	defer r.unlock()
	needs, err := r.needsRepackLocked(ctx)
	if err != nil || !needs {
		return false, err
	}
	return false, r.repackLocked(ctx, time.Time{})
}

// packScoped reports whether the layout applies to r: go-git serves it and it borrows no objects.
func (r *Repository) packScoped() (bool, error) {
	if r.gitDir() != "" {
		return false, nil
	}
	return r.packSelfContained()
}

// packSelfContained reports whether r borrows no objects through objects/info/alternates.
func (r *Repository) packSelfContained() (bool, error) {
	_, err := r.fs.Stat(filepath.Join(r.repoPath, "objects", "info", "alternates"))
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	return false, err
}

// packStorer is what a repack asks of the repository's storer.
type packStorer interface {
	storage.Storer
	storer.LooseObjectStorer
	storer.PackedObjectStorer
	storer.PackfileWriter
}

// repack converges r to the layout its refs call for; expired tells which packs holding unreferenced objects may go.
func (r *Repository) repack(ctx context.Context, expired func(modTime time.Time) bool) error {
	st, ok := r.repo.Storer.(packStorer)
	if !ok {
		return git.ErrPackedObjectsNotSupported
	}
	snap, err := r.repackSnapshot(ctx, st)
	if err != nil {
		return err
	}
	return r.repackFrom(ctx, st, snap, expired)
}

// packSnapshot is the repository as a repack found it; only the loose objects and packs listed here may be deleted.
type packSnapshot struct {
	loose    []plumbing.Hash
	packs    []packInfo
	manifest *packManifest
	targets  packTargets
}

// repackSnapshot lists st's loose objects, r's packs and manifest, then resolves the refs.
func (r *Repository) repackSnapshot(ctx context.Context, st packStorer) (*packSnapshot, error) {
	snap := &packSnapshot{}
	err := st.ForEachObjectHash(func(h plumbing.Hash) error {
		snap.loose = append(snap.loose, h)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if snap.packs, err = packInfos(ctx, r.fs, r.repoPath); err != nil {
		return nil, err
	}
	if snap.manifest, err = r.manifestLoad(); err != nil {
		return nil, err
	}
	if snap.targets, err = packTargetsOf(st); err != nil {
		return nil, err
	}
	return snap, nil
}

// repackFrom publishes A, B and the manifest before deleting, so an interruption loses no object.
func (r *Repository) repackFrom(ctx context.Context, st packStorer, snap *packSnapshot, expired func(modTime time.Time) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a, b, in, err := packSets(st, snap.targets)
	if err != nil {
		return err
	}
	cfg, err := st.Config()
	if err != nil {
		return err
	}
	old := snap.manifest
	if old == nil {
		old = &packManifest{}
	}
	pack := func(prev *packRecord, objs []plumbing.Hash, inA bool) (*packRecord, error) {
		if len(objs) == 0 {
			return nil, nil
		}
		if prev != nil {
			i := slices.IndexFunc(snap.packs, func(p packInfo) bool { return p.hash == prev.hash })
			if i >= 0 && packSame(snap.packs[i], in, inA, len(objs)) {
				return &packRecord{hash: prev.hash, size: prev.size, objects: uint32(len(objs))}, nil
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return r.packWrite(st, objs, cfg.Pack.Window)
	}
	m := &packManifest{others: snap.targets.othersDigest(), tips: snap.targets.tips}
	if m.reusable, err = pack(old.reusable, a, true); err != nil {
		return err
	}
	if m.rest, err = pack(old.rest, b, false); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if data := m.encode(); snap.manifest == nil || !bytes.Equal(data, snap.manifest.encode()) {
		if err := r.manifestStore(data); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, h := range snap.loose {
		if _, ok := in[h]; ok {
			if err := st.DeleteLooseObject(h); err != nil {
				return err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, p := range snap.packs {
		if m.names(p.hash) {
			continue
		}
		uncovered := slices.ContainsFunc(p.objects, func(h plumbing.Hash) bool { _, ok := in[h]; return !ok })
		if uncovered && !expired(p.mtime) {
			continue
		}
		if err := st.DeleteOldObjectPackAndIndex(p.hash, time.Time{}); err != nil {
			return err
		}
	}
	return nil
}

// packSame reports whether p's index lists exactly the n keys of in that map to inA.
func packSame(p packInfo, in map[plumbing.Hash]bool, inA bool, n int) bool {
	return len(p.objects) == n && !slices.ContainsFunc(p.objects, func(h plumbing.Hash) bool {
		v, ok := in[h]
		return !ok || v != inA
	})
}

// packWrite encodes objs into a new pack through st, which publishes it when the writer closes.
func (r *Repository) packWrite(st packStorer, objs []plumbing.Hash, window uint) (*packRecord, error) {
	wc, err := st.PackfileWriter()
	if err != nil {
		return nil, err
	}
	h, err := packfile.NewEncoder(wc, st, false).Encode(objs, window)
	if cerr := wc.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, err
	}
	info, err := r.fs.Stat(packFileBase(r.repoPath, h) + ".pack")
	if err != nil {
		return nil, err
	}
	return &packRecord{hash: h, size: info.Size(), objects: uint32(len(objs))}, nil
}

func packFileBase(repoPath string, h plumbing.Hash) string {
	return filepath.Join(repoPath, "objects", "pack", "pack-"+h.String())
}

// packTargets is what a repository's refs ask of the layout.
type packTargets struct {
	hashRef bool            // some ref is a hash ref
	tips    []plumbing.Hash // T: targets of branches, tags and HEAD, unique and ascending
	others  []plumbing.Hash // O: targets of every other ref, ascending
}

// packTargetsOf resolves st's refs; a symbolic ref whose target is missing names nothing.
func packTargetsOf(st storer.ReferenceStorer) (packTargets, error) {
	var t packTargets
	iter, err := st.IterReferences()
	if err != nil {
		return t, err
	}
	defer iter.Close()
	err = iter.ForEach(func(ref *plumbing.Reference) error {
		name := ref.Name()
		switch ref.Type() {
		case plumbing.HashReference:
			t.hashRef = true
		case plumbing.SymbolicReference:
			resolved, err := storer.ResolveReference(st, ref.Target())
			if errors.Is(err, plumbing.ErrReferenceNotFound) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("resolving reference %q: %w", name, err)
			}
			ref = resolved
		default:
			return nil
		}
		if name == plumbing.HEAD || name.IsBranch() || name.IsTag() {
			t.tips = append(t.tips, ref.Hash())
		} else {
			t.others = append(t.others, ref.Hash())
		}
		return nil
	})
	if err != nil {
		return packTargets{}, err
	}
	plumbing.HashesSort(t.tips)
	t.tips = slices.Compact(t.tips)
	plumbing.HashesSort(t.others)
	return t, nil
}

func (t packTargets) othersDigest() [sha256.Size]byte {
	h := sha256.New()
	for _, o := range t.others {
		h.Write(o.Bytes())
	}
	return [sha256.Size]byte(h.Sum(nil))
}

// packSets lists A, all T reaches, and B, what O adds beyond A; in maps each object of either to whether A holds it.
func packSets(st storer.EncodedObjectStorer, t packTargets) (a, b []plumbing.Hash, in map[plumbing.Hash]bool, err error) {
	if len(t.tips) > 0 {
		if a, err = revlist.Objects(st, t.tips, nil); err != nil {
			return nil, nil, nil, err
		}
	}
	in = make(map[plumbing.Hash]bool, len(a))
	for _, h := range a {
		in[h] = true
	}
	if len(t.others) > 0 {
		rest, err := revlist.Objects(st, t.others, t.tips)
		if err != nil {
			return nil, nil, nil, err
		}
		for _, h := range rest {
			if _, ok := in[h]; !ok {
				in[h] = false
				b = append(b, h)
			}
		}
	}
	return a, b, in, nil
}

// packManifest is a parsed objects/info/hfd-packs.
type packManifest struct {
	reusable *packRecord // A, absent when T is empty
	rest     *packRecord // B, absent when empty
	others   [sha256.Size]byte
	tips     []plumbing.Hash
}

// packRecord names one pack of the layout.
type packRecord struct {
	hash    plumbing.Hash
	size    int64
	objects uint32 // recorded for A only
}

// encode renders m in the one form packManifestParse accepts.
func (m *packManifest) encode() []byte {
	b := []byte("hfd-packs 1\n")
	if p := m.reusable; p != nil {
		b = fmt.Appendf(b, "reusable %s %d %d\n", p.hash, p.size, p.objects)
	}
	if p := m.rest; p != nil {
		b = fmt.Appendf(b, "rest %s %d\n", p.hash, p.size)
	}
	b = fmt.Appendf(b, "others %x\n", m.others[:])
	for _, h := range m.tips {
		b = fmt.Appendf(b, "tip %s\n", h)
	}
	return b
}

func (m *packManifest) records() []*packRecord {
	var ps []*packRecord
	for _, p := range []*packRecord{m.reusable, m.rest} {
		if p != nil {
			ps = append(ps, p)
		}
	}
	return ps
}

func (m *packManifest) names(h plumbing.Hash) bool {
	return slices.ContainsFunc(m.records(), func(p *packRecord) bool { return p.hash == h })
}

// packManifestParse accepts exactly what encode writes, tips strictly ascending; anything else is nil.
func packManifestParse(data []byte) *packManifest {
	m := &packManifest{}
	text, _ := strings.CutSuffix(string(data), "\n")
	for _, line := range strings.Split(text, "\n")[1:] {
		f := strings.Split(line, " ")
		var ok bool
		switch {
		case f[0] == "reusable" && len(f) == 4:
			m.reusable, ok = packRecordParse(f[1], f[2], f[3])
		case f[0] == "rest" && len(f) == 3:
			m.rest, ok = packRecordParse(f[1], f[2], "0")
		case f[0] == "others" && len(f) == 2 && len(f[1]) == hex.EncodedLen(sha256.Size):
			_, err := hex.Decode(m.others[:], []byte(f[1]))
			ok = err == nil
		case f[0] == "tip" && len(f) == 2:
			var h plumbing.Hash
			h, ok = packHashParse(f[1])
			ok = ok && (len(m.tips) == 0 || m.tips[len(m.tips)-1].Compare(h.Bytes()) < 0)
			m.tips = append(m.tips, h)
		}
		if !ok {
			return nil
		}
	}
	if !bytes.Equal(m.encode(), data) {
		return nil
	}
	return m
}

func packRecordParse(hash, size, objects string) (*packRecord, bool) {
	h, ok := packHashParse(hash)
	n, err := strconv.ParseInt(size, 10, 64)
	c, cerr := strconv.ParseUint(objects, 10, 32)
	return &packRecord{hash: h, size: n, objects: uint32(c)}, ok && err == nil && cerr == nil
}

func packHashParse(s string) (plumbing.Hash, bool) {
	h, ok := plumbing.FromHex(s)
	return h, ok && len(s) == h.HexSize()
}

// manifestLoad reads r's manifest; a garbled one, or one out of step with the packs, is none.
func (r *Repository) manifestLoad() (*packManifest, error) {
	data, err := util.ReadFile(r.fs, filepath.Join(r.repoPath, packManifestPath))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	m := packManifestParse(data)
	if m == nil {
		return nil, nil
	}
	for _, p := range m.records() {
		base := packFileBase(r.repoPath, p.hash)
		info, err := r.fs.Stat(base + ".pack")
		if err == nil && info.Size() != p.size {
			return nil, nil
		}
		if err == nil {
			_, err = r.fs.Stat(base + ".idx")
		}
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}

// manifestStore publishes data as r's manifest through a temporary file, closed before the rename as S3 requires.
func (r *Repository) manifestStore(data []byte) error {
	f, err := util.TempFile(r.fs, filepath.Join(r.repoPath, "objects", "info"), "tmp_hfd-packs-")
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = r.fs.Rename(f.Name(), filepath.Join(r.repoPath, packManifestPath))
	}
	if err != nil {
		_ = r.fs.Remove(f.Name())
	}
	return err
}
