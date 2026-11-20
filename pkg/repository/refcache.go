package repository

import (
	"errors"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/filesystem"
)

const (
	// refSnapshotMaxAge bounds how long a write no stamp records, such as another instance's, stays invisible.
	refSnapshotMaxAge = time.Minute
	// refSettleWindow covers timestamp granularity: a write this close to the newest stamp may not move it.
	refSettleWindow = 20 * time.Millisecond
)

const (
	refHeadFile   = "HEAD"
	refPackedFile = "packed-refs"
	refRootDir    = "refs"
)

var (
	_ interface{ Reindex() error } = (*refCache)(nil)
	_ storer.PackedObjectStorer    = (*refCache)(nil)
	_ storer.LooseObjectStorer     = (*refCache)(nil)
	_ storer.PackfileWriter        = (*refCache)(nil)
)

// refCache serves ref listings and lookups from one snapshot, dropped by every ref write through it.
type refCache struct {
	*filesystem.Storage
	now      func() time.Time
	mu       sync.Mutex
	gen      atomic.Uint64
	rebuilds atomic.Uint64
	snap     atomic.Pointer[refSnapshot]
}

// refSnapshot is one inner listing; its stamps were taken before the listing was read.
type refSnapshot struct {
	refs    []*plumbing.Reference
	byName  map[plumbing.ReferenceName]*plumbing.Reference
	stamps  refStamps
	builtAt time.Time
	settled bool
	gen     uint64
}

type refStamps struct {
	head, packed fileStamp
	dirs         []dirStamp // sorted by path
}

type fileStamp struct {
	exists bool
	size   int64
	mtime  time.Time
}

type dirStamp struct {
	path  string
	mtime time.Time
}

func newRefCache(st *filesystem.Storage) *refCache {
	return &refCache{Storage: st, now: time.Now}
}

func (c *refCache) IterReferences() (storer.ReferenceIter, error) {
	s := c.snap.Load()
	if !c.refTrusted(s) || !s.stamps.refUnchanged(c.Filesystem()) {
		var err error
		if s, err = c.refRebuild(s); err != nil {
			return nil, err
		}
	}
	return storer.NewReferenceSliceIter(s.refs), nil
}

// Reference never builds a snapshot: git.Open looks up HEAD, and a build reads every loose ref.
func (c *refCache) Reference(name plumbing.ReferenceName) (*plumbing.Reference, error) {
	// Listings hold no root ref but HEAD, and the inner storer rejects unsafe names without I/O.
	if name != plumbing.HEAD && !(name.IsSafe() && name.IsUnderRefs()) {
		return c.Storage.Reference(name)
	}
	s := c.snap.Load()
	if !c.refTrusted(s) {
		return c.Storage.Reference(name)
	}
	fsys := c.Filesystem()
	if name != plumbing.HEAD {
		parent := string(name[:strings.LastIndexByte(string(name), '/')])
		i, ok := slices.BinarySearchFunc(s.stamps.dirs, parent, func(d dirStamp, path string) int { return strings.Compare(d.path, path) })
		if !ok || !refDirUnchanged(fsys, s.stamps.dirs[i]) {
			return c.Storage.Reference(name)
		}
	}
	if !refFileUnchanged(fsys, refHeadFile, s.stamps.head) || !refFileUnchanged(fsys, refPackedFile, s.stamps.packed) {
		return c.Storage.Reference(name)
	}
	if ref, ok := s.byName[name]; ok {
		return ref, nil
	}
	return nil, plumbing.ErrReferenceNotFound
}

func (c *refCache) SetReference(ref *plumbing.Reference) error {
	defer c.invalidate()
	return c.Storage.SetReference(ref)
}

func (c *refCache) CheckAndSetReference(newRef, old *plumbing.Reference) error {
	defer c.invalidate()
	return c.Storage.CheckAndSetReference(newRef, old)
}

func (c *refCache) RemoveReference(name plumbing.ReferenceName) error {
	defer c.invalidate()
	return c.Storage.RemoveReference(name)
}

func (c *refCache) PackRefs() error {
	defer c.invalidate()
	return c.Storage.PackRefs()
}

// invalidate drops the snapshot; one built across it is neither published nor trusted.
func (c *refCache) invalidate() {
	if c == nil {
		return
	}
	c.gen.Add(1)
	c.snap.Store(nil)
}

// liveReference reads name from the inner storer, never from the snapshot.
func (c *refCache) liveReference(name plumbing.ReferenceName) (*plumbing.Reference, error) {
	if c == nil {
		return nil, plumbing.ErrReferenceNotFound
	}
	return c.Storage.Reference(name)
}

func (c *refCache) rebuildCount() uint64 {
	if c == nil {
		return 0
	}
	return c.rebuilds.Load()
}

// refTrusted checks everything about s but its stamps.
func (c *refCache) refTrusted(s *refSnapshot) bool {
	return s != nil && s.settled && s.gen == c.gen.Load() && c.now().Sub(s.builtAt) < refSnapshotMaxAge
}

// refRebuild builds and publishes a snapshot unless another caller already replaced stale with a trusted one.
func (c *refCache) refRebuild(stale *refSnapshot) (*refSnapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s := c.snap.Load(); s != stale && c.refTrusted(s) && s.stamps.refUnchanged(c.Filesystem()) {
		return s, nil
	}
	c.rebuilds.Add(1)
	s, err := c.refBuild()
	if err != nil {
		// go-git truncates a loose ref before rewriting it, so a failed listing may have read a torn write.
		if s, err = c.refBuild(); err != nil {
			c.snap.Store(nil)
			return nil, err
		}
	}
	if c.gen.Load() == s.gen {
		c.snap.Store(s)
	}
	return s, nil
}

func (c *refCache) refBuild() (*refSnapshot, error) {
	s := &refSnapshot{gen: c.gen.Load(), builtAt: c.now()}
	var err error
	if s.stamps, err = refFingerprint(c.Filesystem()); err != nil {
		return nil, err
	}
	iter, err := c.Storage.IterReferences()
	if err != nil {
		return nil, err
	}
	if err := iter.ForEach(func(ref *plumbing.Reference) error {
		s.refs = append(s.refs, ref)
		return nil
	}); err != nil {
		return nil, err
	}
	s.byName = make(map[plumbing.ReferenceName]*plumbing.Reference, len(s.refs))
	for _, ref := range s.refs {
		if _, ok := s.byName[ref.Name()]; !ok {
			s.byName[ref.Name()] = ref
		}
	}
	newest := s.stamps.head.mtime
	if s.stamps.packed.mtime.After(newest) {
		newest = s.stamps.packed.mtime
	}
	for _, d := range s.stamps.dirs {
		if d.mtime.After(newest) {
			newest = d.mtime
		}
	}
	s.settled = s.builtAt.Sub(newest) >= refSettleWindow
	return s, nil
}

// refUnchanged re-stats every stamp.
func (st *refStamps) refUnchanged(fsys billy.Filesystem) bool {
	if !refFileUnchanged(fsys, refHeadFile, st.head) || !refFileUnchanged(fsys, refPackedFile, st.packed) {
		return false
	}
	for _, d := range st.dirs {
		if !refDirUnchanged(fsys, d) {
			return false
		}
	}
	return true
}

// refFingerprint stamps HEAD, packed-refs and every directory under refs/, each directory before it is listed.
func refFingerprint(fsys billy.Filesystem) (refStamps, error) {
	var st refStamps
	var err error
	if st.head, err = refStatFile(fsys, refHeadFile); err != nil {
		return st, err
	}
	if st.packed, err = refStatFile(fsys, refPackedFile); err != nil {
		return st, err
	}
	info, err := fsys.Stat(refRootDir)
	if errors.Is(err, os.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	if err := refWalkDirs(fsys, refRootDir, info, &st.dirs); err != nil {
		return st, err
	}
	slices.SortFunc(st.dirs, func(a, b dirStamp) int { return strings.Compare(a.path, b.path) })
	return st, nil
}

func refWalkDirs(fsys billy.Filesystem, path string, info os.FileInfo, dirs *[]dirStamp) error {
	// s3fs reports its virtual directories with a zero mtime that no write moves.
	if mtime := info.ModTime(); !mtime.IsZero() {
		*dirs = append(*dirs, dirStamp{path: path, mtime: mtime})
	}
	entries, err := fsys.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		info, err := e.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if err := refWalkDirs(fsys, path+"/"+e.Name(), info, dirs); err != nil {
			return err
		}
	}
	return nil
}

func refStatFile(fsys billy.Filesystem, name string) (fileStamp, error) {
	info, err := fsys.Stat(name)
	if errors.Is(err, os.ErrNotExist) {
		return fileStamp{}, nil
	}
	if err != nil {
		return fileStamp{}, err
	}
	return fileStamp{exists: true, size: info.Size(), mtime: info.ModTime()}, nil
}

func refFileUnchanged(fsys billy.Filesystem, name string, want fileStamp) bool {
	got, err := refStatFile(fsys, name)
	return err == nil && got.exists == want.exists && got.size == want.size && got.mtime.Equal(want.mtime)
}

func refDirUnchanged(fsys billy.Filesystem, d dirStamp) bool {
	info, err := fsys.Stat(d.path)
	return err == nil && info.ModTime().Equal(d.mtime)
}
