package repository

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/filesystem"
	xstorage "github.com/go-git/go-git/v6/x/storage"
)

// sharedObjectsFS is a chroot of dataFS whose repositories borrow objects from one store on dataFS through git alternates.
type sharedObjectsFS struct {
	*chroot.ChrootHelper
	dataFS     billy.Filesystem
	objectsDir string
}

// BindSharedObjects returns the reposDir chroot of dataFS whose repositories share the SHA-1 loose-object store at objectsDir.
func BindSharedObjects(dataFS billy.Filesystem, reposDir, objectsDir string) billy.Filesystem {
	return &sharedObjectsFS{
		ChrootHelper: chroot.New(dataFS, reposDir).(*chroot.ChrootHelper),
		dataFS:       dataFS,
		objectsDir:   objectsDir,
	}
}

// Underlying shadows the promoted ChrootHelper.Underlying, which returns a billy.Basic.
func (fs *sharedObjectsFS) Underlying() billy.Filesystem {
	return fs.dataFS
}

// ensure creates the shared store and points the alternates of repoPath at it, refusing a file that lists several stores.
func (fs *sharedObjectsFS) ensure(repoPath string) error {
	// go-git stages new loose objects in objects/pack.
	if err := fs.dataFS.MkdirAll(filepath.Join(fs.objectsDir, "objects", "pack"), 0o755); err != nil {
		return err
	}
	// C git resolves the line from the repository's objects directory, go-git from the root of dataFS.
	line, err := filepath.Rel(filepath.Join(fs.Root(), repoPath, "objects"), filepath.Join(fs.objectsDir, "objects"))
	if err != nil {
		return err
	}
	info := filepath.Join(repoPath, "objects", "info")
	alternates := filepath.Join(info, "alternates")
	data, err := util.ReadFile(fs, alternates)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if string(data) == line+"\n" {
		return nil
	}
	n := 0
	for l := range bytes.Lines(data) {
		if len(bytes.TrimSpace(l)) > 0 {
			n++
		}
	}
	// Rewriting several lines would drop stores hfd did not add.
	if n > 1 {
		return fmt.Errorf("%s: %d alternates lines, want 1", alternates, n)
	}
	if err := fs.MkdirAll(info, 0o755); err != nil {
		return err
	}
	return util.WriteFile(fs, alternates, []byte(line+"\n"), 0o644)
}

// WalkSharedObjects calls fn with every loose object in the store fs shares, from listings alone; an unbound fs shares none.
func WalkSharedObjects(fs billy.Filesystem, fn func(hash plumbing.Hash, info os.FileInfo) error) error {
	bound, ok := fs.(*sharedObjectsFS)
	if !ok {
		return nil
	}
	objects := filepath.Join(bound.objectsDir, "objects")
	dirs, err := bound.dataFS.ReadDir(objects)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, dir := range dirs {
		if !dir.IsDir() || !isLowerHex(dir.Name(), 2) {
			continue
		}
		entries, err := bound.dataFS.ReadDir(filepath.Join(objects, dir.Name()))
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !entry.Type().IsRegular() || !isLowerHex(entry.Name(), 38) {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if err := fn(plumbing.NewHash(dir.Name()+entry.Name()), info); err != nil {
				return err
			}
		}
	}
	return nil
}

func isLowerHex(name string, n int) bool {
	if len(name) != n {
		return false
	}
	for _, c := range name {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// sharedObjectsDir is the host directory of the store r shares, or "" unless that store is OS-backed.
func (r *Repository) sharedObjectsDir() string {
	bound, ok := r.fs.(*sharedObjectsFS)
	if !ok {
		return ""
	}
	return localDir(bound.dataFS, filepath.Join(bound.objectsDir, "objects"))
}

// sharedStorer writes new objects to the shared store and reads through local's alternates; it must not implement storer.DeltaObjectStorer, whose DeltaObject skips alternates.
type sharedStorer struct {
	storage.Storer
	local    *filesystem.Storage
	pool     *filesystem.ObjectStorage
	closeErr *error // set only on the copy a spooled pack is unpacked through
}

var (
	_ storer.LooseObjectStorer  = (*sharedStorer)(nil)
	_ storer.PackedObjectStorer = (*sharedStorer)(nil)
	_ storer.PackfileWriter     = (*sharedStorer)(nil)
	_ packfile.LowMemoryCapable = (*sharedStorer)(nil)
	_ xstorage.ExtensionChecker = (*sharedStorer)(nil)
)

func (s *sharedStorer) SetEncodedObject(obj plumbing.EncodedObject) (plumbing.Hash, error) {
	return s.pool.SetEncodedObject(obj)
}

func (s *sharedStorer) RawObjectWriter(typ plumbing.ObjectType, size int64) (io.WriteCloser, error) {
	w, err := s.pool.RawObjectWriter(typ, size)
	if err != nil {
		return nil, err
	}
	return &sharedObjectWriter{WriteCloser: w, closeErr: s.closeErr}, nil
}

// PackfileWriter spools the pack: go-git parses in low-memory mode only from a seekable reader.
func (s *sharedStorer) PackfileWriter() (io.WriteCloser, error) {
	file, err := os.CreateTemp("", "hfd-pack-*")
	if err != nil {
		return nil, err
	}
	return &sharedPackWriter{file: file, storer: s}, nil
}

func (s *sharedStorer) LowMemoryMode() bool {
	return true
}

// HashesWithPrefix searches the repository's own objects only: the shared store holds every repository's, and listing it walks all its fan-out directories.
func (s *sharedStorer) HashesWithPrefix(prefix []byte) ([]plumbing.Hash, error) {
	var hashes []plumbing.Hash
	err := s.local.ForEachObjectHash(func(h plumbing.Hash) error {
		if h.HasPrefix(prefix) {
			hashes = append(hashes, h)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	packs, err := s.local.ObjectPacks()
	if err != nil {
		return nil, err
	}
	for _, pack := range packs {
		objects, err := packIndexObjects(s.local.Filesystem(), filepath.Join("objects", "pack", "pack-"+pack.String()+".idx"), pack)
		if err != nil {
			return nil, err
		}
		for _, h := range objects {
			if h.HasPrefix(prefix) {
				hashes = append(hashes, h)
			}
		}
	}
	plumbing.HashesSort(hashes)
	return slices.Compact(hashes), nil
}

func (s *sharedStorer) Reindex() error {
	return s.local.Reindex()
}

func (s *sharedStorer) ForEachObjectHash(fn func(plumbing.Hash) error) error {
	return s.local.ForEachObjectHash(fn)
}

func (s *sharedStorer) LooseObjectTime(hash plumbing.Hash) (time.Time, error) {
	return s.local.LooseObjectTime(hash)
}

func (s *sharedStorer) DeleteLooseObject(hash plumbing.Hash) error {
	return s.local.DeleteLooseObject(hash)
}

func (s *sharedStorer) ObjectPacks() ([]plumbing.Hash, error) {
	return s.local.ObjectPacks()
}

func (s *sharedStorer) DeleteOldObjectPackAndIndex(hash plumbing.Hash, t time.Time) error {
	return s.local.DeleteOldObjectPackAndIndex(hash, t)
}

// SupportsExtension refuses SHA-256 repositories: the shared store holds SHA-1 objects.
func (s *sharedStorer) SupportsExtension(name, value string) bool {
	if name == "objectformat" {
		return value == "sha1"
	}
	return s.local.SupportsExtension(name, value)
}

// sharedPackWriter unpacks the spooled pack into the shared store as loose objects on Close.
type sharedPackWriter struct {
	file      *os.File
	storer    *sharedStorer
	observers []packfile.Observer // see every object the pack holds, already stored ones included
	onRead    func(plumbing.Hash) // sees every object the parser reads back from the store, as it does a delta's base
}

func (w *sharedPackWriter) Write(p []byte) (int, error) {
	return w.file.Write(p)
}

func (w *sharedPackWriter) Close() error {
	defer os.Remove(w.file.Name())
	defer w.file.Close()
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var closeErr error
	st := *w.storer
	st.closeErr = &closeErr
	var objects storer.EncodedObjectStorer = &st
	if w.onRead != nil {
		objects = &readRecordingStorer{sharedStorer: &st, onRead: w.onRead}
	}
	_, err := packfile.NewParser(w.file, packfile.WithStorage(objects), packfile.WithScannerObservers(w.observers...)).Parse()
	if errors.Is(err, packfile.ErrEmptyPackfile) {
		return nil
	}
	if err != nil {
		return err
	}
	return closeErr
}

// readRecordingStorer hands onRead every hash the parser reads back; the embedded storer keeps it in low-memory mode.
type readRecordingStorer struct {
	*sharedStorer
	onRead func(plumbing.Hash)
}

func (s *readRecordingStorer) EncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	s.onRead(h)
	return s.sharedStorer.EncodedObject(t, h)
}

// sharedObjectWriter keeps the error of Close, which renames the loose object into place; go-git's pack parser drops it.
type sharedObjectWriter struct {
	io.WriteCloser
	closeErr *error
}

func (w *sharedObjectWriter) Close() error {
	err := w.WriteCloser.Close()
	if err != nil && w.closeErr != nil && *w.closeErr == nil {
		*w.closeErr = err
	}
	return err
}
