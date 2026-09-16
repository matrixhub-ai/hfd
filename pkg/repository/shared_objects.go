package repository

import (
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/filesystem"
)

type sharedObjectsFS struct {
	billy.Filesystem
	dataFS     billy.Filesystem
	reposDir   string
	objectsDir string
	objectsFS  billy.Filesystem
	ensured    atomic.Bool
}

// BindSharedObjects returns the reposDir chroot of dataFS whose repositories share one
// loose-object store at objectsDir through git alternates.
func BindSharedObjects(dataFS billy.Filesystem, reposDir, objectsDir string) billy.Filesystem {
	return &sharedObjectsFS{
		Filesystem: sharedChroot(dataFS, reposDir),
		dataFS:     dataFS,
		reposDir:   reposDir,
		objectsDir: objectsDir,
		objectsFS:  sharedChroot(dataFS, objectsDir),
	}
}

func sharedChroot(fs billy.Filesystem, dir string) billy.Filesystem {
	sub, err := fs.Chroot(dir)
	if err != nil {
		return chroot.New(fs, dir)
	}
	return sub
}

func (fs *sharedObjectsFS) Capabilities() billy.Capability {
	if capable, ok := fs.Filesystem.(billy.Capable); ok {
		return capable.Capabilities()
	}
	return billy.DefaultCapabilities
}

func (fs *sharedObjectsFS) alternatesLine(repoPath string) string {
	line, _ := filepath.Rel(path.Join(fs.reposDir, repoPath, "objects"),
		path.Join(fs.objectsDir, "objects"))
	return filepath.ToSlash(line)
}

func (fs *sharedObjectsFS) ensure(repoPath string) error {
	if !fs.ensured.Load() {
		if err := fs.dataFS.MkdirAll(path.Join(fs.objectsDir, "objects", "pack"), 0o755); err != nil {
			return err
		}
		fs.ensured.Store(true)
	}
	filename := path.Join(repoPath, "objects/info/alternates")
	want := fs.alternatesLine(repoPath) + "\n"
	data, err := util.ReadFile(fs.Filesystem, filename)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if string(data) == want {
		return nil
	}
	return util.WriteFile(fs.Filesystem, filename, []byte(want), 0o644)
}

// Do not embed filesystem.Storage: its DeltaObject only reads local objects.
type sharedStorer struct {
	storage.Storer
	local   *filesystem.Storage
	objects *filesystem.ObjectStorage
}

func (s *sharedStorer) SetEncodedObject(object plumbing.EncodedObject) (plumbing.Hash, error) {
	return s.objects.SetEncodedObject(object)
}

func (s *sharedStorer) RawObjectWriter(typ plumbing.ObjectType, size int64) (io.WriteCloser, error) {
	return s.objects.RawObjectWriter(typ, size)
}

func (s *sharedStorer) HashesWithPrefix(prefix []byte) ([]plumbing.Hash, error) {
	return s.local.HashesWithPrefix(prefix)
}

func (s *sharedStorer) LowMemoryMode() bool {
	return true
}

// The shared store hashes with SHA-1, so a SHA-256 repository must not open through it.
func (s *sharedStorer) SupportsExtension(name, value string) bool {
	if name == "objectformat" {
		return value == "" || value == "sha1"
	}
	return s.local.SupportsExtension(name, value)
}

// Spool the pack: go-git parses in low-memory mode only from a seekable reader, and its objects must land in the shared store.
func (s *sharedStorer) PackfileWriter() (io.WriteCloser, error) {
	file, err := os.CreateTemp("", "hfd-pack-*")
	if err != nil {
		return nil, err
	}
	return &sharedPackWriter{file: file, storer: s}, nil
}

type sharedPackWriter struct {
	file   *os.File
	storer *sharedStorer
}

type sharedPackStorer struct {
	*sharedStorer
	closeErr error
}

func (s *sharedPackStorer) RawObjectWriter(typ plumbing.ObjectType, size int64) (io.WriteCloser, error) {
	writer, err := s.sharedStorer.RawObjectWriter(typ, size)
	if err != nil {
		return nil, err
	}
	return &sharedObjectWriter{WriteCloser: writer, err: &s.closeErr}, nil
}

type sharedObjectWriter struct {
	io.WriteCloser
	err *error
}

func (w *sharedObjectWriter) Close() error {
	err := w.WriteCloser.Close()
	if err != nil && *w.err == nil {
		*w.err = err
	}
	return err
}

func (w *sharedPackWriter) Write(data []byte) (int, error) {
	return w.file.Write(data)
}

func (w *sharedPackWriter) Close() error {
	defer os.Remove(w.file.Name())
	defer w.file.Close()
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	store := &sharedPackStorer{sharedStorer: w.storer}
	_, err := packfile.NewParser(w.file, packfile.WithStorage(store),
		packfile.WithObjectFormat(formatcfg.DefaultObjectFormat)).Parse()
	if errors.Is(err, packfile.ErrEmptyPackfile) {
		return nil
	}
	if err == nil {
		return store.closeErr
	}
	return err
}

var _ storage.Storer = (*sharedStorer)(nil)
var _ storer.PackfileWriter = (*sharedStorer)(nil)
var _ packfile.LowMemoryCapable = (*sharedStorer)(nil)
var _ billy.Filesystem = (*sharedObjectsFS)(nil)
