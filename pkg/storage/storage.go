package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/osfs"
	xetstorage "github.com/wzshiming/xet/storage"
	xetlocal "github.com/wzshiming/xet/storage/local"
)

// Storage owns repository filesystems, xet content storage, and local caches.
type Storage struct {
	rootDir        string
	fs             billy.Filesystem
	repositoriesFS billy.Filesystem
	xetStorage     xetstorage.Storage
}

// Option defines a functional option for configuring the Storage.
type Option func(*Storage)

// WithRootDir roots local storage and caches; the default is "./data".
func WithRootDir(rootDir string) Option {
	return func(h *Storage) {
		h.rootDir = rootDir
	}
}

// WithFilesystem sets the filesystem holding repositories, e.g. an S3-backed
// one. The default is the host OS rooted at the root directory.
func WithFilesystem(fs billy.Filesystem) Option {
	return func(h *Storage) {
		h.fs = fs
	}
}

// WithXETStorage overrides the default local xet content store.
func WithXETStorage(xs xetstorage.Storage) Option {
	return func(h *Storage) {
		h.xetStorage = xs
	}
}

// NewStorage creates a new Storage with the given options.
func NewStorage(opts ...Option) (*Storage, error) {
	h := &Storage{
		rootDir: "./data",
	}

	for _, opt := range opts {
		opt(h)
	}

	if h.fs == nil {
		h.fs = osfs.New(h.rootDir)
	}

	h.repositoriesFS = chrootFS(h.fs, "/repositories")

	if h.xetStorage == nil {
		xs, err := xetlocal.NewStorage(
			xetlocal.WithBasePath(filepath.Join(h.XETDir(), "storage")),
		)
		if err != nil {
			return nil, fmt.Errorf("create xet storage: %w", err)
		}
		h.xetStorage = xs
	}

	for _, dir := range []string{"chunks", "mirror"} {
		if err := os.MkdirAll(filepath.Join(h.XETDir(), dir), 0755); err != nil {
			return nil, fmt.Errorf("create xet cache dir: %w", err)
		}
	}

	return h, nil
}

// chrootFS scopes fs to dir, preferring the filesystem's own Chroot.
func chrootFS(fs billy.Filesystem, dir string) billy.Filesystem {
	sub, err := fs.Chroot(dir)
	if err != nil {
		return chroot.New(fs, dir)
	}
	return sub
}

// FS returns the filesystem holding repositories.
func (s *Storage) FS() billy.Filesystem {
	return s.fs
}

// RepositoriesFS returns the filesystem holding git repositories.
func (s *Storage) RepositoriesFS() billy.Filesystem {
	return s.repositoriesFS
}

// XETStorage returns the xet content storage holding LFS bytes.
func (s *Storage) XETStorage() xetstorage.Storage {
	return s.xetStorage
}

// XETDir is the local xet directory, even when content is stored remotely.
func (s *Storage) XETDir() string {
	return filepath.Join(s.rootDir, "xet")
}
