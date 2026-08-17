package storage

import (
	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/osfs"
)

// Storage manages the filesystem for git repositories, carved out of one
// backing filesystem. LFS content lives in the xet storage, not here.
type Storage struct {
	rootDir        string
	fs             billy.Filesystem
	repositoriesFS billy.Filesystem
}

// Option defines a functional option for configuring the Storage.
type Option func(*Storage)

// WithRootDir sets the host root directory for storage. The default is
// "./data". It roots the default filesystem.
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

// NewStorage creates a new Storage with the given options.
func NewStorage(opts ...Option) *Storage {
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

	return h
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
