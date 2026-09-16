package storage

import (
	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// Storage manages the filesystem for git repositories, carved out of one
// backing filesystem. LFS content lives in the xet storage, not here.
// Git objects are shared across repositories under /git/sha1 via alternates.
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

	h.repositoriesFS = repository.BindSharedObjects(h.fs, "/repositories", "/git/sha1")

	return h
}

// FS returns the filesystem holding repositories.
func (s *Storage) FS() billy.Filesystem {
	return s.fs
}

// RepositoriesFS returns the filesystem holding git repositories.
func (s *Storage) RepositoriesFS() billy.Filesystem {
	return s.repositoriesFS
}
