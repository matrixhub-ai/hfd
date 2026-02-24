package storage

import (
	"path/filepath"

	"github.com/wzshiming/gitd/pkg/lfs"
)

type Storage struct {
	rootDir         string
	repositoriesDir string

	contentStore *lfs.Content
	s3Store      *lfs.S3
	locksStore   *lfs.LockDB
}

type Option func(*Storage)

func WithRootDir(rootDir string) Option {
	return func(h *Storage) {
		h.rootDir = rootDir
	}
}

// WithLFSS3 configures the LFS S3 storage backend.
func WithLFSS3(s3Store *lfs.S3) Option {
	return func(h *Storage) {
		h.s3Store = s3Store
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

	h.locksStore = lfs.NewLock(filepath.Join(h.rootDir, "lfs", "locks.db"))
	h.contentStore = lfs.NewContent(filepath.Join(h.rootDir, "lfs"))

	h.repositoriesDir = filepath.Join(h.rootDir, "repositories")

	return h
}

func (s *Storage) RootDir() string {
	return s.rootDir
}

func (s *Storage) RepositoriesDir() string {
	return s.repositoriesDir
}

func (s *Storage) S3Store() *lfs.S3 {
	return s.s3Store
}

func (s *Storage) ContentStore() *lfs.Content {
	return s.contentStore
}

func (s *Storage) LocksStore() *lfs.LockDB {
	return s.locksStore
}
