package storage

import (
	"path/filepath"
)

type Storage struct {
	rootDir         string
	repositoriesDir string
	lfsDir          string
}

type Option func(*Storage)

func WithRootDir(rootDir string) Option {
	return func(h *Storage) {
		h.rootDir = rootDir
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

	h.lfsDir = filepath.Join(h.rootDir, "lfs")
	h.repositoriesDir = filepath.Join(h.rootDir, "repositories")

	return h
}

func (s *Storage) RootDir() string {
	return s.rootDir
}

func (s *Storage) RepositoriesDir() string {
	return s.repositoriesDir
}

func (s *Storage) LFSDir() string {
	return s.lfsDir
}
