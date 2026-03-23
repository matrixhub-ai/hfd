package storage

import (
	"path/filepath"
	"strings"
)

// Storage manages the filesystem paths for repositories and LFS objects.
type Storage struct {
	rootDir         string
	repositoriesDir string
	lfsDir          string
	xetDir          string
}

// Option defines a functional option for configuring the Storage.
type Option func(*Storage)

// WithRootDir sets the root directory for storage. The default is "./data".
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
	h.xetDir = filepath.Join(h.rootDir, "xet")

	return h
}

// RootDir returns the root directory for storage.
func (s *Storage) RootDir() string {
	return s.rootDir
}

// RepositoriesDir returns the directory path for storing git repositories.
func (s *Storage) RepositoriesDir() string {
	return s.repositoriesDir
}

// LFSDir returns the directory path for storing LFS objects.
func (s *Storage) LFSDir() string {
	return s.lfsDir
}

// XetDir returns the directory path for storing XET CAS objects (xorbs and shards).
func (s *Storage) XetDir() string {
	return s.xetDir
}

// ResolvePath resolves the given URL path to an absolute filesystem path within the repositories directory.
func (s *Storage) ResolvePath(urlPath string) string {
	urlPath = strings.TrimPrefix(urlPath, "/")
	if urlPath == "" {
		return ""
	}

	if !strings.HasSuffix(urlPath, ".git") {
		urlPath += ".git"
	}

	fullPath := filepath.Join(s.repositoriesDir, urlPath)
	fullPath = filepath.Clean(fullPath)

	// Prevent path traversal outside the repositories directory
	rel, err := filepath.Rel(s.repositoriesDir, fullPath)
	if err != nil || strings.HasPrefix(rel, "..") {
		return ""
	}

	return fullPath
}
