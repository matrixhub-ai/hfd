package xet

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var (
	errSizeMismatch = errors.New("content size does not match")
)

// localStorage provides a simple file system based CAS storage.
type localStorage struct {
	basePath string
}

// NewLocal creates a new local file system based CAS storage.
// The basePath is the root directory where objects will be stored.
func NewLocal(basePath string) Storage {
	return &localStorage{basePath: basePath}
}

// Get retrieves the content of a CAS object by its hash.
func (s *localStorage) Get(hash string) (io.ReadSeekCloser, os.FileInfo, error) {
	path := filepath.Join(s.basePath, transformKey(hash))

	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return f, stat, nil
}

// Put stores a CAS object. The hash is a xet content hash (not SHA-256 of the raw data)
// so no hash verification is performed; only size is validated when size > 0.
func (s *localStorage) Put(hash string, r io.Reader, size int64) error {
	path := filepath.Join(s.basePath, transformKey(hash))

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}

	file, err := os.CreateTemp(dir, "xet_tmp_")
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(file.Name())
	}()

	written, err := io.Copy(file, r)
	if err != nil {
		_ = file.Close()
		return err
	}
	_ = file.Close()

	if size > 0 && written != size {
		return fmt.Errorf("%w: expected %d bytes, got %d bytes", errSizeMismatch, size, written)
	}

	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	return nil
}

// Info returns the file info for a CAS object.
func (s *localStorage) Info(hash string) (os.FileInfo, error) {
	path := filepath.Join(s.basePath, transformKey(hash))
	return os.Stat(path)
}

// Exists returns true if the CAS object exists in the store.
func (s *localStorage) Exists(hash string) bool {
	path := filepath.Join(s.basePath, transformKey(hash))
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

// transformKey converts a hash into a sharded directory path (e.g., "ab/cd/ef01234...")
// to avoid storing too many files in a single directory. Keys shorter than 5
// characters are stored as-is since they cannot be meaningfully sharded.
func transformKey(key string) string {
	if len(key) < 5 {
		return key
	}
	return filepath.Join(key[0:2], key[2:4], key[4:])
}
