package lfs

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
)

var (
	errHashMismatch = errors.New("content hash does not match OID")
	errSizeMismatch = errors.New("content size does not match")
)

// localStorage provides a billy filesystem based storage.
type localStorage struct {
	fs       billy.Filesystem
	basePath string
}

var (
	_ Storage    = (*localStorage)(nil)
	_ Getter     = (*localStorage)(nil)
	_ MovePutter = (*localStorage)(nil)
)

// NewLocal creates a new local file system based Store. The basePath is the root directory where objects will be stored.
func NewLocal(basePath string) Storage {
	// The host-wide filesystem lets MovePut rename sources located outside basePath.
	return &localStorage{fs: osfs.Default, basePath: basePath}
}

// Get takes a Meta object and retreives the content from the store, returning
// it as an io.ReaderCloser. If fromByte > 0, the reader starts from that byte
func (s *localStorage) Get(oid string) (io.ReadSeekCloser, os.FileInfo, error) {
	path := filepath.Join(s.basePath, transformKey(oid))

	f, err := s.fs.Open(path)
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

// Put takes a Meta object and an io.Reader and writes the content to the store.
func (s *localStorage) Put(oid string, r io.Reader, size int64) error {
	tmpDir := filepath.Join(s.basePath, "tmp")
	if err := s.fs.MkdirAll(tmpDir, 0750); err != nil {
		return err
	}
	tmpPath := filepath.Join(tmpDir, oid)
	file, err := s.fs.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer func() {
		_ = s.fs.Remove(tmpPath)
	}()

	hash := sha256.New()
	hw := io.MultiWriter(hash, file)

	written, err := io.Copy(hw, r)
	if err != nil {
		_ = file.Close()
		return err
	}
	_ = file.Close()

	if written != size {
		return fmt.Errorf("%w: expected %d bytes, got %d bytes", errSizeMismatch, size, written)
	}

	shaStr := hex.EncodeToString(hash.Sum(nil))
	if shaStr != oid {
		return errHashMismatch
	}

	path := filepath.Join(s.basePath, transformKey(oid))

	dir := filepath.Dir(path)
	if err := s.fs.MkdirAll(dir, 0750); err != nil {
		return err
	}

	if err := s.fs.Rename(tmpPath, path); err != nil {
		return err
	}
	return nil
}

// MovePut moves a file from the given path to the location determined by the OID.
func (s *localStorage) MovePut(oid, path string) error {
	destPath := filepath.Join(s.basePath, transformKey(oid))
	if err := s.fs.MkdirAll(filepath.Dir(destPath), 0750); err != nil {
		return err
	}
	return s.fs.Rename(path, destPath)
}

func (s *localStorage) Info(oid string) (os.FileInfo, error) {
	return s.fs.Stat(filepath.Join(s.basePath, transformKey(oid)))
}

// Exists returns true if the object exists in the content store.
func (s *localStorage) Exists(oid string) bool {
	if _, err := s.fs.Stat(filepath.Join(s.basePath, transformKey(oid))); os.IsNotExist(err) {
		return false
	}
	return true
}

func transformKey(key string) string {
	if len(key) < 5 {
		return key
	}
	return filepath.Join(key[0:2], key[2:4], key[4:])
}
