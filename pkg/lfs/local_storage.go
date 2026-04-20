package lfs

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var (
	errHashMismatch = errors.New("content hash does not match OID")
	errSizeMismatch = errors.New("content size does not match")
)

// localStorage provides a simple file system based storage.
type localStorage struct {
	basePath string
}

// NewLocal creates a new local file system based Store. The basePath is the root directory where objects will be stored.
func NewLocal(basePath string) Storage {
	return &localStorage{basePath: basePath}
}

// Get takes a Meta object and retreives the content from the store, returning
// it as an io.ReaderCloser. If fromByte > 0, the reader starts from that byte
func (s *localStorage) Get(oid string) (io.ReadSeekCloser, os.FileInfo, error) {
	path := filepath.Join(s.basePath, transformKey(oid))

	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	stat, err := os.Stat(path)
	if err != nil {
		return nil, nil, err
	}
	return f, stat, nil
}

// Put takes a Meta object and an io.Reader and writes the content to the store.
func (s *localStorage) Put(oid string, r io.Reader, size int64) error {
	path := filepath.Join(s.basePath, transformKey(oid))

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}

	file, err := os.CreateTemp(dir, "lfsd_tmp_")
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(file.Name())
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

	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	return nil
}

// MovePut moves a file from the given path to the location determined by the OID.
func (s *localStorage) MovePut(oid, path string) error {
	destPath := filepath.Join(s.basePath, transformKey(oid))
	dir := filepath.Dir(destPath)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	return os.Rename(path, destPath)
}

func (s *localStorage) Info(oid string) (os.FileInfo, error) {
	path := filepath.Join(s.basePath, transformKey(oid))
	return os.Stat(path)
}

// Exists returns true if the object exists in the content store.
func (s *localStorage) Exists(oid string) bool {
	path := filepath.Join(s.basePath, transformKey(oid))
	if _, err := os.Stat(path); os.IsNotExist(err) {
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
