package xet

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var (
	errSizeMismatch = errors.New("content size does not match")
)

// Storage provides content-addressable storage for xorbs and shards.
type Storage struct {
	xorbDir  string
	shardDir string
}

// NewStorage creates a new xet storage rooted at the given base directory.
// Xorbs and shards are stored under xorbs/ and shards/ subdirectories.
func NewStorage(baseDir string) *Storage {
	return &Storage{
		xorbDir:  filepath.Join(baseDir, "xorbs"),
		shardDir: filepath.Join(baseDir, "shards"),
	}
}

// PutXorb stores a xorb with the given prefix and hash. Returns true if the
// xorb was newly inserted, false if it already existed.
func (s *Storage) PutXorb(prefix, hash string, r io.Reader, size int64) (bool, error) {
	if !isValidHex64(hash) {
		return false, fmt.Errorf("invalid xorb hash: %s", hash)
	}

	dir := filepath.Join(s.xorbDir, prefix)
	path := filepath.Join(dir, transformKey(hash))

	if _, err := os.Stat(path); err == nil {
		// Xorb already exists; drain the reader to avoid broken pipe.
		_, _ = io.Copy(io.Discard, r)
		return false, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return false, err
	}

	file, err := os.CreateTemp(filepath.Dir(path), "xorb_tmp_")
	if err != nil {
		return false, err
	}
	defer func() {
		_ = os.Remove(file.Name())
	}()

	written, err := io.Copy(file, r)
	if err != nil {
		_ = file.Close()
		return false, err
	}
	_ = file.Close()

	if size >= 0 && written != size {
		return false, fmt.Errorf("%w: expected %d bytes, got %d bytes", errSizeMismatch, size, written)
	}

	if err := os.Rename(file.Name(), path); err != nil {
		return false, err
	}
	return true, nil
}

// GetXorb returns a reader for the xorb with the given prefix and hash.
// The caller must close the returned ReadSeekCloser.
func (s *Storage) GetXorb(prefix, hash string) (io.ReadSeekCloser, os.FileInfo, error) {
	path := filepath.Join(s.xorbDir, prefix, transformKey(hash))
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

// ShardDir returns the directory path for stored shards.
func (s *Storage) ShardDir() string {
	return s.shardDir
}

// XorbExists returns true if the xorb with the given prefix and hash exists.
func (s *Storage) XorbExists(prefix, hash string) bool {
	path := filepath.Join(s.xorbDir, prefix, transformKey(hash))
	_, err := os.Stat(path)
	return err == nil
}

// PutShard stores a shard. The shard key is derived from its content hash.
// Returns the result code: 1 = newly inserted, 0 = already existed.
func (s *Storage) PutShard(key string, r io.Reader) (int, error) {
	path := filepath.Join(s.shardDir, transformKey(key))

	if _, err := os.Stat(path); err == nil {
		_, _ = io.Copy(io.Discard, r)
		return 0, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return 0, err
	}

	file, err := os.CreateTemp(filepath.Dir(path), "shard_tmp_")
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = os.Remove(file.Name())
	}()

	if _, err := io.Copy(file, r); err != nil {
		_ = file.Close()
		return 0, err
	}
	_ = file.Close()

	if err := os.Rename(file.Name(), path); err != nil {
		return 0, err
	}
	return 1, nil
}

// GetShard returns a reader for the shard with the given key.
func (s *Storage) GetShard(key string) (io.ReadSeekCloser, os.FileInfo, error) {
	path := filepath.Join(s.shardDir, transformKey(key))
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

// transformKey splits the hash into a directory hierarchy to avoid too many
// files in a single directory. Uses the same pattern as LFS: ab/cd/rest.
func transformKey(key string) string {
	if len(key) < 5 {
		return key
	}
	return filepath.Join(key[0:2], key[2:4], key[4:])
}

// isValidHex64 returns true if s is a 64-character lowercase hex string.
func isValidHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
