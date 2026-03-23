package xet

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Storage provides local filesystem storage for XET CAS objects (xorbs and shards).
type Storage struct {
	basePath  string
	xorbDir   string
	shardDir  string
	shardsMu  sync.Mutex
	shardList []string // ordered list of shard file paths
}

// NewStorage creates a new XET CAS storage rooted at basePath.
func NewStorage(basePath string) *Storage {
	s := &Storage{
		basePath: basePath,
		xorbDir:  filepath.Join(basePath, "xorbs"),
		shardDir: filepath.Join(basePath, "shards"),
	}
	return s
}

// PutXorb stores a xorb blob keyed by its hash prefix and hash.
func (s *Storage) PutXorb(prefix, hash string, r io.Reader, size int64) error {
	dir := filepath.Join(s.xorbDir, prefix)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return fmt.Errorf("xet: failed to create xorb directory: %w", err)
	}

	path := filepath.Join(dir, hash)

	file, err := os.CreateTemp(dir, "xorb_tmp_")
	if err != nil {
		return fmt.Errorf("xet: failed to create temp file: %w", err)
	}
	defer func() {
		_ = os.Remove(file.Name())
	}()

	written, err := io.Copy(file, r)
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("xet: failed to write xorb: %w", err)
	}
	_ = file.Close()

	if size > 0 && written != size {
		return fmt.Errorf("xet: xorb size mismatch: expected %d bytes, got %d bytes", size, written)
	}

	if err := os.Rename(file.Name(), path); err != nil {
		return fmt.Errorf("xet: failed to rename xorb: %w", err)
	}
	return nil
}

// GetXorb retrieves a xorb blob by prefix and hash.
func (s *Storage) GetXorb(prefix, hash string) (io.ReadSeekCloser, os.FileInfo, error) {
	path := filepath.Join(s.xorbDir, prefix, hash)
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

// XorbExists returns true if the xorb exists.
func (s *Storage) XorbExists(prefix, hash string) bool {
	path := filepath.Join(s.xorbDir, prefix, hash)
	_, err := os.Stat(path)
	return err == nil
}

// PutShard stores a shard metadata blob. Shards are stored with a timestamp-based name.
func (s *Storage) PutShard(r io.Reader) (string, error) {
	if err := os.MkdirAll(s.shardDir, 0750); err != nil {
		return "", fmt.Errorf("xet: failed to create shard directory: %w", err)
	}

	s.shardsMu.Lock()
	defer s.shardsMu.Unlock()

	name := fmt.Sprintf("shard_%d", time.Now().UnixNano())
	path := filepath.Join(s.shardDir, name)

	file, err := os.CreateTemp(s.shardDir, "shard_tmp_")
	if err != nil {
		return "", fmt.Errorf("xet: failed to create temp file: %w", err)
	}
	defer func() {
		_ = os.Remove(file.Name())
	}()

	if _, err := io.Copy(file, r); err != nil {
		_ = file.Close()
		return "", fmt.Errorf("xet: failed to write shard: %w", err)
	}
	_ = file.Close()

	if err := os.Rename(file.Name(), path); err != nil {
		return "", fmt.Errorf("xet: failed to rename shard: %w", err)
	}

	s.shardList = append(s.shardList, path)
	return name, nil
}

// GetShard retrieves a shard by name.
func (s *Storage) GetShard(name string) (io.ReadSeekCloser, os.FileInfo, error) {
	path := filepath.Join(s.shardDir, name)
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

// ListShards returns the names of all stored shards.
func (s *Storage) ListShards() ([]string, error) {
	entries, err := os.ReadDir(s.shardDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		names = append(names, e.Name())
	}
	return names, nil
}
