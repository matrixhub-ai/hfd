package xet

import (
	"io"
	"os"
)

// Storage is the interface for xet CAS (Content-Addressable Storage) backends.
type Storage interface {
	Put(hash string, r io.Reader, size int64) error
	Get(hash string) (io.ReadSeekCloser, os.FileInfo, error)
	Exists(hash string) bool
	Info(hash string) (os.FileInfo, error)

	// RegisterShard parses a shard binary blob and indexes its file reconstruction data.
	RegisterShard(data []byte) error
	// GetFileReconstruction returns reconstruction info for the given file hash, or nil if not found.
	GetFileReconstruction(fileHash string) (*FileReconstruction, error)
	// GetXorbFooter opens a stored xorb and parses its binary footer.
	GetXorbFooter(hash string) (*XorbFooter, error)
}
