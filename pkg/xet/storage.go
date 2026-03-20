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
}
