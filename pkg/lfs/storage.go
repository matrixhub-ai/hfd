package lfs

import (
	"io"
	"os"
)

// Storage is the base interface for LFS storage backends.
// Both file system (Content) and S3 backends implement this interface.
type Storage interface {
	Put(oid string, r io.Reader, size int64) error
	Info(oid string) (os.FileInfo, error)
	Exists(oid string) bool
}

// Getter is implemented by stores that support direct content retrieval.
// Content storage implements this; S3 does not — use SignGetter instead.
type Getter interface {
	Get(oid string) (io.ReadSeekCloser, os.FileInfo, error)
}

// SignGetter is implemented by stores that support presigned download URLs.
type SignGetter interface {
	SignGet(oid string) (string, error)
}

// SignPutter is implemented by stores that support presigned upload URLs.
type SignPutter interface {
	SignPut(oid string) (string, error)
}

// MovePutter is implemented by stores that support moving files within the storage backend.
type MovePutter interface {
	MovePut(oid, path string) error
}
