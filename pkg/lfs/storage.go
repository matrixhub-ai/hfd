// Package-level storage interfaces follow the capability pattern: Storage is
// the base contract, and backends advertise optional capabilities through the
// narrow interfaces below. Callers select behavior with type assertions:
//
//	Getter — direct content access (always available)
//	SignGetter + SignPutter — presigned URL redirection, present when the
//	backing filesystem hands out presigned URLs (go-billy-s3fs)
package lfs

import (
	"io"
	"os"
)

// Storage is the base interface for LFS storage backends.
// Both the local filesystem and S3 backends implement this interface.
type Storage interface {
	Put(oid string, r io.Reader, size int64) error
	Info(oid string) (os.FileInfo, error)
	Exists(oid string) bool
}

// Getter is implemented by stores that support direct content retrieval.
// The local backend implements this; S3 does not — use SignGetter instead.
type Getter interface {
	Get(oid string) (io.ReadSeekCloser, os.FileInfo, error)
}

// SignGetter is implemented by stores that support presigned download URLs.
// Download grants are self-contained: header is nil today, which callers
// that redirect rely on.
type SignGetter interface {
	SignGet(oid string) (url string, header map[string]string, err error)
}

// SignPutter is implemented by stores that support presigned upload URLs.
// The grant only accepts content hashing to oid and lands it in a staging
// area invisible to readers; the returned header must be sent verbatim with
// the PUT, and PutVerifier moves the object into place afterwards.
type SignPutter interface {
	SignPut(oid string) (url string, header map[string]string, err error)
}

// PutVerifier is implemented by stores whose presigned uploads land in a
// staging area: VerifyPut checks the staged object against the expected
// size and moves it into place, making it visible to readers. It succeeds
// when the object is already in place.
type PutVerifier interface {
	VerifyPut(oid string, size int64) error
}

// MovePutter is implemented by stores that can adopt a host file by renaming
// it into place instead of copying, present when the backing filesystem is
// host-backed. The file must already hold the full verified content.
type MovePutter interface {
	MovePut(oid string, path string) error
}
