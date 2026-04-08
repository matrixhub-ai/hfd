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

// MultipartPart represents a single part in a multipart upload.
type MultipartPart struct {
	PartNumber int    `json:"part_number"`
	URL        string `json:"url"`
	Pos        int64  `json:"pos"`
	Size       int64  `json:"size"`
}

// MultipartUpload contains information about an initiated multipart upload.
type MultipartUpload struct {
	UploadID string
	Parts    []MultipartPart
}

// SignMultipartPutter is implemented by stores that support multipart uploads with presigned URLs.
type SignMultipartPutter interface {
	// SignMultipartPut initiates a multipart upload and returns presigned URLs for each part.
	// The size parameter is the total size of the object being uploaded.
	SignMultipartPut(oid string, size int64) (*MultipartUpload, error)

	// CompleteMultipartUpload completes a multipart upload.
	// The parts parameter contains the ETags returned by the storage backend for each uploaded part.
	CompleteMultipartUpload(oid string, uploadID string, parts map[int]string) error

	// AbortMultipartUpload aborts a multipart upload and cleans up any uploaded parts.
	AbortMultipartUpload(oid string, uploadID string) error
}
