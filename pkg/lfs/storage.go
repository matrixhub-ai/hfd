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
	URL        string `json:"href"`
}

// MultipartUpload represents a multipart upload session.
type MultipartUpload struct {
	UploadID string           `json:"upload_id"`
	Parts    []MultipartPart  `json:"parts"`
}

// SignMultipartPutter is implemented by stores that support multipart uploads with presigned URLs.
type SignMultipartPutter interface {
	// SignMultipartPut creates a multipart upload and returns presigned URLs for each part.
	// partSize is the size of each part in bytes, totalSize is the total file size.
	SignMultipartPut(oid string, partSize int64, totalSize int64) (*MultipartUpload, error)
}
