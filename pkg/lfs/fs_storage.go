package lfs

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/osfs"
	s3fs "github.com/wzshiming/go-billy-s3fs"
)

var (
	errHashMismatch = errors.New("content hash does not match OID")

	// ErrSizeMismatch reports that the content size does not match the
	// expected size, on Put and VerifyPut.
	ErrSizeMismatch = errors.New("content size does not match")
)

// SignExpiry bounds the lifetime of URLs handed out by SignGetter and
// SignPutter, so batch responses can advertise when actions expire.
const SignExpiry = 60 * time.Minute

// fsStorage provides a billy filesystem based storage; objects live at
// their transformed key under the filesystem root.
type fsStorage struct {
	fs billy.Filesystem
}

var (
	_ Storage = (*fsStorage)(nil)
	_ Getter  = (*fsStorage)(nil)
)

// New creates a Storage rooted at the given filesystem. When fs is backed by
// a presigning object store (go-billy-s3fs, possibly behind chroots), the
// returned storage also implements SignGetter and SignPutter, redirecting
// content transfers directly to the object store. When fs is host-backed it
// implements MovePutter, adopting host files by rename instead of copy.
func New(fs billy.Filesystem) Storage {
	s := &fsStorage{fs: fs}
	if p, prefix, ok := presignPrefix(fs); ok {
		return &presignStorage{fsStorage: s, presigner: p, prefix: prefix}
	}
	if bound, ok := fs.(*osfs.BoundOS); ok {
		return &moveStorage{fsStorage: s, root: bound.Root()}
	}
	return s
}

// presignPrefix unwraps chroot layers over a presigning filesystem,
// returning the presigner and the path prefix the layers hide.
func presignPrefix(fs billy.Filesystem) (s3fs.Presigner, string, bool) {
	prefix := "/"
	for {
		switch v := fs.(type) {
		case s3fs.Presigner:
			return v, prefix, true
		case *chroot.ChrootHelper:
			prefix = filepath.Join(v.Root(), prefix)
			under, ok := v.Underlying().(billy.Filesystem)
			if !ok {
				return nil, "", false
			}
			fs = under
		default:
			return nil, "", false
		}
	}
}

// Get takes a Meta object and retreives the content from the store, returning
// it as an io.ReaderCloser. If fromByte > 0, the reader starts from that byte
func (s *fsStorage) Get(oid string) (io.ReadSeekCloser, os.FileInfo, error) {
	f, err := s.fs.Open(transformKey(oid))
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
func (s *fsStorage) Put(oid string, r io.Reader, size int64) error {
	tmpDir := "/tmp"
	if err := s.fs.MkdirAll(tmpDir, 0750); err != nil {
		return err
	}
	file, err := s.fs.TempFile(tmpDir, "put-")
	if err != nil {
		return err
	}
	tmpPath := file.Name()
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
		return fmt.Errorf("%w: expected %d bytes, got %d bytes", ErrSizeMismatch, size, written)
	}

	shaStr := hex.EncodeToString(hash.Sum(nil))
	if shaStr != oid {
		return errHashMismatch
	}

	path := transformKey(oid)

	dir := filepath.Dir(path)
	if err := s.fs.MkdirAll(dir, 0750); err != nil {
		return err
	}

	if err := s.fs.Rename(tmpPath, path); err != nil {
		return err
	}
	return nil
}

func (s *fsStorage) Info(oid string) (os.FileInfo, error) {
	return s.fs.Stat(transformKey(oid))
}

// Exists returns true if the object exists in the content store.
func (s *fsStorage) Exists(oid string) bool {
	if _, err := s.fs.Stat(transformKey(oid)); os.IsNotExist(err) {
		return false
	}
	return true
}

// moveStorage extends fsStorage with host-path rename adoption; root is the
// host directory the filesystem is bound to.
type moveStorage struct {
	*fsStorage
	root string
}

var _ MovePutter = (*moveStorage)(nil)

// MovePut moves a host file to the location determined by the OID.
func (s *moveStorage) MovePut(oid, path string) error {
	destPath := filepath.Join(s.root, transformKey(oid))
	if err := os.MkdirAll(filepath.Dir(destPath), 0750); err != nil {
		return err
	}
	return os.Rename(path, destPath)
}

// presignStorage extends fsStorage with presigned URL redirection.
type presignStorage struct {
	*fsStorage
	presigner s3fs.Presigner
	prefix    string
}

var (
	_ SignGetter  = (*presignStorage)(nil)
	_ SignPutter  = (*presignStorage)(nil)
	_ PutVerifier = (*presignStorage)(nil)
)

func (s *presignStorage) SignGet(oid string) (string, map[string]string, error) {
	req, err := s.presigner.PresignGet(filepath.Join(s.prefix, transformKey(oid)), s3fs.WithExpiry(SignExpiry))
	if err != nil {
		return "", nil, err
	}
	return req.URL, signedHeaderMap(req.SignedHeader), nil
}

// SignPut grants an upload to the staging key tmp/<oid>; VerifyPut moves it
// into place once the size checks out.
func (s *presignStorage) SignPut(oid string) (string, map[string]string, error) {
	req, err := s.presigner.PresignPut(filepath.Join(s.prefix, stagingKey(oid)), s3fs.WithExpiry(SignExpiry), s3fs.WithContentSHA256(oid))
	if err != nil {
		return "", nil, err
	}
	return req.URL, signedHeaderMap(req.SignedHeader), nil
}

// VerifyPut checks the staged upload for oid and renames it to its final
// key. It succeeds without a staged object when the object is already in
// place, so re-verifying is safe.
func (s *presignStorage) VerifyPut(oid string, size int64) error {
	if info, err := s.fs.Stat(transformKey(oid)); err == nil {
		if info.Size() != size {
			return fmt.Errorf("%w: expected %d bytes, got %d bytes", ErrSizeMismatch, size, info.Size())
		}
		return nil
	}

	staged := stagingKey(oid)
	info, err := s.fs.Stat(staged)
	if err != nil {
		return err
	}
	if info.Size() != size {
		return fmt.Errorf("%w: expected %d bytes, got %d bytes", ErrSizeMismatch, size, info.Size())
	}

	path := transformKey(oid)
	if err := s.fs.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return err
	}
	return s.fs.Rename(staged, path)
}

// stagingKey is where presigned uploads land until they are verified.
func stagingKey(oid string) string {
	return filepath.Join("/tmp", oid)
}

// signedHeaderMap flattens the signed headers a presigned request requires.
func signedHeaderMap(h http.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	m := make(map[string]string, len(h))
	for k, vs := range h {
		m[k] = strings.Join(vs, ",")
	}
	return m
}

func transformKey(key string) string {
	if len(key) < 5 {
		return key
	}
	return filepath.Join(key[0:2], key[2:4], key[4:])
}
