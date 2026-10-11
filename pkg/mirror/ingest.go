package mirror

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/go-git/go-git/v6/utils/ioutil"
	xetstorage "github.com/wzshiming/xet/storage"
)

// uploadPattern names the staging files of uploads under uploadDir.
const uploadPattern = "upload-*"

func (m *Mirror) uploadDir() string {
	return filepath.Join(m.dataDir, "uploads")
}

// removeStaleUploads drops the staging files of uploads that a crash
// interrupted; nothing is in flight while the mirror is constructed.
func (m *Mirror) removeStaleUploads() error {
	dir := m.uploadDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("list stale uploads: %w", err)
	}
	for _, e := range entries {
		if stale, _ := filepath.Match(uploadPattern, e.Name()); !stale || e.IsDir() {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove stale upload: %w", err)
		}
	}
	return nil
}

// PutObject stages the stream in its own temporary file, verifies it against
// the OID and size, and only then ingests it into the xet storage as
// chunk-deduplicated xorbs and shards; the object is then servable by OID.
func (m *Mirror) PutObject(ctx context.Context, oid string, r io.Reader, size int64) error {
	digest, ok := parseOID(oid)
	if !ok {
		return fmt.Errorf("invalid OID %q", oid)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("stage object: %w", err)
	}

	dir := m.uploadDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create upload dir: %w", err)
	}
	f, err := os.CreateTemp(dir, uploadPattern)
	if err != nil {
		return fmt.Errorf("create upload file: %w", err)
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}()

	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(hash, f), ioutil.NewContextReader(ctx, r))
	if err != nil {
		return fmt.Errorf("stage object: %w", err)
	}
	if size >= 0 && written != size {
		return fmt.Errorf("content size does not match: expected %d bytes, got %d", size, written)
	}
	if !bytes.Equal(hash.Sum(nil), digest[:]) {
		return fmt.Errorf("content hash does not match OID %s", hex.EncodeToString(digest[:]))
	}
	// Empty files carry no SHA-256 in xet shards, so storing one would record nothing servable.
	if written == 0 {
		return nil
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind upload file: %w", err)
	}
	if _, err := xetstorage.PutFile(ctx, m.xetStorage, f); err != nil {
		return fmt.Errorf("ingest object %s: %w", oid, err)
	}
	return nil
}
