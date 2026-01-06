package gitd

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
)

var (
	errHashMismatch = errors.New("Content hash does not match OID")
	errSizeMismatch = errors.New("Content size does not match")
)

// handlePutContent receives data from the client and puts it into the content store
func (h *Handler) handlePutContent(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
	if err := h.contentStore.Put(rv.Oid, r.Body, r.ContentLength); err != nil {
		http.Error(w, "Failed to store content", http.StatusInternalServerError)
		return
	}
}

// handleGetContent gets the content from the content store
func (h *Handler) handleGetContent(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
	content, stat, err := h.contentStore.Get(rv.Oid)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer content.Close()

	filename := r.URL.Query().Get("filename")
	if filename != "" {
		w.Header().Set("Content-Disposition", "attachment; filename="+strconv.Quote(filename))
	}

	http.ServeContent(w, r, "", stat.ModTime(), content)
}

func (h *Handler) handleVerifyObject(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
	info, err := h.contentStore.Info(rv.Oid)
	if err != nil {
		if !os.IsNotExist(err) {
			http.Error(w, "Failed to verify object", http.StatusInternalServerError)
			return
		}
		http.NotFound(w, r)
		return
	}

	if info.Size() != rv.Size {
		http.Error(w, "Size mismatch", http.StatusBadRequest)
		return
	}
}

// lfsContent provides a simple file system based storage.
type lfsContent struct {
	basePath string
}

// Get takes a Meta object and retreives the content from the store, returning
// it as an io.ReaderCloser. If fromByte > 0, the reader starts from that byte
func (s *lfsContent) Get(oid string) (io.ReadSeekCloser, os.FileInfo, error) {
	path := filepath.Join(s.basePath, transformKey(oid))

	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	stat, err := os.Stat(path)
	if err != nil {
		return nil, nil, err
	}
	return f, stat, nil
}

// Put takes a Meta object and an io.Reader and writes the content to the store.
func (s *lfsContent) Put(oid string, r io.Reader, size int64) error {
	path := filepath.Join(s.basePath, transformKey(oid))

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}

	file, err := os.CreateTemp(dir, "lfsd_tmp_")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())

	hash := sha256.New()
	hw := io.MultiWriter(hash, file)

	written, err := io.Copy(hw, r)
	if err != nil {
		file.Close()
		return err
	}
	file.Close()

	if written != size {
		return errSizeMismatch
	}

	shaStr := hex.EncodeToString(hash.Sum(nil))
	if shaStr != oid {
		return errHashMismatch
	}

	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	return nil
}

func (s *lfsContent) Info(oid string) (os.FileInfo, error) {
	path := filepath.Join(s.basePath, transformKey(oid))
	return os.Stat(path)
}

// Exists returns true if the object exists in the content store.
func (s *lfsContent) Exists(oid string) bool {
	path := filepath.Join(s.basePath, transformKey(oid))
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

func transformKey(key string) string {
	if len(key) < 5 {
		return key
	}
	return filepath.Join(key[0:2], key[2:4], key[4:])
}
