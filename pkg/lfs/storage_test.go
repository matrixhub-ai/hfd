package lfs_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"testing"

	"github.com/go-git/go-billy/v6/osfs"

	"github.com/matrixhub-ai/hfd/pkg/lfs"
)

func TestContentStorage(t *testing.T) {
	dir, err := os.MkdirTemp("", "lfs-store-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	storage := lfs.New(osfs.New(dir))

	data := []byte("hello world")
	hash := sha256.Sum256(data)
	oid := hex.EncodeToString(hash[:])
	size := int64(len(data))

	// Test Exists for non-existent object
	if storage.Exists(oid) {
		t.Fatal("Expected object to not exist")
	}

	// Test Put
	if err := storage.Put(oid, bytes.NewReader(data), size); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Exists for existing object
	if !storage.Exists(oid) {
		t.Fatal("Expected object to exist after Put")
	}

	// Test Info
	info, err := storage.Info(oid)
	if err != nil {
		t.Fatalf("Info failed: %v", err)
	}
	if info.Size() != size {
		t.Fatalf("Info size = %d, want %d", info.Size(), size)
	}

	// Test Get (Content implements Getter)
	reader, stat, err := storage.(lfs.Getter).Get(oid)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer reader.Close()

	if stat.Size() != size {
		t.Fatalf("Get stat size = %d, want %d", stat.Size(), size)
	}

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get data = %q, want %q", got, data)
	}
}

func oidOf(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func TestPutSizeMismatch(t *testing.T) {
	storage := lfs.New(osfs.New(t.TempDir()))

	data := []byte("hello world")
	oid := oidOf(data)

	if err := storage.Put(oid, bytes.NewReader(data), int64(len(data))+1); err == nil {
		t.Fatal("expected error for size mismatch, got nil")
	}
	if storage.Exists(oid) {
		t.Fatal("object must not be stored after failed Put")
	}
}

func TestPutHashMismatch(t *testing.T) {
	storage := lfs.New(osfs.New(t.TempDir()))

	data := []byte("hello world")
	wrongOid := oidOf([]byte("other content"))

	if err := storage.Put(wrongOid, bytes.NewReader(data), int64(len(data))); err == nil {
		t.Fatal("expected error for hash mismatch, got nil")
	}
	if storage.Exists(wrongOid) {
		t.Fatal("object must not be stored after failed Put")
	}
}

func TestPutOverwriteIsIdempotent(t *testing.T) {
	storage := lfs.New(osfs.New(t.TempDir()))

	data := []byte("hello world")
	oid := oidOf(data)

	for i := 0; i < 2; i++ {
		if err := storage.Put(oid, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatalf("Put #%d failed: %v", i+1, err)
		}
	}
	if info, err := storage.Info(oid); err != nil {
		t.Fatalf("Info failed: %v", err)
	} else if info.Size() != int64(len(data)) {
		t.Fatalf("Info size = %d, want %d", info.Size(), len(data))
	}
}

func TestGetAndInfoMissingObject(t *testing.T) {
	storage := lfs.New(osfs.New(t.TempDir()))

	oid := oidOf([]byte("never stored"))
	if _, _, err := storage.(lfs.Getter).Get(oid); err == nil {
		t.Fatal("expected error for missing object Get, got nil")
	}
	if _, err := storage.Info(oid); err == nil {
		t.Fatal("expected error for missing object Info, got nil")
	}
}
