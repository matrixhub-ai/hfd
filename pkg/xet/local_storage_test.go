package xet_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/xet"
)

func TestCASStorage(t *testing.T) {
	dir, err := os.MkdirTemp("", "xet-store-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	storage := xet.NewLocal(dir)

	data := []byte("hello xet cas world")
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

	// Test Get
	reader, stat, err := storage.Get(oid)
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

func TestCASStorageHashMismatch(t *testing.T) {
	dir, err := os.MkdirTemp("", "xet-store-hash-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	storage := xet.NewLocal(dir)

	data := []byte("some data")
	wrongOID := "0000000000000000000000000000000000000000000000000000000000000000"
	size := int64(len(data))

	err = storage.Put(wrongOID, bytes.NewReader(data), size)
	if err == nil {
		t.Fatal("Expected error for hash mismatch")
	}
}

func TestCASStorageSizeMismatch(t *testing.T) {
	dir, err := os.MkdirTemp("", "xet-store-size-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	storage := xet.NewLocal(dir)

	data := []byte("some data")
	hash := sha256.Sum256(data)
	oid := hex.EncodeToString(hash[:])

	err = storage.Put(oid, bytes.NewReader(data), 999)
	if err == nil {
		t.Fatal("Expected error for size mismatch")
	}
}
