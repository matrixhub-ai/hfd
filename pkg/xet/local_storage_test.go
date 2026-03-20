package xet_test

import (
	"bytes"
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

	// Xet uses its own content hash (not SHA-256 of raw data)
	data := []byte("hello xet cas world")
	oid := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	size := int64(len(data))

	// Test Exists for non-existent object
	if storage.Exists(oid) {
		t.Fatal("Expected object to not exist")
	}

	// Test Put (no hash verification since xet hash != SHA-256)
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

func TestCASStorageSizeMismatch(t *testing.T) {
	dir, err := os.MkdirTemp("", "xet-store-size-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	storage := xet.NewLocal(dir)

	data := []byte("some data")
	oid := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	err = storage.Put(oid, bytes.NewReader(data), 999)
	if err == nil {
		t.Fatal("Expected error for size mismatch")
	}
}
