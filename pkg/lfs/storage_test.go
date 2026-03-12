package lfs_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"testing"

	"github.com/wzshiming/hfd/pkg/lfs"
)

func TestContentStorage(t *testing.T) {
	dir, err := os.MkdirTemp("", "lfs-store-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	store := lfs.NewLocal(dir)

	data := []byte("hello world")
	hash := sha256.Sum256(data)
	oid := hex.EncodeToString(hash[:])
	size := int64(len(data))

	// Test Exists for non-existent object
	if store.Exists(oid) {
		t.Fatal("Expected object to not exist")
	}

	// Test Put
	if err := store.Put(oid, bytes.NewReader(data), size); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Exists for existing object
	if !store.Exists(oid) {
		t.Fatal("Expected object to exist after Put")
	}

	// Test Info
	info, err := store.Info(oid)
	if err != nil {
		t.Fatalf("Info failed: %v", err)
	}
	if info.Size() != size {
		t.Fatalf("Info size = %d, want %d", info.Size(), size)
	}

	// Test Get (Content implements Getter)
	reader, stat, err := store.(lfs.Getter).Get(oid)
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
