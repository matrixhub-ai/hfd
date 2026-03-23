package xet_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/xet"
)

func TestXorbPutAndGet(t *testing.T) {
	dir, err := os.MkdirTemp("", "xet-storage-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	storage := xet.NewStorage(dir)

	data := []byte("hello xorb content")
	prefix := "ab"
	hash := "abcdef1234567890"

	// Initially should not exist
	if storage.XorbExists(prefix, hash) {
		t.Error("Expected xorb to not exist before Put")
	}

	// Put xorb
	err = storage.PutXorb(prefix, hash, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("PutXorb failed: %v", err)
	}

	// Should exist now
	if !storage.XorbExists(prefix, hash) {
		t.Error("Expected xorb to exist after Put")
	}

	// Get and verify content
	reader, stat, err := storage.GetXorb(prefix, hash)
	if err != nil {
		t.Fatalf("GetXorb failed: %v", err)
	}
	defer reader.Close()

	if stat.Size() != int64(len(data)) {
		t.Errorf("Size mismatch: got %d, want %d", stat.Size(), len(data))
	}

	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read xorb content: %v", err)
	}

	if !bytes.Equal(content, data) {
		t.Errorf("Content mismatch: got %q, want %q", content, data)
	}
}

func TestXorbIdempotentPut(t *testing.T) {
	dir, err := os.MkdirTemp("", "xet-storage-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	storage := xet.NewStorage(dir)

	data := []byte("some content")
	prefix := "cd"
	hash := "cdef1234567890ab"

	// Put twice should succeed (idempotent behavior)
	err = storage.PutXorb(prefix, hash, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("First PutXorb failed: %v", err)
	}

	err = storage.PutXorb(prefix, hash, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("Second PutXorb failed: %v", err)
	}
}

func TestXorbSizeMismatch(t *testing.T) {
	dir, err := os.MkdirTemp("", "xet-storage-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	storage := xet.NewStorage(dir)

	data := []byte("short")
	prefix := "ef"
	hash := "ef1234567890abcd"

	// Put with wrong size should fail
	err = storage.PutXorb(prefix, hash, bytes.NewReader(data), 100)
	if err == nil {
		t.Error("Expected error for size mismatch, got nil")
	}
}

func TestXorbNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "xet-storage-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	storage := xet.NewStorage(dir)

	_, _, err = storage.GetXorb("ab", "nonexistent")
	if !os.IsNotExist(err) {
		t.Errorf("Expected not-exist error, got: %v", err)
	}
}

func TestShardPutAndGet(t *testing.T) {
	dir, err := os.MkdirTemp("", "xet-storage-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	storage := xet.NewStorage(dir)

	data := []byte("shard metadata content")

	// Put shard
	name, err := storage.PutShard(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("PutShard failed: %v", err)
	}

	if name == "" {
		t.Error("Expected non-empty shard name")
	}

	// Get and verify content
	reader, stat, err := storage.GetShard(name)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	defer reader.Close()

	if stat.Size() != int64(len(data)) {
		t.Errorf("Size mismatch: got %d, want %d", stat.Size(), len(data))
	}

	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read shard content: %v", err)
	}

	if !bytes.Equal(content, data) {
		t.Errorf("Content mismatch: got %q, want %q", content, data)
	}
}

func TestListShards(t *testing.T) {
	dir, err := os.MkdirTemp("", "xet-storage-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	storage := xet.NewStorage(dir)

	// List shards on empty dir should return empty
	shards, err := storage.ListShards()
	if err != nil {
		t.Fatalf("ListShards failed: %v", err)
	}
	if len(shards) != 0 {
		t.Errorf("Expected 0 shards, got %d", len(shards))
	}

	// Add some shards
	for i := 0; i < 3; i++ {
		data := []byte("shard data")
		_, err := storage.PutShard(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("PutShard failed: %v", err)
		}
	}

	shards, err = storage.ListShards()
	if err != nil {
		t.Fatalf("ListShards failed: %v", err)
	}
	if len(shards) != 3 {
		t.Errorf("Expected 3 shards, got %d", len(shards))
	}
}
