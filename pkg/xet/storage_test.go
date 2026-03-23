package xet

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestStorage_PutGetXorb(t *testing.T) {
	dir := t.TempDir()
	s := NewStorage(dir)

	data := []byte("test xorb content")
	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	// Put should succeed and return inserted=true
	inserted, err := s.PutXorb("default", hash, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("PutXorb: %v", err)
	}
	if !inserted {
		t.Fatal("expected was_inserted=true for new xorb")
	}

	// Second put should return inserted=false
	inserted, err = s.PutXorb("default", hash, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("PutXorb (duplicate): %v", err)
	}
	if inserted {
		t.Fatal("expected was_inserted=false for duplicate xorb")
	}

	// Xorb should exist
	if !s.XorbExists("default", hash) {
		t.Fatal("expected xorb to exist")
	}

	// Get should return the data
	rc, stat, err := s.GetXorb("default", hash)
	if err != nil {
		t.Fatalf("GetXorb: %v", err)
	}
	defer rc.Close()

	if stat.Size() != int64(len(data)) {
		t.Fatalf("expected size %d, got %d", len(data), stat.Size())
	}

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading xorb: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch: got %q, want %q", got, data)
	}
}

func TestStorage_PutGetShard(t *testing.T) {
	dir := t.TempDir()
	s := NewStorage(dir)

	data := []byte("test shard content")
	key := HashShardContent(data)

	// Put should succeed and return 1 (SyncPerformed)
	result, err := s.PutShard(key, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("PutShard: %v", err)
	}
	if result != 1 {
		t.Fatalf("expected result=1 for new shard, got %d", result)
	}

	// Second put should return 0 (already exists)
	result, err = s.PutShard(key, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("PutShard (duplicate): %v", err)
	}
	if result != 0 {
		t.Fatalf("expected result=0 for duplicate shard, got %d", result)
	}

	// Get should return the data
	rc, stat, err := s.GetShard(key)
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	defer rc.Close()

	if stat.Size() != int64(len(data)) {
		t.Fatalf("expected size %d, got %d", len(data), stat.Size())
	}

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading shard: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch: got %q, want %q", got, data)
	}
}

func TestStorage_XorbNotExists(t *testing.T) {
	dir := t.TempDir()
	s := NewStorage(dir)

	hash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if s.XorbExists("default", hash) {
		t.Fatal("expected xorb to not exist")
	}

	_, _, err := s.GetXorb("default", hash)
	if !os.IsNotExist(err) {
		t.Fatalf("expected not-exist error, got: %v", err)
	}
}

func TestStorage_InvalidHash(t *testing.T) {
	dir := t.TempDir()
	s := NewStorage(dir)

	_, err := s.PutXorb("default", "invalid", bytes.NewReader(nil), 0)
	if err == nil {
		t.Fatal("expected error for invalid hash")
	}
}

func TestStorage_SizeMismatch(t *testing.T) {
	dir := t.TempDir()
	s := NewStorage(dir)

	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	_, err := s.PutXorb("default", hash, bytes.NewReader([]byte("data")), 100)
	if err == nil {
		t.Fatal("expected error for size mismatch")
	}
}

func TestStorage_ShardDir(t *testing.T) {
	dir := t.TempDir()
	s := NewStorage(dir)

	expected := filepath.Join(dir, "shards")
	if s.ShardDir() != expected {
		t.Fatalf("expected shard dir %q, got %q", expected, s.ShardDir())
	}
}

func TestIsValidHex64(t *testing.T) {
	tests := []struct {
		input string
		valid bool
	}{
		{"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", true},
		{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true},
		{"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", false}, // uppercase
		{"short", false},
		{"", false},
		{"gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg", false}, // invalid hex
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := isValidHex64(tt.input); got != tt.valid {
				t.Fatalf("isValidHex64(%q) = %v, want %v", tt.input, got, tt.valid)
			}
		})
	}
}

func TestTransformKey(t *testing.T) {
	key := "0123456789abcdef"
	expected := filepath.Join("01", "23", "456789abcdef")
	if got := transformKey(key); got != expected {
		t.Fatalf("transformKey(%q) = %q, want %q", key, got, expected)
	}

	// Short key should be returned as-is
	if got := transformKey("ab"); got != "ab" {
		t.Fatalf("transformKey(%q) = %q, want %q", "ab", got, "ab")
	}
}

func TestHashShardContent(t *testing.T) {
	data := []byte("test data")
	hash := HashShardContent(data)
	if len(hash) != 64 {
		t.Fatalf("expected 64-char hex hash, got %q (len=%d)", hash, len(hash))
	}
	// Same content should give same hash
	hash2 := HashShardContent(data)
	if hash != hash2 {
		t.Fatal("same content should give same hash")
	}
	// Different content should give different hash
	hash3 := HashShardContent([]byte("different"))
	if hash == hash3 {
		t.Fatal("different content should give different hash")
	}
}

// buildTestShard creates a minimal valid shard binary for testing.
func buildTestShard(entries []ShardFileEntry) []byte {
	var buf bytes.Buffer

	// Magic
	buf.Write(shardMagic[:])

	// Version
	binary.Write(&buf, binary.LittleEndian, shardVersion)

	// File entries
	for _, entry := range entries {
		// File hash (32 bytes)
		hashBytes, _ := hex.DecodeString(entry.FileHash)
		if len(hashBytes) < 32 {
			padded := make([]byte, 32)
			copy(padded, hashBytes)
			hashBytes = padded
		}
		buf.Write(hashBytes[:32])

		// Data length
		binary.Write(&buf, binary.LittleEndian, entry.DataLength)

		// Entry count
		binary.Write(&buf, binary.LittleEndian, uint64(len(entry.Chunks)))

		// Chunk entries
		for _, chunk := range entry.Chunks {
			xorbHashBytes, _ := hex.DecodeString(chunk.XorbHash)
			if len(xorbHashBytes) < 32 {
				padded := make([]byte, 32)
				copy(padded, xorbHashBytes)
				xorbHashBytes = padded
			}
			buf.Write(xorbHashBytes[:32])
			binary.Write(&buf, binary.LittleEndian, chunk.StartChunk)
			binary.Write(&buf, binary.LittleEndian, chunk.EndChunk)
		}
	}

	// Bookend
	buf.Write(bookendBytes[:])

	return buf.Bytes()
}

func TestParseShard(t *testing.T) {
	fileHash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	xorbHash := "1111111111111111111111111111111111111111111111111111111111111111"

	testEntries := []ShardFileEntry{
		{
			FileHash:   fileHash,
			DataLength: 1024,
			Chunks: []ShardChunkRef{
				{XorbHash: xorbHash, StartChunk: 0, EndChunk: 5},
			},
		},
	}

	data := buildTestShard(testEntries)
	entries, err := ParseShard(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ParseShard: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	if entries[0].FileHash != fileHash {
		t.Fatalf("expected file hash %q, got %q", fileHash, entries[0].FileHash)
	}

	if entries[0].DataLength != 1024 {
		t.Fatalf("expected data length 1024, got %d", entries[0].DataLength)
	}

	if len(entries[0].Chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(entries[0].Chunks))
	}

	if entries[0].Chunks[0].XorbHash != xorbHash {
		t.Fatalf("expected xorb hash %q, got %q", xorbHash, entries[0].Chunks[0].XorbHash)
	}

	if entries[0].Chunks[0].StartChunk != 0 || entries[0].Chunks[0].EndChunk != 5 {
		t.Fatalf("expected chunk range [0, 5), got [%d, %d)", entries[0].Chunks[0].StartChunk, entries[0].Chunks[0].EndChunk)
	}
}

func TestParseShard_MultipleEntries(t *testing.T) {
	fileHash1 := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	fileHash2 := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	xorbHash := "1111111111111111111111111111111111111111111111111111111111111111"

	testEntries := []ShardFileEntry{
		{
			FileHash:   fileHash1,
			DataLength: 500,
			Chunks: []ShardChunkRef{
				{XorbHash: xorbHash, StartChunk: 0, EndChunk: 3},
			},
		},
		{
			FileHash:   fileHash2,
			DataLength: 800,
			Chunks: []ShardChunkRef{
				{XorbHash: xorbHash, StartChunk: 3, EndChunk: 8},
			},
		},
	}

	data := buildTestShard(testEntries)
	entries, err := ParseShard(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ParseShard: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	if entries[0].FileHash != fileHash1 {
		t.Fatalf("expected first file hash %q, got %q", fileHash1, entries[0].FileHash)
	}

	if entries[1].FileHash != fileHash2 {
		t.Fatalf("expected second file hash %q, got %q", fileHash2, entries[1].FileHash)
	}
}

func TestParseShard_InvalidMagic(t *testing.T) {
	data := []byte("not a shard")
	_, err := ParseShard(bytes.NewReader(data))
	if err == nil {
		t.Fatal("expected error for invalid magic")
	}
}

func TestParseShard_InvalidVersion(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(shardMagic[:])
	binary.Write(&buf, binary.LittleEndian, uint64(99)) // unsupported version
	buf.Write(bookendBytes[:])

	_, err := ParseShard(bytes.NewReader(buf.Bytes()))
	if err == nil {
		t.Fatal("expected error for invalid version")
	}
}

func TestParseShard_EmptyShard(t *testing.T) {
	testEntries := []ShardFileEntry{}
	data := buildTestShard(testEntries)
	entries, err := ParseShard(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ParseShard: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
}
