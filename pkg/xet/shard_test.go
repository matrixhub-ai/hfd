package xet_test

import (
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/xet"
)

// buildShardData constructs a synthetic shard binary blob with the given file entries.
type testFileEntry struct {
	fileHash [32]byte
	flags    uint32
	segments []testSegment
}

type testSegment struct {
	xorbHash      [32]byte
	xorbFlags     uint32
	unpackedBytes uint32
	chunkStart    uint32
	chunkEnd      uint32
}

func buildShardData(t *testing.T, files []testFileEntry) []byte {
	t.Helper()

	var buf []byte

	// MDBShardFileHeader (48 bytes)
	header := make([]byte, 48)
	// magic tag
	tag := []byte{
		'H', 'F', 'R', 'e', 'p', 'o', 'M', 'e',
		't', 'a', 'D', 'a', 't', 'a', 0x00,
		0x55, 0x69, 0x67, 0x45, 0x6a, 0x7b, 0x81, 0x57,
		0x83, 0xa5, 0xbd, 0xd9, 0x5c, 0xcd, 0xd1, 0x4a, 0xa9,
	}
	copy(header[0:32], tag)
	binary.LittleEndian.PutUint64(header[32:40], 2) // version
	binary.LittleEndian.PutUint64(header[40:48], 0) // footer_size
	buf = append(buf, header...)

	for _, f := range files {
		// FileDataSequenceHeader (48 bytes)
		fh := make([]byte, 48)
		copy(fh[0:32], f.fileHash[:])
		binary.LittleEndian.PutUint32(fh[32:36], f.flags)
		binary.LittleEndian.PutUint32(fh[36:40], uint32(len(f.segments)))
		binary.LittleEndian.PutUint64(fh[40:48], 0)
		buf = append(buf, fh...)

		// Entries
		for _, seg := range f.segments {
			entry := make([]byte, 48)
			copy(entry[0:32], seg.xorbHash[:])
			binary.LittleEndian.PutUint32(entry[32:36], seg.xorbFlags)
			binary.LittleEndian.PutUint32(entry[36:40], seg.unpackedBytes)
			binary.LittleEndian.PutUint32(entry[40:44], seg.chunkStart)
			binary.LittleEndian.PutUint32(entry[44:48], seg.chunkEnd)
			buf = append(buf, entry...)
		}

		// Verification entries if flag set
		if f.flags&1 != 0 {
			buf = append(buf, make([]byte, len(f.segments)*48)...)
		}
		// Metadata ext if flag set
		if f.flags&2 != 0 {
			buf = append(buf, make([]byte, 48)...)
		}
	}

	// Bookend (all 0xFF for file_hash)
	bookend := make([]byte, 48)
	for i := 0; i < 32; i++ {
		bookend[i] = 0xFF
	}
	buf = append(buf, bookend...)

	return buf
}

func hashFromHex(t *testing.T, s string) [32]byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex: %v", err)
	}
	var h [32]byte
	copy(h[:], b)
	return h
}

func TestParseShardBasic(t *testing.T) {
	fileHash := "aabbccdd00000000000000000000000000000000000000000000000000000001"
	xorbHash := "1122334400000000000000000000000000000000000000000000000000000002"

	data := buildShardData(t, []testFileEntry{
		{
			fileHash: hashFromHex(t, fileHash),
			flags:    0,
			segments: []testSegment{
				{
					xorbHash:      hashFromHex(t, xorbHash),
					xorbFlags:     0,
					unpackedBytes: 1024,
					chunkStart:    0,
					chunkEnd:      3,
				},
			},
		},
	})

	files, err := xet.ParseShard(data)
	if err != nil {
		t.Fatalf("ParseShard failed: %v", err)
	}

	if len(files) != 1 {
		t.Fatalf("Expected 1 file, got %d", len(files))
	}

	f := files[0]
	if f.FileHash != fileHash {
		t.Errorf("FileHash = %s, want %s", f.FileHash, fileHash)
	}
	if len(f.Segments) != 1 {
		t.Fatalf("Expected 1 segment, got %d", len(f.Segments))
	}

	seg := f.Segments[0]
	if seg.XorbHash != xorbHash {
		t.Errorf("XorbHash = %s, want %s", seg.XorbHash, xorbHash)
	}
	if seg.UnpackedSegmentBytes != 1024 {
		t.Errorf("UnpackedSegmentBytes = %d, want 1024", seg.UnpackedSegmentBytes)
	}
	if seg.ChunkIndexStart != 0 || seg.ChunkIndexEnd != 3 {
		t.Errorf("ChunkRange = [%d, %d), want [0, 3)", seg.ChunkIndexStart, seg.ChunkIndexEnd)
	}
}

func TestParseShardMultipleFiles(t *testing.T) {
	file1Hash := "aa00000000000000000000000000000000000000000000000000000000000001"
	file2Hash := "bb00000000000000000000000000000000000000000000000000000000000002"
	xorbHash1 := "cc00000000000000000000000000000000000000000000000000000000000003"
	xorbHash2 := "dd00000000000000000000000000000000000000000000000000000000000004"

	data := buildShardData(t, []testFileEntry{
		{
			fileHash: hashFromHex(t, file1Hash),
			flags:    0,
			segments: []testSegment{
				{
					xorbHash:      hashFromHex(t, xorbHash1),
					unpackedBytes: 500,
					chunkStart:    0,
					chunkEnd:      2,
				},
				{
					xorbHash:      hashFromHex(t, xorbHash2),
					unpackedBytes: 300,
					chunkStart:    0,
					chunkEnd:      1,
				},
			},
		},
		{
			fileHash: hashFromHex(t, file2Hash),
			flags:    0,
			segments: []testSegment{
				{
					xorbHash:      hashFromHex(t, xorbHash1),
					unpackedBytes: 200,
					chunkStart:    2,
					chunkEnd:      3,
				},
			},
		},
	})

	files, err := xet.ParseShard(data)
	if err != nil {
		t.Fatalf("ParseShard failed: %v", err)
	}

	if len(files) != 2 {
		t.Fatalf("Expected 2 files, got %d", len(files))
	}

	if files[0].FileHash != file1Hash {
		t.Errorf("files[0].FileHash = %s, want %s", files[0].FileHash, file1Hash)
	}
	if len(files[0].Segments) != 2 {
		t.Errorf("files[0] segments = %d, want 2", len(files[0].Segments))
	}

	if files[1].FileHash != file2Hash {
		t.Errorf("files[1].FileHash = %s, want %s", files[1].FileHash, file2Hash)
	}
	if len(files[1].Segments) != 1 {
		t.Errorf("files[1] segments = %d, want 1", len(files[1].Segments))
	}
}

func TestParseShardWithFlags(t *testing.T) {
	fileHash := "ee00000000000000000000000000000000000000000000000000000000000001"
	xorbHash := "ff00000000000000000000000000000000000000000000000000000000000002"

	// Test with verification flag (1) and metadata_ext flag (2) = flags 3
	data := buildShardData(t, []testFileEntry{
		{
			fileHash: hashFromHex(t, fileHash),
			flags:    3,
			segments: []testSegment{
				{
					xorbHash:      hashFromHex(t, xorbHash),
					unpackedBytes: 2048,
					chunkStart:    1,
					chunkEnd:      5,
				},
			},
		},
	})

	files, err := xet.ParseShard(data)
	if err != nil {
		t.Fatalf("ParseShard failed: %v", err)
	}

	if len(files) != 1 {
		t.Fatalf("Expected 1 file, got %d", len(files))
	}

	seg := files[0].Segments[0]
	if seg.ChunkIndexStart != 1 || seg.ChunkIndexEnd != 5 {
		t.Errorf("ChunkRange = [%d, %d), want [1, 5)", seg.ChunkIndexStart, seg.ChunkIndexEnd)
	}
}

func TestParseShardBadMagic(t *testing.T) {
	data := make([]byte, 48)
	copy(data, "BADMAGICBADMAGICBADMAGICBADMAGIC")

	_, err := xet.ParseShard(data)
	if err == nil {
		t.Fatal("Expected error for bad magic")
	}
}

func TestParseShardTruncated(t *testing.T) {
	_, err := xet.ParseShard([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("Expected error for truncated data")
	}
}

func TestParseShardBadVersion(t *testing.T) {
	data := make([]byte, 48)
	tag := []byte{
		'H', 'F', 'R', 'e', 'p', 'o', 'M', 'e',
		't', 'a', 'D', 'a', 't', 'a', 0x00,
		0x55, 0x69, 0x67, 0x45, 0x6a, 0x7b, 0x81, 0x57,
		0x83, 0xa5, 0xbd, 0xd9, 0x5c, 0xcd, 0xd1, 0x4a, 0xa9,
	}
	copy(data[0:32], tag)
	binary.LittleEndian.PutUint64(data[32:40], 99) // bad version

	_, err := xet.ParseShard(data)
	if err == nil {
		t.Fatal("Expected error for bad version")
	}
}

func TestParseShardEmpty(t *testing.T) {
	// Shard with only a bookend (no files)
	data := buildShardData(t, nil)

	files, err := xet.ParseShard(data)
	if err != nil {
		t.Fatalf("ParseShard failed: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("Expected 0 files, got %d", len(files))
	}
}
