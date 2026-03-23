package xet

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
)

// Shard binary format constants.
// Reference: xet-core xet_core_structures/src/metadata_shard/shard_format.rs
const (
	// shardMagicTag is the 32-byte magic tag at the start of a shard file.
	// It starts with "HFRepoMetaData" followed by zero-padding.
	shardMagicSize = 32

	// shardVersion is the expected shard format version.
	shardVersion uint64 = 2

	// fileDataSequenceHeaderSize is 48 bytes: 32-byte file hash + 8-byte data length + 8-byte entry count.
	fileDataSequenceHeaderSize = 48

	// fileDataSequenceEntrySize is 40 bytes: 32-byte xorb hash + 4-byte start chunk + 4-byte end chunk.
	fileDataSequenceEntrySize = 40
)

var shardMagic = [shardMagicSize]byte{
	'H', 'F', 'R', 'e', 'p', 'o', 'M', 'e',
	't', 'a', 'D', 'a', 't', 'a', 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
}

// bookendBytes is the all-0xFF terminator.
var bookendBytes [fileDataSequenceHeaderSize]byte

func init() {
	for i := range bookendBytes {
		bookendBytes[i] = 0xFF
	}
}

// ShardFileEntry represents a single file entry parsed from a shard.
type ShardFileEntry struct {
	FileHash   string // 64-char hex hash identifying the file
	DataLength uint64
	Chunks     []ShardChunkRef
}

// ShardChunkRef describes a range of chunks within a xorb that belong to a file.
type ShardChunkRef struct {
	XorbHash   string // 64-char hex hash of the xorb
	StartChunk uint32 // inclusive
	EndChunk   uint32 // exclusive
}

// ParseShard parses a shard binary blob and returns the list of file entries.
func ParseShard(r io.Reader) ([]ShardFileEntry, error) {
	// Read and verify magic
	var magic [shardMagicSize]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return nil, fmt.Errorf("reading shard magic: %w", err)
	}
	if magic != shardMagic {
		return nil, fmt.Errorf("invalid shard magic")
	}

	// Read and verify version
	var version uint64
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return nil, fmt.Errorf("reading shard version: %w", err)
	}
	if version != shardVersion {
		return nil, fmt.Errorf("unsupported shard version %d, expected %d", version, shardVersion)
	}

	var entries []ShardFileEntry
	for {
		// Read file header (or bookend)
		var hdr [fileDataSequenceHeaderSize]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, fmt.Errorf("reading shard file header: %w", err)
		}

		// Check for bookend (all 0xFF)
		if hdr == bookendBytes {
			break
		}

		fileHash := hex.EncodeToString(hdr[0:32])
		dataLength := binary.LittleEndian.Uint64(hdr[32:40])
		entryCount := binary.LittleEndian.Uint64(hdr[40:48])

		entry := ShardFileEntry{
			FileHash:   fileHash,
			DataLength: dataLength,
			Chunks:     make([]ShardChunkRef, 0, entryCount),
		}

		for i := uint64(0); i < entryCount; i++ {
			var entryBuf [fileDataSequenceEntrySize]byte
			if _, err := io.ReadFull(r, entryBuf[:]); err != nil {
				return nil, fmt.Errorf("reading shard chunk entry: %w", err)
			}
			ref := ShardChunkRef{
				XorbHash:   hex.EncodeToString(entryBuf[0:32]),
				StartChunk: binary.LittleEndian.Uint32(entryBuf[32:36]),
				EndChunk:   binary.LittleEndian.Uint32(entryBuf[36:40]),
			}
			entry.Chunks = append(entry.Chunks, ref)
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

// HashShardContent computes a SHA-256 hash of the shard content for use as a storage key.
func HashShardContent(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
