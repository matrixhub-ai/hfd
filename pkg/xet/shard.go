package xet

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
)

const (
	merkleHashSize             = 32
	shardHeaderSize            = 48
	fileDataSequenceHeaderSize = 48
	fileDataSequenceEntrySize  = 48

	shardVersion = 2

	fileFlagVerification = 1
	fileFlagMetadataExt  = 2
)

var (
	// First 32 bytes of the shard header (magic tag).
	shardMagicTag = []byte{
		'H', 'F', 'R', 'e', 'p', 'o', 'M', 'e',
		't', 'a', 'D', 'a', 't', 'a', 0x00,
		0x55, 0x69, 0x67, 0x45, 0x6a, 0x7b, 0x81, 0x57,
		0x83, 0xa5, 0xbd, 0xd9, 0x5c, 0xcd, 0xd1, 0x4a, 0xa9,
	}

	errBadShardMagic   = errors.New("invalid shard magic tag")
	errBadShardVersion = errors.New("unsupported shard version")
	errShardTruncated  = errors.New("shard data truncated")
)

// FileSegment describes a range of chunks within a xorb needed to reconstruct part of a file.
type FileSegment struct {
	XorbHash             string
	XorbFlags            uint32
	UnpackedSegmentBytes uint32
	ChunkIndexStart      uint32
	ChunkIndexEnd        uint32
}

// FileReconstruction maps a file hash to the ordered list of xorb segments needed to reconstruct it.
type FileReconstruction struct {
	FileHash string
	Segments []FileSegment
}

// ParseShard parses a shard binary blob and extracts file reconstruction info.
func ParseShard(data []byte) ([]FileReconstruction, error) {
	if len(data) < shardHeaderSize {
		return nil, fmt.Errorf("%w: need at least %d bytes, got %d", errShardTruncated, shardHeaderSize, len(data))
	}

	// Validate magic tag (32 bytes)
	if len(shardMagicTag) != 32 {
		return nil, fmt.Errorf("internal: shardMagicTag length %d != 32", len(shardMagicTag))
	}
	for i := 0; i < 32; i++ {
		if data[i] != shardMagicTag[i] {
			return nil, fmt.Errorf("%w at byte %d: got 0x%02x, want 0x%02x", errBadShardMagic, i, data[i], shardMagicTag[i])
		}
	}

	// Read version (u64 LE at offset 32)
	version := binary.LittleEndian.Uint64(data[32:40])
	if version != shardVersion {
		return nil, fmt.Errorf("%w: got %d, want %d", errBadShardVersion, version, shardVersion)
	}

	// footer_size at offset 40 (u64 LE) - informational, not needed for parsing
	// _ = binary.LittleEndian.Uint64(data[40:48])

	offset := shardHeaderSize
	var results []FileReconstruction

	for {
		// Need at least fileDataSequenceHeaderSize bytes for the next header
		if offset+fileDataSequenceHeaderSize > len(data) {
			break
		}

		// Read FileDataSequenceHeader (48 bytes)
		fileHash := data[offset : offset+merkleHashSize]

		// Check for bookend (all 0xFF)
		allFF := true
		for _, b := range fileHash {
			if b != 0xFF {
				allFF = false
				break
			}
		}
		if allFF {
			// End of file section
			offset += fileDataSequenceHeaderSize
			break
		}

		fileFlags := binary.LittleEndian.Uint32(data[offset+32 : offset+36])
		numEntries := binary.LittleEndian.Uint32(data[offset+36 : offset+40])
		// _unused := binary.LittleEndian.Uint64(data[offset+40 : offset+48])
		offset += fileDataSequenceHeaderSize

		fileHashHex := hex.EncodeToString(fileHash)

		// Read entries
		segments := make([]FileSegment, 0, numEntries)
		for i := uint32(0); i < numEntries; i++ {
			if offset+fileDataSequenceEntrySize > len(data) {
				return nil, fmt.Errorf("%w: entry %d of file %s", errShardTruncated, i, fileHashHex)
			}
			xorbHash := hex.EncodeToString(data[offset : offset+merkleHashSize])
			xorbFlags := binary.LittleEndian.Uint32(data[offset+32 : offset+36])
			unpackedBytes := binary.LittleEndian.Uint32(data[offset+36 : offset+40])
			chunkStart := binary.LittleEndian.Uint32(data[offset+40 : offset+44])
			chunkEnd := binary.LittleEndian.Uint32(data[offset+44 : offset+48])
			offset += fileDataSequenceEntrySize

			segments = append(segments, FileSegment{
				XorbHash:             xorbHash,
				XorbFlags:            xorbFlags,
				UnpackedSegmentBytes: unpackedBytes,
				ChunkIndexStart:      chunkStart,
				ChunkIndexEnd:        chunkEnd,
			})
		}

		// Skip verification entries if flag set
		if fileFlags&fileFlagVerification != 0 {
			skip := int(numEntries) * fileDataSequenceEntrySize
			if offset+skip > len(data) {
				return nil, fmt.Errorf("%w: verification entries for file %s", errShardTruncated, fileHashHex)
			}
			offset += skip
		}

		// Skip metadata_ext if flag set (48 bytes)
		if fileFlags&fileFlagMetadataExt != 0 {
			if offset+48 > len(data) {
				return nil, fmt.Errorf("%w: metadata_ext for file %s", errShardTruncated, fileHashHex)
			}
			offset += 48
		}

		results = append(results, FileReconstruction{
			FileHash: fileHashHex,
			Segments: segments,
		})
	}

	return results, nil
}
