package xet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var (
	xorbMagic    = []byte("XETBLOB")
	hashMagic    = []byte("XBLBHSH")
	boundMagic   = []byte("XBLBMDT")
	errBadMagic  = errors.New("invalid xorb magic")
	errBadFooter = errors.New("invalid xorb footer")
)

// maxXorbChunks is the maximum number of chunks in a xorb (safety limit).
// Xet-core MAX_XORB_CHUNKS is 8*1024, using 1M as a generous upper bound.
const maxXorbChunks = 1_000_000

// XorbFooter holds the parsed footer from a xorb file.
type XorbFooter struct {
	NumChunks            uint32
	ChunkBoundaryOffsets []uint32 // compressed byte boundaries per chunk
	UnpackedChunkOffsets []uint32 // cumulative uncompressed sizes per chunk
}

// GetByteOffset returns the compressed byte range [start, end) for chunks [chunkStart, chunkEnd).
// chunkStart and chunkEnd use 0-based chunk indices matching the shard's chunk_index_start/end.
func (f *XorbFooter) GetByteOffset(chunkStart, chunkEnd uint32) (start, end uint32, err error) {
	if chunkEnd > f.NumChunks {
		return 0, 0, fmt.Errorf("chunkEnd %d exceeds num_chunks %d", chunkEnd, f.NumChunks)
	}
	if chunkStart > chunkEnd {
		return 0, 0, fmt.Errorf("chunkStart %d > chunkEnd %d", chunkStart, chunkEnd)
	}

	if chunkStart == 0 {
		start = 0
	} else {
		start = f.ChunkBoundaryOffsets[chunkStart-1]
	}
	if chunkEnd == 0 {
		end = 0
	} else {
		end = f.ChunkBoundaryOffsets[chunkEnd-1]
	}
	return start, end, nil
}

// UnpackedChunkLength returns the uncompressed length of a single chunk at the given 0-based index.
func (f *XorbFooter) UnpackedChunkLength(chunkIdx uint32) (uint32, error) {
	if chunkIdx >= f.NumChunks {
		return 0, fmt.Errorf("chunkIdx %d >= num_chunks %d", chunkIdx, f.NumChunks)
	}
	if chunkIdx == 0 {
		return f.UnpackedChunkOffsets[0], nil
	}
	return f.UnpackedChunkOffsets[chunkIdx] - f.UnpackedChunkOffsets[chunkIdx-1], nil
}

// TotalUnpackedSize returns the total uncompressed size across chunks [chunkStart, chunkEnd).
func (f *XorbFooter) TotalUnpackedSize(chunkStart, chunkEnd uint32) (uint32, error) {
	if chunkEnd > f.NumChunks {
		return 0, fmt.Errorf("chunkEnd %d exceeds num_chunks %d", chunkEnd, f.NumChunks)
	}
	if chunkStart > chunkEnd {
		return 0, fmt.Errorf("chunkStart %d > chunkEnd %d", chunkStart, chunkEnd)
	}
	if chunkEnd == 0 {
		return 0, nil
	}
	endSize := f.UnpackedChunkOffsets[chunkEnd-1]
	var startSize uint32
	if chunkStart > 0 {
		startSize = f.UnpackedChunkOffsets[chunkStart-1]
	}
	return endSize - startSize, nil
}

// ParseXorbFooter reads a xorb footer from a ReadSeeker (typically an open xorb file).
// The footer is at the end of the file.
func ParseXorbFooter(rs io.ReadSeeker) (*XorbFooter, error) {
	// Read last 4 bytes: info_length
	if _, err := rs.Seek(-4, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek to info_length: %w", err)
	}
	var infoLength uint32
	if err := binary.Read(rs, binary.LittleEndian, &infoLength); err != nil {
		return nil, fmt.Errorf("read info_length: %w", err)
	}

	// Seek to start of info block
	if _, err := rs.Seek(-int64(infoLength)-4, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek to info block: %w", err)
	}

	// Read ident (7 bytes) + version (1 byte)
	ident := make([]byte, 7)
	if _, err := io.ReadFull(rs, ident); err != nil {
		return nil, fmt.Errorf("read ident: %w", err)
	}
	if string(ident) != string(xorbMagic) {
		return nil, fmt.Errorf("%w: got %q", errBadMagic, ident)
	}
	var version uint8
	if err := binary.Read(rs, binary.LittleEndian, &version); err != nil {
		return nil, fmt.Errorf("read version: %w", err)
	}
	if version != 1 {
		return nil, fmt.Errorf("%w: unsupported version %d", errBadFooter, version)
	}

	// Read xorb_hash (32 bytes) - skip
	if _, err := rs.Seek(32, io.SeekCurrent); err != nil {
		return nil, fmt.Errorf("skip xorb_hash: %w", err)
	}

	// Read hash section ident (7 bytes) + version (1 byte)
	hashIdent := make([]byte, 7)
	if _, err := io.ReadFull(rs, hashIdent); err != nil {
		return nil, fmt.Errorf("read hash ident: %w", err)
	}
	if string(hashIdent) != string(hashMagic) {
		return nil, fmt.Errorf("%w: bad hash section magic %q", errBadFooter, hashIdent)
	}
	var hashVersion uint8
	if err := binary.Read(rs, binary.LittleEndian, &hashVersion); err != nil {
		return nil, fmt.Errorf("read hash version: %w", err)
	}
	if hashVersion != 1 {
		return nil, fmt.Errorf("%w: unsupported hash version %d", errBadFooter, hashVersion)
	}

	// Read num_chunks_2
	var numChunks2 uint32
	if err := binary.Read(rs, binary.LittleEndian, &numChunks2); err != nil {
		return nil, fmt.Errorf("read num_chunks_2: %w", err)
	}
	if numChunks2 > maxXorbChunks {
		return nil, fmt.Errorf("%w: num_chunks %d exceeds limit %d", errBadFooter, numChunks2, maxXorbChunks)
	}

	// Skip chunk_hashes: numChunks2 * 32 bytes
	if _, err := rs.Seek(int64(numChunks2)*32, io.SeekCurrent); err != nil {
		return nil, fmt.Errorf("skip chunk_hashes: %w", err)
	}

	// Read boundary section ident (7 bytes) + version (1 byte)
	boundIdent := make([]byte, 7)
	if _, err := io.ReadFull(rs, boundIdent); err != nil {
		return nil, fmt.Errorf("read boundary ident: %w", err)
	}
	if string(boundIdent) != string(boundMagic) {
		return nil, fmt.Errorf("%w: bad boundary section magic %q", errBadFooter, boundIdent)
	}
	var boundVersion uint8
	if err := binary.Read(rs, binary.LittleEndian, &boundVersion); err != nil {
		return nil, fmt.Errorf("read boundary version: %w", err)
	}
	if boundVersion != 1 {
		return nil, fmt.Errorf("%w: unsupported boundary version %d", errBadFooter, boundVersion)
	}

	// Read num_chunks_3 (must equal num_chunks_2)
	var numChunks3 uint32
	if err := binary.Read(rs, binary.LittleEndian, &numChunks3); err != nil {
		return nil, fmt.Errorf("read num_chunks_3: %w", err)
	}
	if numChunks3 != numChunks2 {
		return nil, fmt.Errorf("%w: num_chunks mismatch: %d vs %d", errBadFooter, numChunks2, numChunks3)
	}

	// Read chunk_boundary_offsets: numChunks3 * u32
	boundaryOffsets := make([]uint32, numChunks3)
	if err := binary.Read(rs, binary.LittleEndian, boundaryOffsets); err != nil {
		return nil, fmt.Errorf("read chunk_boundary_offsets: %w", err)
	}

	// Read unpacked_chunk_offsets: numChunks3 * u32
	unpackedOffsets := make([]uint32, numChunks3)
	if err := binary.Read(rs, binary.LittleEndian, unpackedOffsets); err != nil {
		return nil, fmt.Errorf("read unpacked_chunk_offsets: %w", err)
	}

	// Read trailing fields: num_chunks, hashes_offset, boundary_offset, _buffer(16), info_length
	var numChunks uint32
	if err := binary.Read(rs, binary.LittleEndian, &numChunks); err != nil {
		return nil, fmt.Errorf("read num_chunks: %w", err)
	}
	if numChunks != numChunks2 {
		return nil, fmt.Errorf("%w: final num_chunks mismatch: %d vs %d", errBadFooter, numChunks, numChunks2)
	}

	return &XorbFooter{
		NumChunks:            numChunks,
		ChunkBoundaryOffsets: boundaryOffsets,
		UnpackedChunkOffsets: unpackedOffsets,
	}, nil
}
