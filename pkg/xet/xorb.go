package xet

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Xorb binary format constants.
// Reference: xet-core xet_core_structures/src/xorb_object/xorb_object_format.rs
const (
	// xorbMagic is the 7-byte magic string at the end of a xorb footer.
	xorbMagicString = "XETBLOB"
	xorbMagicSize   = 7

	// The last 4 bytes of a xorb file are info_length (u32 LE),
	// indicating how many bytes before it constitute the footer.
	xorbInfoLengthSize = 4
)

// XorbChunkBoundary represents a single chunk boundary within a xorb.
type XorbChunkBoundary struct {
	Offset uint64 // byte offset of this chunk within the unpacked data
}

// ParseXorbFooter reads a xorb file (or its tail) and extracts chunk boundaries.
// The footer is at the end of the file. The last 4 bytes are the info_length (u32 LE),
// and the footer payload precedes those 4 bytes.
func ParseXorbFooter(r io.ReadSeeker) ([]XorbChunkBoundary, error) {
	// Seek to the end to find the file size
	size, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seeking to end: %w", err)
	}

	if size < int64(xorbInfoLengthSize) {
		return nil, fmt.Errorf("xorb file too small (%d bytes)", size)
	}

	// Read info_length from the last 4 bytes
	if _, err := r.Seek(size-int64(xorbInfoLengthSize), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking to info_length: %w", err)
	}
	var infoLength uint32
	if err := binary.Read(r, binary.LittleEndian, &infoLength); err != nil {
		return nil, fmt.Errorf("reading info_length: %w", err)
	}

	footerEnd := size - int64(xorbInfoLengthSize)
	footerStart := footerEnd - int64(infoLength)
	if footerStart < 0 {
		return nil, fmt.Errorf("invalid info_length %d for file size %d", infoLength, size)
	}

	// Seek to footer start and read it
	if _, err := r.Seek(footerStart, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking to footer: %w", err)
	}
	footer := make([]byte, infoLength)
	if _, err := io.ReadFull(r, footer); err != nil {
		return nil, fmt.Errorf("reading footer: %w", err)
	}

	// Verify magic at the start of footer
	if len(footer) < xorbMagicSize+1 {
		return nil, fmt.Errorf("footer too small")
	}
	if string(footer[:xorbMagicSize]) != xorbMagicString {
		return nil, fmt.Errorf("invalid xorb magic: %q", string(footer[:xorbMagicSize]))
	}

	// Version byte follows magic
	version := footer[xorbMagicSize]
	if version != 1 {
		return nil, fmt.Errorf("unsupported xorb footer version %d", version)
	}

	// After magic(7) + version(1), the rest is chunk boundaries as u64 LE values
	boundaryData := footer[xorbMagicSize+1:]
	numBoundaries := len(boundaryData) / 8

	boundaries := make([]XorbChunkBoundary, 0, numBoundaries)
	for i := 0; i < numBoundaries; i++ {
		offset := binary.LittleEndian.Uint64(boundaryData[i*8 : (i+1)*8])
		boundaries = append(boundaries, XorbChunkBoundary{Offset: offset})
	}

	return boundaries, nil
}

// XorbContentRange returns the byte range within a xorb file for the given chunk range [startChunk, endChunk).
// boundaries must be the full list of chunk boundaries parsed from the xorb footer.
func XorbContentRange(boundaries []XorbChunkBoundary, startChunk, endChunk uint32) (startByte, endByte uint64, err error) {
	if len(boundaries) == 0 {
		return 0, 0, fmt.Errorf("no chunk boundaries")
	}
	if int(endChunk) > len(boundaries) {
		return 0, 0, fmt.Errorf("endChunk %d exceeds boundary count %d", endChunk, len(boundaries))
	}

	if startChunk == 0 {
		startByte = 0
	} else {
		startByte = boundaries[startChunk-1].Offset
	}

	endByte = boundaries[endChunk-1].Offset
	return startByte, endByte, nil
}
