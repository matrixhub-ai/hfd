package xet_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/xet"
)

// buildXorbFile constructs a synthetic xorb file with a valid footer.
// chunkData is the raw compressed content before the footer.
// boundaryOffsets and unpackedOffsets define per-chunk cumulative sizes.
func buildXorbFile(t *testing.T, chunkData []byte, boundaryOffsets, unpackedOffsets []uint32) []byte {
	t.Helper()
	if len(boundaryOffsets) != len(unpackedOffsets) {
		t.Fatal("boundary and unpacked offsets must have same length")
	}
	numChunks := uint32(len(boundaryOffsets))

	var info bytes.Buffer

	// ident + version
	info.Write([]byte("XETBLOB"))
	info.WriteByte(1)

	// xorb_hash: 32 zero bytes
	info.Write(make([]byte, 32))

	// hash section
	info.Write([]byte("XBLBHSH"))
	info.WriteByte(1)
	_ = binary.Write(&info, binary.LittleEndian, numChunks)
	// chunk_hashes: numChunks * 32 bytes of zeros
	info.Write(make([]byte, int(numChunks)*32))

	// boundary section
	info.Write([]byte("XBLBMDT"))
	info.WriteByte(1)
	_ = binary.Write(&info, binary.LittleEndian, numChunks)
	_ = binary.Write(&info, binary.LittleEndian, boundaryOffsets)
	_ = binary.Write(&info, binary.LittleEndian, unpackedOffsets)

	// trailing: num_chunks, hashes_offset, boundary_offset, _buffer(16)
	_ = binary.Write(&info, binary.LittleEndian, numChunks)
	_ = binary.Write(&info, binary.LittleEndian, uint32(0)) // hashes_section_offset
	_ = binary.Write(&info, binary.LittleEndian, uint32(0)) // boundary_section_offset
	info.Write(make([]byte, 16))                            // _buffer

	infoBytes := info.Bytes()

	var buf bytes.Buffer
	buf.Write(chunkData)
	buf.Write(infoBytes)
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(infoBytes)))

	return buf.Bytes()
}

func TestParseXorbFooter(t *testing.T) {
	// 3 chunks with boundary offsets [100, 250, 400] and unpacked [200, 500, 900]
	boundaries := []uint32{100, 250, 400}
	unpacked := []uint32{200, 500, 900}
	data := buildXorbFile(t, []byte("fake compressed data"), boundaries, unpacked)

	rs := bytes.NewReader(data)
	footer, err := xet.ParseXorbFooter(rs)
	if err != nil {
		t.Fatalf("ParseXorbFooter failed: %v", err)
	}

	if footer.NumChunks != 3 {
		t.Fatalf("NumChunks = %d, want 3", footer.NumChunks)
	}

	// Test ChunkBoundaryOffsets
	for i, want := range boundaries {
		if footer.ChunkBoundaryOffsets[i] != want {
			t.Errorf("ChunkBoundaryOffsets[%d] = %d, want %d", i, footer.ChunkBoundaryOffsets[i], want)
		}
	}

	// Test UnpackedChunkOffsets
	for i, want := range unpacked {
		if footer.UnpackedChunkOffsets[i] != want {
			t.Errorf("UnpackedChunkOffsets[%d] = %d, want %d", i, footer.UnpackedChunkOffsets[i], want)
		}
	}
}

func TestXorbFooterGetByteOffset(t *testing.T) {
	boundaries := []uint32{100, 250, 400}
	unpacked := []uint32{200, 500, 900}
	data := buildXorbFile(t, []byte("data"), boundaries, unpacked)

	rs := bytes.NewReader(data)
	footer, err := xet.ParseXorbFooter(rs)
	if err != nil {
		t.Fatalf("ParseXorbFooter failed: %v", err)
	}

	tests := []struct {
		start, end     uint32
		wantStart, wantEnd uint32
	}{
		{0, 1, 0, 100},     // first chunk
		{0, 2, 0, 250},     // first two chunks
		{0, 3, 0, 400},     // all chunks
		{1, 3, 100, 400},   // last two chunks
		{1, 2, 100, 250},   // middle chunk
		{2, 3, 250, 400},   // last chunk
	}

	for _, tc := range tests {
		s, e, err := footer.GetByteOffset(tc.start, tc.end)
		if err != nil {
			t.Errorf("GetByteOffset(%d, %d) error: %v", tc.start, tc.end, err)
			continue
		}
		if s != tc.wantStart || e != tc.wantEnd {
			t.Errorf("GetByteOffset(%d, %d) = (%d, %d), want (%d, %d)",
				tc.start, tc.end, s, e, tc.wantStart, tc.wantEnd)
		}
	}

	// Error cases
	if _, _, err := footer.GetByteOffset(0, 4); err == nil {
		t.Error("Expected error for chunkEnd > NumChunks")
	}
	if _, _, err := footer.GetByteOffset(2, 1); err == nil {
		t.Error("Expected error for chunkStart > chunkEnd")
	}
}

func TestXorbFooterUnpackedChunkLength(t *testing.T) {
	boundaries := []uint32{100, 250, 400}
	unpacked := []uint32{200, 500, 900}
	data := buildXorbFile(t, []byte("data"), boundaries, unpacked)

	rs := bytes.NewReader(data)
	footer, err := xet.ParseXorbFooter(rs)
	if err != nil {
		t.Fatalf("ParseXorbFooter failed: %v", err)
	}

	tests := []struct {
		idx  uint32
		want uint32
	}{
		{0, 200},
		{1, 300}, // 500 - 200
		{2, 400}, // 900 - 500
	}

	for _, tc := range tests {
		got, err := footer.UnpackedChunkLength(tc.idx)
		if err != nil {
			t.Errorf("UnpackedChunkLength(%d) error: %v", tc.idx, err)
			continue
		}
		if got != tc.want {
			t.Errorf("UnpackedChunkLength(%d) = %d, want %d", tc.idx, got, tc.want)
		}
	}

	if _, err := footer.UnpackedChunkLength(3); err == nil {
		t.Error("Expected error for idx >= NumChunks")
	}
}

func TestXorbFooterTotalUnpackedSize(t *testing.T) {
	boundaries := []uint32{100, 250, 400}
	unpacked := []uint32{200, 500, 900}
	data := buildXorbFile(t, []byte("data"), boundaries, unpacked)

	rs := bytes.NewReader(data)
	footer, err := xet.ParseXorbFooter(rs)
	if err != nil {
		t.Fatalf("ParseXorbFooter failed: %v", err)
	}

	tests := []struct {
		start, end uint32
		want       uint32
	}{
		{0, 3, 900},
		{0, 1, 200},
		{1, 3, 700},
		{1, 2, 300},
		{0, 0, 0},
	}

	for _, tc := range tests {
		got, err := footer.TotalUnpackedSize(tc.start, tc.end)
		if err != nil {
			t.Errorf("TotalUnpackedSize(%d, %d) error: %v", tc.start, tc.end, err)
			continue
		}
		if got != tc.want {
			t.Errorf("TotalUnpackedSize(%d, %d) = %d, want %d", tc.start, tc.end, got, tc.want)
		}
	}
}

func TestParseXorbFooterBadMagic(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte("BADDATA"))
	buf.WriteByte(1)
	buf.Write(make([]byte, 32))
	infoBytes := buf.Bytes()

	var file bytes.Buffer
	file.Write(infoBytes)
	_ = binary.Write(&file, binary.LittleEndian, uint32(len(infoBytes)))

	rs := bytes.NewReader(file.Bytes())
	_, err := xet.ParseXorbFooter(rs)
	if err == nil {
		t.Fatal("Expected error for bad magic")
	}
}
