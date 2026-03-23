package xet

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// buildTestXorb creates a minimal xorb binary with a footer for testing.
func buildTestXorb(content []byte, boundaries []uint64) []byte {
	var buf bytes.Buffer

	// Write content data
	buf.Write(content)

	// Build footer: magic(7) + version(1) + boundaries(8 each)
	var footer bytes.Buffer
	footer.Write([]byte(xorbMagicString))
	footer.WriteByte(1) // version
	for _, b := range boundaries {
		binary.Write(&footer, binary.LittleEndian, b)
	}

	footerBytes := footer.Bytes()
	buf.Write(footerBytes)

	// Write info_length (size of footer, as u32 LE)
	infoLength := uint32(len(footerBytes))
	binary.Write(&buf, binary.LittleEndian, infoLength)

	return buf.Bytes()
}

func TestParseXorbFooter(t *testing.T) {
	content := []byte("chunk0chunk1chunk2")
	// Boundaries: each chunk is 6 bytes, boundaries at [6, 12, 18]
	boundaries := []uint64{6, 12, 18}

	data := buildTestXorb(content, boundaries)
	reader := bytes.NewReader(data)

	parsed, err := ParseXorbFooter(reader)
	if err != nil {
		t.Fatalf("ParseXorbFooter: %v", err)
	}

	if len(parsed) != 3 {
		t.Fatalf("expected 3 boundaries, got %d", len(parsed))
	}

	expectedOffsets := []uint64{6, 12, 18}
	for i, b := range parsed {
		if b.Offset != expectedOffsets[i] {
			t.Fatalf("boundary[%d] offset = %d, want %d", i, b.Offset, expectedOffsets[i])
		}
	}
}

func TestParseXorbFooter_EmptyBoundaries(t *testing.T) {
	content := []byte("data")
	data := buildTestXorb(content, nil)
	reader := bytes.NewReader(data)

	parsed, err := ParseXorbFooter(reader)
	if err != nil {
		t.Fatalf("ParseXorbFooter: %v", err)
	}

	if len(parsed) != 0 {
		t.Fatalf("expected 0 boundaries, got %d", len(parsed))
	}
}

func TestParseXorbFooter_InvalidMagic(t *testing.T) {
	// Build a "xorb" with invalid magic
	var buf bytes.Buffer
	buf.Write([]byte("content"))
	footer := []byte("INVALID1") // 8 bytes: 7 magic + 1 version
	buf.Write(footer)
	binary.Write(&buf, binary.LittleEndian, uint32(len(footer)))

	reader := bytes.NewReader(buf.Bytes())
	_, err := ParseXorbFooter(reader)
	if err == nil {
		t.Fatal("expected error for invalid magic")
	}
}

func TestParseXorbFooter_UnsupportedVersion(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte("content"))
	footer := append([]byte(xorbMagicString), 99) // version 99
	buf.Write(footer)
	binary.Write(&buf, binary.LittleEndian, uint32(len(footer)))

	reader := bytes.NewReader(buf.Bytes())
	_, err := ParseXorbFooter(reader)
	if err == nil {
		t.Fatal("expected error for unsupported version")
	}
}

func TestParseXorbFooter_TooSmall(t *testing.T) {
	data := []byte{0x01, 0x02}
	reader := bytes.NewReader(data)
	_, err := ParseXorbFooter(reader)
	if err == nil {
		t.Fatal("expected error for too-small file")
	}
}

func TestXorbContentRange(t *testing.T) {
	boundaries := []XorbChunkBoundary{
		{Offset: 100},
		{Offset: 250},
		{Offset: 400},
		{Offset: 600},
	}

	tests := []struct {
		name       string
		start, end uint32
		wantStart  uint64
		wantEnd    uint64
		wantErr    bool
	}{
		{"first chunk", 0, 1, 0, 100, false},
		{"second chunk", 1, 2, 100, 250, false},
		{"all chunks", 0, 4, 0, 600, false},
		{"middle range", 1, 3, 100, 400, false},
		{"out of bounds", 0, 5, 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, err := XorbContentRange(boundaries, tt.start, tt.end)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if start != tt.wantStart {
				t.Fatalf("start = %d, want %d", start, tt.wantStart)
			}
			if end != tt.wantEnd {
				t.Fatalf("end = %d, want %d", end, tt.wantEnd)
			}
		})
	}
}

func TestXorbContentRange_Empty(t *testing.T) {
	_, _, err := XorbContentRange(nil, 0, 1)
	if err == nil {
		t.Fatal("expected error for empty boundaries")
	}
}
