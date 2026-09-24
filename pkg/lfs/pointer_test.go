package lfs

import (
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func TestParsePointer(t *testing.T) {
	const valid = `version https://git-lfs.github.com/spec/v1
oid sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
size 1024
`
	tests := []struct {
		name        string
		content     string
		oneByte     bool
		expectOid   string
		expectSize  int64
		expectNil   bool
		expectError bool
	}{
		{
			name:        "valid LFS pointer",
			content:     valid,
			expectOid:   "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			expectSize:  1024,
			expectError: false,
		},
		{
			name:        "invalid pointer - not LFS format",
			content:     "Hello, world!",
			expectError: true,
		},
		{
			name:      "empty content is not a pointer",
			content:   "",
			expectNil: true,
		},
		{
			name:       "valid pointer through one-byte reads",
			content:    valid,
			oneByte:    true,
			expectOid:  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			expectSize: 1024,
		},
		{
			name:       "pointer padded to one byte below the cutoff",
			content:    valid + strings.Repeat(" ", MaxLFSPointerSize-1-len(valid)),
			expectOid:  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			expectSize: 1024,
		},
		{
			name:      "pointer padded to the cutoff",
			content:   valid + strings.Repeat(" ", MaxLFSPointerSize-len(valid)),
			expectNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var r io.Reader = strings.NewReader(tt.content)
			if tt.oneByte {
				r = iotest.OneByteReader(r)
			}
			ptr, err := DecodePointer(r)
			if tt.expectError {
				if err == nil {
					t.Errorf("ParsePointer() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("ParsePointer() unexpected error: %v", err)
			}

			if tt.expectNil {
				if ptr != nil {
					t.Fatalf("ParsePointer() = %v, want nil", ptr)
				}
				return
			}
			if ptr == nil {
				t.Fatal("ParsePointer() = nil, want pointer")
			}

			if ptr.OID() != tt.expectOid {
				t.Errorf("ParsePointer() Oid = %q, want %q", ptr.OID(), tt.expectOid)
			}

			if ptr.Size() != tt.expectSize {
				t.Errorf("ParsePointer() Size = %d, want %d", ptr.Size(), tt.expectSize)
			}
		})
	}
}
