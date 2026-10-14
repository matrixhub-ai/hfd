package repository

import (
	"bytes"
	"io"
	"testing"
)

// Loose blobs above 1 MiB must stream from disk; at exactly 1 MiB they stay buffered and seekable.
func TestLargeObjectBlobReader(t *testing.T) {
	tests := []struct {
		name       string
		size       int
		wantSeeker bool
	}{
		{name: "AtThreshold", size: 1 << 20, wantSeeker: true},
		{name: "AboveThreshold", size: 1<<20 + 1, wantSeeker: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := make([]byte, tt.size)
			for i := range content {
				content[i] = byte(i*31 + 7)
			}
			repo := initTestRepo(t)
			mustCommit(t, repo, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "model.bin", Content: content})

			blob, err := repo.Blob("main", "model.bin")
			if err != nil {
				t.Fatalf("Blob: %v", err)
			}
			if got := blob.Size(); got != int64(tt.size) {
				t.Errorf("Size() = %d, want %d", got, tt.size)
			}
			for n := 1; n <= 2; n++ {
				r, err := blob.NewReader()
				if err != nil {
					t.Fatalf("NewReader #%d: %v", n, err)
				}
				if _, ok := r.(io.Seeker); ok != tt.wantSeeker {
					t.Errorf("NewReader #%d of a %d-byte blob: io.Seeker = %t, want %t (LargeObjectThreshold = 1 MiB)", n, tt.size, ok, tt.wantSeeker)
				}
				got, err := io.ReadAll(r)
				_ = r.Close()
				if err != nil {
					t.Fatalf("NewReader #%d: ReadAll: %v", n, err)
				}
				if !bytes.Equal(got, content) {
					t.Errorf("NewReader #%d: content differs from the %d committed bytes (read %d)", n, len(content), len(got))
				}
			}
		})
	}
}
