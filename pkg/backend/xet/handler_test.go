package xet

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	pkgxet "github.com/matrixhub-ai/hfd/pkg/xet"
)

func TestHandlerUploadXorb(t *testing.T) {
	dir := t.TempDir()
	storage := pkgxet.NewStorage(dir)
	h := NewHandler(WithStorage(storage))

	data := []byte("test xorb data")
	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	req := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/"+hash, bytes.NewReader(data))
	req.ContentLength = int64(len(data))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp uploadXorbResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if !resp.WasInserted {
		t.Fatal("expected was_inserted=true")
	}

	// Upload same xorb again
	req2 := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/"+hash, bytes.NewReader(data))
	req2.ContentLength = int64(len(data))
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w2.Code)
	}

	var resp2 uploadXorbResponse
	json.Unmarshal(w2.Body.Bytes(), &resp2)
	if resp2.WasInserted {
		t.Fatal("expected was_inserted=false for duplicate")
	}
}

func TestHandlerUploadXorb_InvalidPrefix(t *testing.T) {
	dir := t.TempDir()
	storage := pkgxet.NewStorage(dir)
	h := NewHandler(WithStorage(storage))

	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	req := httptest.NewRequest(http.MethodPost, "/v1/xorbs/invalid/"+hash, bytes.NewReader(nil))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandlerGetXorb(t *testing.T) {
	dir := t.TempDir()
	storage := pkgxet.NewStorage(dir)
	h := NewHandler(WithStorage(storage))

	data := []byte("test xorb content for GET")
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	// First upload
	storage.PutXorb("default", hash, bytes.NewReader(data), int64(len(data)))

	// Then GET
	req := httptest.NewRequest(http.MethodGet, "/v1/xorbs/default/"+hash, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if !bytes.Equal(w.Body.Bytes(), data) {
		t.Fatalf("data mismatch")
	}
}

func TestHandlerGetXorb_NotFound(t *testing.T) {
	dir := t.TempDir()
	storage := pkgxet.NewStorage(dir)
	h := NewHandler(WithStorage(storage))

	hash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	req := httptest.NewRequest(http.MethodGet, "/v1/xorbs/default/"+hash, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandlerUploadShard(t *testing.T) {
	dir := t.TempDir()
	storage := pkgxet.NewStorage(dir)
	h := NewHandler(WithStorage(storage))

	// Build a valid shard
	shardData := buildTestShard(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/shards", bytes.NewReader(shardData))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp uploadShardResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Result != 1 {
		t.Fatalf("expected result=1 (SyncPerformed), got %d", resp.Result)
	}
}

func TestHandlerUploadShard_Duplicate(t *testing.T) {
	dir := t.TempDir()
	storage := pkgxet.NewStorage(dir)
	h := NewHandler(WithStorage(storage))

	shardData := buildTestShard(t)

	// First upload
	req1 := httptest.NewRequest(http.MethodPost, "/v1/shards", bytes.NewReader(shardData))
	w1 := httptest.NewRecorder()
	h.ServeHTTP(w1, req1)

	if w1.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w1.Code)
	}

	// Second upload (duplicate)
	req2 := httptest.NewRequest(http.MethodPost, "/v1/shards", bytes.NewReader(shardData))
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w2.Code)
	}

	var resp uploadShardResponse
	json.Unmarshal(w2.Body.Bytes(), &resp)
	if resp.Result != 0 {
		t.Fatalf("expected result=0 for duplicate, got %d", resp.Result)
	}
}

func TestHandlerUploadShard_InvalidFormat(t *testing.T) {
	dir := t.TempDir()
	storage := pkgxet.NewStorage(dir)
	h := NewHandler(WithStorage(storage))

	req := httptest.NewRequest(http.MethodPost, "/v1/shards", bytes.NewReader([]byte("not a shard")))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandlerGetChunk_NotFound(t *testing.T) {
	dir := t.TempDir()
	storage := pkgxet.NewStorage(dir)
	h := NewHandler(WithStorage(storage))

	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	req := httptest.NewRequest(http.MethodGet, "/v1/chunks/default-merkledb/"+hash, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	// Always 404 since we don't implement global deduplication
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandlerGetChunk_InvalidPrefix(t *testing.T) {
	dir := t.TempDir()
	storage := pkgxet.NewStorage(dir)
	h := NewHandler(WithStorage(storage))

	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	req := httptest.NewRequest(http.MethodGet, "/v1/chunks/invalid/"+hash, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandlerReconstruction_NotFound(t *testing.T) {
	dir := t.TempDir()
	storage := pkgxet.NewStorage(dir)
	h := NewHandler(WithStorage(storage))

	fileID := "0000000000000000000000000000000000000000000000000000000000000000"
	req := httptest.NewRequest(http.MethodGet, "/v1/reconstructions/"+fileID, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandlerReconstructionV2_NotFound(t *testing.T) {
	dir := t.TempDir()
	storage := pkgxet.NewStorage(dir)
	h := NewHandler(WithStorage(storage))

	fileID := "0000000000000000000000000000000000000000000000000000000000000000"
	req := httptest.NewRequest(http.MethodGet, "/v2/reconstructions/"+fileID, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandlerReconstructionV1_WithShard(t *testing.T) {
	dir := t.TempDir()
	storage := pkgxet.NewStorage(dir)
	h := NewHandler(WithStorage(storage))

	fileHash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	xorbHash := "1111111111111111111111111111111111111111111111111111111111111111"

	// Upload a shard containing the file entry
	shardData := buildTestShardWithHashes(t, fileHash, xorbHash)
	req1 := httptest.NewRequest(http.MethodPost, "/v1/shards", bytes.NewReader(shardData))
	w1 := httptest.NewRecorder()
	h.ServeHTTP(w1, req1)
	if w1.Code != http.StatusOK {
		t.Fatalf("shard upload: expected 200, got %d: %s", w1.Code, w1.Body.String())
	}

	// Query reconstruction
	req2 := httptest.NewRequest(http.MethodGet, "/v1/reconstructions/"+fileHash, nil)
	req2.Host = "localhost:8080"
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("reconstruction: expected 200, got %d: %s", w2.Code, w2.Body.String())
	}

	var resp queryReconstructionResponse
	if err := json.Unmarshal(w2.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal reconstruction: %v", err)
	}

	if len(resp.Terms) == 0 {
		t.Fatal("expected at least one term")
	}

	if resp.Terms[0].Hash != xorbHash {
		t.Fatalf("expected term hash %q, got %q", xorbHash, resp.Terms[0].Hash)
	}
}

func TestHandlerNotFound_FallsThrough(t *testing.T) {
	dir := t.TempDir()
	storage := pkgxet.NewStorage(dir)

	nextCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusOK)
	})

	h := NewHandler(WithStorage(storage), WithNext(next))

	req := httptest.NewRequest(http.MethodGet, "/unknown/path", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if !nextCalled {
		t.Fatal("expected next handler to be called for unknown path")
	}
}

func TestHandlerNoStorage(t *testing.T) {
	h := NewHandler() // no storage configured

	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"upload xorb", http.MethodPost, "/v1/xorbs/default/" + hash},
		{"upload shard", http.MethodPost, "/v1/shards"},
		{"get chunk", http.MethodGet, "/v1/chunks/default-merkledb/" + hash},
		{"get reconstruction", http.MethodGet, "/v1/reconstructions/" + hash},
		{"get reconstruction v2", http.MethodGet, "/v2/reconstructions/" + hash},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body io.Reader
			if tt.method == http.MethodPost {
				body = bytes.NewReader([]byte("data"))
			}
			req := httptest.NewRequest(tt.method, tt.path, body)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected 503, got %d", w.Code)
			}
		})
	}
}

// Shard binary format constants (duplicated here for test building)
const (
	shardMagicSize             = 32
	fileDataSequenceHeaderSize = 48
	fileDataSequenceEntrySize  = 40
)

var shardMagic = [shardMagicSize]byte{
	'H', 'F', 'R', 'e', 'p', 'o', 'M', 'e',
	't', 'a', 'D', 'a', 't', 'a', 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
}

var bookendBytes [fileDataSequenceHeaderSize]byte

func init() {
	for i := range bookendBytes {
		bookendBytes[i] = 0xFF
	}
}

func buildTestShard(t *testing.T) []byte {
	t.Helper()
	fileHash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	xorbHash := "1111111111111111111111111111111111111111111111111111111111111111"
	return buildTestShardWithHashes(t, fileHash, xorbHash)
}

func buildTestShardWithHashes(t *testing.T, fileHash, xorbHash string) []byte {
	t.Helper()

	var buf bytes.Buffer

	// Magic
	buf.Write(shardMagic[:])

	// Version
	binary.Write(&buf, binary.LittleEndian, uint64(2))

	// File entry header: 32-byte file hash + 8-byte data length + 8-byte entry count
	fh, _ := hex.DecodeString(fileHash)
	if len(fh) < 32 {
		padded := make([]byte, 32)
		copy(padded, fh)
		fh = padded
	}
	buf.Write(fh[:32])
	binary.Write(&buf, binary.LittleEndian, uint64(1024)) // data length
	binary.Write(&buf, binary.LittleEndian, uint64(1))    // 1 chunk entry

	// Chunk entry: 32-byte xorb hash + 4-byte start + 4-byte end
	xh, _ := hex.DecodeString(xorbHash)
	if len(xh) < 32 {
		padded := make([]byte, 32)
		copy(padded, xh)
		xh = padded
	}
	buf.Write(xh[:32])
	binary.Write(&buf, binary.LittleEndian, uint32(0)) // start
	binary.Write(&buf, binary.LittleEndian, uint32(5)) // end

	// Bookend
	buf.Write(bookendBytes[:])

	return buf.Bytes()
}
