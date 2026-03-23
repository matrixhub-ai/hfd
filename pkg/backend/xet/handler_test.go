package xet_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	backendxet "github.com/matrixhub-ai/hfd/pkg/backend/xet"
	"github.com/matrixhub-ai/hfd/pkg/xet"
)

func setupXetHandler(t *testing.T) (*httptest.Server, string) {
	t.Helper()

	dir, err := os.MkdirTemp("", "xet-handler-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	xetStorage := xet.NewStorage(dir)

	handler := backendxet.NewHandler(
		backendxet.WithStorage(xetStorage),
	)

	server := httptest.NewServer(handler)
	t.Cleanup(func() { server.Close() })

	return server, dir
}

func TestXorbUploadAndDownload(t *testing.T) {
	server, _ := setupXetHandler(t)

	data := []byte("xorb blob content for testing")
	prefix := "ab"
	hash := "abcdef1234567890deadbeef"

	// Upload xorb
	uploadURL := server.URL + "/api/v1/xorbs/" + prefix + "/" + hash
	resp, err := http.Post(uploadURL, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Failed to upload xorb: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("Expected 201, got %d", resp.StatusCode)
	}

	// Upload same xorb again (should be idempotent)
	resp, err = http.Post(uploadURL, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Failed to re-upload xorb: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 for existing xorb, got %d", resp.StatusCode)
	}

	// Download xorb
	downloadURL := server.URL + "/api/v1/xorbs/" + prefix + "/" + hash
	resp, err = http.Get(downloadURL)
	if err != nil {
		t.Fatalf("Failed to download xorb: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200, got %d", resp.StatusCode)
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}
	if !bytes.Equal(content, data) {
		t.Errorf("Content mismatch: got %d bytes, want %d bytes", len(content), len(data))
	}
}

func TestXorbNotFound(t *testing.T) {
	server, _ := setupXetHandler(t)

	resp, err := http.Get(server.URL + "/api/v1/xorbs/ab/nonexistent")
	if err != nil {
		t.Fatalf("Failed to get xorb: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", resp.StatusCode)
	}
}

func TestShardUpload(t *testing.T) {
	server, _ := setupXetHandler(t)

	data := []byte("shard metadata content for testing")

	resp, err := http.Post(server.URL+"/api/v1/shards", "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Failed to upload shard: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("Expected 201, got %d", resp.StatusCode)
	}
}

func TestChunkRetrieval(t *testing.T) {
	server, _ := setupXetHandler(t)

	// First upload a xorb
	data := []byte("xorb with chunk data")
	prefix := "cd"
	hash := "cdef1234567890deadbeef"

	resp, err := http.Post(server.URL+"/api/v1/xorbs/"+prefix+"/"+hash, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Failed to upload xorb: %v", err)
	}
	resp.Body.Close()

	// Retrieve via chunks endpoint
	resp, err = http.Get(server.URL + "/api/v1/chunks/" + prefix + "/" + hash)
	if err != nil {
		t.Fatalf("Failed to get chunk: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200, got %d", resp.StatusCode)
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read chunk: %v", err)
	}
	if !bytes.Equal(content, data) {
		t.Errorf("Content mismatch: got %d bytes, want %d bytes", len(content), len(data))
	}
}

func TestReconstructionEndpoints(t *testing.T) {
	server, _ := setupXetHandler(t)

	// Test V1 reconstruction
	resp, err := http.Get(server.URL + "/api/v1/reconstructions/some-file-id")
	if err != nil {
		t.Fatalf("Failed to get V1 reconstruction: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 for V1 reconstruction, got %d", resp.StatusCode)
	}

	// Test V2 reconstruction
	resp, err = http.Get(server.URL + "/api/v2/reconstructions/some-file-id")
	if err != nil {
		t.Fatalf("Failed to get V2 reconstruction: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 for V2 reconstruction, got %d", resp.StatusCode)
	}
}
