package xet

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/xet"
)

func setupTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	dir, err := os.MkdirTemp("", "xet-handler-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	storage := xet.NewLocal(dir)

	handler := NewHandler(
		WithXetStorage(storage),
	)

	server := httptest.NewServer(handler)
	t.Cleanup(func() { server.Close() })

	return server
}

func TestCASPutAndGet(t *testing.T) {
	server := setupTestServer(t)

	data := []byte("hello xet cas")
	hash := sha256.Sum256(data)
	oid := hex.EncodeToString(hash[:])

	// PUT object
	req, err := http.NewRequest(http.MethodPut, server.URL+"/xet/cas/objects/"+oid, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.ContentLength = int64(len(data))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for PUT, got %d", resp.StatusCode)
	}

	// GET object
	resp, err = http.Get(server.URL + "/xet/cas/objects/" + oid)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for GET, got %d", resp.StatusCode)
	}

	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get data = %q, want %q", got, data)
	}

	// Verify ETag header
	if etag := resp.Header.Get("ETag"); etag != "\""+oid+"\"" {
		t.Errorf("Expected ETag %q, got %q", "\""+oid+"\"", etag)
	}
}

func TestCASGetNotFound(t *testing.T) {
	server := setupTestServer(t)

	resp, err := http.Get(server.URL + "/xet/cas/objects/0000000000000000000000000000000000000000000000000000000000000000")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404, got %d", resp.StatusCode)
	}
}

func TestCASHas(t *testing.T) {
	server := setupTestServer(t)

	// Store an object first
	data := []byte("test object for has check")
	hash := sha256.Sum256(data)
	existingOID := hex.EncodeToString(hash[:])

	req, err := http.NewRequest(http.MethodPut, server.URL+"/xet/cas/objects/"+existingOID, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.ContentLength = int64(len(data))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	resp.Body.Close()

	// Batch has check
	missingOID := "0000000000000000000000000000000000000000000000000000000000000000"
	hasReq := hasRequest{
		Hashes: []string{existingOID, missingOID},
	}
	body, _ := json.Marshal(hasReq)

	resp, err = http.Post(server.URL+"/xet/cas/objects/has", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Has failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var hasResp hasResponse
	if err := json.NewDecoder(resp.Body).Decode(&hasResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if !hasResp.Exists[existingOID] {
		t.Errorf("Expected existing OID %q to be reported as existing", existingOID)
	}
	if hasResp.Exists[missingOID] {
		t.Errorf("Expected missing OID %q to be reported as not existing", missingOID)
	}
}

func TestCASPutHashMismatch(t *testing.T) {
	server := setupTestServer(t)

	data := []byte("some data")
	wrongOID := "0000000000000000000000000000000000000000000000000000000000000000"

	req, err := http.NewRequest(http.MethodPut, server.URL+"/xet/cas/objects/"+wrongOID, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.ContentLength = int64(len(data))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("Expected 500 for hash mismatch, got %d", resp.StatusCode)
	}
}
