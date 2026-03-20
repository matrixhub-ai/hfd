package xet

import (
	"bytes"
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

func TestXorbUploadAndFetch(t *testing.T) {
	server := setupTestServer(t)

	data := []byte("compressed xorb data content")
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	// POST xorb
	resp, err := http.Post(server.URL+"/v1/xorbs/default/"+hash, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Post xorb failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 for POST xorb, got %d: %s", resp.StatusCode, body)
	}

	var uploadResp uploadXorbResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		t.Fatalf("Failed to decode upload response: %v", err)
	}
	if !uploadResp.WasInserted {
		t.Error("Expected was_inserted=true")
	}

	// HEAD xorb - check it exists
	req, _ := http.NewRequest(http.MethodHead, server.URL+"/v1/xorbs/default/"+hash, nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Head xorb failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for HEAD xorb, got %d", resp.StatusCode)
	}

	// GET reconstruction (V1) for the stored xorb
	resp, err = http.Get(server.URL + "/v1/reconstructions/" + hash)
	if err != nil {
		t.Fatalf("Get reconstruction failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 for GET reconstruction, got %d: %s", resp.StatusCode, body)
	}

	var reconResp queryReconstructionResponse
	if err := json.NewDecoder(resp.Body).Decode(&reconResp); err != nil {
		t.Fatalf("Failed to decode reconstruction response: %v", err)
	}

	if len(reconResp.Terms) != 1 || reconResp.Terms[0].Hash != hash {
		t.Errorf("Unexpected reconstruction terms: %+v", reconResp.Terms)
	}

	fetchInfos, ok := reconResp.FetchInfo[hash]
	if !ok || len(fetchInfos) == 0 {
		t.Fatal("Expected fetch_info for hash")
	}

	// Fetch the actual data via fetch_term URL
	fetchURL := fetchInfos[0].URL
	resp, err = http.Get(fetchURL)
	if err != nil {
		t.Fatalf("Fetch term failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 for fetch_term, got %d: %s", resp.StatusCode, body)
	}

	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, data) {
		t.Fatalf("Fetch term data = %q, want %q", got, data)
	}
}

func TestXorbNotFound(t *testing.T) {
	server := setupTestServer(t)

	hash := "0000000000000000000000000000000000000000000000000000000000000000"

	// HEAD xorb - should be 404
	req, _ := http.NewRequest(http.MethodHead, server.URL+"/v1/xorbs/default/"+hash, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Head xorb failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404, got %d", resp.StatusCode)
	}

	// GET reconstruction - should be 404
	resp, err = http.Get(server.URL + "/v1/reconstructions/" + hash)
	if err != nil {
		t.Fatalf("Get reconstruction failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404 for reconstruction, got %d", resp.StatusCode)
	}
}

func TestShardUpload(t *testing.T) {
	server := setupTestServer(t)

	shardData := []byte("shard metadata content")
	resp, err := http.Post(server.URL+"/shards", "application/octet-stream", bytes.NewReader(shardData))
	if err != nil {
		t.Fatalf("Post shard failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 for POST shard, got %d: %s", resp.StatusCode, body)
	}

	var shardResp uploadShardResponse
	if err := json.NewDecoder(resp.Body).Decode(&shardResp); err != nil {
		t.Fatalf("Failed to decode shard response: %v", err)
	}
	if shardResp.Result != 1 {
		t.Errorf("Expected result=1 (SyncPerformed), got %d", shardResp.Result)
	}
}

func TestHealthCheck(t *testing.T) {
	server := setupTestServer(t)

	resp, err := http.Get(server.URL + "/health")
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for health check, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Cache-Control") == "" {
		t.Error("Expected Cache-Control header on health check")
	}
}

func TestInvalidHashRejected(t *testing.T) {
	server := setupTestServer(t)

	// Path traversal attempt should be rejected by route regex
	resp, err := http.Get(server.URL + "/v1/reconstructions/..")
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404 for invalid hash, got %d", resp.StatusCode)
	}
}
