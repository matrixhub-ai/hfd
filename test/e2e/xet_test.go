package e2e_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	backendxet "github.com/matrixhub-ai/hfd/pkg/backend/xet"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/storage"
	"github.com/matrixhub-ai/hfd/pkg/xet"
)

func setupXetTestServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "xet-e2e-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	st := storage.NewStorage(storage.WithRootDir(dataDir))
	lfsStorage := lfs.NewLocal(st.LFSDir())
	xetStorage := xet.NewStorage(st.XetDir())

	tokenSignValidator := authenticate.NewTokenSignValidator([]byte("test-sign-key"))

	// Set up handler chain with xet enabled
	var handler http.Handler

	handler = backendhf.NewHandler(
		backendhf.WithStorage(st),
		backendhf.WithLFSStorage(lfsStorage),
		backendhf.WithXetEnabled(true),
		backendhf.WithTokenSignValidator(tokenSignValidator),
	)

	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(st),
		backendlfs.WithNext(handler),
		backendlfs.WithLFSStorage(lfsStorage),
		backendlfs.WithXetEnabled(true),
		backendlfs.WithTokenSignValidator(tokenSignValidator),
	)

	handler = backendxet.NewHandler(
		backendxet.WithNext(handler),
		backendxet.WithStorage(xetStorage),
	)

	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(st),
		backendhttp.WithNext(handler),
	)

	handler = authenticate.AnonymousAuthenticateHandler(handler)

	server := httptest.NewServer(handler)
	t.Cleanup(func() { server.Close() })

	return server, dataDir
}

func TestXetTransferNegotiation(t *testing.T) {
	server, _ := setupXetTestServer(t)

	// Create a repository first
	resp, err := httpPost(server.URL+"/api/repos/create",
		`{"type":"model","name":"xet-model","organization":"xet-user"}`)
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Expected 200 creating repo, got %d", resp.StatusCode)
	}

	// Test LFS batch with xet transfer offered
	batchBody := `{
		"operation": "upload",
		"transfers": ["xet", "basic"],
		"objects": [{"oid": "abc123def456", "size": 1024}]
	}`

	req, _ := http.NewRequest("POST",
		server.URL+"/xet-user/xet-model.git/info/lfs/objects/batch",
		strings.NewReader(batchBody))
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Batch request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, body)
	}

	var batchResp struct {
		Transfer string `json:"transfer"`
		Objects  []struct {
			Oid  string `json:"oid"`
			Size int64  `json:"size"`
		} `json:"objects"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
		t.Fatalf("Failed to decode batch response: %v", err)
	}

	if batchResp.Transfer != "xet" {
		t.Errorf("Expected transfer 'xet', got %q", batchResp.Transfer)
	}
}

func TestXetTransferFallbackToBasic(t *testing.T) {
	server, _ := setupXetTestServer(t)

	// Create a repository first
	resp, err := httpPost(server.URL+"/api/repos/create",
		`{"type":"model","name":"basic-model","organization":"basic-user"}`)
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	// Test LFS batch with only basic transfer offered (no xet)
	batchBody := `{
		"operation": "upload",
		"transfers": ["basic"],
		"objects": [{"oid": "abc123def456", "size": 1024}]
	}`

	req, _ := http.NewRequest("POST",
		server.URL+"/basic-user/basic-model.git/info/lfs/objects/batch",
		strings.NewReader(batchBody))
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Batch request failed: %v", err)
	}
	defer resp.Body.Close()

	var batchResp struct {
		Transfer string `json:"transfer"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
		t.Fatalf("Failed to decode batch response: %v", err)
	}

	if batchResp.Transfer != "basic" {
		t.Errorf("Expected transfer 'basic', got %q", batchResp.Transfer)
	}
}

func TestXetTokenEndpoints(t *testing.T) {
	server, _ := setupXetTestServer(t)

	// Create a repository first
	resp, err := httpPost(server.URL+"/api/repos/create",
		`{"type":"model","name":"token-model","organization":"token-user"}`)
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	// Test xet-read-token endpoint
	resp, err = http.Post(server.URL+"/api/models/token-user/token-model/xet-read-token/main", "application/json", nil)
	if err != nil {
		t.Fatalf("Failed to get xet read token: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("Expected 200 for xet-read-token, got %d", resp.StatusCode)
	}

	endpoint := resp.Header.Get("X-Xet-Endpoint")
	if endpoint == "" {
		t.Error("Expected X-Xet-Endpoint header")
	}
	if !strings.Contains(endpoint, "/api/v1") {
		t.Errorf("Expected endpoint to contain /api/v1, got %q", endpoint)
	}

	expiration := resp.Header.Get("X-Xet-Expiration")
	if expiration == "" {
		t.Error("Expected X-Xet-Expiration header")
	}

	// Test xet-write-token endpoint
	resp, err = http.Post(server.URL+"/api/models/token-user/token-model/xet-write-token/main", "application/json", nil)
	if err != nil {
		t.Fatalf("Failed to get xet write token: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("Expected 200 for xet-write-token, got %d", resp.StatusCode)
	}

	endpoint = resp.Header.Get("X-Xet-Endpoint")
	if endpoint == "" {
		t.Error("Expected X-Xet-Endpoint header for write token")
	}
}

func TestXetCASEndpoints(t *testing.T) {
	server, _ := setupXetTestServer(t)

	// Test xorb upload
	xorbData := []byte("test xorb data for e2e")
	resp, err := http.Post(server.URL+"/api/v1/xorbs/ab/abcdef1234",
		"application/octet-stream", bytes.NewReader(xorbData))
	if err != nil {
		t.Fatalf("Failed to upload xorb: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("Expected 201 for xorb upload, got %d", resp.StatusCode)
	}

	// Test xorb download
	resp, err = http.Get(server.URL + "/api/v1/xorbs/ab/abcdef1234")
	if err != nil {
		t.Fatalf("Failed to download xorb: %v", err)
	}
	content, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 for xorb download, got %d", resp.StatusCode)
	}
	if !bytes.Equal(content, xorbData) {
		t.Error("Xorb content mismatch")
	}

	// Test shard upload
	shardData := []byte("test shard metadata")
	resp, err = http.Post(server.URL+"/api/v1/shards",
		"application/octet-stream", bytes.NewReader(shardData))
	if err != nil {
		t.Fatalf("Failed to upload shard: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("Expected 201 for shard upload, got %d", resp.StatusCode)
	}

	// Test chunk retrieval
	resp, err = http.Get(server.URL + "/api/v1/chunks/ab/abcdef1234")
	if err != nil {
		t.Fatalf("Failed to get chunk: %v", err)
	}
	content, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 for chunk download, got %d", resp.StatusCode)
	}
	if !bytes.Equal(content, xorbData) {
		t.Error("Chunk content mismatch")
	}

	// Test reconstruction endpoints
	resp, err = http.Get(server.URL + "/api/v1/reconstructions/test-file")
	if err != nil {
		t.Fatalf("Failed to get reconstruction: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 for V1 reconstruction, got %d", resp.StatusCode)
	}

	resp, err = http.Get(server.URL + "/api/v2/reconstructions/test-file")
	if err != nil {
		t.Fatalf("Failed to get V2 reconstruction: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 for V2 reconstruction, got %d", resp.StatusCode)
	}
}
