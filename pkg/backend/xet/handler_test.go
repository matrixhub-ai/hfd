package xet

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

func TestGetXorb(t *testing.T) {
	server := setupTestServer(t)

	data := []byte("direct download xorb content here and more data to make it bigger")
	hash := "1111111111111111111111111111111111111111111111111111111111111111"

	// Upload xorb
	resp, err := http.Post(server.URL+"/v1/xorbs/default/"+hash, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Post xorb failed: %v", err)
	}
	resp.Body.Close()

	// GET /v1/get_xorb/{prefix}/{hash}/ - full download
	resp, err = http.Get(server.URL + "/v1/get_xorb/default/" + hash + "/")
	if err != nil {
		t.Fatalf("Get xorb failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 for GET xorb, got %d: %s", resp.StatusCode, body)
	}

	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, data) {
		t.Fatalf("Get xorb data = %q, want %q", got, data)
	}

	// GET /v1/get_xorb with Range header
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/get_xorb/default/"+hash+"/", nil)
	req.Header.Set("Range", "bytes=0-4")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Range request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 206 for range request, got %d: %s", resp.StatusCode, body)
	}

	got, _ = io.ReadAll(resp.Body)
	if !bytes.Equal(got, data[:5]) {
		t.Fatalf("Range data = %q, want %q", got, data[:5])
	}
}

func TestGetXorbNotFound(t *testing.T) {
	server := setupTestServer(t)

	hash := "0000000000000000000000000000000000000000000000000000000000000000"
	resp, err := http.Get(server.URL + "/v1/get_xorb/default/" + hash + "/")
	if err != nil {
		t.Fatalf("Get xorb failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404, got %d", resp.StatusCode)
	}
}

func TestBatchGetReconstruction(t *testing.T) {
	server := setupTestServer(t)

	data1 := []byte("first xorb data")
	hash1 := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	data2 := []byte("second xorb data")
	hash2 := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	hashMissing := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

	// Upload two xorbs
	resp, _ := http.Post(server.URL+"/v1/xorbs/default/"+hash1, "application/octet-stream", bytes.NewReader(data1))
	resp.Body.Close()
	resp, _ = http.Post(server.URL+"/v1/xorbs/default/"+hash2, "application/octet-stream", bytes.NewReader(data2))
	resp.Body.Close()

	// Batch query with both existing and missing hashes
	url := fmt.Sprintf("%s/v1/reconstructions?file_id=%s&file_id=%s&file_id=%s", server.URL, hash1, hash2, hashMissing)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("Batch get reconstruction failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, body)
	}

	var batchResp batchReconstructionResponse
	if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
		t.Fatalf("Failed to decode batch response: %v", err)
	}

	// Should have 2 terms (hash1, hash2), missing hash skipped
	if len(batchResp.Terms) != 2 {
		t.Fatalf("Expected 2 terms, got %d", len(batchResp.Terms))
	}

	if _, ok := batchResp.FetchInfo[hash1]; !ok {
		t.Error("Expected fetch_info for hash1")
	}
	if _, ok := batchResp.FetchInfo[hash2]; !ok {
		t.Error("Expected fetch_info for hash2")
	}
	if _, ok := batchResp.FetchInfo[hashMissing]; ok {
		t.Error("Did not expect fetch_info for missing hash")
	}

	// Also test the /reconstructions alias
	url = fmt.Sprintf("%s/reconstructions?file_id=%s", server.URL, hash1)
	resp, err = http.Get(url)
	if err != nil {
		t.Fatalf("Batch get reconstruction (alias) failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for alias, got %d", resp.StatusCode)
	}
}

func TestBatchGetReconstructionEmpty(t *testing.T) {
	server := setupTestServer(t)

	resp, err := http.Get(server.URL + "/v1/reconstructions")
	if err != nil {
		t.Fatalf("Batch get reconstruction failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, body)
	}

	var batchResp batchReconstructionResponse
	if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
		t.Fatalf("Failed to decode batch response: %v", err)
	}
	if len(batchResp.Terms) != 0 {
		t.Fatalf("Expected 0 terms, got %d", len(batchResp.Terms))
	}
}

func TestFetchTermV2WithMultipleRanges(t *testing.T) {
	server := setupTestServer(t)

	// Create data large enough to have meaningful ranges
	data := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
	hash := "2222222222222222222222222222222222222222222222222222222222222222"

	// Upload xorb
	resp, _ := http.Post(server.URL+"/v1/xorbs/default/"+hash, "application/octet-stream", bytes.NewReader(data))
	resp.Body.Close()

	// Test V2 term with single embedded range
	payload := fmt.Sprintf("%s:0-10", hash)
	term := base64.RawURLEncoding.EncodeToString([]byte(payload))
	url := fmt.Sprintf("%s/v1/fetch_term?term=%s", server.URL, term)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("Fetch term failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 206 for single range fetch, got %d: %s", resp.StatusCode, body)
	}

	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, data[:10]) {
		t.Fatalf("Single range data = %q, want %q", got, data[:10])
	}

	// Test V2 term with multiple embedded ranges -> multipart response
	payload = fmt.Sprintf("%s:0-5,10-15", hash)
	term = base64.RawURLEncoding.EncodeToString([]byte(payload))
	url = fmt.Sprintf("%s/v1/fetch_term?term=%s", server.URL, term)
	resp, err = http.Get(url)
	if err != nil {
		t.Fatalf("Fetch term multi-range failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 206 for multi-range fetch, got %d: %s", resp.StatusCode, body)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "multipart/byteranges") {
		t.Fatalf("Expected multipart/byteranges content type, got %q", ct)
	}
	if !strings.Contains(ct, "boundary=xet_multipart_boundary") {
		t.Fatalf("Expected boundary in content type, got %q", ct)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	// Verify multipart structure contains both ranges
	if !strings.Contains(bodyStr, "Content-Range: bytes 0-4/") {
		t.Errorf("Expected first range header, got:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, "Content-Range: bytes 10-14/") {
		t.Errorf("Expected second range header, got:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, string(data[:5])) {
		t.Errorf("Expected first range data in body")
	}
	if !strings.Contains(bodyStr, string(data[10:15])) {
		t.Errorf("Expected second range data in body")
	}
}

func TestFetchTermV2WithHTTPRangeOverride(t *testing.T) {
	server := setupTestServer(t)

	data := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
	hash := "3333333333333333333333333333333333333333333333333333333333333333"

	// Upload xorb
	resp, _ := http.Post(server.URL+"/v1/xorbs/default/"+hash, "application/octet-stream", bytes.NewReader(data))
	resp.Body.Close()

	// V2 term with embedded range, but HTTP Range header overrides
	payload := fmt.Sprintf("%s:0-30", hash)
	term := base64.RawURLEncoding.EncodeToString([]byte(payload))
	url := fmt.Sprintf("%s/v1/fetch_term?term=%s", server.URL, term)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Range", "bytes=5-9")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Fetch term with HTTP range failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 206, got %d: %s", resp.StatusCode, body)
	}

	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, data[5:10]) {
		t.Fatalf("HTTP range override data = %q, want %q", got, data[5:10])
	}
}

func TestFetchTermV1WithRangeHeader(t *testing.T) {
	server := setupTestServer(t)

	data := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
	hash := "4444444444444444444444444444444444444444444444444444444444444444"

	// Upload xorb
	resp, _ := http.Post(server.URL+"/v1/xorbs/default/"+hash, "application/octet-stream", bytes.NewReader(data))
	resp.Body.Close()

	// V1 term (hash only) with Range header
	term := base64.RawURLEncoding.EncodeToString([]byte(hash))
	url := fmt.Sprintf("%s/v1/fetch_term?term=%s", server.URL, term)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Range", "bytes=10-19")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Fetch term V1 with range failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 206, got %d: %s", resp.StatusCode, body)
	}

	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, data[10:20]) {
		t.Fatalf("V1 range data = %q, want %q", got, data[10:20])
	}
}

func TestReconstructionV2(t *testing.T) {
	server := setupTestServer(t)

	data := []byte("v2 reconstruction test data")
	hash := "5555555555555555555555555555555555555555555555555555555555555555"

	// Upload xorb
	resp, _ := http.Post(server.URL+"/v1/xorbs/default/"+hash, "application/octet-stream", bytes.NewReader(data))
	resp.Body.Close()

	// GET V2 reconstruction
	resp, err := http.Get(server.URL + "/v2/reconstructions/" + hash)
	if err != nil {
		t.Fatalf("Get V2 reconstruction failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 for V2 reconstruction, got %d: %s", resp.StatusCode, body)
	}

	var reconResp queryReconstructionResponseV2
	if err := json.NewDecoder(resp.Body).Decode(&reconResp); err != nil {
		t.Fatalf("Failed to decode V2 reconstruction response: %v", err)
	}

	if len(reconResp.Terms) != 1 || reconResp.Terms[0].Hash != hash {
		t.Errorf("Unexpected V2 reconstruction terms: %+v", reconResp.Terms)
	}

	xorbFetches, ok := reconResp.Xorbs[hash]
	if !ok || len(xorbFetches) == 0 {
		t.Fatal("Expected xorbs entry for hash")
	}

	if len(xorbFetches[0].Ranges) != 1 {
		t.Fatalf("Expected 1 range descriptor, got %d", len(xorbFetches[0].Ranges))
	}

	// Fetch data via the V2 URL
	fetchURL := xorbFetches[0].URL
	resp, err = http.Get(fetchURL)
	if err != nil {
		t.Fatalf("V2 fetch failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 206 for V2 fetch, got %d: %s", resp.StatusCode, body)
	}

	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, data) {
		t.Fatalf("V2 fetch data = %q, want %q", got, data)
	}
}

func TestHeadFile(t *testing.T) {
	server := setupTestServer(t)

	data := []byte("file size test content")
	hash := "6666666666666666666666666666666666666666666666666666666666666666"

	// Upload xorb
	resp, _ := http.Post(server.URL+"/v1/xorbs/default/"+hash, "application/octet-stream", bytes.NewReader(data))
	resp.Body.Close()

	// HEAD /v1/files/{file_id}
	req, _ := http.NewRequest(http.MethodHead, server.URL+"/v1/files/"+hash, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Head file failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for HEAD file, got %d", resp.StatusCode)
	}

	cl := resp.Header.Get("Content-Length")
	if cl != fmt.Sprintf("%d", len(data)) {
		t.Fatalf("Expected Content-Length %d, got %s", len(data), cl)
	}
}

func TestHeadFileNotFound(t *testing.T) {
	server := setupTestServer(t)

	hash := "0000000000000000000000000000000000000000000000000000000000000000"
	req, _ := http.NewRequest(http.MethodHead, server.URL+"/v1/files/"+hash, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Head file failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404 for HEAD file, got %d", resp.StatusCode)
	}
}
