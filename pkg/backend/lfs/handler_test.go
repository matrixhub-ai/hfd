package lfs_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wzshiming/gitd/internal/utils"
	backendlfs "github.com/wzshiming/gitd/pkg/backend/lfs"
	"github.com/wzshiming/gitd/pkg/lfs"
	"github.com/wzshiming/gitd/pkg/repository"
	"github.com/wzshiming/gitd/pkg/storage"
)

func TestLFSProxyMode(t *testing.T) {
	// Create temp directories
	upstreamDir, err := os.MkdirTemp("", "lfs-proxy-test-upstream")
	if err != nil {
		t.Fatalf("Failed to create temp upstream dir: %v", err)
	}
	defer os.RemoveAll(upstreamDir)

	proxyDir, err := os.MkdirTemp("", "lfs-proxy-test-proxy")
	if err != nil {
		t.Fatalf("Failed to create temp proxy dir: %v", err)
	}
	defer os.RemoveAll(proxyDir)

	// Set up upstream storage and LFS handler
	upstreamStorage := storage.NewStorage(storage.WithRootDir(upstreamDir))

	// Create a repository on upstream that looks like a mirror source
	repoName := "test-repo"
	repoPath := filepath.Join(upstreamStorage.RepositoriesDir(), repoName+".git")
	if err := os.MkdirAll(filepath.Dir(repoPath), 0755); err != nil {
		t.Fatalf("Failed to create repos dir: %v", err)
	}

	_, err = repository.Init(repoPath, "main")
	if err != nil {
		t.Fatalf("Failed to init repository: %v", err)
	}

	// Put an LFS object into the upstream content store
	lfsContent := []byte("Hello, this is LFS content for proxy test!")
	lfsHash := sha256.Sum256(lfsContent)
	lfsOid := fmt.Sprintf("%x", lfsHash)
	lfsSize := int64(len(lfsContent))

	err = upstreamStorage.ContentStore().Put(lfsOid, bytes.NewReader(lfsContent), lfsSize)
	if err != nil {
		t.Fatalf("Failed to put LFS object: %v", err)
	}

	// Set up upstream LFS handler
	upstreamHandler := backendlfs.NewHandler(
		backendlfs.WithStorage(upstreamStorage),
	)
	upstreamServer := httptest.NewServer(upstreamHandler)
	defer upstreamServer.Close()

	// Set up proxy storage pointing to upstream
	proxyStorage := storage.NewStorage(
		storage.WithRootDir(proxyDir),
	)

	// Create a mirror repo on the proxy that points to upstream
	proxyRepoPath := filepath.Join(proxyStorage.RepositoriesDir(), repoName+".git")
	if err := os.MkdirAll(filepath.Dir(proxyRepoPath), 0755); err != nil {
		t.Fatalf("Failed to create proxy repos dir: %v", err)
	}

	_, err = repository.Init(proxyRepoPath, "main")
	if err != nil {
		t.Fatalf("Failed to init proxy repo: %v", err)
	}
	// Configure as mirror pointing to upstream
	configPath := filepath.Join(proxyRepoPath, "config")
	configContent := fmt.Sprintf(`[core]
	repositoryformatversion = 0
	filemode = true
	bare = true
[remote "origin"]
	url = %s/%s.git
	fetch = +refs/heads/*:refs/heads/*
	mirror = true
`, upstreamServer.URL, repoName)
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	// Set up proxy LFS handler
	lfsProxyManager := lfs.NewProxyManager(
		utils.HTTPClient,
		proxyStorage.ContentStore().Put,
		proxyStorage.ContentStore().Exists,
	)
	proxyHandler := backendlfs.NewHandler(
		backendlfs.WithStorage(proxyStorage),
		backendlfs.WithLFSProxyManager(lfsProxyManager),
	)
	proxyServer := httptest.NewServer(proxyHandler)
	defer proxyServer.Close()

	t.Run("BatchDownloadProxiesFromUpstream", func(t *testing.T) {
		// Verify the object doesn't exist locally on proxy
		if proxyStorage.ContentStore().Exists(lfsOid) {
			t.Fatal("LFS object should not exist on proxy initially")
		}

		// Make a batch download request to the proxy
		batchReq := map[string]any{
			"operation": "download",
			"transfers": []string{"basic"},
			"objects": []map[string]any{
				{"oid": lfsOid, "size": lfsSize},
			},
		}
		body, _ := json.Marshal(batchReq)

		url := fmt.Sprintf("%s/%s.git/info/lfs/objects/batch", proxyServer.URL, repoName)
		req, err := http.NewRequest("POST", url, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}
		req.Header.Set("Accept", "application/vnd.git-lfs+json")
		req.Header.Set("Content-Type", "application/vnd.git-lfs+json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to make batch request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200, got %d", resp.StatusCode)
		}

		var batchResp struct {
			Objects []struct {
				Oid     string `json:"oid"`
				Size    int64  `json:"size"`
				Actions map[string]struct {
					Href string `json:"href"`
				} `json:"actions"`
				Error *struct {
					Code    int    `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			} `json:"objects"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if len(batchResp.Objects) != 1 {
			t.Fatalf("Expected 1 object, got %d", len(batchResp.Objects))
		}

		obj := batchResp.Objects[0]
		if obj.Error != nil {
			t.Fatalf("Expected no error, got %d: %s", obj.Error.Code, obj.Error.Message)
		}

		if _, ok := obj.Actions["download"]; !ok {
			t.Fatal("Expected download action to be present")
		}

		// Verify the object is eventually cached locally on the proxy (async fetch)
		cached := false
		for range 50 {
			if proxyStorage.ContentStore().Exists(lfsOid) {
				cached = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !cached {
			t.Fatal("LFS object should be cached on proxy after fetch")
		}
	})

	t.Run("BatchDownloadUsesCache", func(t *testing.T) {
		// Stop upstream server to prove proxy uses cache
		upstreamServer.Close()

		batchReq := map[string]any{
			"operation": "download",
			"transfers": []string{"basic"},
			"objects": []map[string]any{
				{"oid": lfsOid, "size": lfsSize},
			},
		}
		body, _ := json.Marshal(batchReq)

		url := fmt.Sprintf("%s/%s.git/info/lfs/objects/batch", proxyServer.URL, repoName)
		req, err := http.NewRequest("POST", url, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}
		req.Header.Set("Accept", "application/vnd.git-lfs+json")
		req.Header.Set("Content-Type", "application/vnd.git-lfs+json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to make batch request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200, got %d", resp.StatusCode)
		}

		var batchResp struct {
			Objects []struct {
				Oid     string `json:"oid"`
				Actions map[string]struct {
					Href string `json:"href"`
				} `json:"actions"`
				Error *struct {
					Code int `json:"code"`
				} `json:"error"`
			} `json:"objects"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if len(batchResp.Objects) != 1 {
			t.Fatalf("Expected 1 object, got %d", len(batchResp.Objects))
		}

		obj := batchResp.Objects[0]
		if obj.Error != nil {
			t.Fatalf("Expected cached object to succeed, got error code %d", obj.Error.Code)
		}
		if _, ok := obj.Actions["download"]; !ok {
			t.Fatal("Expected download action for cached object")
		}
	})
}

func TestLFSNoProxyWhenNotConfigured(t *testing.T) {
	dir, err := os.MkdirTemp("", "lfs-noproxy-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// No proxy URL configured
	stor := storage.NewStorage(storage.WithRootDir(dir))

	handler := backendlfs.NewHandler(
		backendlfs.WithStorage(stor),
	)
	server := httptest.NewServer(handler)
	defer server.Close()

	// Create a repo so the batch endpoint resolves
	repoPath := filepath.Join(stor.RepositoriesDir(), "test-repo.git")
	if err := os.MkdirAll(filepath.Dir(repoPath), 0755); err != nil {
		t.Fatalf("Failed to create repos dir: %v", err)
	}
	_, err = repository.Init(repoPath, "main")
	if err != nil {
		t.Fatalf("Failed to init repository: %v", err)
	}

	// Make a batch download request for a non-existent object
	batchReq := map[string]any{
		"operation": "download",
		"transfers": []string{"basic"},
		"objects": []map[string]any{
			{"oid": "deadbeef0000000000000000000000000000000000000000000000000000dead", "size": 100},
		},
	}
	body, _ := json.Marshal(batchReq)

	url := fmt.Sprintf("%s/test-repo.git/info/lfs/objects/batch", server.URL)
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to make batch request: %v", err)
	}
	defer resp.Body.Close()

	var batchResp struct {
		Objects []struct {
			Error *struct {
				Code int `json:"code"`
			} `json:"error"`
		} `json:"objects"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(batchResp.Objects) != 1 {
		t.Fatalf("Expected 1 object, got %d", len(batchResp.Objects))
	}

	if batchResp.Objects[0].Error == nil || batchResp.Objects[0].Error.Code != 404 {
		t.Fatal("Expected 404 error for missing object without proxy")
	}
}
