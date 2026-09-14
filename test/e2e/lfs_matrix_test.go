package e2e_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestLFSOperationsMatrix tests LFS operations across different scenarios
func TestLFSOperationsMatrix(t *testing.T) {
	if _, err := exec.LookPath("git-lfs"); err != nil {
		t.Skip("git-lfs not available, skipping LFS matrix test")
	}

	testCases := []struct {
		name         string
		filePatterns []string
		files        map[string][]byte
	}{
		{
			name:         "SingleBinaryFile",
			filePatterns: []string{"*.bin"},
			files: map[string][]byte{
				"model.bin": makeBinaryData(1024, 0),
			},
		},
		{
			name:         "MultipleFileTypes",
			filePatterns: []string{"*.bin", "*.weights", "*.safetensors"},
			files: map[string][]byte{
				"model.bin":         makeBinaryData(512, 1),
				"weights.weights":   makeBinaryData(256, 2),
				"model.safetensors": makeBinaryData(128, 3),
			},
		},
		{
			name:         "LargeFile",
			filePatterns: []string{"*.large"},
			files: map[string][]byte{
				"large.large": makeBinaryData(2048, 4),
			},
		},
		{
			name:         "MultiplePatterns",
			filePatterns: []string{"*.pt", "*.pth"},
			files: map[string][]byte{
				"model.pt":  makeBinaryData(512, 5),
				"state.pth": makeBinaryData(256, 6),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := newE2EServer(t)
			endpoint := s.httpURL

			clientDir, err := os.MkdirTemp("", "lfs-matrix-client")
			if err != nil {
				t.Fatalf("Failed to create temp client dir: %v", err)
			}
			defer os.RemoveAll(clientDir)

			// Create a repo
			resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
				strings.NewReader(`{"type":"model","name":"lfs-test","organization":"lfs-org"}`))
			if err != nil {
				t.Fatalf("Failed to create repo: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("Expected 200 creating repo, got %d", resp.StatusCode)
			}

			httpGitURL := endpoint + "/lfs-org/lfs-test.git"
			env := []string{"GIT_TERMINAL_PROMPT=0"}

			// Clone the repo
			cloneDir := filepath.Join(clientDir, "clone")
			runGit(t, "", env, "clone", httpGitURL, cloneDir)

			// Configure user
			runGit(t, cloneDir, env, "config", "user.email", "test@test.com")
			runGit(t, cloneDir, env, "config", "user.name", "Test User")

			// Track files with LFS
			for _, pattern := range tc.filePatterns {
				runGit(t, cloneDir, env, "lfs", "track", pattern)
			}

			// Create files
			for name, content := range tc.files {
				if err := os.WriteFile(filepath.Join(cloneDir, name), content, 0644); err != nil {
					t.Fatalf("Failed to create file %s: %v", name, err)
				}
			}

			// Also create a regular text file
			if err := os.WriteFile(filepath.Join(cloneDir, "README.md"), []byte("# LFS Test\n"), 0644); err != nil {
				t.Fatalf("Failed to create README: %v", err)
			}

			// Add and commit
			runGit(t, cloneDir, env, "add", ".")
			runGit(t, cloneDir, env, "commit", "-m", "Add LFS tracked files")
			runGit(t, cloneDir, env, "push", "origin", "main")

			// Clone into a new directory and verify LFS content
			verifyDir := filepath.Join(clientDir, "verify")
			runGit(t, "", env, "clone", httpGitURL, verifyDir)

			// Pull LFS content
			runGit(t, verifyDir, env, "lfs", "pull")

			// Verify all files
			for name, expectedContent := range tc.files {
				verifyContent, err := os.ReadFile(filepath.Join(verifyDir, name))
				if err != nil {
					t.Fatalf("Failed to read %s from verify clone: %v", name, err)
				}
				if len(verifyContent) != len(expectedContent) {
					t.Errorf("File %s size mismatch: got %d, want %d", name, len(verifyContent), len(expectedContent))
				} else {
					for i := range expectedContent {
						if verifyContent[i] != expectedContent[i] {
							t.Errorf("File %s content mismatch at byte %d", name, i)
							break
						}
					}
				}
			}

			// Verify the text file content
			readmeContent, err := os.ReadFile(filepath.Join(verifyDir, "README.md"))
			if err != nil {
				t.Fatalf("Failed to read README.md from verify clone: %v", err)
			}
			if string(readmeContent) != "# LFS Test\n" {
				t.Errorf("Unexpected README content: %q", readmeContent)
			}
		})
	}

	t.Run("RangeRequests", func(t *testing.T) {
		s := newE2EServer(t)
		s.createRepo(t, "lfs-org", "lfs-range")
		repoID := "lfs-org/lfs-range"
		data := makeBinaryData(256*1024, 10)
		sum := sha256.Sum256(data)
		oid := hex.EncodeToString(sum[:])
		pushViaXetBatch(t, s, repoID, data)
		objectURL := s.httpURL + "/objects/" + oid
		if got := mustGet(t, objectURL); !bytes.Equal(got, data) {
			t.Fatalf("initial object GET bytes mismatch: got %d bytes, want %d", len(got), len(data))
		}

		client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		resp, err := client.Get(s.httpURL + "/" + repoID + "/resolve/main/" + transferMatrixFile)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusFound {
			t.Fatalf("resolve status = %d, want 302; headers=%v", resp.StatusCode, resp.Header)
		}
		location, err := resp.Location()
		if err != nil || location.Path != "/xet-bridge/"+oid {
			t.Fatalf("resolve Location = %q, err=%v; headers=%v", resp.Header.Get("Location"), err, resp.Header)
		}
		for _, endpoint := range []struct{ name, url string }{
			{"Objects", objectURL},
			{"Bridge", location.String()},
		} {
			t.Run(endpoint.name, func(t *testing.T) {
				for _, request := range []struct {
					name, method, byteRange, contentRange string
					status                                int
					want                                  []byte
				}{
					{"Prefix", http.MethodGet, "bytes=0-1023", fmt.Sprintf("bytes 0-1023/%d", len(data)), http.StatusPartialContent, data[:1024]},
					{"Suffix", http.MethodGet, "bytes=-1024", fmt.Sprintf("bytes %d-%d/%d", len(data)-1024, len(data)-1, len(data)), http.StatusPartialContent, data[len(data)-1024:]},
					{"Unsatisfiable", http.MethodGet, fmt.Sprintf("bytes=%d-", len(data)), fmt.Sprintf("bytes */%d", len(data)), http.StatusRequestedRangeNotSatisfiable, nil},
					{"Full", http.MethodGet, "", "", http.StatusOK, data},
					{"Head", http.MethodHead, "", "", http.StatusOK, nil},
				} {
					t.Run(request.name, func(t *testing.T) {
						req, err := http.NewRequestWithContext(t.Context(), request.method, endpoint.url, nil)
						if err != nil {
							t.Fatal(err)
						}
						if request.byteRange != "" {
							req.Header.Set("Range", request.byteRange)
						}
						resp, err := client.Do(req)
						if err != nil {
							t.Fatalf("%s Range=%q: %v", request.method, request.byteRange, err)
						}
						defer resp.Body.Close()
						body, err := io.ReadAll(resp.Body)
						if err != nil || resp.StatusCode != request.status || resp.Header.Get("Content-Range") != request.contentRange {
							t.Fatalf("Range=%q: status=%d want=%d headers=%v readErr=%v; want Content-Range=%q", request.byteRange, resp.StatusCode, request.status, resp.Header, err, request.contentRange)
						}
						if request.status != http.StatusRequestedRangeNotSatisfiable && !bytes.Equal(body, request.want) {
							t.Fatalf("Range=%q: bytes mismatch: got=%d want=%d status=%d headers=%v", request.byteRange, len(body), len(request.want), resp.StatusCode, resp.Header)
						}
						if request.status == http.StatusOK && resp.Header.Get("Content-Length") != fmt.Sprint(len(data)) {
							t.Fatalf("%s: Content-Length=%q want=%d status=%d headers=%v", request.method, resp.Header.Get("Content-Length"), len(data), resp.StatusCode, resp.Header)
						}
					})
				}
			})
		}
	})

	// TreeAPILFSMetadata: the tree API must mark a pushed LFS file with an
	// lfs object carrying the oid and size, and report the real byte size.
	t.Run("TreeAPILFSMetadata", func(t *testing.T) {
		s := newE2EServer(t)
		s.createRepo(t, "lfs-org", "lfs-tree")
		repoID := "lfs-org/lfs-tree"
		data := makeBinaryData(2048, 7)
		sum := sha256.Sum256(data)
		oid := hex.EncodeToString(sum[:])
		remote, env := s.httpRemote(repoID)
		pushViaGitLFS(t, s, remote, env, repoID, data)

		resp, err := http.Get(s.httpURL + "/api/models/" + repoID + "/tree/main/")
		if err != nil {
			t.Fatalf("get tree: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("tree status = %d, want 200", resp.StatusCode)
		}
		var entries []struct {
			Path string `json:"path"`
			Size int64  `json:"size"`
			LFS  *struct {
				OID  string `json:"oid"`
				Size int64  `json:"size"`
			} `json:"lfs"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
			t.Fatalf("decode tree response: %v", err)
		}
		found := false
		for _, entry := range entries {
			if entry.Path != transferMatrixFile {
				continue
			}
			found = true
			if entry.LFS == nil {
				t.Fatalf("expected %s to be marked as LFS, got regular entry", transferMatrixFile)
			}
			if entry.LFS.OID != oid {
				t.Errorf("lfs oid = %q, want %q", entry.LFS.OID, oid)
			}
			if entry.LFS.Size != int64(len(data)) {
				t.Errorf("lfs size = %d, want %d", entry.LFS.Size, len(data))
			}
			if entry.Size != int64(len(data)) {
				t.Errorf("tree size = %d, want %d", entry.Size, len(data))
			}
			break
		}
		if !found {
			t.Fatalf("%s not found in tree response", transferMatrixFile)
		}
	})

	// IdempotentReupload: pushing the same LFS content a second time from a
	// fresh clone must not error and must leave the resolved content intact.
	t.Run("IdempotentReupload", func(t *testing.T) {
		s := newE2EServer(t)
		s.createRepo(t, "lfs-org", "lfs-idempotent")
		repoID := "lfs-org/lfs-idempotent"
		data := makeBinaryData(4096, 8)
		remote, env := s.httpRemote(repoID)
		pushViaGitLFS(t, s, remote, env, repoID, data)

		dir := filepath.Join(t.TempDir(), "reupload")
		env = append(append([]string{}, env...), "GIT_LFS_SKIP_SMUDGE=1")
		runGit(t, "", env, "clone", remote, dir)
		runGit(t, dir, env, "config", "user.email", "test@test.com")
		runGit(t, dir, env, "config", "user.name", "Test User")
		runGit(t, dir, env, "lfs", "install", "--local")
		if err := os.WriteFile(filepath.Join(dir, "model2.bin"), data, 0644); err != nil {
			t.Fatalf("write reupload file: %v", err)
		}
		runGit(t, dir, env, "add", ".")
		runGit(t, dir, env, "commit", "-m", "reupload same lfs content")
		runGit(t, dir, env, "push", "origin", "main")

		for _, name := range []string{transferMatrixFile, "model2.bin"} {
			resp, err := http.Get(s.httpURL + "/" + repoID + "/resolve/main/" + name)
			if err != nil {
				t.Fatalf("resolve %s: %v", name, err)
			}
			got, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatalf("read resolved %s: %v", name, err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("resolve %s status = %d, want 200", name, resp.StatusCode)
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("resolved %s content mismatch: got %d bytes, want %d", name, len(got), len(data))
			}
		}
	})

	// LargeFile11MB: a file over the hub's 10MB threshold must round-trip
	// byte-identically through the LFS path.
	t.Run("LargeFile11MB", func(t *testing.T) {
		s := newE2EServer(t)
		s.createRepo(t, "lfs-org", "lfs-large")
		repoID := "lfs-org/lfs-large"
		data := makeBinaryData(11*1024*1024, 9)
		remote, env := s.httpRemote(repoID)
		pushViaGitLFS(t, s, remote, env, repoID, data)
		verifyGitLFSPull(t, s, remote, env, repoID, data)
	})
}

// makeBinaryData creates binary data for testing
func makeBinaryData(size int, seed byte) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i + int(seed)) % 256)
	}
	return data
}
