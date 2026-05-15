package mirror

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	xetserver "github.com/wzshiming/xet/server"
	xetstorage "github.com/wzshiming/xet/storage"
)

func TestPushToRemote(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	// Set up the local "server" bare repo with a commit
	local := setupUpstreamRepo(t, root)

	// Set up an empty remote destination repo
	remote := filepath.Join(root, "remote.git")
	git(t, "", "init", "--bare", "--initial-branch=main", remote)

	// Push the initial commit from local to remote so that remote has the same state
	git(t, local, "push", remote, "+refs/heads/main:refs/heads/main")

	// Record the hash that both local and remote share before the update
	remoteRefsBefore, err := repository.GetRemoteRefs(ctx, remote)
	if err != nil {
		t.Fatalf("get remote refs before update: %v", err)
	}
	oldHash := remoteRefsBefore["refs/heads/main"]

	// Make a new commit in the local repo
	work := filepath.Join(root, "work2")
	git(t, "", "init", "--initial-branch=main", work)
	git(t, work, "config", "user.email", "test@example.com")
	git(t, work, "config", "user.name", "Test User")
	git(t, work, "remote", "add", "origin", local)
	git(t, work, "fetch", "origin")
	git(t, work, "checkout", "-b", "main", "origin/main")

	if err := os.WriteFile(filepath.Join(work, "update.txt"), []byte("updated\n"), 0o644); err != nil {
		t.Fatalf("write update file: %v", err)
	}
	git(t, work, "add", ".")
	git(t, work, "commit", "-m", "update")
	git(t, work, "push", "origin", "main")

	// Get the updated local ref
	localRepo, err := repository.Open(local)
	if err != nil {
		t.Fatalf("open local repo: %v", err)
	}
	localRefs, err := localRepo.Refs()
	if err != nil {
		t.Fatalf("get local refs: %v", err)
	}
	newHash := localRefs["refs/heads/main"]

	// Simulate a post-receive update (oldHash → newHash is an update, not a creation)
	updates := []receive.RefUpdate{
		receive.NewRefUpdate(oldHash, newHash, "refs/heads/main", local),
	}

	m := NewMirror(
		WithMirrorDestinationFunc(func(ctx context.Context, repoName string) (string, bool, error) {
			return remote, true, nil
		}),
	)

	if err := m.PushToRemote(ctx, local, "sample", updates); err != nil {
		t.Fatalf("PushToRemote failed: %v", err)
	}

	// Verify the update was pushed to the remote
	remoteRefs, err := repository.GetRemoteRefs(ctx, remote)
	if err != nil {
		t.Fatalf("get remote refs: %v", err)
	}
	if got, ok := remoteRefs["refs/heads/main"]; !ok {
		t.Fatalf("expected refs/heads/main in remote")
	} else if got != newHash {
		t.Fatalf("refs/heads/main hash mismatch: got %s, want %s", got, newHash)
	}
}

func TestPushToRemote_NotConfigured(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	local := setupUpstreamRepo(t, root)

	updates := []receive.RefUpdate{
		receive.NewRefUpdate(receive.ZeroHash, "abc123", "refs/heads/main", local),
	}

	// Mirror without mirrorDestFunc configured should be a no-op
	m := NewMirror()
	if err := m.PushToRemote(ctx, local, "sample", updates); err != nil {
		t.Fatalf("PushToRemote should be a no-op when mirrorDestFunc is not set: %v", err)
	}
}

func TestPushToRemote_DestNotEnabled(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	local := setupUpstreamRepo(t, root)

	updates := []receive.RefUpdate{
		receive.NewRefUpdate(receive.ZeroHash, "abc123", "refs/heads/main", local),
	}

	// mirrorDestFunc returns false → should be a no-op
	m := NewMirror(
		WithMirrorDestinationFunc(func(ctx context.Context, repoName string) (string, bool, error) {
			return "", false, nil
		}),
	)
	if err := m.PushToRemote(ctx, local, "sample", updates); err != nil {
		t.Fatalf("PushToRemote should be a no-op when dest is not enabled: %v", err)
	}
}

func TestPushToRemote_DeleteRef(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	local := setupUpstreamRepo(t, root)

	// Set up a remote with both main and a feature branch
	remote := filepath.Join(root, "remote.git")
	git(t, "", "init", "--bare", "--initial-branch=main", remote)
	git(t, local, "push", remote, "+refs/heads/main:refs/heads/main")

	// Create a feature branch in local and push it to the remote too
	work := filepath.Join(root, "work2")
	git(t, "", "init", "--initial-branch=main", work)
	git(t, work, "config", "user.email", "test@example.com")
	git(t, work, "config", "user.name", "Test User")
	git(t, work, "remote", "add", "origin", local)
	git(t, work, "fetch", "origin")
	git(t, work, "checkout", "-b", "feature", "origin/main")
	git(t, work, "push", "origin", "feature")
	git(t, local, "push", remote, "+refs/heads/feature:refs/heads/feature")

	// Verify the feature branch is in the remote
	remoteRefs, err := repository.GetRemoteRefs(ctx, remote)
	if err != nil {
		t.Fatalf("get remote refs: %v", err)
	}
	if _, ok := remoteRefs["refs/heads/feature"]; !ok {
		t.Fatalf("expected refs/heads/feature in remote before delete")
	}
	oldHash := remoteRefs["refs/heads/feature"]

	// Simulate deleting the feature branch on the local side
	updates := []receive.RefUpdate{
		receive.NewRefUpdate(oldHash, receive.ZeroHash, "refs/heads/feature", local),
	}

	m := NewMirror(
		WithMirrorDestinationFunc(func(ctx context.Context, repoName string) (string, bool, error) {
			return remote, true, nil
		}),
	)

	if err := m.PushToRemote(ctx, local, "sample", updates); err != nil {
		t.Fatalf("PushToRemote delete failed: %v", err)
	}

	// Verify the feature branch was deleted from the remote
	remoteRefs, err = repository.GetRemoteRefs(ctx, remote)
	if err != nil {
		t.Fatalf("get remote refs after delete: %v", err)
	}
	if _, ok := remoteRefs["refs/heads/feature"]; ok {
		t.Fatalf("expected refs/heads/feature to be absent from remote after delete")
	}
}

func TestPushToRemote_DeduplicatesConcurrentPushes(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	local := setupUpstreamRepo(t, root)

	remote := filepath.Join(root, "remote.git")
	git(t, "", "init", "--bare", "--initial-branch=main", remote)
	git(t, local, "push", remote, "+refs/heads/main:refs/heads/main")

	remoteRefsBefore, err := repository.GetRemoteRefs(ctx, remote)
	if err != nil {
		t.Fatalf("get remote refs before update: %v", err)
	}
	oldHash := remoteRefsBefore["refs/heads/main"]

	work := filepath.Join(root, "work-concurrent")
	git(t, "", "init", "--initial-branch=main", work)
	git(t, work, "config", "user.email", "test@example.com")
	git(t, work, "config", "user.name", "Test User")
	git(t, work, "remote", "add", "origin", local)
	git(t, work, "fetch", "origin")
	git(t, work, "checkout", "-b", "main", "origin/main")

	if err := os.WriteFile(filepath.Join(work, "update.txt"), []byte("updated\n"), 0o644); err != nil {
		t.Fatalf("write update file: %v", err)
	}
	git(t, work, "add", ".")
	git(t, work, "commit", "-m", "update")
	git(t, work, "push", "origin", "main")

	localRepo, err := repository.Open(local)
	if err != nil {
		t.Fatalf("open local repo: %v", err)
	}
	localRefs, err := localRepo.Refs()
	if err != nil {
		t.Fatalf("get local refs: %v", err)
	}
	newHash := localRefs["refs/heads/main"]

	updates := []receive.RefUpdate{
		receive.NewRefUpdate(oldHash, newHash, "refs/heads/main", local),
	}

	hookCountFile := filepath.Join(root, "push-count.txt")
	hookPath := filepath.Join(remote, "hooks", "pre-receive")
	hookScript := fmt.Sprintf("#!/bin/sh\nprintf 'push\\n' >> %q\nsleep 1\nexit 0\n", hookCountFile)
	if err := os.WriteFile(hookPath, []byte(hookScript), 0o755); err != nil {
		t.Fatalf("write pre-receive hook: %v", err)
	}

	m := NewMirror(
		WithMirrorDestinationFunc(func(ctx context.Context, repoName string) (string, bool, error) {
			return remote, true, nil
		}),
	)

	start := make(chan struct{})
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errCh <- m.PushToRemote(ctx, local, "sample", updates)
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("PushToRemote failed: %v", err)
		}
	}

	countData, err := os.ReadFile(hookCountFile)
	if err != nil {
		t.Fatalf("read hook count file: %v", err)
	}
	if got := strings.Count(string(countData), "push\n"); got != 1 {
		t.Fatalf("expected exactly one push execution, got %d", got)
	}
}

func TestPushToRemote_LFSObjects(t *testing.T) {
	root := t.TempDir()

	// ---- Local LFS storage ----
	localLFSDir := filepath.Join(root, "lfs-local")
	localLFS := lfs.NewLocal(localLFSDir)

	// Create a test LFS object in local storage
	content := []byte("lfs binary content for push mirror test")
	sum := sha256.Sum256(content)
	oid := hex.EncodeToString(sum[:])
	size := int64(len(content))
	if err := localLFS.Put(oid, bytes.NewReader(content), size); err != nil {
		t.Fatalf("put local LFS object: %v", err)
	}

	// ---- Remote LFS server ----
	// Simple inline LFS server that implements batch + PUT, backed by local storage.
	remoteLFS := lfs.NewLocal(filepath.Join(root, "lfs-remote"))
	var remoteServer *httptest.Server
	remoteServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/info/lfs/objects/batch"):
			var reqBody struct {
				Operation string `json:"operation"`
				Objects   []struct {
					Oid  string `json:"oid"`
					Size int64  `json:"size"`
				} `json:"objects"`
			}
			if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			type action struct {
				Href string `json:"href"`
			}
			type obj struct {
				Oid     string            `json:"oid"`
				Size    int64             `json:"size"`
				Actions map[string]action `json:"actions,omitempty"`
			}
			var objects []obj
			for _, o := range reqBody.Objects {
				ob := obj{Oid: o.Oid, Size: o.Size}
				if reqBody.Operation == "upload" && !remoteLFS.Exists(o.Oid) {
					ob.Actions = map[string]action{
						"upload": {Href: remoteServer.URL + "/objects/" + o.Oid},
					}
				}
				objects = append(objects, ob)
			}
			w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transfer": "basic",
				"objects":  objects,
			}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}

		case strings.HasPrefix(r.URL.Path, "/objects/") && r.Method == http.MethodPut:
			parts := strings.Split(r.URL.Path, "/")
			uploadOID := parts[len(parts)-1]
			if err := remoteLFS.Put(uploadOID, r.Body, r.ContentLength); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)

		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(remoteServer.Close)

	destURL := remoteServer.URL + "/sample"

	// ---- Local git repo with an LFS pointer committed ----
	// Build the LFS pointer file content manually (no git-lfs binary needed).
	lfsPointer := fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, size)

	local := setupUpstreamRepo(t, root)

	// Add the LFS pointer as a committed file in the repo via a work tree.
	work := filepath.Join(root, "work-lfs")
	git(t, "", "init", "--initial-branch=main", work)
	git(t, work, "config", "user.email", "test@example.com")
	git(t, work, "config", "user.name", "Test User")
	git(t, work, "remote", "add", "origin", local)
	git(t, work, "fetch", "origin")
	git(t, work, "checkout", "-b", "main", "origin/main")
	if err := os.WriteFile(filepath.Join(work, "model.bin"), []byte(lfsPointer), 0o644); err != nil {
		t.Fatalf("write lfs pointer file: %v", err)
	}
	git(t, work, "add", "model.bin")
	git(t, work, "commit", "-m", "add lfs pointer")
	git(t, work, "push", "origin", "main")

	localRepo, err := repository.Open(local)
	if err != nil {
		t.Fatalf("open local repo: %v", err)
	}

	// ---- Push mirror LFS ----
	m := NewMirror(
		WithLFSStorage(localLFS),
	)

	if remoteLFS.Exists(oid) {
		t.Fatalf("expected LFS object to not exist in remote before push")
	}

	if err := m.pushMirrorLFS(localRepo, destURL); err != nil {
		t.Fatalf("pushMirrorLFS failed: %v", err)
	}

	if !remoteLFS.Exists(oid) {
		t.Fatalf("expected LFS object to exist in remote after pushMirrorLFS")
	}
}

func TestPushToRemote_LFSObjects_AlreadyOnRemote(t *testing.T) {
	root := t.TempDir()

	// ---- Local LFS storage ----
	localLFS := lfs.NewLocal(filepath.Join(root, "lfs-local"))
	content := []byte("lfs content already on remote")
	sum := sha256.Sum256(content)
	oid := hex.EncodeToString(sum[:])
	size := int64(len(content))
	if err := localLFS.Put(oid, bytes.NewReader(content), size); err != nil {
		t.Fatalf("put local LFS object: %v", err)
	}

	// Remote already has the object
	remoteLFS := lfs.NewLocal(filepath.Join(root, "lfs-remote"))
	if err := remoteLFS.Put(oid, bytes.NewReader(content), size); err != nil {
		t.Fatalf("pre-populate remote LFS: %v", err)
	}

	uploadCalled := false
	var remoteServer *httptest.Server
	remoteServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/info/lfs/objects/batch"):
			var reqBody struct {
				Operation string `json:"operation"`
				Objects   []struct {
					Oid  string `json:"oid"`
					Size int64  `json:"size"`
				} `json:"objects"`
			}
			if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			type obj struct {
				Oid  string `json:"oid"`
				Size int64  `json:"size"`
			}
			// Return no actions — remote already has the object
			objects := []obj{{Oid: reqBody.Objects[0].Oid, Size: reqBody.Objects[0].Size}}
			w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
			_ = json.NewEncoder(w).Encode(map[string]any{"transfer": "basic", "objects": objects})

		case strings.HasPrefix(r.URL.Path, "/objects/") && r.Method == http.MethodPut:
			uploadCalled = true
			w.WriteHeader(http.StatusOK)

		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(remoteServer.Close)

	destURL := remoteServer.URL + "/sample"
	lfsPointer := fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, size)

	local := setupUpstreamRepo(t, root)
	work := filepath.Join(root, "work-lfs2")
	git(t, "", "init", "--initial-branch=main", work)
	git(t, work, "config", "user.email", "test@example.com")
	git(t, work, "config", "user.name", "Test User")
	git(t, work, "remote", "add", "origin", local)
	git(t, work, "fetch", "origin")
	git(t, work, "checkout", "-b", "main", "origin/main")
	if err := os.WriteFile(filepath.Join(work, "model.bin"), []byte(lfsPointer), 0o644); err != nil {
		t.Fatalf("write lfs pointer file: %v", err)
	}
	git(t, work, "add", "model.bin")
	git(t, work, "commit", "-m", "add lfs pointer")
	git(t, work, "push", "origin", "main")

	localRepo, err := repository.Open(local)
	if err != nil {
		t.Fatalf("open local repo: %v", err)
	}

	m := NewMirror(WithLFSStorage(localLFS))
	if err := m.pushMirrorLFS(localRepo, destURL); err != nil {
		t.Fatalf("pushMirrorLFS failed: %v", err)
	}

	if uploadCalled {
		t.Fatalf("expected no upload when remote already has the object")
	}
}

// TestPushToRemote_LFSObjects_XET verifies the end-to-end XET upload path:
// a mirror with enableXET=true sends the batch request with ["xet","basic"],
// the remote LFS server selects the "xet" transfer, and the object is uploaded
// to a real (in-process) XET CAS server.
func TestPushToRemote_LFSObjects_XET(t *testing.T) {
	root := t.TempDir()

	// ---- Local LFS storage ----
	localLFS := lfs.NewLocal(filepath.Join(root, "lfs-local"))
	content := []byte("xet lfs binary content for push mirror test")
	sum := sha256.Sum256(content)
	oid := hex.EncodeToString(sum[:])
	size := int64(len(content))
	if err := localLFS.Put(oid, bytes.NewReader(content), size); err != nil {
		t.Fatalf("put local LFS object: %v", err)
	}

	// ---- XET CAS server (in-process) ----
	xetStorageDir := filepath.Join(root, "xet-storage")
	xetStore, err := xetstorage.NewFileStorage(
		xetstorage.WithBasePath(xetStorageDir),
	)
	if err != nil {
		t.Fatalf("create xet storage: %v", err)
	}
	casHandler := xetserver.NewHandler(
		xetserver.WithStorage(xetStore),
	)
	casServer := httptest.NewServer(casHandler)
	t.Cleanup(casServer.Close)
	casURL := casServer.URL
	casToken := "test-token"

	// ---- Remote LFS batch server that advertises "xet" transfer ----
	// It returns the CAS server URL and a static token in the upload action headers.
	var remoteServer *httptest.Server
	remoteServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/info/lfs/objects/batch"):
			var reqBody struct {
				Operation string   `json:"operation"`
				Transfers []string `json:"transfers"`
				Objects   []struct {
					Oid  string `json:"oid"`
					Size int64  `json:"size"`
				} `json:"objects"`
			}
			if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			// Check whether the client offered the "xet" transfer protocol.
			supportsXET := false
			for _, t := range reqBody.Transfers {
				if strings.EqualFold(t, "xet") {
					supportsXET = true
					break
				}
			}

			if !supportsXET || reqBody.Operation != "upload" {
				http.Error(w, "expected xet upload request", http.StatusBadRequest)
				return
			}

			type action struct {
				Href   string            `json:"href"`
				Header map[string]string `json:"header"`
			}
			type obj struct {
				Oid     string            `json:"oid"`
				Size    int64             `json:"size"`
				Actions map[string]action `json:"actions"`
			}
			objects := []obj{
				{
					Oid:  reqBody.Objects[0].Oid,
					Size: reqBody.Objects[0].Size,
					Actions: map[string]action{
						"upload": {
							Href: remoteServer.URL + "/noop",
							Header: map[string]string{
								"X-Xet-Cas-Url":      casURL,
								"X-Xet-Access-Token": casToken,
							},
						},
					},
				},
			}
			w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
			_ = json.NewEncoder(w).Encode(map[string]any{"transfer": "xet", "objects": objects})

		case r.URL.Path == "/noop":
			// Placeholder href in the upload action; the XET client does not use it.
			w.WriteHeader(http.StatusOK)

		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(remoteServer.Close)

	destURL := remoteServer.URL + "/sample"
	lfsPointer := fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, size)

	// ---- Local git repo with LFS pointer ----
	local := setupUpstreamRepo(t, root)
	work := filepath.Join(root, "work-xet")
	git(t, "", "init", "--initial-branch=main", work)
	git(t, work, "config", "user.email", "test@example.com")
	git(t, work, "config", "user.name", "Test User")
	git(t, work, "remote", "add", "origin", local)
	git(t, work, "fetch", "origin")
	git(t, work, "checkout", "-b", "main", "origin/main")
	if err := os.WriteFile(filepath.Join(work, "model.bin"), []byte(lfsPointer), 0o644); err != nil {
		t.Fatalf("write lfs pointer file: %v", err)
	}
	git(t, work, "add", "model.bin")
	git(t, work, "commit", "-m", "add lfs pointer")
	git(t, work, "push", "origin", "main")

	localRepo, err := repository.Open(local)
	if err != nil {
		t.Fatalf("open local repo: %v", err)
	}

	// ---- Mirror with XET enabled ----
	m := NewMirror(
		WithLFSStorage(localLFS),
		WithXET(true),
		WithCacheDir(filepath.Join(root, "xet-cache")),
	)

	if err := m.pushMirrorLFS(localRepo, destURL); err != nil {
		t.Fatalf("pushMirrorLFS with XET failed: %v", err)
	}

	// Verify the XET server received the upload by checking that a shard was
	// stored for the file's content (the XET server indexes files by their shard).
	// Since the XET storage is file-based, we can check the shards directory.
	shardsDir := filepath.Join(xetStorageDir, "shards")
	entries, err := os.ReadDir(shardsDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read XET shards dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatalf("expected at least one shard in XET storage after XET upload, got none")
	}
}

func TestNewMirrorConfiguresXETIdleEvict(t *testing.T) {
	t.Run("defaults before func to time now", func(t *testing.T) {
		m := NewMirror()
		if m.lfsTeeCache == nil {
			t.Fatal("expected tee cache to be initialized")
		}
		if m.lfsTeeCache.xetEvictBeforeFunc == nil {
			t.Fatal("expected default xet evict before func")
		}
		before := m.lfsTeeCache.xetEvictBeforeFunc()
		if before.IsZero() {
			t.Fatal("expected default xet evict before time to be non-zero")
		}
	})

	t.Run("propagates custom evict settings", func(t *testing.T) {
		expectedBefore := time.Unix(123, 0)
		m := NewMirror(
			WithXETIdleEvictMaxBytes(42),
			WithXETIdleEvictBeforeFunc(func() time.Time { return expectedBefore }),
		)

		if got := m.lfsTeeCache.xetEvictMaxBytes; got != 42 {
			t.Fatalf("xetEvictMaxBytes = %d, want 42", got)
		}
		if got := m.lfsTeeCache.xetEvictBeforeFunc(); !got.Equal(expectedBefore) {
			t.Fatalf("xetEvictBeforeFunc() = %v, want %v", got, expectedBefore)
		}
	})
}

func setupUpstreamRepo(t *testing.T, root string) string {
	t.Helper()

	upstream := filepath.Join(root, "upstream.git")
	git(t, "", "init", "--bare", "--initial-branch=main", upstream)

	work := filepath.Join(root, "work")
	git(t, "", "init", "--initial-branch=main", work)
	git(t, work, "config", "user.email", "test@example.com")
	git(t, work, "config", "user.name", "Test User")

	if err := os.WriteFile(filepath.Join(work, "file.txt"), []byte("content"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	git(t, work, "add", ".")
	git(t, work, "commit", "-m", "initial")
	git(t, work, "remote", "add", "origin", upstream)
	git(t, work, "push", "-u", "origin", "main")

	return upstream
}

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=safe.bareRepository",
		"GIT_CONFIG_VALUE_0=all",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
}
