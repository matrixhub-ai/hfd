package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixhub-ai/hfd/internal/utils"
	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/storage"
	xetserver "github.com/wzshiming/xet/server"
	xetstorage "github.com/wzshiming/xet/storage"
)

// xetLFSBatchHandler wraps next, intercepting LFS batch upload requests and
// returning XET transfer credentials instead of the standard basic PUT URLs.
// It only intercepts POST requests to paths ending in /info/lfs/objects/batch
// where the client advertises the "xet" transfer protocol and the operation is "upload".
func xetLFSBatchHandler(casURL, casToken string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/info/lfs/objects/batch") {
			next.ServeHTTP(w, r)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body: "+err.Error(), http.StatusInternalServerError)
			return
		}
		// Re-buffer so that next handler can read the body if we fall through.
		r.Body = io.NopCloser(bytes.NewReader(body))

		var req struct {
			Operation string   `json:"operation"`
			Transfers []string `json:"transfers"`
			Objects   []struct {
				Oid  string `json:"oid"`
				Size int64  `json:"size"`
			} `json:"objects"`
		}
		if err := json.Unmarshal(body, &req); err != nil || req.Operation != "upload" {
			// Not an upload (or unparseable) — let the standard handler deal with it.
			next.ServeHTTP(w, r)
			return
		}

		// Only intercept if the client advertises the "xet" transfer protocol.
		hasXET := false
		for _, tr := range req.Transfers {
			if strings.EqualFold(tr, "xet") {
				hasXET = true
				break
			}
		}
		if !hasXET {
			next.ServeHTTP(w, r)
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

		objects := make([]obj, 0, len(req.Objects))
		for _, o := range req.Objects {
			objects = append(objects, obj{
				Oid:  o.Oid,
				Size: o.Size,
				Actions: map[string]action{
					"upload": {
						// Href is a required field in the LFS spec even for XET transfers;
						// the XET upload path reads its credentials from the headers and
						// does not use this URL.
						Href: r.URL.String(),
						Header: map[string]string{
							"X-Xet-Cas-Url":      casURL,
							"X-Xet-Access-Token": casToken,
						},
					},
				},
			})
		}

		w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"transfer": "xet",
			"objects":  objects,
		})
	})
}

// TestXETPushMirror_E2E is an end-to-end test for the XET upload path in the
// push-mirror flow.  The scenario mirrors a real deployment:
//
//  1. A git client pushes an LFS-tracked binary to a "source" HFD server.
//  2. The post-receive hook on the source server calls Mirror.PushToRemote
//     with XET enabled.
//  3. PushToRemote pushes the git refs to a "destination" HFD server and
//     uploads the LFS object via the XET CAS protocol.
//  4. The test asserts that the XET CAS server persisted at least one shard,
//     confirming the full XET upload path executed.
func TestXETPushMirror_E2E(t *testing.T) {
	if _, err := exec.LookPath("git-lfs"); err != nil {
		t.Skip("git-lfs not available, skipping XET push mirror e2e test")
	}

	root := t.TempDir()

	// ------------------------------------------------------------------ //
	// 1.  XET CAS server (in-process)                                      //
	// ------------------------------------------------------------------ //
	xetStorageDir := filepath.Join(root, "xet-storage")
	xetStore, err := xetstorage.NewFileStorage(
		xetstorage.WithBasePath(xetStorageDir),
	)
	if err != nil {
		t.Fatalf("create xet storage: %v", err)
	}
	casHandler := xetserver.NewHandler(xetserver.WithStorage(xetStore))
	casServer := httptest.NewServer(casHandler)
	t.Cleanup(casServer.Close)
	casURL := casServer.URL
	casToken := "test-xet-token"

	// ------------------------------------------------------------------ //
	// 2.  Destination HFD server (git backend + XET-aware LFS batch)       //
	// ------------------------------------------------------------------ //
	destStorage := storage.NewStorage(storage.WithRootDir(filepath.Join(root, "dest")))
	destLFSStorage := lfs.NewLocal(destStorage.LFSDir())

	var destHandler http.Handler
	destHandler = backendhf.NewHandler(
		backendhf.WithStorage(destStorage),
		backendhf.WithLFSStorage(destLFSStorage),
	)
	// Wrap the inner HF+LFS handler with the XET-aware LFS batch interceptor,
	// then add the LFS object-upload handler on top, then the git handler.
	destHandler = backendlfs.NewHandler(
		backendlfs.WithStorage(destStorage),
		backendlfs.WithNext(destHandler),
		backendlfs.WithLFSStorage(destLFSStorage),
	)
	destHandler = xetLFSBatchHandler(casURL, casToken, destHandler)
	destHandler = backendhttp.NewHandler(
		backendhttp.WithStorage(destStorage),
		backendhttp.WithNext(destHandler),
	)

	destServer := httptest.NewServer(destHandler)
	t.Cleanup(destServer.Close)

	// Create the repository on the destination server via the HF API.
	const (
		org      = "xet-mirror-org"
		repoName = "xet-mirror-repo"
	)
	createRepoJSON := `{"type":"model","name":"` + repoName + `","organization":"` + org + `"}`
	destResp, err := http.Post(destServer.URL+"/api/repos/create", "application/json",
		strings.NewReader(createRepoJSON))
	if err != nil {
		t.Fatalf("create dest repo: %v", err)
	}
	destResp.Body.Close()
	if destResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 creating dest repo, got %d", destResp.StatusCode)
	}

	// ------------------------------------------------------------------ //
	// 3.  Source HFD server with a post-receive hook that calls             //
	//     Mirror.PushToRemote with XET enabled.                            //
	// ------------------------------------------------------------------ //
	sourceStorage := storage.NewStorage(storage.WithRootDir(filepath.Join(root, "source")))
	sourceLFSStorage := lfs.NewLocal(sourceStorage.LFSDir())

	sharedMirror := mirror.NewMirror(
		mirror.WithMirrorDestinationFunc(func(_ context.Context, name string) (string, bool, error) {
			return destServer.URL + "/" + name, true, nil
		}),
		mirror.WithLFSStorage(sourceLFSStorage),
		mirror.WithPushXET(true),
		mirror.WithCacheDir(filepath.Join(root, "xet-cache")),
	)

	postHook := func(ctx context.Context, name string, updates []receive.RefUpdate) error {
		repoPath := sourceStorage.ResolvePath(name)
		return sharedMirror.PushToRemote(ctx, repoPath, name)
	}

	var sourceHandler http.Handler
	sourceHandler = backendhf.NewHandler(
		backendhf.WithStorage(sourceStorage),
		backendhf.WithLFSStorage(sourceLFSStorage),
	)
	sourceHandler = backendlfs.NewHandler(
		backendlfs.WithStorage(sourceStorage),
		backendlfs.WithNext(sourceHandler),
		backendlfs.WithLFSStorage(sourceLFSStorage),
	)
	sourceHandler = backendhttp.NewHandler(
		backendhttp.WithStorage(sourceStorage),
		backendhttp.WithNext(sourceHandler),
		backendhttp.WithPostReceiveHookFunc(postHook),
	)

	sourceServer := httptest.NewServer(sourceHandler)
	t.Cleanup(sourceServer.Close)

	// Create the repository on the source server.
	srcResp, err := http.Post(sourceServer.URL+"/api/repos/create", "application/json",
		strings.NewReader(createRepoJSON))
	if err != nil {
		t.Fatalf("create source repo: %v", err)
	}
	srcResp.Body.Close()
	if srcResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 creating source repo, got %d", srcResp.StatusCode)
	}

	// ------------------------------------------------------------------ //
	// 4.  Client: clone → track with LFS → push                           //
	// ------------------------------------------------------------------ //
	clientDir := filepath.Join(root, "client")
	if err := os.MkdirAll(clientDir, 0755); err != nil {
		t.Fatalf("mkdir client: %v", err)
	}

	env := []string{"GIT_TERMINAL_PROMPT=0"}
	sourceGitURL := sourceServer.URL + "/" + org + "/" + repoName + ".git"
	cloneDir := filepath.Join(clientDir, "clone")

	runXETGitCmd(t, "", env, "clone", sourceGitURL, cloneDir)
	runXETGitCmd(t, cloneDir, env, "config", "user.email", "xet@test.com")
	runXETGitCmd(t, cloneDir, env, "config", "user.name", "XET Test User")
	runXETGitCmd(t, cloneDir, env, "lfs", "track", "*.bin")

	// Write a binary file that git-lfs will track.
	binContent := makeBinaryData(1024, 99)
	if err := os.WriteFile(filepath.Join(cloneDir, "model.bin"), binContent, 0644); err != nil {
		t.Fatalf("write binary file: %v", err)
	}

	runXETGitCmd(t, cloneDir, env, "add", ".")
	runXETGitCmd(t, cloneDir, env, "commit", "-m", "add lfs model")
	// This push triggers the post-receive hook on the source server, which calls
	// Mirror.PushToRemote.  PushToRemote:
	//   a) pushes the git refs to the destination server, and
	//   b) calls pushMirrorLFS which performs the XET upload to the CAS server.
	runXETGitCmd(t, cloneDir, env, "push", "origin", "main")

	// ------------------------------------------------------------------ //
	// 5.  Assert: XET CAS server should have persisted at least one shard  //
	// ------------------------------------------------------------------ //
	shardsDir := filepath.Join(xetStorageDir, "shards")
	entries, err := os.ReadDir(shardsDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read xet shards dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected XET CAS server to have at least one shard after push, got none")
	}
}

// runXETGitCmd runs a git command with the given environment for XET e2e tests.
func runXETGitCmd(t *testing.T, dir string, env []string, args ...string) {
	t.Helper()
	cmd := utils.Command(t.Context(), "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = append(os.Environ(), env...)
	if output, err := cmd.Output(); err != nil {
		t.Fatalf("git %s failed: %v\noutput: %s", strings.Join(args, " "), err, output)
	}
}
