package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// TestXETPushMirror_E2E is an end-to-end test for the XET upload path in the
// push-mirror flow.  The scenario mirrors a real deployment:
//
//  1. A git client pushes an LFS-tracked binary to a "source" HFD server.
//  2. The post-receive hook on the source server calls Mirror.PushToRemote
//     with XET enabled.
//  3. PushToRemote pushes the git refs to a "destination" HFD server whose
//     LFS batch endpoint negotiates the xet transfer and hands out CAS
//     credentials for its own data plane.
//  4. The test asserts that the destination's xet storage holds the object
//     and serves back the original bytes.
func TestXETPushMirror_E2E(t *testing.T) {
	if _, err := exec.LookPath("git-lfs"); err != nil {
		t.Skip("git-lfs not available, skipping XET push mirror e2e test")
	}

	root := t.TempDir()

	// ------------------------------------------------------------------ //
	// 1.  Destination HFD server (git backend + xet-capable LFS batch)     //
	// ------------------------------------------------------------------ //
	destStorage := newTestStorage(t, newDataDir(t, "xet-mirror-dest"))

	// Data-plane-only mirror: no pull upstream, no push destination; it
	// provides the CAS server, token issuer, and xet storage for LFS content.
	destMirror, err := mirror.NewMirror(
		mirror.WithRepositoriesFS(destStorage.RepositoriesFS()),
		mirror.WithDataDir(filepath.Join(root, "dest-xet")),
	)
	if err != nil {
		t.Fatalf("create dest mirror: %v", err)
	}

	var destHandler http.Handler
	destHandler = backendhf.NewHandler(
		backendhf.WithStorage(destStorage),
		backendhf.WithMirror(destMirror),
	)
	destHandler = backendlfs.NewHandler(
		backendlfs.WithStorage(destStorage),
		backendlfs.WithNext(destHandler),
		backendlfs.WithMirror(destMirror),
	)
	destHandler = backendhttp.NewHandler(
		backendhttp.WithStorage(destStorage),
		backendhttp.WithNext(destHandler),
	)
	destHandler = mountDataPlane(t, destMirror, destHandler)

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
	// 2.  Source HFD server with a post-receive hook that calls             //
	//     Mirror.PushToRemote with XET enabled.                            //
	// ------------------------------------------------------------------ //
	sourceStorage := newTestStorage(t, newDataDir(t, "xet-mirror-source"))

	sharedMirror, err := mirror.NewMirror(
		mirror.WithMirrorDestinationFunc(func(_ context.Context, name string) (string, bool, error) {
			return destServer.URL + "/" + name, true, nil
		}),
		mirror.WithRepositoriesFS(sourceStorage.RepositoriesFS()),
		mirror.WithDataDir(filepath.Join(root, "xet-cache")),
	)
	if err != nil {
		t.Fatalf("create mirror: %v", err)
	}

	postHook := func(ctx context.Context, name string, updates []receive.RefUpdate) error {
		repoPath := repository.ResolvePath(name)
		return sharedMirror.PushToRemote(ctx, repoPath, name, nil)
	}

	var sourceHandler http.Handler
	sourceHandler = backendhf.NewHandler(
		backendhf.WithStorage(sourceStorage),
		backendhf.WithMirror(sharedMirror),
	)
	sourceHandler = backendlfs.NewHandler(
		backendlfs.WithStorage(sourceStorage),
		backendlfs.WithNext(sourceHandler),
		backendlfs.WithMirror(sharedMirror),
	)
	sourceHandler = backendhttp.NewHandler(
		backendhttp.WithStorage(sourceStorage),
		backendhttp.WithNext(sourceHandler),
		backendhttp.WithPostReceiveHookFunc(postHook),
	)
	sourceHandler = mountDataPlane(t, sharedMirror, sourceHandler)

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
	// 3.  Client: clone → track with LFS → push                           //
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
	//   b) calls pushMirrorLFS, which negotiates the xet transfer with the
	//      destination's LFS batch endpoint and uploads to its CAS.
	runXETGitCmd(t, cloneDir, env, "push", "origin", "main")

	// ------------------------------------------------------------------ //
	// 4.  Assert: destination xet storage serves the object by its OID     //
	// ------------------------------------------------------------------ //
	sum := sha256.Sum256(binContent)
	oid := hex.EncodeToString(sum[:])
	ctx := context.Background()

	deadline := time.Now().Add(30 * time.Second)
	for !destMirror.HasObject(ctx, oid) {
		if time.Now().After(deadline) {
			t.Fatalf("destination never received object %s via xet upload", oid)
		}
		time.Sleep(50 * time.Millisecond)
	}

	rs, size, err := destMirror.OpenObject(ctx, oid)
	if err != nil {
		t.Fatalf("open uploaded object: %v", err)
	}
	defer rs.Close()
	if size != int64(len(binContent)) {
		t.Fatalf("uploaded object size = %d, want %d", size, len(binContent))
	}
	got, err := io.ReadAll(rs)
	if err != nil {
		t.Fatalf("read uploaded object: %v", err)
	}
	if !bytes.Equal(got, binContent) {
		t.Fatal("uploaded object bytes mismatch")
	}
}

// runXETGitCmd runs a git command with the given environment for XET e2e tests.
func runXETGitCmd(t *testing.T, dir string, env []string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = append(os.Environ(), env...)
	if output, err := cmd.Output(); err != nil {
		t.Fatalf("git %s failed: %v\noutput: %s", strings.Join(args, " "), err, output)
	}
}
