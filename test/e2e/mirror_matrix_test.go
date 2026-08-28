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

// seedMirrorUpstream stands up a plain harness server as the upstream and
// pushes readme to repoID on it, returning the server, the working clone
// (for scenarios that push more refs), and the git env.
func seedMirrorUpstream(t *testing.T, repoID, readme string) (*e2eServer, string, []string) {
	t.Helper()
	upstream := newE2EServer(t)
	org, name, _ := strings.Cut(repoID, "/")
	upstream.createRepo(t, org, name)
	remote, env := upstream.httpRemote(repoID)

	dir := filepath.Join(t.TempDir(), "upstream-clone")
	runGit(t, "", env, "clone", remote, dir)
	runGit(t, dir, env, "config", "user.email", "test@test.com")
	runGit(t, dir, env, "config", "user.name", "Test User")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte(readme), 0644); err != nil {
		t.Fatalf("Failed to write README.md: %v", err)
	}
	runGit(t, dir, env, "add", "README.md")
	runGit(t, dir, env, "commit", "-m", "Initial commit")
	runGit(t, dir, env, "push", "origin", "main")
	return upstream, dir, env
}

// newMirrorProxy stands up a pull-through proxy of upstream, with the SSH
// transport when ssh is set.
func newMirrorProxy(t *testing.T, upstream *e2eServer, ssh bool, extra ...e2eOption) *e2eServer {
	t.Helper()
	opts := append([]e2eOption{withMirrorSource(upstream.httpURL)}, extra...)
	if ssh {
		opts = append(opts, withSSH())
	}
	return newE2EServer(t, opts...)
}

func mirrorProxyRemote(proxy *e2eServer, ssh bool, repoID string) (string, []string) {
	if ssh {
		return proxy.sshRemote(repoID)
	}
	return proxy.httpRemote(repoID)
}

// TestMirrorMatrix drives the pull-through mirror behaviors over both git
// transports. Each cell builds its own upstream and proxy, so the cache and
// mirror state never leak between cells.
func TestMirrorMatrix(t *testing.T) {
	protocols := []struct {
		name string
		ssh  bool
	}{
		{name: "HTTP"},
		{name: "SSH", ssh: true},
	}

	scenarios := []struct {
		name string
		run  func(t *testing.T, ssh bool)
	}{
		{name: "PullThroughClone", run: testMirrorPullThroughClone},
		{name: "CachedClone", run: testMirrorCachedClone},
		{name: "NonexistentRepo", run: testMirrorNonexistentRepo},
		{name: "PushForbidden", run: testMirrorPushForbidden},
		{name: "RefFilter", run: testMirrorRefFilter},
	}

	for _, protocol := range protocols {
		t.Run(protocol.name, func(t *testing.T) {
			for _, scenario := range scenarios {
				t.Run(scenario.name, func(t *testing.T) {
					scenario.run(t, protocol.ssh)
				})
			}
		})
	}
}

// testMirrorPullThroughClone: the repo does not exist on the proxy; the first
// clone auto-mirrors it from the upstream and serves the content.
func testMirrorPullThroughClone(t *testing.T, ssh bool) {
	const repoID = "proxy-org/proxy-repo"
	const readme = "# Proxy Test\n"
	upstream, _, _ := seedMirrorUpstream(t, repoID, readme)
	proxy := newMirrorProxy(t, upstream, ssh)
	remote, env := mirrorProxyRemote(proxy, ssh, repoID)

	cloneDir := filepath.Join(t.TempDir(), "proxy-clone")
	runGit(t, "", env, "clone", remote, cloneDir)

	content, err := os.ReadFile(filepath.Join(cloneDir, "README.md"))
	if err != nil {
		t.Fatalf("Failed to read README.md from proxy clone: %v", err)
	}
	if string(content) != readme {
		t.Errorf("Unexpected content from proxy clone: %q", content)
	}
}

// testMirrorCachedClone: after the first clone mirrors the repo, the second
// clone is served from the proxy's own copy.
func testMirrorCachedClone(t *testing.T, ssh bool) {
	const repoID = "proxy-org/cached-repo"
	const readme = "# Proxy Test\n"
	upstream, _, _ := seedMirrorUpstream(t, repoID, readme)
	proxy := newMirrorProxy(t, upstream, ssh)
	remote, env := mirrorProxyRemote(proxy, ssh, repoID)

	runGit(t, "", env, "clone", remote, filepath.Join(t.TempDir(), "proxy-clone"))

	cloneDir := filepath.Join(t.TempDir(), "proxy-clone-cached")
	runGit(t, "", env, "clone", remote, cloneDir)

	content, err := os.ReadFile(filepath.Join(cloneDir, "README.md"))
	if err != nil {
		t.Fatalf("Failed to read README.md from cached proxy clone: %v", err)
	}
	if string(content) != readme {
		t.Errorf("Unexpected content from cached proxy clone: %q", content)
	}
}

// testMirrorNonexistentRepo: a repo the upstream does not have cannot be
// mirrored; HTTP reports 404 on info/refs and an SSH clone fails.
func testMirrorNonexistentRepo(t *testing.T, ssh bool) {
	upstream := newE2EServer(t)
	proxy := newMirrorProxy(t, upstream, ssh)

	if !ssh {
		r, err := http.Get(proxy.httpURL + "/nobody/doesnotexist.git/info/refs?service=git-upload-pack")
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		r.Body.Close()
		if r.StatusCode != http.StatusNotFound {
			t.Errorf("Expected 404, got %d", r.StatusCode)
		}
		return
	}

	remote, env := proxy.sshRemote("nobody/doesnotexist")
	cloneDir := filepath.Join(t.TempDir(), "clone")
	if _, stderr, err := gitCmd(t, "", env, "clone", remote, cloneDir); err == nil {
		t.Errorf("Expected SSH clone of nonexistent repo to fail, but it succeeded:\n%s", stderr)
	}
}

// testMirrorPushForbidden: mirrors are read-only; HTTP refuses the
// receive-pack advertisement and an SSH push fails.
func testMirrorPushForbidden(t *testing.T, ssh bool) {
	const repoID = "proxy-org/push-forbidden"
	upstream, _, _ := seedMirrorUpstream(t, repoID, "# Push Forbidden\n")
	proxy := newMirrorProxy(t, upstream, ssh)
	remote, env := mirrorProxyRemote(proxy, ssh, repoID)

	// Materialize the mirror on the proxy first.
	cloneDir := filepath.Join(t.TempDir(), "clone")
	runGit(t, "", env, "clone", remote, cloneDir)

	if !ssh {
		r, err := http.Get(proxy.httpURL + "/" + repoID + ".git/info/refs?service=git-receive-pack")
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		r.Body.Close()
		if r.StatusCode == http.StatusOK {
			t.Errorf("Expected push to mirror to be forbidden, got 200")
		}
		return
	}

	runGit(t, cloneDir, env, "config", "user.email", "test@test.com")
	runGit(t, cloneDir, env, "config", "user.name", "Test User")
	if err := os.WriteFile(filepath.Join(cloneDir, "update.txt"), []byte("update\n"), 0644); err != nil {
		t.Fatalf("Failed to write update.txt: %v", err)
	}
	runGit(t, cloneDir, env, "add", "update.txt")
	runGit(t, cloneDir, env, "commit", "-m", "Update")
	if _, stderr, err := gitCmd(t, cloneDir, env, "push", "origin", "main"); err == nil {
		t.Errorf("Expected push to mirror to fail, but it succeeded:\n%s", stderr)
	}
}

// testMirrorRefFilter: with a filter that only lets refs/heads/main through,
// the mirror serves main's content but never receives the feature branch or
// the v1.0 tag.
func testMirrorRefFilter(t *testing.T, ssh bool) {
	const repoID = "ref-filter-org/ref-filter-repo"
	const readme = "# Ref Filter Test\n"
	upstream, upstreamCloneDir, env := seedMirrorUpstream(t, repoID, readme)

	// Push a feature branch and a v1.0 tag that the filter must drop.
	runGit(t, upstreamCloneDir, env, "checkout", "-b", "feature")
	if err := os.WriteFile(filepath.Join(upstreamCloneDir, "feature.txt"), []byte("feature\n"), 0644); err != nil {
		t.Fatalf("Failed to write feature.txt: %v", err)
	}
	runGit(t, upstreamCloneDir, env, "add", "feature.txt")
	runGit(t, upstreamCloneDir, env, "commit", "-m", "Feature commit")
	runGit(t, upstreamCloneDir, env, "push", "origin", "feature")
	runGit(t, upstreamCloneDir, env, "checkout", "main")
	runGit(t, upstreamCloneDir, env, "tag", "v1.0")
	runGit(t, upstreamCloneDir, env, "push", "origin", "v1.0")

	onlyMainFilter := func(_ context.Context, _ string, refs []string) ([]string, error) {
		var filtered []string
		for _, ref := range refs {
			if ref == "refs/heads/main" {
				filtered = append(filtered, ref)
			}
		}
		return filtered, nil
	}
	proxy := newMirrorProxy(t, upstream, ssh, withRefFilter(onlyMainFilter))
	remote, cloneEnv := mirrorProxyRemote(proxy, ssh, repoID)

	cloneDir := filepath.Join(t.TempDir(), "filtered-proxy-clone")
	runGit(t, "", cloneEnv, "clone", remote, cloneDir)
	content, err := os.ReadFile(filepath.Join(cloneDir, "README.md"))
	if err != nil {
		t.Fatalf("Failed to read README.md from proxy clone: %v", err)
	}
	if string(content) != readme {
		t.Errorf("Unexpected content from proxy clone: %q", content)
	}

	// The filtered refs must not exist in the mirror itself.
	repo, err := repository.Open(proxy.storage.RepositoriesFS(), repository.ResolvePath(repoID))
	if err != nil {
		t.Fatalf("Failed to open proxy mirror repo: %v", err)
	}
	branches, err := repo.Branches()
	if err != nil {
		t.Fatalf("Failed to list branches: %v", err)
	}
	for _, b := range branches {
		if b == "feature" {
			t.Error("feature branch should not be mirrored, but found it")
		}
	}
	tags, err := repo.Tags()
	if err != nil {
		t.Fatalf("Failed to list tags: %v", err)
	}
	for _, tag := range tags {
		if tag == "v1.0" {
			t.Error("v1.0 tag should not be mirrored, but found it")
		}
	}
}

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
	destMirror, destDataPlane := newTestMirror(t, filepath.Join(root, "dest-xet"), "", false,
		mirror.WithRepositoriesFS(destStorage.RepositoriesFS()),
	)

	var destHandler http.Handler
	destHandler = backendhf.NewHandler(
		backendhf.WithStorage(destStorage),
		backendhf.WithMirror(destMirror),
		backendhf.WithNext(destDataPlane),
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

	sharedMirror, dataPlane := newTestMirror(t, filepath.Join(root, "xet-cache"), "", false,
		mirror.WithMirrorDestinationFunc(func(_ context.Context, name string) (string, bool, error) {
			return destServer.URL + "/" + name, true, nil
		}),
		mirror.WithRepositoriesFS(sourceStorage.RepositoriesFS()),
	)

	postHook := func(ctx context.Context, name string, updates []receive.RefUpdate) error {
		repoPath := repository.ResolvePath(name)
		return sharedMirror.PushToRemote(ctx, repoPath, name, nil)
	}

	var sourceHandler http.Handler
	sourceHandler = backendhf.NewHandler(
		backendhf.WithStorage(sourceStorage),
		backendhf.WithMirror(sharedMirror),
		backendhf.WithNext(dataPlane),
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

	runGit(t, "", env, "clone", sourceGitURL, cloneDir)
	runGit(t, cloneDir, env, "config", "user.email", "xet@test.com")
	runGit(t, cloneDir, env, "config", "user.name", "XET Test User")
	runGit(t, cloneDir, env, "lfs", "track", "*.bin")

	// Write a binary file that git-lfs will track.
	binContent := makeBinaryData(1024, 99)
	if err := os.WriteFile(filepath.Join(cloneDir, "model.bin"), binContent, 0644); err != nil {
		t.Fatalf("write binary file: %v", err)
	}

	runGit(t, cloneDir, env, "add", ".")
	runGit(t, cloneDir, env, "commit", "-m", "add lfs model")
	// This push triggers the post-receive hook on the source server, which calls
	// Mirror.PushToRemote.  PushToRemote:
	//   a) pushes the git refs to the destination server, and
	//   b) calls pushMirrorLFS, which negotiates the xet transfer with the
	//      destination's LFS batch endpoint and uploads to its CAS.
	runGit(t, cloneDir, env, "push", "origin", "main")

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
