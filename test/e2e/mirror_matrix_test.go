package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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

const gatePrefix = 128 * 1024

// gatedUpstream strips xet Link headers so the engine fetches plain bytes, holds object bodies after gatePrefix, and records the requests it saw.
type gatedUpstream struct {
	url       string
	gets      atomic.Int32
	delivered atomic.Int64
	held      chan struct{}
	heldOnce  sync.Once
	gate      chan struct{}
	release   func()

	mu      sync.Mutex
	paths   []string
	auths   map[string]int // resolve requests per Authorization value
	objects int            // requests naming the object by file name or OID
}

// newGatedUpstream proxies origin; withhold answers object requests with 404 instead.
func newGatedUpstream(t *testing.T, origin *e2eServer, oid string, withhold bool) *gatedUpstream {
	t.Helper()
	target, err := url.Parse(origin.httpURL)
	if err != nil {
		t.Fatalf("parse origin URL: %v", err)
	}
	g := &gatedUpstream{held: make(chan struct{}), gate: make(chan struct{}), auths: make(map[string]int)}
	g.release = sync.OnceFunc(func() { close(g.gate) })
	isObject := func(p string) bool {
		return strings.HasSuffix(p, "/"+oid) || strings.HasSuffix(p, "/"+transferMatrixFile)
	}
	proxy := &httputil.ReverseProxy{
		// The inbound Host is kept so the origin's redirects point back through the gate.
		Rewrite:       func(r *httputil.ProxyRequest) { r.SetURL(target); r.Out.Host = r.In.Host },
		FlushInterval: -1,
		ModifyResponse: func(resp *http.Response) error {
			resp.Header.Del("Link")
			resp.Header.Del("X-Xet-Hash")
			if resp.Request.Method == http.MethodGet && resp.StatusCode/100 == 2 && isObject(resp.Request.URL.Path) {
				g.gets.Add(1)
				resp.Body = &gatedBody{ReadCloser: resp.Body, ctx: resp.Request.Context(), g: g}
			}
			return nil
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		object := isObject(r.URL.Path)
		g.mu.Lock()
		g.paths = append(g.paths, r.URL.Path)
		if object {
			g.objects++
		}
		if strings.Contains(r.URL.Path, "/resolve/") {
			g.auths[r.Header.Get("Authorization")]++
		}
		g.mu.Unlock()
		if withhold && object {
			http.NotFound(w, r)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	g.url = srv.URL
	return g
}

// authValues lists the distinct Authorization headers seen on resolve requests.
func (g *gatedUpstream) authValues() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Sorted(maps.Keys(g.auths))
}

// pathContaining returns the first recorded request path containing frag, or "".
func (g *gatedUpstream) pathContaining(frag string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	if i := slices.IndexFunc(g.paths, func(p string) bool { return strings.Contains(p, frag) }); i >= 0 {
		return g.paths[i]
	}
	return ""
}

func (g *gatedUpstream) objectRequests() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.objects
}

type gatedBody struct {
	io.ReadCloser
	ctx context.Context
	g   *gatedUpstream
}

func (b *gatedBody) Read(p []byte) (int, error) {
	allow := gatePrefix - b.g.delivered.Load()
	if allow <= 0 {
		b.g.heldOnce.Do(func() { close(b.g.held) })
		select {
		case <-b.g.gate:
		case <-b.ctx.Done():
			return 0, b.ctx.Err()
		}
		return b.ReadCloser.Read(p)
	}
	if int64(len(p)) > allow {
		p = p[:allow]
	}
	n, err := b.ReadCloser.Read(p)
	b.g.delivered.Add(int64(n))
	return n, err
}

func negotiateLFSDownload(t *testing.T, s *e2eServer, repoID, oid string, size int) lfsBatchAction {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	body := fmt.Sprintf(`{"operation":"download","objects":[{"oid":%q,"size":%d}]}`, oid, size)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.httpURL+"/"+repoID+".git/info/lfs/objects/batch", strings.NewReader(body))
	if err != nil {
		t.Fatalf("build batch request: %v", err)
	}
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("batch request: %v", err)
	}
	defer resp.Body.Close()
	var batch struct {
		Objects []struct {
			Oid     string                    `json:"oid"`
			Actions map[string]lfsBatchAction `json:"actions"`
			Error   *struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		} `json:"objects"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&batch); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("batch status=%d decode=%v", resp.StatusCode, err)
	}
	if len(batch.Objects) != 1 || batch.Objects[0].Oid != oid || batch.Objects[0].Error != nil {
		t.Fatalf("batch objects = %+v, want %s without error", batch.Objects, oid)
	}
	download, ok := batch.Objects[0].Actions["download"]
	if !ok {
		t.Fatalf("batch response has no download action: %+v", batch.Objects[0].Actions)
	}
	return download
}

func ingested(t *testing.T, s *e2eServer, oid string) bool {
	t.Helper()
	raw, err := hex.DecodeString(oid)
	if err != nil || len(raw) != sha256.Size {
		t.Fatalf("bad oid %q: %v", oid, err)
	}
	var digest [sha256.Size]byte
	copy(digest[:], raw)
	_, err = s.storage.XETStorage().GetFileHashBySHA256(t.Context(), "default", digest)
	return err == nil
}

func waitIngested(t *testing.T, s *e2eServer, oid string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !ingested(t, s, oid) {
		if time.Now().After(deadline) {
			t.Fatalf("object %s never got indexed after the upstream was released", oid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func readExact(t *testing.T, r io.Reader, want []byte) []byte {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(r, got); err != nil {
		t.Fatalf("read %d bytes: %v", len(want), err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("read %d bytes: content mismatch", len(want))
	}
	return got
}

func checkObjectHeaders(t *testing.T, resp *http.Response, size int64, oid string) {
	t.Helper()
	if resp.StatusCode != http.StatusOK || resp.ContentLength != size ||
		resp.Header.Get("X-Linked-Size") != fmt.Sprint(size) || resp.Header.Get("X-Linked-Etag") != `"`+oid+`"` {
		t.Fatalf("status=%d Content-Length=%d headers=%v, want 200 with size %d and etag %s", resp.StatusCode, resp.ContentLength, resp.Header, size, oid)
	}
}

func checkRange(t *testing.T, resp *http.Response, start, end, size int64) {
	t.Helper()
	want := fmt.Sprintf("bytes %d-%d/%d", start, end, size)
	if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != want {
		t.Fatalf("range status=%d Content-Range=%q, want 206 %q", resp.StatusCode, resp.Header.Get("Content-Range"), want)
	}
}

func streamingHead(t *testing.T, objectURL string, size int64) http.Header {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, objectURL, nil)
	if err != nil {
		t.Fatalf("build HEAD request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HEAD resolve: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ContentLength != size || resp.Header.Get("X-Linked-Size") != fmt.Sprint(size) {
		t.Fatalf("HEAD status=%d Content-Length=%d headers=%v, want 200 with size %d", resp.StatusCode, resp.ContentLength, resp.Header, size)
	}
	return resp.Header
}

func verifyStreamWhileIngesting(t *testing.T, up *gatedUpstream, proxy *e2eServer, repoID, objectURL string, header map[string]string, data []byte, oid string, alias bool) {
	t.Helper()
	size := int64(len(data))
	hfURL := proxy.httpURL + "/" + repoID + "/resolve/main/" + transferMatrixFile
	get := func(ctx context.Context, rng string) *http.Response {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, objectURL, nil)
		if err != nil {
			t.Fatalf("build GET request: %v", err)
		}
		for k, v := range header {
			req.Header.Set(k, v)
		}
		if rng != "" {
			req.Header.Set("Range", rng)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s range %q: %v (upstream object downloads = %d)", objectURL, rng, err, up.gets.Load())
		}
		return resp
	}
	prefix := data[:gatePrefix/2]

	ctxA, cancelA := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelA()
	a := get(ctxA, "")
	defer func() { _ = a.Body.Close() }()
	checkObjectHeaders(t, a, size, oid)
	readExact(t, a.Body, prefix)

	ctxB, cancelB := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelB()
	b := get(ctxB, "")
	defer func() { _ = b.Body.Close() }()
	checkObjectHeaders(t, b, size, oid)
	gotB := readExact(t, b.Body, prefix)

	select {
	case <-up.held:
	case <-time.After(10 * time.Second):
		t.Fatal("upstream body never reached the gate")
	}
	if n := up.gets.Load(); n != 1 {
		t.Fatalf("upstream object downloads = %d, want 1", n)
	}
	if ingested(t, proxy, oid) {
		t.Fatal("object indexed while the upstream is still held")
	}
	if cold := streamingHead(t, objectURL, size); cold.Get("X-Xet-Hash") != "" || cold.Get("Link") != "" {
		t.Fatalf("cold HEAD advertised xet metadata: %v", cold)
	}
	if alias {
		// The HF route names the repository without the transport's .git suffix and must join the running ingest.
		streamingHead(t, hfURL, size)
		ctxH, cancelH := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancelH()
		req, err := http.NewRequestWithContext(ctxH, http.MethodGet, hfURL, nil)
		if err != nil {
			t.Fatalf("build GET request: %v", err)
		}
		h, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v (upstream object downloads = %d)", hfURL, err, up.gets.Load())
		}
		defer func() { _ = h.Body.Close() }()
		checkObjectHeaders(t, h, size, oid)
		got := make([]byte, len(prefix))
		if _, err := io.ReadFull(h.Body, got); err != nil || !bytes.Equal(got, prefix) {
			t.Fatalf("HF alias read: err=%v (upstream object downloads = %d), want the held ingest's prefix", err, up.gets.Load())
		}
		if n := up.gets.Load(); n != 1 {
			t.Fatalf("upstream object downloads = %d after the HF alias request, want 1", n)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	r1 := get(ctx, "bytes=0-1023")
	checkRange(t, r1, 0, 1023, size)
	rangeData, err := io.ReadAll(r1.Body)
	_ = r1.Body.Close()
	if err != nil || !bytes.Equal(rangeData, data[:1024]) {
		t.Fatalf("prefix range: err=%v, got %d bytes, want 1024", err, len(rangeData))
	}

	start, end := int64(gatePrefix-1024), int64(gatePrefix+1023)
	r2 := get(ctx, fmt.Sprintf("bytes=%d-%d", start, end))
	defer func() { _ = r2.Body.Close() }()
	checkRange(t, r2, start, end, size)
	readExact(t, r2.Body, data[start:gatePrefix])

	// A leaves while the upstream is still held; B and the straddling range stay attached.
	cancelA()
	up.release()

	rest, err := io.ReadAll(b.Body)
	if err != nil || !bytes.Equal(append(gotB, rest...), data) {
		t.Fatalf("reader B after release: err=%v, got %d bytes, want %d", err, len(gotB)+len(rest), size)
	}
	tail, err := io.ReadAll(r2.Body)
	if err != nil || !bytes.Equal(tail, data[gatePrefix:end+1]) {
		t.Fatalf("straddling range tail: err=%v, got %d bytes, want %d", err, len(tail), end+1-gatePrefix)
	}

	waitIngested(t, proxy, oid)
	if warm := streamingHead(t, hfURL, size); warm.Get("X-Xet-Hash") == "" || !strings.Contains(warm.Get("Link"), "xet-reconstruction-info") {
		t.Fatalf("warm HEAD lacks xet metadata: %v", warm)
	}
	ctxW, cancelW := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelW()
	w := get(ctxW, "")
	defer func() { _ = w.Body.Close() }()
	got, err := io.ReadAll(w.Body)
	if err != nil || w.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("warm read: status=%d err=%v bytes=%d", w.StatusCode, err, len(got))
	}
	if n := up.gets.Load(); n != 1 {
		t.Fatalf("upstream object downloads = %d after release, want 1", n)
	}
}

func TestMirrorStreamingMatrix(t *testing.T) {
	rows := []struct {
		name  string
		git   bool
		ssh   bool
		alias bool // the git transport's .git name and the HF name must share one ingest
	}{
		{name: "HFResolve"},
		{name: "GitHTTP", git: true, alias: true},
		{name: "GitSSH", git: true, ssh: true, alias: true},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			const repoID = "stream-org/stream-repo"
			data := make([]byte, 4*gatePrefix)
			if _, err := rand.New(rand.NewSource(1)).Read(data); err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(data)
			oid := hex.EncodeToString(sum[:])

			origin := newE2EServer(t)
			origin.createRepo(t, "stream-org", "stream-repo")
			pushViaXetBatch(t, origin, repoID, data)
			up := newGatedUpstream(t, origin, oid, false)
			opts := []e2eOption{withMirrorSource(up.url)}
			if row.ssh {
				opts = append(opts, withSSH())
			}
			proxy := newE2EServer(t, opts...)
			// Registered after the proxy so it runs before the mirror and server waits (LIFO).
			t.Cleanup(up.release)

			objectURL := proxy.httpURL + "/" + repoID + "/resolve/main/" + transferMatrixFile
			var header map[string]string
			if row.git {
				remote, env := mirrorProxyRemote(proxy, row.ssh, repoID)
				dir := filepath.Join(t.TempDir(), "clone")
				runGit(t, "", append(append([]string{}, env...), "GIT_LFS_SKIP_SMUDGE=1"), "clone", remote, dir)
				pointer, err := os.ReadFile(filepath.Join(dir, transferMatrixFile))
				if err != nil || !strings.Contains(string(pointer), "oid sha256:"+oid) {
					t.Fatalf("cloned pointer = %q (%v), want pointer to %s", pointer, err, oid)
				}
				action := negotiateLFSDownload(t, proxy, repoID, oid, len(data))
				objectURL, header = action.Href, action.Header
			}
			verifyStreamWhileIngesting(t, up, proxy, repoID, objectURL, header, data, oid, row.alias)
		})
	}
}

// seedLFSOrigin stands up a plain origin holding repoID with data as its LFS file and returns it with the object's OID.
func seedLFSOrigin(t *testing.T, repoID string, data []byte) (*e2eServer, string) {
	t.Helper()
	origin := newE2EServer(t)
	org, name, _ := strings.Cut(repoID, "/")
	origin.createRepo(t, org, name)
	pushViaXetBatch(t, origin, repoID, data)
	sum := sha256.Sum256(data)
	return origin, hex.EncodeToString(sum[:])
}

// newUpstreamProxy stands up a pull-through proxy whose hubs come from fn, with the SSH transport when ssh is set.
func newUpstreamProxy(t *testing.T, ssh bool, fn upstreamMap) *e2eServer {
	t.Helper()
	opts := []e2eOption{withMirrorSources(fn)}
	if ssh {
		opts = append(opts, withSSH())
	}
	return newE2EServer(t, opts...)
}

// clonePointer clones repoID through the proxy without smudging and checks the pointer names oid.
func clonePointer(t *testing.T, proxy *e2eServer, ssh bool, repoID, oid string) (dir string, env []string) {
	t.Helper()
	remote, env := mirrorProxyRemote(proxy, ssh, repoID)
	dir = filepath.Join(t.TempDir(), "clone")
	runGit(t, "", append(append([]string{}, env...), "GIT_LFS_SKIP_SMUDGE=1"), "clone", remote, dir)
	pointer, err := os.ReadFile(filepath.Join(dir, transferMatrixFile))
	if err != nil || !strings.Contains(string(pointer), "oid sha256:"+oid) {
		t.Fatalf("cloned pointer = %q (%v), want pointer to %s", pointer, err, oid)
	}
	return dir, env
}

// TestMirrorUpstreamMatrix drives one proxy over two hubs selected per repository, on both transports.
func TestMirrorUpstreamMatrix(t *testing.T) {
	for _, protocol := range []struct {
		name string
		ssh  bool
	}{{name: "HTTP"}, {name: "SSH", ssh: true}} {
		t.Run(protocol.name, func(t *testing.T) {
			t.Run("PerRepoUpstream", func(t *testing.T) { testMirrorPerRepoUpstream(t, protocol.ssh) })
			t.Run("Remap", func(t *testing.T) { testMirrorRemap(t, protocol.ssh) })
		})
	}
}

// testMirrorPerRepoUpstream: each pull-through reaches only its own hub with its own bearer.
func testMirrorPerRepoUpstream(t *testing.T, ssh bool) {
	dataA, dataB := makeBinaryData(64*1024, 1), makeBinaryData(64*1024, 2)
	originA, oidA := seedLFSOrigin(t, "org/a", dataA)
	originB, oidB := seedLFSOrigin(t, "org/b", dataB)
	pa := newGatedUpstream(t, originA, oidA, false)
	pa.release()
	pb := newGatedUpstream(t, originB, oidB, false)
	pb.release()
	hubs := map[string]struct{ url, token string }{"org/a": {pa.url, "tok-a"}, "org/b": {pb.url, "tok-b"}}
	proxy := newUpstreamProxy(t, ssh, func(repoName string) (string, string, bool) {
		hub, ok := hubs[repoName]
		return hub.url, hub.token, ok
	})

	for _, repo := range []struct {
		id, oid string
		data    []byte
	}{{"org/a", oidA, dataA}, {"org/b", oidB, dataB}} {
		clonePointer(t, proxy, ssh, repo.id, repo.oid)
		action := negotiateLFSDownload(t, proxy, repo.id, repo.oid, len(repo.data))
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, action.Href, nil)
		if err != nil {
			t.Fatalf("build LFS download request: %v", err)
		}
		for k, v := range action.Header {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("LFS download %s: %v", action.Href, err)
		}
		got, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, repo.data) {
			t.Fatalf("LFS download of %s: status=%d err=%v bytes=%d, want %d bytes", repo.id, resp.StatusCode, err, len(got), len(repo.data))
		}
		if got := mustGet(t, proxy.httpURL+"/"+repo.id+"/resolve/main/"+transferMatrixFile); !bytes.Equal(got, repo.data) {
			t.Fatalf("HF resolve of %s returned %d bytes, want %d", repo.id, len(got), len(repo.data))
		}
	}
	if got := pa.authValues(); !slices.Equal(got, []string{"Bearer tok-a"}) {
		t.Fatalf("hub A saw Authorization %q, want only Bearer tok-a", got)
	}
	if got := pb.authValues(); !slices.Equal(got, []string{"Bearer tok-b"}) {
		t.Fatalf("hub B saw Authorization %q, want only Bearer tok-b", got)
	}
	if p := pa.pathContaining("org/b"); p != "" {
		t.Fatalf("hub A saw %s, want org/a traffic only", p)
	}
	if p := pb.pathContaining("org/a"); p != "" {
		t.Fatalf("hub B saw %s, want org/b traffic only", p)
	}
}

// testMirrorRemap: after a runtime remap the next pull-through ingests from the new hub.
func testMirrorRemap(t *testing.T, ssh bool) {
	const repoID = "org/a"
	data := makeBinaryData(64*1024, 3)
	originA, oid := seedLFSOrigin(t, repoID, data)
	originB, _ := seedLFSOrigin(t, repoID, data)
	pa := newGatedUpstream(t, originA, oid, true)
	pa.release()
	pb := newGatedUpstream(t, originB, oid, false)
	pb.release()
	var mu sync.Mutex
	hub := pa
	proxy := newUpstreamProxy(t, ssh, func(repoName string) (string, string, bool) {
		mu.Lock()
		defer mu.Unlock()
		return hub.url, "", repoName == repoID
	})
	resolveURL := proxy.httpURL + "/" + repoID + "/resolve/main/" + transferMatrixFile

	dir, env := clonePointer(t, proxy, ssh, repoID, oid)
	resp, err := http.Get(resolveURL)
	if err != nil {
		t.Fatalf("GET %s: %v", resolveURL, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET %s status = %d while hub A withholds the object, want 404", resolveURL, resp.StatusCode)
	}
	if ingested(t, proxy, oid) {
		t.Fatal("object indexed although hub A withheld it")
	}
	proxy.mirror.Wait() // the failed prefetches must finish before hub A's requests are counted
	before := pa.objectRequests()

	mu.Lock()
	hub = pb
	mu.Unlock()
	runGit(t, dir, env, "fetch", "origin")
	waitIngested(t, proxy, oid)
	if got := mustGet(t, resolveURL); !bytes.Equal(got, data) {
		t.Fatalf("HF resolve after the remap returned %d bytes, want %d", len(got), len(data))
	}
	if pb.objectRequests() == 0 {
		t.Fatal("hub B never saw an object request after the remap")
	}
	if n := pa.objectRequests(); n != before {
		t.Fatalf("hub A object requests grew from %d to %d after the remap", before, n)
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
	destMirror, destXET := newTestMirror(t, destStorage, nil,
		mirror.WithRepositoriesFS(destStorage.RepositoriesFS()),
	)

	var destHandler http.Handler
	destHandler = backendhf.NewHandler(append(catalogOptions(destStorage),
		backendhf.WithStorage(destStorage),
		backendhf.WithMirror(destMirror),
		backendhf.WithNext(http.NotFoundHandler()),
	)...)
	destHandler = backendlfs.NewHandler(
		backendlfs.WithStorage(destStorage),
		backendlfs.WithNext(destHandler),
		backendlfs.WithMirror(destMirror),
	)
	destHandler = backendhttp.NewHandler(
		backendhttp.WithStorage(destStorage),
		backendhttp.WithNext(destHandler),
	)
	destHandler = destXET.casServer(destHandler)

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

	sharedMirror, srcXET := newTestMirror(t, sourceStorage, nil,
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
	sourceHandler = backendhf.NewHandler(append(catalogOptions(sourceStorage),
		backendhf.WithStorage(sourceStorage),
		backendhf.WithMirror(sharedMirror),
		backendhf.WithNext(http.NotFoundHandler()),
	)...)
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
	sourceHandler = srcXET.casServer(sourceHandler)

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
