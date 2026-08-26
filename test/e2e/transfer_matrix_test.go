package e2e_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	xetclient "github.com/wzshiming/xet/client"
	xethf "github.com/wzshiming/xet/hf"
	"golang.org/x/crypto/ssh"

	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	backendssh "github.com/matrixhub-ai/hfd/pkg/backend/ssh"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
)

const transferMatrixFile = "model.bin"

// transferMatrixServer bundles an HTTP server (hf + LFS + git + xet data
// plane) and an SSH git server sharing the same storage and mirror.
type transferMatrixServer struct {
	httpURL string
	sshURL  string
	sshEnv  []string
}

// newTransferMatrixServer wires the full production handler chain. During the
// S3 pass the xet storage lives in the fake S3 bucket, like production.
// Optional wrappers are applied outermost, so they observe every request
// including CAS traffic.
func newTransferMatrixServer(t *testing.T, wrap ...func(http.Handler) http.Handler) *transferMatrixServer {
	t.Helper()

	dataDir := newDataDir(t, "transfer-matrix-data")
	st := newTestStorage(t, dataDir)

	// The mirror handler serves the xet token routes, so the data plane needs
	// an upstream; fully ingested objects never contact it. During the S3
	// pass the xet storage lives in the fake S3 bucket, like production.
	upstream := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(upstream.Close)

	sharedMirror, dataPlane := newTestMirror(t, dataDir, upstream.URL, testS3Client != nil,
		mirror.WithRepositoriesFS(st.RepositoriesFS()),
	)

	var handler http.Handler
	handler = backendhf.NewHandler(
		backendhf.WithStorage(st),
		backendhf.WithMirror(sharedMirror),
		backendhf.WithNext(dataPlane),
	)
	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(st),
		backendlfs.WithNext(handler),
		backendlfs.WithMirror(sharedMirror),
	)
	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(st),
		backendhttp.WithNext(handler),
	)
	for _, w := range wrap {
		handler = w(handler)
	}

	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)

	keyFile := filepath.Join(t.TempDir(), "id_matrix")
	pubKey := generateTestKeyFile(t, keyFile)

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate host key: %v", err)
	}
	hostKey, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("create host key signer: %v", err)
	}
	sshServer := backendssh.NewServer(
		backendssh.WithHostKey(hostKey),
		backendssh.WithStorage(st),
		backendssh.WithPublicKeyCallback(backendssh.AuthorizedKeysCallback([]ssh.PublicKey{pubKey})),
	)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for SSH: %v", err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		_ = sshServer.Serve(t.Context(), listener)
	}()

	addr := listener.Addr().(*net.TCPAddr)
	port := strings.Split(addr.String(), ":")[1]

	return &transferMatrixServer{
		httpURL: httpServer.URL,
		sshURL:  "ssh://git@" + addr.String() + "/",
		sshEnv:  sshGitEnv(keyFile, port),
	}
}

func (s *transferMatrixServer) createRepo(t *testing.T, org, name string) {
	t.Helper()
	body := fmt.Sprintf(`{"type":"model","name":%q,"organization":%q}`, name, org)
	resp, err := http.Post(s.httpURL+"/api/repos/create", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create repo status = %d, want 200", resp.StatusCode)
	}
}

func (s *transferMatrixServer) httpRemote(repoID string) (string, []string) {
	return s.httpURL + "/" + repoID + ".git", []string{"GIT_TERMINAL_PROMPT=0"}
}

func (s *transferMatrixServer) sshRemote(repoID string) (string, []string) {
	return s.sshURL + repoID + ".git", s.sshEnv
}

// TestTransferProtocolMatrix pushes one LFS-tracked file through each write
// path (git-lfs over HTTP, git-lfs over SSH, xet transfer via the batch API)
// and verifies each read path (git-lfs pull, plain hf resolve, xet-capable hf
// resolve, the basic /objects endpoint) returns the same bytes. TestMain runs
// the suite twice, covering local and S3 storage.
func TestTransferProtocolMatrix(t *testing.T) {
	if _, err := exec.LookPath("git-lfs"); err != nil {
		t.Skip("git-lfs not available, skipping transfer protocol matrix test")
	}

	pushes := []struct {
		name   string
		repo   string
		push   func(t *testing.T, s *transferMatrixServer, repoID string, data []byte)
		remote func(s *transferMatrixServer, repoID string) (string, []string)
	}{
		{
			name: "GitHTTPBasicLFS",
			repo: "matrix-org/transfer-git-http",
			push: func(t *testing.T, s *transferMatrixServer, repoID string, data []byte) {
				remote, env := s.httpRemote(repoID)
				pushViaGitLFS(t, s, remote, env, repoID, data)
			},
			remote: (*transferMatrixServer).httpRemote,
		},
		{
			name: "GitSSHBasicLFS",
			repo: "matrix-org/transfer-git-ssh",
			push: func(t *testing.T, s *transferMatrixServer, repoID string, data []byte) {
				remote, env := s.sshRemote(repoID)
				pushViaGitLFS(t, s, remote, env, repoID, data)
			},
			remote: (*transferMatrixServer).sshRemote,
		},
		{
			name:   "XetTransferUpload",
			repo:   "matrix-org/transfer-xet",
			push:   pushViaXetBatch,
			remote: (*transferMatrixServer).httpRemote,
		},
	}

	for i, p := range pushes {
		t.Run(p.name, func(t *testing.T) {
			s := newTransferMatrixServer(t)
			org, name, _ := strings.Cut(p.repo, "/")
			s.createRepo(t, org, name)

			data := makeBinaryData(128*1024, byte(10+i))
			sum := sha256.Sum256(data)
			oid := hex.EncodeToString(sum[:])

			p.push(t, s, p.repo, data)

			t.Run("ReadGitLFSPull", func(t *testing.T) {
				remote, env := p.remote(s, p.repo)
				verifyGitLFSPull(t, s, remote, env, p.repo, data)
			})
			t.Run("ReadHFResolvePlain", func(t *testing.T) {
				verifyHFResolvePlain(t, s, p.repo, oid, data)
			})
			t.Run("ReadHFResolveXet", func(t *testing.T) {
				verifyHFResolveXet(t, s, p.repo, data)
			})
			t.Run("ReadLFSObjectsEndpoint", func(t *testing.T) {
				verifyObjectsEndpoint(t, s, oid, data)
			})
		})
	}
}

// pushViaGitLFS clones over the given remote and pushes an LFS-tracked file;
// the content lands through the basic transfer PUT endpoint. The server's
// initial .gitattributes already tracks *.bin.
func pushViaGitLFS(t *testing.T, s *transferMatrixServer, remote string, env []string, repoID string, data []byte) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "push")
	env = append(append([]string{}, env...), "GIT_LFS_SKIP_SMUDGE=1")

	runXETGitCmd(t, "", env, "clone", remote, dir)
	runXETGitCmd(t, dir, env, "config", "user.email", "matrix@test.com")
	runXETGitCmd(t, dir, env, "config", "user.name", "Matrix Test")
	runXETGitCmd(t, dir, env, "lfs", "install", "--local")
	// The SSH server has no LFS endpoint; point git-lfs at the HTTP server.
	runXETGitCmd(t, dir, env, "config", "lfs.url", s.httpURL+"/"+repoID+".git/info/lfs")

	if err := os.WriteFile(filepath.Join(dir, transferMatrixFile), data, 0644); err != nil {
		t.Fatalf("write LFS file: %v", err)
	}
	runXETGitCmd(t, dir, env, "add", ".")
	runXETGitCmd(t, dir, env, "commit", "-m", "add lfs file")
	runXETGitCmd(t, dir, env, "push", "origin", "main")
}

// pushViaXetBatch commits the LFS pointer with plain git the way hub-style
// clients do, negotiates the xet transfer through the batch API, uploads the
// bytes to the CAS with the minted credentials, and fires the verify action.
func pushViaXetBatch(t *testing.T, s *transferMatrixServer, repoID string, data []byte) {
	t.Helper()
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])

	dir := filepath.Join(t.TempDir(), "push")
	remote, env := s.httpRemote(repoID)
	runXETGitCmd(t, "", env, "clone", remote, dir)
	runXETGitCmd(t, dir, env, "config", "user.email", "matrix@test.com")
	runXETGitCmd(t, dir, env, "config", "user.name", "Matrix Test")
	// The object bytes go through the CAS, not the pre-push hook.
	runXETGitCmd(t, dir, env, "config", "lfs.allowincompletepush", "true")

	pointer := fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, len(data))
	if err := os.WriteFile(filepath.Join(dir, transferMatrixFile), []byte(pointer), 0644); err != nil {
		t.Fatalf("write pointer file: %v", err)
	}
	runXETGitCmd(t, dir, env, "add", ".")
	runXETGitCmd(t, dir, env, "commit", "-m", "add lfs pointer")
	runXETGitCmd(t, dir, env, "push", "origin", "main")

	// Batch negotiation: advertising xet must select the xet transfer and
	// hand out CAS credentials on the upload action.
	batchBody := fmt.Sprintf(`{"operation":"upload","transfers":["xet","basic"],"objects":[{"oid":%q,"size":%d}]}`, oid, len(data))
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, s.httpURL+"/"+repoID+".git/info/lfs/objects/batch", strings.NewReader(batchBody))
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
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("batch status = %d, want 200", resp.StatusCode)
	}

	var batch struct {
		Transfer string `json:"transfer"`
		Objects  []struct {
			Oid     string `json:"oid"`
			Actions map[string]struct {
				Href   string            `json:"href"`
				Header map[string]string `json:"header"`
			} `json:"actions"`
		} `json:"objects"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&batch); err != nil {
		t.Fatalf("decode batch response: %v", err)
	}
	if batch.Transfer != "xet" {
		t.Fatalf("batch transfer = %q, want xet", batch.Transfer)
	}
	if len(batch.Objects) != 1 {
		t.Fatalf("batch objects = %d, want 1", len(batch.Objects))
	}
	upload, ok := batch.Objects[0].Actions["upload"]
	if !ok {
		t.Fatal("batch response has no upload action")
	}
	casURL := upload.Header["X-Xet-Cas-Url"]
	casToken := upload.Header["X-Xet-Access-Token"]
	if casURL == "" || casToken == "" {
		t.Fatalf("upload action missing CAS credentials: %v", upload.Header)
	}

	xc, err := xetclient.NewClient(xetclient.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("create xet client: %v", err)
	}
	provider := xetclient.StaticAuthProvider(casURL, casToken)
	if _, err := xc.UploadFileWithAuthProvider(t.Context(), provider, bytes.NewReader(data)); err != nil {
		t.Fatalf("xet upload: %v", err)
	}

	verify, ok := batch.Objects[0].Actions["verify"]
	if !ok {
		t.Fatal("batch response has no verify action")
	}
	verifyBody := fmt.Sprintf(`{"oid":%q,"size":%d}`, oid, len(data))
	vreq, err := http.NewRequestWithContext(t.Context(), http.MethodPost, verify.Href, strings.NewReader(verifyBody))
	if err != nil {
		t.Fatalf("build verify request: %v", err)
	}
	vreq.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	for k, v := range verify.Header {
		vreq.Header.Set(k, v)
	}
	vresp, err := http.DefaultClient.Do(vreq)
	if err != nil {
		t.Fatalf("verify request: %v", err)
	}
	vresp.Body.Close()
	if vresp.StatusCode != http.StatusOK {
		t.Fatalf("verify status = %d, want 200", vresp.StatusCode)
	}
}

// verifyGitLFSPull clones over the given remote and pulls the LFS content
// through the basic transfer (batch API + /objects download).
func verifyGitLFSPull(t *testing.T, s *transferMatrixServer, remote string, env []string, repoID string, want []byte) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "read")
	env = append(append([]string{}, env...), "GIT_LFS_SKIP_SMUDGE=1")

	runXETGitCmd(t, "", env, "clone", remote, dir)
	runXETGitCmd(t, dir, env, "config", "lfs.url", s.httpURL+"/"+repoID+".git/info/lfs")
	runXETGitCmd(t, dir, env, "lfs", "pull")

	got, err := os.ReadFile(filepath.Join(dir, transferMatrixFile))
	if err != nil {
		t.Fatalf("read pulled file: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("git lfs pull bytes mismatch: got %d bytes, want %d", len(got), len(want))
	}
}

// verifyHFResolvePlain checks the hub resolve contract for plain clients:
// metadata headers plus a 302 to the sha256 bridge that serves the bytes.
func verifyHFResolvePlain(t *testing.T, s *transferMatrixServer, repoID, oid string, want []byte) {
	t.Helper()
	noRedirect := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	resolveURL := s.httpURL + "/" + repoID + "/resolve/main/" + transferMatrixFile
	resp, err := noRedirect.Get(resolveURL)
	if err != nil {
		t.Fatalf("resolve request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("resolve status = %d, want 302", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Linked-Size"); got != fmt.Sprint(len(want)) {
		t.Fatalf("X-Linked-Size = %q, want %d", got, len(want))
	}
	if got := resp.Header.Get("X-Linked-Etag"); got != `"`+oid+`"` {
		t.Fatalf("X-Linked-Etag = %q, want %q", got, oid)
	}
	loc := resp.Header.Get("Location")
	if !strings.HasSuffix(loc, "/xet-bridge/"+oid) {
		t.Fatalf("Location = %q, want suffix /xet-bridge/%s", loc, oid)
	}

	bridgeResp, err := http.Get(loc)
	if err != nil {
		t.Fatalf("bridge request: %v", err)
	}
	defer bridgeResp.Body.Close()
	if bridgeResp.StatusCode != http.StatusOK {
		t.Fatalf("bridge status = %d, want 200", bridgeResp.StatusCode)
	}
	got, err := io.ReadAll(bridgeResp.Body)
	if err != nil {
		t.Fatalf("read bridge body: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("bridge bytes mismatch")
	}
}

// verifyHFResolveXet downloads through the xet protocol the way xet-capable
// hub clients do: Link headers to token and reconstruction info, then chunk
// fetches through the CAS.
func verifyHFResolveXet(t *testing.T, s *transferMatrixServer, repoID string, want []byte) {
	t.Helper()
	resolveURL := s.httpURL + "/" + repoID + "/resolve/main/" + transferMatrixFile
	// nil client: the xet metadata rides on the 302 itself, so redirects
	// must not be followed.
	fileHash, provider, err := xethf.ResolveDownload(t.Context(), nil, resolveURL)
	if err != nil {
		t.Fatalf("resolve xet download: %v", err)
	}

	xc, err := xetclient.NewClient(xetclient.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("create xet client: %v", err)
	}
	f, err := os.Create(filepath.Join(t.TempDir(), "xet-read"))
	if err != nil {
		t.Fatalf("create download file: %v", err)
	}
	defer f.Close()
	if err := xc.DownloadFileWithAuthProvider(t.Context(), provider, fileHash, f); err != nil {
		t.Fatalf("xet download: %v", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("rewind download file: %v", err)
	}
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("xet download bytes mismatch")
	}
}

// verifyObjectsEndpoint fetches the object through the basic transfer
// download route the batch API hands out.
func verifyObjectsEndpoint(t *testing.T, s *transferMatrixServer, oid string, want []byte) {
	t.Helper()
	resp, err := http.Get(s.httpURL + "/objects/" + oid)
	if err != nil {
		t.Fatalf("objects request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("objects status = %d, want 200", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read objects body: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("objects bytes mismatch")
	}
}
