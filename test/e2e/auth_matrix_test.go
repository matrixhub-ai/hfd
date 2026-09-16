package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	xetclient "github.com/wzshiming/xet/client"
	xethf "github.com/wzshiming/xet/client/hf"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
)

const (
	authMatrixUser = "admin"
	authMatrixPass = "secret123"
	// authMatrixReadme is what the fixtures push and what the read-back
	// entries assert.
	authMatrixReadme = "# Auth Git Test\n"
)

// authMatrixCred is one credential row: how to attach the credential to a
// direct HTTP request or a git remote URL, and what the authenticate layer is
// expected to do with it. The layer only establishes identity — anonymous
// requests pass through and nothing is enforced — so only credentials that
// are present and wrong ever fail.
type authMatrixCred struct {
	name string
	// apply attaches the credential to a direct HTTP request; nil stays
	// anonymous.
	apply func(t *testing.T, req *http.Request)
	// gitUserinfo is the user:pass pair embedded into git remote URLs; ""
	// keeps the plain URL.
	gitUserinfo string
	// git reports whether the git entries are meaningful for this
	// credential: git authenticates over HTTP with basic credentials only
	// and never sends a Bearer header.
	git bool
	// wantOK is whether direct HTTP requests are expected to pass.
	wantOK bool
	// wantStatus is the expected failure status; 0 asserts only non-200,
	// mirroring the original invalid-bearer snapshot.
	wantStatus int
}

// checkAuthStatus asserts the response status the credential row expects.
func checkAuthStatus(t *testing.T, c authMatrixCred, status int) {
	t.Helper()
	switch {
	case c.wantOK:
		if status != http.StatusOK {
			t.Fatalf("Expected 200, got %d", status)
		}
	case c.wantStatus != 0:
		if status != c.wantStatus {
			t.Fatalf("Expected %d, got %d", c.wantStatus, status)
		}
	default:
		if status == http.StatusOK {
			t.Fatalf("Expected failure, but got 200 OK")
		}
	}
}

// TestAuthMatrix drives every credential kind through every entry point of a
// server whose authenticate layer is assembled the way cmd/hfd does. The
// expectations are behavior snapshots carried over from the pre-matrix auth
// tests, not aspirations: anonymous passes through everywhere, and git's
// anonymous fallback makes even wrong basic credentials succeed on the git
// entries.
func TestAuthMatrix(t *testing.T) {
	s := newE2EServer(t, withAuth(authMatrixUser, authMatrixPass))
	// Signs tokens with the same key as the server's sign validator.
	signer := authenticate.NewTokenSignValidator([]byte(authMatrixPass))

	creds := []authMatrixCred{
		{
			// No credentials means the anonymous user, which is let through.
			name:   "Anonymous",
			git:    true,
			wantOK: true,
		},
		{
			name: "BasicValid",
			apply: func(t *testing.T, req *http.Request) {
				req.SetBasicAuth(authMatrixUser, authMatrixPass)
			},
			gitUserinfo: authMatrixUser + ":" + authMatrixPass,
			git:         true,
			wantOK:      true,
		},
		{
			// Basic credentials that are present but wrong are rejected on
			// direct requests; git still succeeds via its anonymous
			// fallback (see runAuthGitClone).
			name: "BasicInvalid",
			apply: func(t *testing.T, req *http.Request) {
				req.SetBasicAuth(authMatrixUser, "wrong-password")
			},
			gitUserinfo: authMatrixUser + ":wrong-password",
			git:         true,
			wantStatus:  http.StatusUnauthorized,
		},
		{
			// A token signed for the exact (method, path) of the request.
			name: "BearerValid",
			apply: func(t *testing.T, req *http.Request) {
				token, err := signer.Sign(req.Context(), req.Method, req.URL.Path, authenticate.NewIdentity(authMatrixUser, ""), time.Hour)
				if err != nil {
					t.Fatalf("Failed to sign token: %v", err)
				}
				req.Header.Set("Authorization", "Bearer "+token)
			},
			wantOK: true,
		},
		{
			name: "BearerInvalid",
			apply: func(t *testing.T, req *http.Request) {
				req.Header.Set("Authorization", "Bearer wrong-token")
			},
		},
	}

	entries := []struct {
		name string
		// needsGit marks entries driven by the git client.
		needsGit bool
		run      func(t *testing.T, s *e2eServer, c authMatrixCred)
	}{
		{name: "APICreateRepo", run: runAuthAPICreateRepo},
		{name: "GitClone", needsGit: true, run: runAuthGitClone},
		{name: "GitPush", needsGit: true, run: runAuthGitPush},
		{name: "Resolve", run: runAuthResolve},
	}

	for _, cred := range creds {
		t.Run(cred.name, func(t *testing.T) {
			for _, entry := range entries {
				t.Run(entry.name, func(t *testing.T) {
					if entry.needsGit && !cred.git {
						// supported=false: git never sends a Bearer header
						// over HTTP, and a signed token is bound to a single
						// (method, path) so it could not span the info/refs
						// and pack-exchange requests of one git command.
						t.Skipf("%s not supported for %s: git does not send bearer tokens", entry.name, cred.name)
					}
					entry.run(t, s, cred)
				})
			}
		})
	}
}

// TestAuthenticatedTransferMatrix drives real LFS and xet clients through the
// authenticated chain and rejects invalid basic, signed-action, and CAS tokens.
func TestAuthenticatedTransferMatrix(t *testing.T) {
	rec := &requestRecorder{}
	s := newE2EServer(t, withAuth(authMatrixUser, authMatrixPass), withWrap(rec.wrap), withSSH())
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("requests:\n%s", rec.dump())
		}
	})
	request := func(t *testing.T, method, target string, body io.Reader, headers http.Header, want int) []byte {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), method, target, body)
		if err != nil {
			t.Fatal(err)
		}
		req.Header = headers
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v\n%s", method, target, err, rec.dump())
		}
		defer resp.Body.Close()
		got, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read %s %s: %v\nbody: %s\n%s", method, target, err, got, rec.dump())
		}
		if resp.StatusCode != want {
			t.Fatalf("%s %s status = %d, want %d\nbody: %s\n%s", method, target, resp.StatusCode, want, got, rec.dump())
		}
		return got
	}
	tamper := func(t *testing.T, token string) string {
		t.Helper()
		signature := strings.LastIndexByte(token, '.') + 1
		if signature == 0 || signature == len(token) {
			t.Fatalf("token has no signature: %q", token)
		}
		replacement := "A"
		if token[signature] == 'A' {
			replacement = "B"
		}
		return token[:signature] + replacement + token[signature+1:]
	}

	t.Run("GitLFSBasic", func(t *testing.T) {
		if _, err := exec.LookPath("git-lfs"); err != nil {
			t.Skip("git-lfs not available")
		}
		repoID := "auth-org/transfer-lfs"
		s.createRepo(t, "auth-org", "transfer-lfs")
		remote, env := s.httpRemote(repoID)
		remote = credRemote(remote, authMatrixCred{gitUserinfo: authMatrixUser + ":" + authMatrixPass})
		env = append(env, "GIT_CONFIG_COUNT=2", "GIT_CONFIG_KEY_1=lfs.url", "GIT_CONFIG_VALUE_1="+remote+"/info/lfs")
		data := makeBinaryData(64*1024, 81)
		pushViaGitLFS(t, s, remote, env, repoID, data)
		verifyGitLFSPull(t, s, remote, env, repoID, data)

		fresh := makeBinaryData(64*1024, 82)
		batchBody := fmt.Sprintf(`{"operation":"upload","objects":[{"oid":"%x","size":%d}]}`, sha256.Sum256(fresh), len(fresh))
		headers := http.Header{
			"Accept":       {"application/vnd.git-lfs+json"},
			"Content-Type": {"application/vnd.git-lfs+json"},
		}
		body := request(t, http.MethodPost, remote+"/info/lfs/objects/batch", strings.NewReader(batchBody), headers, http.StatusOK)
		var batch struct {
			Objects []struct {
				Actions map[string]struct {
					Href   string            `json:"href"`
					Header map[string]string `json:"header"`
				} `json:"actions"`
			} `json:"objects"`
		}
		if err := json.Unmarshal(body, &batch); err != nil || len(batch.Objects) != 1 {
			t.Fatalf("decode batch: %v\nbody: %s", err, body)
		}
		upload := batch.Objects[0].Actions["upload"]
		authorization := upload.Header["Authorization"]
		if upload.Href == "" || !strings.HasPrefix(authorization, "Bearer ") {
			t.Fatalf("missing signed upload action\nbody: %s", body)
		}
		request(t, http.MethodPut, upload.Href, bytes.NewReader(fresh), http.Header{
			"Authorization": {tamper(t, authorization)},
		}, http.StatusUnauthorized)
		plain, _ := s.httpRemote(repoID)
		wrong := credRemote(plain, authMatrixCred{gitUserinfo: authMatrixUser + ":wrong-password"})
		request(t, http.MethodPost, wrong+"/info/lfs/objects/batch", strings.NewReader(batchBody), headers, http.StatusUnauthorized)
	})

	t.Run("XetGoClient", func(t *testing.T) {
		repoID := "auth-org/transfer-xet-go"
		s.createRepo(t, "auth-org", "transfer-xet-go")
		data := makeBinaryData(64*1024, 83)
		pushViaXetBatch(t, s, repoID, data)
		verifyHFResolveXet(t, s, repoID, data)

		base := credRemote(s.httpURL, authMatrixCred{gitUserinfo: authMatrixUser + ":" + authMatrixPass})
		body := request(t, http.MethodGet, base+"/api/models/"+repoID+"/xet-read-token/main", nil, nil, http.StatusOK)
		var token struct {
			AccessToken string `json:"accessToken"`
			CASURL      string `json:"casUrl"`
		}
		if err := json.Unmarshal(body, &token); err != nil || token.AccessToken == "" || token.CASURL == "" {
			t.Fatalf("decode CAS token: %v\nbody: %s", err, body)
		}
		fileHash, _, err := xethf.ResolveDownload(t.Context(), nil, s.httpURL+"/"+repoID+"/resolve/main/"+transferMatrixFile)
		if err != nil {
			t.Fatalf("resolve file hash: %v\n%s", err, rec.dump())
		}
		route := fmt.Sprintf("%s/v1/reconstructions/%s", token.CASURL, fileHash)
		request(t, http.MethodGet, route, nil, http.Header{
			"Authorization": {"Bearer " + token.AccessToken},
		}, http.StatusOK)
		request(t, http.MethodGet, route, nil, http.Header{
			"Authorization": {"Bearer " + tamper(t, token.AccessToken)},
		}, http.StatusUnauthorized)
	})

	t.Run("HFCliXetCore", func(t *testing.T) {
		requireUpDownMatrixTools(t)
		repoID := "auth-org/transfer-hf-xet"
		s.createRepo(t, "auth-org", "transfer-hf-xet")
		runHF := func(token string, args ...string) (string, error) {
			t.Helper()
			var env []string
			for _, entry := range testEnv() {
				if !strings.HasPrefix(entry, "HF_HUB_DISABLE_XET=") {
					env = append(env, entry)
				}
			}
			env = append(env,
				"HF_ENDPOINT="+s.httpURL,
				"HF_HUB_DISABLE_TELEMETRY=1",
				"HF_TOKEN="+token,
				"HF_HOME="+t.TempDir(),
				"HF_HUB_VERBOSITY=debug",
			)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, "hf", args...)
			cmd.Env = env
			cmd.WaitDelay = 10 * time.Second
			output, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("hf %v timed out: %v\n%s\n%s", args, ctx.Err(), output, rec.dump())
			}
			return string(output), err
		}
		data := makeBinaryData(64*1024, 84)
		src := filepath.Join(t.TempDir(), transferMatrixFile)
		if err := os.WriteFile(src, data, 0644); err != nil {
			t.Fatal(err)
		}
		rec.reset()
		output, err := runHF(authMatrixPass, "upload", repoID, src, transferMatrixFile, "--commit-message", "authenticated xet upload")
		if err != nil {
			t.Fatalf("hf upload: %v\n%s\n%s", err, output, rec.dump())
		}
		if !rec.saw("", "/xorbs/") && !rec.saw("", "/shards") {
			t.Fatalf("hf upload never wrote to CAS\n%s\n%s", output, rec.dump())
		}
		rec.reset()
		dst := t.TempDir()
		output, err = runHF(authMatrixPass, "download", repoID, transferMatrixFile, "--local-dir", dst)
		if err != nil {
			t.Fatalf("hf download: %v\n%s\n%s", err, output, rec.dump())
		}
		got, err := os.ReadFile(filepath.Join(dst, transferMatrixFile))
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("hf download bytes mismatch: %v (got %d, want %d bytes)\n%s\n%s", err, len(got), len(data), output, rec.dump())
		}
		if !rec.saw(http.MethodGet, "reconstructions") {
			t.Fatalf("hf download never queried reconstructions\n%s\n%s", output, rec.dump())
		}
		rec.reset()
		output, err = runHF("wrong-token", "upload", repoID, src, "rejected.bin", "--commit-message", "invalid token upload")
		if err == nil || !strings.Contains(output, "401") {
			t.Fatalf("hf upload with wrong token: err = %v, want 401 failure\n%s\n%s", err, output, rec.dump())
		}
		request(t, http.MethodGet, s.httpURL+"/api/whoami-v2", nil, http.Header{
			"Authorization": {"Bearer wrong-token"},
		}, http.StatusUnauthorized)
	})
}

// seedAuthRepo creates repoID and pushes authMatrixReadme as README.md over
// the plain remote; the anonymous fixture path is itself covered by the
// Anonymous row.
func seedAuthRepo(t *testing.T, s *e2eServer, repoID string) {
	t.Helper()
	org, name, _ := strings.Cut(repoID, "/")
	s.createRepo(t, org, name)
	remote, env := s.httpRemote(repoID)
	dir := filepath.Join(t.TempDir(), "seed")
	runGit(t, "", env, "clone", remote, dir)
	runGit(t, dir, env, "config", "user.email", "test@test.com")
	runGit(t, dir, env, "config", "user.name", "Test User")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte(authMatrixReadme), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	runGit(t, dir, env, "add", "README.md")
	runGit(t, dir, env, "commit", "-m", "Initial commit")
	runGit(t, dir, env, "push", "origin", "main")
}

// credRemote embeds the row's userinfo into the plain remote URL.
func credRemote(remote string, c authMatrixCred) string {
	if c.gitUserinfo == "" {
		return remote
	}
	return strings.Replace(remote, "http://", "http://"+c.gitUserinfo+"@", 1)
}

func runAuthAPICreateRepo(t *testing.T, s *e2eServer, c authMatrixCred) {
	name := "create-" + strings.ToLower(c.name)
	body := fmt.Sprintf(`{"type":"model","name":%q,"organization":"auth-org"}`, name)
	req, err := http.NewRequest(http.MethodPost, s.httpURL+"/api/repos/create", strings.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apply != nil {
		c.apply(t, req)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	resp.Body.Close()
	checkAuthStatus(t, c, resp.StatusCode)
}

func runAuthGitClone(t *testing.T, s *e2eServer, c authMatrixCred) {
	repoID := "auth-org/clone-" + strings.ToLower(c.name)
	seedAuthRepo(t, s, repoID)
	remote, env := s.httpRemote(repoID)

	// With anonymous fallback, wrong credentials never block the clone: the
	// server serves the anonymous request, so git succeeds without its
	// (wrong) password ever being rejected.
	cloneDir := filepath.Join(t.TempDir(), "clone")
	runGit(t, "", env, "clone", credRemote(remote, c), cloneDir)

	if _, err := os.Stat(filepath.Join(cloneDir, ".git")); os.IsNotExist(err) {
		t.Errorf(".git directory not found in cloned repository")
	}
	content, err := os.ReadFile(filepath.Join(cloneDir, "README.md"))
	if err != nil {
		t.Fatalf("Failed to read README.md from clone: %v", err)
	}
	if string(content) != authMatrixReadme {
		t.Errorf("Unexpected content: %q", content)
	}
}

func runAuthGitPush(t *testing.T, s *e2eServer, c authMatrixCred) {
	repoID := "auth-org/push-" + strings.ToLower(c.name)
	org, name, _ := strings.Cut(repoID, "/")
	s.createRepo(t, org, name)
	remote, env := s.httpRemote(repoID)

	workDir := filepath.Join(t.TempDir(), "clone")
	runGit(t, "", env, "clone", credRemote(remote, c), workDir)
	runGit(t, workDir, env, "config", "user.email", "test@test.com")
	runGit(t, workDir, env, "config", "user.name", "Test User")
	if err := os.WriteFile(filepath.Join(workDir, "README.md"), []byte(authMatrixReadme), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	runGit(t, workDir, env, "add", "README.md")
	runGit(t, workDir, env, "commit", "-m", "Commit with auth")
	// Like the clone row, wrong credentials ride the anonymous fallback:
	// the push lands as the anonymous user because nothing challenges it.
	runGit(t, workDir, env, "push", "origin", "main")

	// Read back the pushed file over resolve to prove the bytes landed. The
	// row's credential gets its documented direct-HTTP treatment (wrong basic
	// credentials still 401 here), so those rows re-read anonymously for the
	// content assertion.
	req, err := http.NewRequest(http.MethodGet, s.httpURL+"/"+repoID+"/resolve/main/README.md", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	if c.apply != nil {
		c.apply(t, req)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to read back pushed file: %v", err)
	}
	checkAuthStatus(t, c, resp.StatusCode)
	if !c.wantOK {
		resp.Body.Close()
		resp, err = http.Get(s.httpURL + "/" + repoID + "/resolve/main/README.md")
		if err != nil {
			t.Fatalf("Failed to read back pushed file anonymously: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200 reading back anonymously, got %d", resp.StatusCode)
		}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != authMatrixReadme {
		t.Errorf("Unexpected pushed content: %q", body)
	}
}

func runAuthResolve(t *testing.T, s *e2eServer, c authMatrixCred) {
	repoID := "auth-org/resolve-" + strings.ToLower(c.name)
	seedAuthRepo(t, s, repoID)

	req, err := http.NewRequest(http.MethodGet, s.httpURL+"/"+repoID+"/resolve/main/README.md", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	if c.apply != nil {
		c.apply(t, req)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to get file: %v", err)
	}
	defer resp.Body.Close()
	checkAuthStatus(t, c, resp.StatusCode)
	if !c.wantOK {
		return
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != authMatrixReadme {
		t.Errorf("Unexpected content: %q", body)
	}
}

func TestCASAuthBoundary(t *testing.T) {
	s := newE2EServer(t, withAuth("admin", "secret"))
	fileHash := xet.FileHash{1, 2, 3}
	token, _, err := s.issuer.Sign(auth.Grant{Permission: auth.Read, File: &fileHash})
	if err != nil {
		t.Fatal(err)
	}
	reconstruction := "/v1/reconstructions/" + fileHash.String()
	other := xet.FileHash{4, 5, 6}.String()
	for _, tc := range []struct {
		name       string
		path       string
		token      string
		wantStatus int
	}{
		{"CASGrant", reconstruction, token, http.StatusNotFound},
		{"MissingGrant", reconstruction, "", http.StatusUnauthorized},
		{"CASGrantIsNotUser", "/api/whoami-v2", token, http.StatusUnauthorized},
		{"UserTokenIsNotGrant", reconstruction, "secret", http.StatusUnauthorized},
		{"CASGrantOtherFile", "/v1/reconstructions/" + other, token, http.StatusForbidden},
		{"CASGrantMixedBatch", "/reconstructions?file_id=" + fileHash.String() + "&file_id=" + other, token, http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, s.httpURL+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
		})
	}
}

// TestLFSBatchTokenBoundToOID checks the batch's CAS token registers only its intended object.
func TestLFSBatchTokenBoundToOID(t *testing.T) {
	s := newE2EServer(t)
	repoID := "auth-org/lfs-token-bound"
	s.createRepo(t, "auth-org", "lfs-token-bound")
	dataA := makeBinaryData(64*1024, 91)
	dataB := makeBinaryData(64*1024, 92)
	oidA := fmt.Sprintf("%x", sha256.Sum256(dataA))
	oidB := fmt.Sprintf("%x", sha256.Sum256(dataB))
	upload, _ := negotiateXetUpload(t, s, repoID, oidA, len(dataA))
	xc, err := xetclient.NewClient(xetclient.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("create xet client: %v", err)
	}
	provider := xetclient.StaticAuthProvider(upload.Header["X-Xet-Cas-Url"], upload.Header["X-Xet-Access-Token"])
	if _, err := xc.UploadFileWithAuthProvider(t.Context(), provider, bytes.NewReader(dataB)); err == nil {
		t.Fatal("upload of other content succeeded with the token for oidA")
	}
	resp, err := http.Get(s.httpURL + "/objects/" + oidB)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("other object status = %d, want 404", resp.StatusCode)
	}
	if _, err := xc.UploadFileWithAuthProvider(t.Context(), provider, bytes.NewReader(dataA)); err != nil {
		t.Fatalf("xet upload: %v", err)
	}
	verifyObjectsEndpoint(t, s, oidA, dataA)
}
