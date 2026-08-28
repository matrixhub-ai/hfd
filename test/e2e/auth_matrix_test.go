package e2e_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
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
				token, err := signer.Sign(req.Context(), req.Method, req.URL.Path, authMatrixUser, time.Hour)
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

// TestCASTokenTraversesAuth pins the wire.go chain-head order: the CAS scope
// check runs before the per-URL sign validator that would otherwise 401 it.
func TestCASTokenTraversesAuth(t *testing.T) {
	validator := authenticate.NewTokenSignValidator([]byte("secret"))
	mint, authFn, err := mirror.NewXETTokenScheme(validator)
	if err != nil {
		t.Fatalf("new token scheme: %v", err)
	}

	var gotUser string
	sentinel := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, _ := authenticate.GetUserInfo(r.Context())
		gotUser = u.User
		w.WriteHeader(http.StatusNoContent)
	})
	var handler http.Handler = sentinel
	handler = authenticate.NewHandler(
		authenticate.WithNext(handler),
		authenticate.WithTokenSignValidator(validator),
	)
	handler = authenticate.TokenValidatorHandler(authenticate.NewTokenRecognizer("xet-cas", authFn), handler)

	get := func(bearer string) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/xorbs/default/abc", nil)
		req.Header.Set("Authorization", "Bearer "+bearer)
		handler.ServeHTTP(rec, req)
		return rec.Code
	}

	tok, _ := mint(time.Now())
	if code := get(tok); code != http.StatusNoContent {
		t.Fatalf("CAS token status = %d, want 204", code)
	}
	if gotUser != "xet-cas" {
		t.Fatalf("CAS token user = %q, want xet-cas", gotUser)
	}
	// A forged signed token still dies at the sign validator.
	if code := get("sign:forged.forged.forged"); code != http.StatusUnauthorized {
		t.Fatalf("forged token status = %d, want 401", code)
	}
}
