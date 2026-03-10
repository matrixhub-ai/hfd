package e2e_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/hfd/internal/utils"
	"github.com/wzshiming/hfd/pkg/authenticate"
	backendhttp "github.com/wzshiming/hfd/pkg/backend/http"
	backendhuggingface "github.com/wzshiming/hfd/pkg/backend/huggingface"
	backendlfs "github.com/wzshiming/hfd/pkg/backend/lfs"
	"github.com/wzshiming/hfd/pkg/storage"
)

// setupAuthTestServer creates an HTTP test server with basic authentication enabled.
func setupAuthTestServer(t *testing.T, username, password string) (*httptest.Server, string) {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "auth-e2e-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	store := storage.NewStorage(storage.WithRootDir(dataDir))

	var handler http.Handler

	handler = backendhuggingface.NewHandler(
		backendhuggingface.WithStorage(store),
	)

	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(store),
		backendlfs.WithNext(handler),
	)

	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(store),
		backendhttp.WithNext(handler),
	)

	basicAuth := authenticate.NewSimpleBasicAuthValidator(username, password)
	tokenAuth := authenticate.NewSimpleTokenValidator(username, password)
	tokenSignValidator := authenticate.NewTokenSignValidator([]byte(password))
	handler = authenticate.AnonymousAuthenticateHandler(handler)
	handler = authenticate.TokenValidatorHandler(tokenAuth, handler)
	handler = authenticate.TokenSignValidatorHandler(tokenSignValidator, handler)
	handler = authenticate.BasicAuthHandler(basicAuth, handler)

	server := httptest.NewServer(handler)
	t.Cleanup(func() { server.Close() })

	return server, dataDir
}

func TestHTTPAuthCreateRepoWithBasicAuth(t *testing.T) {
	requireSuite(t, suiteGitHTTP)

	server, _ := setupAuthTestServer(t, "admin", "secret123")
	endpoint := server.URL

	t.Run("ValidBasicAuth", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPost, endpoint+"/api/repos/create", strings.NewReader(`{"type":"model","name":"auth-model","organization":"auth-user"}`))
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.SetBasicAuth("admin", "secret123")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200, got %d", resp.StatusCode)
		}
	})

	t.Run("InvalidBasicAuth", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPost, endpoint+"/api/repos/create", strings.NewReader(`{"type":"model","name":"auth-model-2","organization":"auth-user"}`))
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.SetBasicAuth("admin", "wrong-password")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("Expected 401, got %d", resp.StatusCode)
		}
	})

	t.Run("NoAuth", func(t *testing.T) {
		resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
			strings.NewReader(`{"type":"model","name":"auth-model-3","organization":"auth-user"}`))
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		resp.Body.Close()
		// With anonymous fallback, no credentials means anonymous user (200 OK)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200 (anonymous), got %d", resp.StatusCode)
		}
	})
}

func TestHTTPAuthBearerToken(t *testing.T) {
	requireSuite(t, suiteGitHTTP)

	server, _ := setupAuthTestServer(t, "admin", "my-secret-token")
	endpoint := server.URL

	// Generate a valid signed token for POST /api/repos/create
	tokenSignValidator := authenticate.NewTokenSignValidator([]byte("my-secret-token"))
	validToken := tokenSignValidator.Sign(t.Context(), http.MethodPost, "/api/repos/create", "admin", time.Hour)

	t.Run("ValidBearerToken", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPost, endpoint+"/api/repos/create", strings.NewReader(`{"type":"model","name":"bearer-model","organization":"bearer-user"}`))
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+validToken)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200, got %d", resp.StatusCode)
		}
	})

	t.Run("InvalidBearerToken", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPost, endpoint+"/api/repos/create", strings.NewReader(`{"type":"model","name":"bearer-model-2","organization":"bearer-user"}`))
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer wrong-token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			t.Fatalf("Expected failure with invalid token, but got 200 OK")
		}
	})
}

func TestHTTPAuthGitClonePush(t *testing.T) {
	requireSuite(t, suiteGitHTTP)

	server, _ := setupAuthTestServer(t, "gituser", "gitpass")
	endpoint := server.URL

	clientDir, err := os.MkdirTemp("", "auth-git-e2e-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	// Create repo with auth
	req, err := http.NewRequest(http.MethodPost, endpoint+"/api/repos/create", strings.NewReader(`{"type":"model","name":"auth-git-model","organization":"git-auth-user"}`))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("gituser", "gitpass")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 creating repo, got %d", resp.StatusCode)
	}

	// Build authenticated URL
	authURL := strings.Replace(endpoint, "http://", "http://gituser:gitpass@", 1)

	t.Run("CloneWithAuth", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-auth")
		cmd := utils.Command(t.Context(), "git", "clone", authURL+"/git-auth-user/auth-git-model.git", cloneDir)
		cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		output, err := cmd.Output()
		if err != nil {
			t.Fatalf("Git clone with auth failed: %v\n%s", err, output)
		}

		gitDir := filepath.Join(cloneDir, ".git")
		if _, err := os.Stat(gitDir); os.IsNotExist(err) {
			t.Errorf(".git directory not found in cloned repository")
		}
	})

	t.Run("PushWithAuth", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-auth")

		for _, args := range [][]string{
			{"config", "user.email", "test@test.com"},
			{"config", "user.name", "Test User"},
		} {
			cmd := utils.Command(t.Context(), "git", args...)
			cmd.Dir = workDir
			cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
			if output, err := cmd.Output(); err != nil {
				t.Fatalf("Git command failed: git %s\n%v\n%s", strings.Join(args, " "), err, output)
			}
		}

		testFile := filepath.Join(workDir, "README.md")
		if err := os.WriteFile(testFile, []byte("# Auth Git Test\n"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		for _, args := range [][]string{
			{"add", "README.md"},
			{"commit", "-m", "Commit with auth"},
			{"push", "origin", "main"},
		} {
			cmd := utils.Command(t.Context(), "git", args...)
			cmd.Dir = workDir
			cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
			if output, err := cmd.Output(); err != nil {
				t.Fatalf("Git command failed: git %s\n%v\n%s", strings.Join(args, " "), err, output)
			}
		}
	})

	t.Run("CloneWithWrongAuth", func(t *testing.T) {
		wrongURL := strings.Replace(endpoint, "http://", "http://gituser:wrongpass@", 1)
		cloneDir := filepath.Join(clientDir, "clone-wrong-auth")
		cmd := utils.Command(t.Context(), "git", "clone", wrongURL+"/git-auth-user/auth-git-model.git", cloneDir)
		cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		output, err := cmd.Output()
		// With anonymous fallback, wrong credentials cause 401 but git retries
		// without credentials, which falls through to anonymous and succeeds.
		if err != nil {
			t.Fatalf("Expected clone to succeed with anonymous fallback, but it failed: %v\n%s", err, output)
		}
	})

	t.Run("VerifyPushedContent", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, endpoint+"/git-auth-user/auth-git-model/resolve/main/README.md", nil)
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}
		req.SetBasicAuth("gituser", "gitpass")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to get file: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200, got %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "# Auth Git Test\n" {
			t.Errorf("Unexpected content: %q", body)
		}
	})
}
