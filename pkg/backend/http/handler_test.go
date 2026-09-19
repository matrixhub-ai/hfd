package backend_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

var _ permission.MirrorRoles = (*mirror.Mirror)(nil)

type pullOnlyMirrorRoles struct{}

func (pullOnlyMirrorRoles) IsMirrorSource(context.Context, string) (bool, error) {
	return true, nil
}

func (pullOnlyMirrorRoles) IsMirrorDestination(context.Context, string) (bool, error) {
	return false, nil
}

func TestHTTPHandlerPullMirrorReadOnly(t *testing.T) {
	forEachMode(t, testHTTPHandlerPullMirrorReadOnly)
}

func testHTTPHandlerPullMirrorReadOnly(t *testing.T, native bool) {
	dataDir := t.TempDir()
	repoPath := filepath.Join(dataDir, "repositories", "test-repo.git")
	if err := os.MkdirAll(filepath.Dir(repoPath), 0755); err != nil {
		t.Fatal(err)
	}
	runGitCmd(t, "", "init", "--bare", repoPath)
	handler := backendhttp.NewHandler(
		backendhttp.WithStorage(newStorage(dataDir, native)),
		backendhttp.WithPermissionHookFunc(permission.PullMirrorReadOnly(pullOnlyMirrorRoles{})),
	)
	for _, test := range []struct {
		service string
		want    int
	}{
		{"git-receive-pack", http.StatusForbidden},
		{"git-upload-pack", http.StatusOK},
	} {
		t.Run(test.service, func(t *testing.T) {
			response := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/test-repo.git/info/refs?service="+test.service, nil)
			handler.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Errorf("status = %d, want %d; body = %q", response.Code, test.want, response.Body.String())
			}
			if test.want == http.StatusForbidden && strings.TrimSpace(response.Body.String()) != "permission denied" {
				t.Errorf("body = %q, want permission denied", response.Body.String())
			}
			if test.want == http.StatusOK && !strings.Contains(response.Body.String(), wantAgent(native)) {
				t.Errorf("advertisement lacks %q: %q", wantAgent(native), response.Body.String())
			}
		})
	}
}

// runGitCmd runs a git command in the specified directory.
func runGitCmd(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("Git command failed: git %s\nError: %v\nOutput: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

func TestHTTPHandler(t *testing.T) {
	forEachMode(t, testHTTPHandler)
}

func testHTTPHandler(t *testing.T, native bool) {
	// Create a temporary directory for the upstream server
	upstreamDir, err := os.MkdirTemp("", "http-test-upstream")
	if err != nil {
		t.Fatalf("Failed to create temp upstream dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(upstreamDir)
	}()

	// Create a temporary directory for client operations
	clientDir, err := os.MkdirTemp("", "http-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(clientDir)
	}()

	upstreamStorage := newStorage(upstreamDir, native)

	// Create a bare repository on the upstream
	repoName := "test-repo"
	repoPath := filepath.Join(upstreamDir, "repositories", repoName+".git")
	if err := os.MkdirAll(filepath.Dir(repoPath), 0755); err != nil {
		t.Fatalf("Failed to create repos dir: %v", err)
	}
	runGitCmd(t, "", "init", "--bare", repoPath)

	// Set up upstream HTTP handler
	upstreamHandler := backendhttp.NewHandler(
		backendhttp.WithStorage(upstreamStorage),
	)
	upstreamServer := httptest.NewServer(upstreamHandler)
	defer upstreamServer.Close()

	upstreamURL := upstreamServer.URL + "/" + repoName + ".git"
	requireAgent(t, upstreamURL, native)

	t.Run("CloneEmptyRepository", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-empty")
		runGitCmd(t, "", "clone", upstreamURL, cloneDir)

		hfdir := filepath.Join(cloneDir, ".git")
		if _, err := os.Stat(hfdir); os.IsNotExist(err) {
			t.Errorf(".git directory not found in cloned repository")
		}
	})

	t.Run("PushToRepository", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-empty")

		runGitCmd(t, workDir, "config", "user.email", "test@test.com")
		runGitCmd(t, workDir, "config", "user.name", "Test User")

		testFile := filepath.Join(workDir, "README.md")
		if err := os.WriteFile(testFile, []byte("# Test Repository\n"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		runGitCmd(t, workDir, "add", "README.md")
		runGitCmd(t, workDir, "commit", "-m", "Initial commit")
		runGitCmd(t, workDir, "push", "-u", "origin", "master")
	})

	t.Run("CloneWithContent", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-with-content")
		runGitCmd(t, "", "clone", upstreamURL, cloneDir)

		readmePath := filepath.Join(cloneDir, "README.md")
		content, err := os.ReadFile(readmePath)
		if err != nil {
			t.Fatalf("Failed to read README.md: %v", err)
		}
		if string(content) != "# Test Repository\n" {
			t.Errorf("Unexpected content: %s", content)
		}
	})
}

func TestHTTPHandlerAuthHook(t *testing.T) {
	forEachMode(t, testHTTPHandlerAuthHook)
}

func testHTTPHandlerAuthHook(t *testing.T, native bool) {
	// Create a temporary directory for the upstream server
	upstreamDir, err := os.MkdirTemp("", "http-test-authhook")
	if err != nil {
		t.Fatalf("Failed to create temp upstream dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(upstreamDir)
	}()

	// Create a temporary directory for client operations
	clientDir, err := os.MkdirTemp("", "http-test-authhook-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(clientDir)
	}()

	upstreamStorage := newStorage(upstreamDir, native)

	// Create a bare repository on the upstream
	repoName := "test-repo"
	repoPath := filepath.Join(upstreamDir, "repositories", repoName+".git")
	if err := os.MkdirAll(filepath.Dir(repoPath), 0755); err != nil {
		t.Fatalf("Failed to create repos dir: %v", err)
	}
	runGitCmd(t, "", "init", "--bare", repoPath)

	t.Run("AuthHookAllowsRead", func(t *testing.T) {
		// Auth hook that allows all operations
		handler := backendhttp.NewHandler(
			backendhttp.WithStorage(upstreamStorage),
			backendhttp.WithPermissionHookFunc(func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) (bool, error) {
				return true, nil
			}),
		)
		server := httptest.NewServer(handler)
		defer server.Close()
		requireAgent(t, server.URL+"/"+repoName+".git", native)

		// Clone should succeed
		cloneDir := filepath.Join(clientDir, "clone-allowed")
		runGitCmd(t, "", "clone", server.URL+"/"+repoName+".git", cloneDir)

		hfdir := filepath.Join(cloneDir, ".git")
		if _, err := os.Stat(hfdir); os.IsNotExist(err) {
			t.Errorf(".git directory not found in cloned repository")
		}
	})

	t.Run("AuthHookDeniesRead", func(t *testing.T) {
		// Auth hook that denies all operations
		handler := backendhttp.NewHandler(
			backendhttp.WithStorage(upstreamStorage),
			backendhttp.WithPermissionHookFunc(func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) (bool, error) {
				return false, nil
			}),
		)
		server := httptest.NewServer(handler)
		defer server.Close()

		// info/refs request should return 403
		resp, err := http.Get(server.URL + "/" + repoName + ".git/info/refs?service=git-upload-pack")
		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("Expected 403, got %d", resp.StatusCode)
		}
	})

	t.Run("AuthHookDeniesWriteAllowsRead", func(t *testing.T) {
		// Auth hook that allows fetches but denies pushes
		handler := backendhttp.NewHandler(
			backendhttp.WithStorage(upstreamStorage),
			backendhttp.WithPermissionHookFunc(func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) (bool, error) {
				if op == permission.OperationUpdateRepo {
					return false, nil
				}
				return true, nil
			}),
		)
		server := httptest.NewServer(handler)
		defer server.Close()

		// Read (git-upload-pack) should succeed
		resp, err := http.Get(server.URL + "/" + repoName + ".git/info/refs?service=git-upload-pack")
		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusForbidden {
			t.Errorf("Expected read to be allowed, got 403")
		}

		// Write (git-receive-pack) should be denied
		resp2, err := http.Get(server.URL + "/" + repoName + ".git/info/refs?service=git-receive-pack")
		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}
		defer resp2.Body.Close()
		if resp2.StatusCode != http.StatusForbidden {
			t.Errorf("Expected write to be denied (403), got %d", resp2.StatusCode)
		}
	})

	t.Run("AuthHookReceivesCorrectRepoName", func(t *testing.T) {
		var capturedRepo string
		var capturedOp permission.Operation
		handler := backendhttp.NewHandler(
			backendhttp.WithStorage(upstreamStorage),
			backendhttp.WithPermissionHookFunc(func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) (bool, error) {
				capturedRepo = repo
				capturedOp = op
				return true, nil
			}),
		)
		server := httptest.NewServer(handler)
		defer server.Close()

		resp, err := http.Get(server.URL + "/" + repoName + ".git/info/refs?service=git-upload-pack")
		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}
		defer resp.Body.Close()

		if capturedRepo != repoName {
			t.Errorf("Expected repo name %q, got %q", repoName, capturedRepo)
		}
		if capturedOp != permission.OperationReadRepo {
			t.Errorf("Expected operation %v, got %v", permission.OperationReadRepo, capturedOp)
		}
	})
}

func TestHTTPHandlerPreOpenHook(t *testing.T) {
	forEachMode(t, testHTTPHandlerPreOpenHook)
}

func testHTTPHandlerPreOpenHook(t *testing.T, native bool) {
	dataDir := t.TempDir()
	st := newStorage(dataDir, native)
	runGitCmd(t, "", "init", "--bare", filepath.Join(dataDir, "repositories", "test-repo.git"))

	type call struct {
		name  string
		write bool
	}
	var calls []call
	handler := backendhttp.NewHandler(
		backendhttp.WithStorage(st),
		backendhttp.WithPreOpenHookFunc(func(ctx context.Context, repoName string, write bool) error {
			calls = append(calls, call{repoName, write})
			if repoName == "late" {
				_, err := repository.Init(ctx, st.RepositoriesFS(), repository.ResolvePath(repoName), "main")
				return err
			}
			return nil
		}),
	)
	for _, test := range []struct {
		name   string
		method string
		target string
		body   string
		want   int
		calls  []call
	}{
		{"InfoRefsRead", http.MethodGet, "/test-repo.git/info/refs?service=git-upload-pack", "", http.StatusOK, []call{{"test-repo", false}}},
		{"InfoRefsWrite", http.MethodGet, "/test-repo.git/info/refs?service=git-receive-pack", "", http.StatusOK, []call{{"test-repo", true}}},
		{"ReceivePack", http.MethodPost, "/test-repo.git/git-receive-pack", "0000", http.StatusOK, []call{{"test-repo", true}}},
		{"HookCreatesRepository", http.MethodGet, "/late.git/info/refs?service=git-upload-pack", "", http.StatusOK, []call{{"late", false}}},
		{"MissingRepository", http.MethodGet, "/missing.git/info/refs?service=git-upload-pack", "", http.StatusNotFound, []call{{"missing", false}}},
		// The router redirects to the cleaned path, so the handler never sees an interior "..".
		{"InvalidName", http.MethodGet, "/org/../repo.git/info/refs?service=git-upload-pack", "", http.StatusMovedPermanently, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls = nil
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(test.method, test.target, strings.NewReader(test.body)))
			if rec.Code != test.want {
				t.Errorf("status = %d, want %d; body = %q", rec.Code, test.want, rec.Body.String())
			}
			if !slices.Equal(calls, test.calls) {
				t.Errorf("hook calls = %v, want %v", calls, test.calls)
			}
		})
	}
}
