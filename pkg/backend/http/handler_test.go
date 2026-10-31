package backend_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

func TestHTTPHandlerPreOpenRefusesWrites(t *testing.T) {
	forEachMode(t, testHTTPHandlerPreOpenRefusesWrites)
}

func testHTTPHandlerPreOpenRefusesWrites(t *testing.T, native bool) {
	dataDir := t.TempDir()
	repoPath := filepath.Join(dataDir, "repositories", "test-repo.git")
	if err := os.MkdirAll(filepath.Dir(repoPath), 0755); err != nil {
		t.Fatal(err)
	}
	runGitCmd(t, "", "init", "--bare", repoPath)
	handler := backendhttp.NewHandler(
		backendhttp.WithStorage(newStorage(t, dataDir, native)),
		backendhttp.WithPreOpenHookFunc(func(_ context.Context, _ string, write bool) error {
			if write {
				return fs.ErrPermission
			}
			return nil
		}),
	)
	for _, test := range []struct {
		name        string
		method      string
		target      string
		contentType string
		want        int
	}{
		{"InfoRefsReceivePack", http.MethodGet, "/test-repo.git/info/refs?service=git-receive-pack", "", http.StatusForbidden},
		{"ReceivePack", http.MethodPost, "/test-repo.git/git-receive-pack", "application/x-git-receive-pack-request", http.StatusForbidden},
		{"InfoRefsUploadPack", http.MethodGet, "/test-repo.git/info/refs?service=git-upload-pack", "", http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, test.target, strings.NewReader("0000"))
			if test.contentType != "" {
				request.Header.Set("Content-Type", test.contentType)
			}
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

	upstreamStorage := newStorage(t, upstreamDir, native)

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

	upstreamStorage := newStorage(t, upstreamDir, native)

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
			backendhttp.WithPreOpenHookFunc(func(context.Context, string, bool) error {
				return nil
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
			backendhttp.WithPreOpenHookFunc(func(context.Context, string, bool) error {
				return fs.ErrPermission
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

		// The upload-pack POST should return 403 too
		resp2, err := http.Post(server.URL+"/"+repoName+".git/git-upload-pack", "application/x-git-upload-pack-request", strings.NewReader("0000"))
		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}
		defer resp2.Body.Close()
		if resp2.StatusCode != http.StatusForbidden {
			t.Errorf("Expected upload-pack POST 403, got %d", resp2.StatusCode)
		}
	})

	t.Run("AuthHookDeniesWriteAllowsRead", func(t *testing.T) {
		// Auth hook that allows fetches but denies pushes
		handler := backendhttp.NewHandler(
			backendhttp.WithStorage(upstreamStorage),
			backendhttp.WithPreOpenHookFunc(func(_ context.Context, _ string, write bool) error {
				if write {
					return fs.ErrPermission
				}
				return nil
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
		type call struct {
			repo  string
			write bool
		}
		var calls []call
		handler := backendhttp.NewHandler(
			backendhttp.WithStorage(upstreamStorage),
			backendhttp.WithPreOpenHookFunc(func(_ context.Context, repo string, write bool) error {
				calls = append(calls, call{repo, write})
				return nil
			}),
		)

		for _, service := range []string{"git-upload-pack", "git-receive-pack"} {
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/"+repoName+".git/info/refs?service="+service, nil))
		}

		if want := []call{{repoName, false}, {repoName, true}}; !slices.Equal(calls, want) {
			t.Errorf("hook calls = %v, want %v", calls, want)
		}
	})
}

func TestHTTPHandlerPreOpenHook(t *testing.T) {
	forEachMode(t, testHTTPHandlerPreOpenHook)
}

func testHTTPHandlerPreOpenHook(t *testing.T, native bool) {
	dataDir := t.TempDir()
	st := newStorage(t, dataDir, native)
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
		name        string
		method      string
		target      string
		body        string
		contentType string
		want        int
		calls       []call
	}{
		{"InfoRefsRead", http.MethodGet, "/test-repo.git/info/refs?service=git-upload-pack", "", "", http.StatusOK, []call{{"test-repo", false}}},
		{"InfoRefsWrite", http.MethodGet, "/test-repo.git/info/refs?service=git-receive-pack", "", "", http.StatusOK, []call{{"test-repo", true}}},
		{"ReceivePack", http.MethodPost, "/test-repo.git/git-receive-pack", "0000", "application/x-git-receive-pack-request", http.StatusOK, []call{{"test-repo", true}}},
		{"HookCreatesRepository", http.MethodGet, "/late.git/info/refs?service=git-upload-pack", "", "", http.StatusOK, []call{{"late", false}}},
		{"MissingRepository", http.MethodGet, "/missing.git/info/refs?service=git-upload-pack", "", "", http.StatusNotFound, []call{{"missing", false}}},
		// The router redirects to the cleaned path, so the handler never sees an interior "..".
		{"InvalidName", http.MethodGet, "/org/../repo.git/info/refs?service=git-upload-pack", "", "", http.StatusMovedPermanently, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls = nil
			req := httptest.NewRequest(test.method, test.target, strings.NewReader(test.body))
			if test.contentType != "" {
				req.Header.Set("Content-Type", test.contentType)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != test.want {
				t.Errorf("status = %d, want %d; body = %q", rec.Code, test.want, rec.Body.String())
			}
			if !slices.Equal(calls, test.calls) {
				t.Errorf("hook calls = %v, want %v", calls, test.calls)
			}
		})
	}
}

func TestGitRoutes(t *testing.T) {
	// A served repository makes any output of a refused service visible.
	dataDir := t.TempDir()
	runGitCmd(t, "", "init", "--bare", filepath.Join(dataDir, "repositories", "org", "name.git"))
	st := newStorage(t, dataDir, true)
	for _, row := range []struct {
		name        string
		method      string
		path        string
		contentType string
		repo        string
		write       bool
		status      int
	}{
		{"DiscoveryRead", http.MethodGet, "/info/refs?service=git-upload-pack", "", "org/name", false, http.StatusForbidden},
		{"DiscoveryWrite", http.MethodGet, "/info/refs?service=git-receive-pack", "", "org/name", true, http.StatusForbidden},
		{"DiscoveryEmptyService", http.MethodGet, "/info/refs?service=", "", "", false, http.StatusBadRequest},
		{"DiscoveryUnknownService", http.MethodGet, "/info/refs?service=git-foo", "", "", false, http.StatusForbidden},
		{"DiscoveryNoService", http.MethodGet, "/info/refs", "", "", false, http.StatusTeapot},
		{"DiscoveryWrongMethod", http.MethodPost, "/info/refs?service=git-upload-pack", "", "", false, http.StatusMethodNotAllowed},
		{"ResolveHead", http.MethodHead, "/resolve/main/info/refs", "", "", false, http.StatusTeapot},
		{"UploadPack", http.MethodPost, "/git-upload-pack", "application/x-git-upload-pack-request", "org/name", false, http.StatusForbidden},
		{"ReceivePackParams", http.MethodPost, "/git-receive-pack", "application/x-git-receive-pack-request; charset=utf-8", "org/name", true, http.StatusForbidden},
		{"UploadPackNoType", http.MethodPost, "/git-upload-pack", "", "", false, http.StatusTeapot},
		{"UploadPackOtherType", http.MethodPost, "/git-upload-pack", "application/json", "", false, http.StatusTeapot},
		{"UploadPackCrossService", http.MethodPost, "/git-upload-pack", "application/x-git-receive-pack-request", "", false, http.StatusTeapot},
		{"UploadPackMalformedType", http.MethodPost, "/git-upload-pack", "application/x-git-upload-pack-request; charset", "", false, http.StatusTeapot},
		{"UploadPackWrongMethod", http.MethodGet, "/git-upload-pack", "application/x-git-upload-pack-request", "", false, http.StatusMethodNotAllowed},
	} {
		for _, suffix := range []string{".git", ""} {
			t.Run(row.name+suffix, func(t *testing.T) {
				var gotRepo string
				var gotWrite bool
				var calls int
				handler := backendhttp.NewHandler(
					backendhttp.WithStorage(st),
					backendhttp.WithPreOpenHookFunc(func(_ context.Context, repoName string, write bool) error {
						gotRepo, gotWrite, calls = repoName, write, calls+1
						return fmt.Errorf("repository %q is read-only: %w", repoName, fs.ErrPermission)
					}),
					backendhttp.WithNext(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						w.WriteHeader(http.StatusTeapot)
					})),
				)
				req := httptest.NewRequest(row.method, "/org/name"+suffix+row.path, strings.NewReader("0000"))
				if row.contentType != "" {
					req.Header.Set("Content-Type", row.contentType)
				}
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				wantCalls := 0
				if row.repo != "" {
					wantCalls = 1
				}
				if rec.Code != row.status || calls != wantCalls || gotRepo != row.repo || gotWrite != row.write {
					t.Fatalf("status=%d calls=%d repo=%q write=%v; want status=%d calls=%d repo=%q write=%v; body=%s", rec.Code, calls, gotRepo, gotWrite, row.status, wantCalls, row.repo, row.write, rec.Body.String())
				}
				if wantCalls == 1 && strings.TrimSpace(rec.Body.String()) != "permission denied" {
					t.Fatalf("denied body = %q, want the fixed %q and no service output", rec.Body.String(), "permission denied")
				}
			})
		}
	}
}

func TestHTTPHandlerPreOpenErrors(t *testing.T) {
	dataDir := t.TempDir()
	runGitCmd(t, "", "init", "--bare", filepath.Join(dataDir, "repositories", "test-repo.git"))
	st := newStorage(t, dataDir, true)
	for _, test := range []struct {
		name   string
		err    error
		status int
		body   string
		auth   string
	}{
		{"Unauthenticated", fmt.Errorf("sign in first: %w", authenticate.ErrUnauthenticated), http.StatusUnauthorized, "Unauthorized", `Basic realm="hfd"`},
		{"NotExists", fmt.Errorf("no upstream: %w", repository.ErrRepositoryNotExists), http.StatusNotFound, `repository "test-repo" not found`, ""},
		{"Other", errors.New("boom"), http.StatusInternalServerError, `Failed to open repository "test-repo": boom`, ""},
	} {
		handler := backendhttp.NewHandler(
			backendhttp.WithStorage(st),
			backendhttp.WithPreOpenHookFunc(func(context.Context, string, bool) error {
				return test.err
			}),
		)
		for _, route := range []struct{ name, method, target, contentType string }{
			{"InfoRefs", http.MethodGet, "/test-repo.git/info/refs?service=git-upload-pack", ""},
			{"ReceivePack", http.MethodPost, "/test-repo.git/git-receive-pack", "application/x-git-receive-pack-request"},
		} {
			t.Run(test.name+route.name, func(t *testing.T) {
				req := httptest.NewRequest(route.method, route.target, strings.NewReader("0000"))
				if route.contentType != "" {
					req.Header.Set("Content-Type", route.contentType)
				}
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				if auth := rec.Header().Get("WWW-Authenticate"); rec.Code != test.status || strings.TrimSpace(rec.Body.String()) != test.body || auth != test.auth {
					t.Errorf("status = %d, body = %q, WWW-Authenticate = %q; want %d, %q, %q", rec.Code, rec.Body.String(), auth, test.status, test.body, test.auth)
				}
			})
		}
	}
}

type bodyReadSpy struct{ read bool }

func (s *bodyReadSpy) Read([]byte) (int, error) {
	s.read = true
	return 0, io.EOF
}

func TestHTTPHandlerDeniedReceivePackLeavesBodyUnread(t *testing.T) {
	dataDir := t.TempDir()
	runGitCmd(t, "", "init", "--bare", filepath.Join(dataDir, "repositories", "test-repo.git"))
	handler := backendhttp.NewHandler(
		backendhttp.WithStorage(newStorage(t, dataDir, true)),
		backendhttp.WithPreOpenHookFunc(func(context.Context, string, bool) error {
			return fs.ErrPermission
		}),
	)
	body := &bodyReadSpy{}
	req := httptest.NewRequest(http.MethodPost, "/test-repo.git/git-receive-pack", body)
	req.Header.Set("Content-Type", "application/x-git-receive-pack-request")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden || body.read {
		t.Errorf("status = %d, body read = %v; want 403 with the request body unread", rec.Code, body.read)
	}
}
