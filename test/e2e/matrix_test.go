package e2e_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wzshiming/hfd/internal/utils"
	backendhttp "github.com/wzshiming/hfd/pkg/backend/http"
	backendhuggingface "github.com/wzshiming/hfd/pkg/backend/huggingface"
	backendlfs "github.com/wzshiming/hfd/pkg/backend/lfs"
	backendssh "github.com/wzshiming/hfd/pkg/backend/ssh"
	"github.com/wzshiming/hfd/pkg/lfs"
	"github.com/wzshiming/hfd/pkg/permission"
	"github.com/wzshiming/hfd/pkg/receive"
	"github.com/wzshiming/hfd/pkg/repository"
	"github.com/wzshiming/hfd/pkg/storage"
	"golang.org/x/crypto/ssh"
)

// BackendType represents the type of backend being tested
type BackendType string

const (
	BackendHTTP BackendType = "HTTP"
	BackendSSH  BackendType = "SSH"
)

// TestBackend provides a unified interface for testing different backends
type TestBackend struct {
	Type         BackendType
	HTTPServer   *httptest.Server
	SSHListener  net.Listener
	DataDir      string
	Storage      *storage.Storage
	KeyFile      string // For SSH authentication
	t            *testing.T
}

// BackendOptions configures the backend setup
type BackendOptions struct {
	PreReceiveHook  receive.PreReceiveHook
	PostReceiveHook receive.PostReceiveHook
	PermissionHook  permission.PermissionHook
	MirrorSource    repository.MirrorSourceFunc
	MirrorRefFilter repository.MirrorRefFilterFunc
	AuthFunc        func(username, password string) bool
	SSHAuthorized   []ssh.PublicKey
}

// SetupBackend creates a test backend based on the specified type
func SetupBackend(t *testing.T, backendType BackendType, opts *BackendOptions) *TestBackend {
	t.Helper()

	if opts == nil {
		opts = &BackendOptions{}
	}

	dataDir, err := os.MkdirTemp("", "e2e-matrix-data")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	store := storage.NewStorage(storage.WithRootDir(dataDir))
	lfsStore := lfs.NewLocal(store.LFSDir())

	backend := &TestBackend{
		Type:    backendType,
		DataDir: dataDir,
		Storage: store,
		t:       t,
	}

	// Build HTTP handler chain (always needed for repo creation API)
	var httpOpts []backendhttp.Option
	httpOpts = append(httpOpts, backendhttp.WithStorage(store))
	if opts.PreReceiveHook != nil {
		httpOpts = append(httpOpts, backendhttp.WithPreReceiveHookFunc(opts.PreReceiveHook))
	}
	if opts.PostReceiveHook != nil {
		httpOpts = append(httpOpts, backendhttp.WithPostReceiveHookFunc(opts.PostReceiveHook))
	}
	if opts.PermissionHook != nil {
		httpOpts = append(httpOpts, backendhttp.WithPermissionHookFunc(opts.PermissionHook))
	}
	if opts.MirrorSource != nil {
		httpOpts = append(httpOpts, backendhttp.WithMirrorSourceFunc(opts.MirrorSource))
	}
	if opts.MirrorRefFilter != nil {
		httpOpts = append(httpOpts, backendhttp.WithMirrorRefFilterFunc(opts.MirrorRefFilter))
	}

	var handler http.Handler
	handler = backendhuggingface.NewHandler(
		backendhuggingface.WithStorage(store),
		backendhuggingface.WithLFSStore(lfsStore),
	)
	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(store),
		backendlfs.WithNext(handler),
		backendlfs.WithLFSStore(lfsStore),
	)
	httpOpts = append(httpOpts, backendhttp.WithNext(handler))
	handler = backendhttp.NewHandler(httpOpts...)

	// Apply auth wrapper if provided
	if opts.AuthFunc != nil {
		handler = wrapWithAuth(handler, opts.AuthFunc)
	}

	// Start HTTP server
	backend.HTTPServer = httptest.NewServer(handler)
	t.Cleanup(func() { backend.HTTPServer.Close() })

	// Setup SSH server if needed
	if backendType == BackendSSH {
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("Failed to generate host key: %v", err)
		}
		hostKey, err := ssh.NewSignerFromKey(priv)
		if err != nil {
			t.Fatalf("Failed to create host key signer: %v", err)
		}

		var sshOpts []backendssh.Option
		if opts.PreReceiveHook != nil {
			sshOpts = append(sshOpts, backendssh.WithPreReceiveHookFunc(opts.PreReceiveHook))
		}
		if opts.PostReceiveHook != nil {
			sshOpts = append(sshOpts, backendssh.WithPostReceiveHookFunc(opts.PostReceiveHook))
		}
		if opts.PermissionHook != nil {
			sshOpts = append(sshOpts, backendssh.WithPermissionHookFunc(opts.PermissionHook))
		}
		if opts.MirrorSource != nil {
			sshOpts = append(sshOpts, backendssh.WithMirrorSourceFunc(opts.MirrorSource))
		}
		if opts.MirrorRefFilter != nil {
			sshOpts = append(sshOpts, backendssh.WithMirrorRefFilterFunc(opts.MirrorRefFilter))
		}
		if len(opts.SSHAuthorized) > 0 {
			callback := backendssh.AuthorizedKeysCallback(opts.SSHAuthorized)
			sshOpts = append(sshOpts, backendssh.WithPublicKeyCallback(callback))
		}

		sshServer := backendssh.NewServer(store.RepositoriesDir(), hostKey, sshOpts...)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Failed to listen for SSH: %v", err)
		}
		t.Cleanup(func() { listener.Close() })

		go func() {
			_ = sshServer.Serve(t.Context(), listener)
		}()

		backend.SSHListener = listener
	}

	return backend
}

// GitURL returns the appropriate git URL for this backend
func (b *TestBackend) GitURL(repoPath string) string {
	switch b.Type {
	case BackendHTTP:
		return b.HTTPServer.URL + "/" + repoPath + ".git"
	case BackendSSH:
		addr := b.SSHListener.Addr().(*net.TCPAddr)
		return "ssh://git@" + addr.String() + "/" + repoPath + ".git"
	default:
		b.t.Fatalf("Unknown backend type: %s", b.Type)
		return ""
	}
}

// APIURL returns the HTTP API endpoint URL
func (b *TestBackend) APIURL() string {
	return b.HTTPServer.URL
}

// GitEnv returns the environment variables needed for git commands
func (b *TestBackend) GitEnv() []string {
	switch b.Type {
	case BackendHTTP:
		return []string{"GIT_TERMINAL_PROMPT=0"}
	case BackendSSH:
		if b.KeyFile == "" {
			b.t.Fatal("SSH backend requires KeyFile to be set")
		}
		addr := b.SSHListener.Addr().(*net.TCPAddr)
		port := strings.Split(addr.String(), ":")[1]
		sshCmd := fmt.Sprintf("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s -p %s", b.KeyFile, port)
		return []string{
			"GIT_TERMINAL_PROMPT=0",
			"GIT_SSH_COMMAND=" + sshCmd,
		}
	default:
		b.t.Fatalf("Unknown backend type: %s", b.Type)
		return nil
	}
}

// RunGitCmd runs a git command with proper environment for this backend
func (b *TestBackend) RunGitCmd(dir string, args ...string) {
	b.t.Helper()
	cmd := utils.Command(b.t.Context(), "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), b.GitEnv()...)
	if output, err := cmd.Output(); err != nil {
		b.t.Fatalf("Git command failed: git %s\nError: %v\nOutput: %s", strings.Join(args, " "), err, output)
	}
}

// CreateRepo creates a repository via the HuggingFace API
func (b *TestBackend) CreateRepo(org, name string) {
	b.t.Helper()
	resp, err := http.Post(b.APIURL()+"/api/repos/create", "application/json",
		strings.NewReader(fmt.Sprintf(`{"type":"model","name":"%s","organization":"%s"}`, name, org)))
	if err != nil {
		b.t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b.t.Fatalf("Expected 200 creating repo, got %d", resp.StatusCode)
	}
}

// InitWorkDir clones a repo and sets up git config
func (b *TestBackend) InitWorkDir(repoPath, localDir string) {
	b.t.Helper()
	b.RunGitCmd("", "clone", b.GitURL(repoPath), localDir)
	b.RunGitCmd(localDir, "config", "user.email", "test@test.com")
	b.RunGitCmd(localDir, "config", "user.name", "Test User")
}

// SetupSSHKey generates an SSH key for this backend and returns the public key
func (b *TestBackend) SetupSSHKey() ssh.PublicKey {
	b.t.Helper()
	if b.Type != BackendSSH {
		b.t.Fatal("SetupSSHKey called on non-SSH backend")
	}

	clientDir, err := os.MkdirTemp("", "ssh-key")
	if err != nil {
		b.t.Fatalf("Failed to create key dir: %v", err)
	}
	b.t.Cleanup(func() { os.RemoveAll(clientDir) })

	keyFile := filepath.Join(clientDir, "id_ed25519")
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		b.t.Fatalf("Failed to generate client key: %v", err)
	}

	privKeyPEM, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		b.t.Fatalf("Failed to marshal private key: %v", err)
	}

	// Use pem package to encode properly
	pemData := pem.EncodeToMemory(privKeyPEM)
	if err := os.WriteFile(keyFile, pemData, 0600); err != nil {
		b.t.Fatalf("Failed to write private key: %v", err)
	}

	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		b.t.Fatalf("Failed to create signer: %v", err)
	}

	b.KeyFile = keyFile
	return signer.PublicKey()
}

// wrapWithAuth wraps a handler with basic authentication
func wrapWithAuth(next http.Handler, authFunc func(username, password string) bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok || !authFunc(username, password) {
			w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// RunMatrixTests runs a test function across all specified backends
func RunMatrixTests(t *testing.T, backends []BackendType, opts *BackendOptions, testFunc func(t *testing.T, backend *TestBackend)) {
	for _, backendType := range backends {
		t.Run(string(backendType), func(t *testing.T) {
			// Setup SSH key if this is an SSH backend
			if backendType == BackendSSH {
				// Generate a key first
				keyDir, err := os.MkdirTemp("", "ssh-key")
				if err != nil {
					t.Fatalf("Failed to create key dir: %v", err)
				}
				defer os.RemoveAll(keyDir)

				keyFile := filepath.Join(keyDir, "id_ed25519")
				_, priv, err := ed25519.GenerateKey(rand.Reader)
				if err != nil {
					t.Fatalf("Failed to generate client key: %v", err)
				}

				privKeyPEM, err := ssh.MarshalPrivateKey(priv, "")
				if err != nil {
					t.Fatalf("Failed to marshal private key: %v", err)
				}

				pemData := pem.EncodeToMemory(privKeyPEM)
				if err := os.WriteFile(keyFile, pemData, 0600); err != nil {
					t.Fatalf("Failed to write private key: %v", err)
				}

				signer, err := ssh.NewSignerFromKey(priv)
				if err != nil {
					t.Fatalf("Failed to create signer: %v", err)
				}
				pubKey := signer.PublicKey()

				// Create options with this key authorized
				newOpts := &BackendOptions{}
				if opts != nil {
					*newOpts = *opts
				}
				newOpts.SSHAuthorized = []ssh.PublicKey{pubKey}

				backend := SetupBackend(t, backendType, newOpts)
				backend.KeyFile = keyFile
				testFunc(t, backend)
			} else {
				backend := SetupBackend(t, backendType, opts)
				testFunc(t, backend)
			}
		})
	}
}
