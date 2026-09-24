package ssh_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	backendssh "github.com/matrixhub-ai/hfd/pkg/backend/ssh"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
	"golang.org/x/crypto/ssh"
)

// goGitFS hides the OS filesystem type so repositories on it are served by go-git.
type goGitFS struct{ billy.Filesystem }

func (f goGitFS) Chroot(path string) (billy.Filesystem, error) {
	sub, err := f.Filesystem.Chroot(path)
	if err != nil {
		return nil, err
	}
	return goGitFS{sub}, nil
}

// newStorage returns storage rooted at root whose repositories are served natively or by go-git.
func newStorage(t *testing.T, root string, native bool) *storage.Storage {
	t.Helper()
	opts := []storage.Option{storage.WithRootDir(root)}
	if !native {
		opts = append(opts, storage.WithFilesystem(goGitFS{osfs.New(root)}))
	}
	st, err := storage.NewStorage(opts...)
	if err != nil {
		t.Fatalf("new storage: %v", err)
	}
	return st
}

// modeName labels the serving mode of a test.
func modeName(native bool) string {
	if native {
		return "native"
	}
	return "go-git"
}

// forEachMode runs fn with repositories served by go-git and again by the git binary.
func forEachMode(t *testing.T, fn func(t *testing.T, native bool)) {
	for _, native := range []bool{false, true} {
		t.Run(modeName(native), func(t *testing.T) { fn(t, native) })
	}
}

// wantAgent is the agent capability prefix advertised by the serving mode.
func wantAgent(native bool) string {
	if native {
		return "agent=git/"
	}
	return "agent=go-git/"
}

// anonymousClient is the client configuration of an unauthenticated git user.
var anonymousClient = &ssh.ClientConfig{User: "git", HostKeyCallback: ssh.InsecureIgnoreHostKey()}

// hookRecorder counts receive hook calls and holds the pre-receive verdict.
type hookRecorder struct {
	mu        sync.Mutex
	pre, post int
	lastPost  []string // "old new ref" of the latest post-receive call
	deny      error
}

func (h *hookRecorder) preReceive(_ context.Context, _ string, _ []receive.RefUpdate) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pre++
	return h.deny == nil, h.deny
}

func (h *hookRecorder) postReceive(_ context.Context, _ string, updates []receive.RefUpdate) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.post++
	h.lastPost = nil
	for _, u := range updates {
		h.lastPost = append(h.lastPost, u.OldRev()+" "+u.NewRev()+" "+u.RefName())
	}
	return nil
}

// requireNoStdinFeeders fails when a native stdin copier outlives the client that fed it.
func requireNoStdinFeeders(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var buf bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&buf, 1)
		if !bytes.Contains(buf.Bytes(), []byte("repository.feedStdin")) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("stdin copier still running:\n%s", buf.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// runGitCmd runs a git command in the specified directory.
func runGitCmd(t *testing.T, dir string, env []string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir = dir
	cmd.Env = append(append(os.Environ(), "GIT_TERMINAL_PROMPT=0"), env...)
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("Git command failed: git %s\nError: %v\nOutput: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

// tracedGit runs git with GIT_TRACE_PACKET enabled and returns the pkt-line trace.
func tracedGit(t *testing.T, dir string, env []string, args ...string) string {
	t.Helper()
	trace := filepath.Join(t.TempDir(), "packet.trace")
	runGitCmd(t, dir, append(slices.Clone(env), "GIT_TRACE_PACKET="+trace), args...)
	data, err := os.ReadFile(trace)
	if err != nil {
		t.Fatalf("read packet trace: %v", err)
	}
	return string(data)
}

// requireWireVersion asserts the traced pkt-lines carry the version the server actually spoke.
func requireWireVersion(t *testing.T, trace string, protoVer int, receive bool) {
	t.Helper()
	want := protoVer
	if receive && protoVer == 2 {
		want = 0 // receive-pack has no v2: both engines answer a v0 advertisement
	}
	got := 0
	switch {
	case strings.Contains(trace, "< version 2\n"):
		got = 2
	case strings.Contains(trace, "< version 1\n"):
		got = 1
	}
	if got != want || (want == 2 && !strings.Contains(trace, "> command=ls-refs\n")) {
		t.Fatalf("wire version = %d, want %d; trace:\n%s", got, want, trace)
	}
}

// sshAdvertisement runs cmd over a raw session that requests protoVer the way git does and ends the service with a lone flush.
func sshAdvertisement(t *testing.T, addr string, cfg *ssh.ClientConfig, protoVer int, cmd string) []byte {
	t.Helper()
	client, err := ssh.Dial("tcp", addr, cfg)
	if err != nil {
		t.Fatalf("Failed to dial SSH: %v", err)
	}
	defer client.Close()
	session, err := client.NewSession()
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	defer session.Close()
	if protoVer > 0 {
		if err := session.Setenv("GIT_PROTOCOL", fmt.Sprintf("version=%d", protoVer)); err != nil {
			t.Fatalf("Setenv GIT_PROTOCOL: %v", err)
		}
	}
	session.Stdin = strings.NewReader("0000")
	adv, err := session.Output(cmd)
	if err != nil {
		t.Fatalf("%s: %v", cmd, err)
	}
	return adv
}

func TestSSHProtocolServer(t *testing.T) {
	forEachMode(t, func(t *testing.T, native bool) {
		for _, protoVer := range []int{0, 1, 2} {
			t.Run(fmt.Sprintf("ProtocolV%d", protoVer), func(t *testing.T) { testSSHProtocolServer(t, native, protoVer) })
		}
	})
}

func testSSHProtocolServer(t *testing.T, native bool, protoVer int) {
	// Create a temporary directory for repositories
	repoDir, err := os.MkdirTemp("", "sshprotocol-test-repos")
	if err != nil {
		t.Fatalf("Failed to create temp repo dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(repoDir)
	}()

	// Create a temporary directory for client operations
	clientDir, err := os.MkdirTemp("", "sshprotocol-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(clientDir)
	}()

	st := newStorage(t, repoDir, native)
	hooks := &hookRecorder{}

	// Create a bare repository
	repoName := "test-repo.git"
	repoPath := filepath.Join(repoDir, "repositories", repoName)
	runGitCmd(t, "", nil, "init", "--bare", repoPath)

	// Generate a host key for the SSH server
	hostKey, err := generateHostKey()
	if err != nil {
		t.Fatalf("Failed to generate host key: %v", err)
	}

	// Start SSH server on a random port
	server := backendssh.NewServer(backendssh.WithHostKey(hostKey), backendssh.WithStorage(st),
		backendssh.WithPreReceiveHookFunc(hooks.preReceive), backendssh.WithPostReceiveHookFunc(hooks.postReceive))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	go func() {
		_ = server.Serve(t.Context(), listener)
	}()

	addr := listener.Addr().(*net.TCPAddr)
	sshURL := "ssh://git@" + addr.String() + "/" + repoName

	// Configure SSH to skip host key verification; git adds SendEnv=GIT_PROTOCOL itself for v1/v2.
	sshCmd := "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p " + strings.Split(addr.String(), ":")[1]
	env := []string{
		"GIT_TERMINAL_PROMPT=0",
		"GIT_SSH_COMMAND=" + sshCmd,
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=protocol.version",
		fmt.Sprintf("GIT_CONFIG_VALUE_0=%d", protoVer),
	}

	t.Run("Advertisement", func(t *testing.T) {
		for _, service := range []string{repository.GitUploadPack, repository.GitReceivePack} {
			adv := sshAdvertisement(t, addr.String(), anonymousClient, protoVer, service+" '"+repoName+"'")
			if !bytes.Contains(adv, []byte(wantAgent(native))) {
				t.Fatalf("%s advertisement lacks %q: %q", service, wantAgent(native), adv)
			}
			// Only upload-pack speaks v2; receive-pack answers a v2 request with a v0 advertisement.
			want := ""
			switch {
			case protoVer == 1:
				want = "000eversion 1\n"
			case protoVer == 2 && service == repository.GitUploadPack:
				want = "000eversion 2\n"
			}
			if (want == "" && bytes.HasPrefix(adv, []byte("000eversion "))) || (want != "" && !bytes.HasPrefix(adv, []byte(want))) {
				t.Fatalf("%s advertisement = %q, want version prefix %q", service, adv, want)
			}
			if want == "000eversion 2\n" && (!bytes.Contains(adv, []byte("ls-refs")) || !bytes.Contains(adv, []byte("fetch"))) {
				t.Fatalf("v2 capability advertisement = %q", adv)
			}
		}
	})

	t.Run("CloneEmptyRepository", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-empty")

		requireWireVersion(t, tracedGit(t, "", env, "clone", sshURL, cloneDir), protoVer, false)

		// Verify .git directory exists
		hfdir := filepath.Join(cloneDir, ".git")
		if _, err := os.Stat(hfdir); os.IsNotExist(err) {
			t.Errorf(".git directory not found in cloned repository")
		}
	})

	t.Run("PushToRepository", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-empty")

		runGitCmd(t, workDir, env, "config", "user.email", "test@test.com")
		runGitCmd(t, workDir, env, "config", "user.name", "Test User")

		// Create a test file
		testFile := filepath.Join(workDir, "README.md")
		if err := os.WriteFile(testFile, []byte("# Test Repository\n"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		runGitCmd(t, workDir, env, "add", "README.md")
		runGitCmd(t, workDir, env, "commit", "-m", "Initial commit")
		requireWireVersion(t, tracedGit(t, workDir, env, "push", "-u", "origin", "master"), protoVer, true)

		master := strings.TrimSpace(runGitCmd(t, workDir, env, "rev-parse", "master"))
		hooks.mu.Lock()
		defer hooks.mu.Unlock()
		if want := []string{receive.ZeroHash + " " + master + " refs/heads/master"}; hooks.pre != 1 || hooks.post != 1 || !slices.Equal(hooks.lastPost, want) {
			t.Fatalf("hooks after push: pre = %d, post = %d, last post = %v; want 1, 1, %v", hooks.pre, hooks.post, hooks.lastPost, want)
		}
	})

	t.Run("CloneWithContent", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-with-content")

		runGitCmd(t, "", env, "clone", sshURL, cloneDir)

		// Verify README.md exists
		readmePath := filepath.Join(cloneDir, "README.md")
		content, err := os.ReadFile(readmePath)
		if err != nil {
			t.Fatalf("Failed to read README.md: %v", err)
		}
		if string(content) != "# Test Repository\n" {
			t.Errorf("Unexpected content: %s", content)
		}
	})

	t.Run("FetchFromRepository", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-with-content")
		runGitCmd(t, workDir, env, "fetch", "origin")
	})

	t.Run("PushMoreCommits", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-with-content")

		runGitCmd(t, workDir, env, "config", "user.email", "test@test.com")
		runGitCmd(t, workDir, env, "config", "user.name", "Test User")

		testFile := filepath.Join(workDir, "file2.txt")
		if err := os.WriteFile(testFile, []byte("Second file\n"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		runGitCmd(t, workDir, env, "add", "file2.txt")
		runGitCmd(t, workDir, env, "commit", "-m", "Add second file")
		runGitCmd(t, workDir, env, "push")
	})

	t.Run("PullChanges", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-empty")

		runGitCmd(t, workDir, env, "config", "user.email", "test@test.com")
		runGitCmd(t, workDir, env, "config", "user.name", "Test User")

		runGitCmd(t, workDir, env, "pull")

		// Verify file2.txt exists
		file2Path := filepath.Join(workDir, "file2.txt")
		if _, err := os.Stat(file2Path); os.IsNotExist(err) {
			t.Errorf("file2.txt not found after pull")
		}
	})

	t.Run("PushDenied", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-with-content")
		// An incompressible blob larger than the SSH channel window keeps the client sending while the server already refused.
		big := make([]byte, 5<<20)
		if _, err := rand.Read(big); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(workDir, "big.bin"), big, 0644); err != nil {
			t.Fatal(err)
		}
		runGitCmd(t, workDir, env, "add", "big.bin")
		runGitCmd(t, workDir, env, "commit", "-q", "-m", "big")
		before := strings.TrimSpace(runGitCmd(t, repoPath, nil, "rev-parse", "master"))

		hooks.mu.Lock()
		hooks.deny = errors.New("branch is frozen")
		pre, post := hooks.pre, hooks.post
		hooks.mu.Unlock()
		defer func() {
			hooks.mu.Lock()
			hooks.deny = nil
			hooks.mu.Unlock()
		}()

		cmd := exec.CommandContext(t.Context(), "git", "push", "origin", "master")
		cmd.Dir = workDir
		cmd.Env = append(os.Environ(), env...)
		output, err := cmd.CombinedOutput()
		if err == nil || !strings.Contains(string(output), "[remote rejected]") || !strings.Contains(string(output), "branch is frozen") {
			t.Fatalf("denied push: err = %v, want a remote rejection carrying the hook reason; output:\n%s", err, output)
		}
		// The server keeps reading the pack until the client is done, so the client sees only the rejection.
		if strings.Contains(string(output), "hung up") || strings.Contains(string(output), "Broken pipe") {
			t.Fatalf("denied push tore down the connection early:\n%s", output)
		}
		if after := strings.TrimSpace(runGitCmd(t, repoPath, nil, "rev-parse", "master")); after != before {
			t.Fatalf("master moved on a denied push: %s -> %s", before, after)
		}
		hooks.mu.Lock()
		defer hooks.mu.Unlock()
		if hooks.pre != pre+1 || hooks.post != post {
			t.Fatalf("hooks after denial: pre = %d, post = %d; want %d, %d", hooks.pre, hooks.post, pre+1, post)
		}
		requireNoStdinFeeders(t)
	})

	// lastPost returns the latest post-receive updates sorted for order-independent comparison.
	lastPost := func() []string {
		hooks.mu.Lock()
		defer hooks.mu.Unlock()
		got := slices.Clone(hooks.lastPost)
		slices.Sort(got)
		return got
	}

	t.Run("PushCreateDeleteRefs", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-with-content")
		runGitCmd(t, workDir, env, "branch", "feature", "origin/master")
		runGitCmd(t, workDir, env, "tag", "-a", "v1", "-m", "v1", "origin/master")
		master := strings.TrimSpace(runGitCmd(t, workDir, env, "rev-parse", "origin/master"))
		tag := strings.TrimSpace(runGitCmd(t, workDir, env, "rev-parse", "v1"))

		runGitCmd(t, workDir, env, "push", "origin", "refs/heads/feature", "refs/tags/v1")
		want := []string{receive.ZeroHash + " " + master + " refs/heads/feature", receive.ZeroHash + " " + tag + " refs/tags/v1"}
		slices.Sort(want)
		if got := lastPost(); !slices.Equal(got, want) {
			t.Fatalf("post-receive after create = %v, want %v", got, want)
		}
		refs := runGitCmd(t, workDir, env, "ls-remote", "origin")
		if !strings.Contains(refs, master+"\trefs/heads/feature") || !strings.Contains(refs, tag+"\trefs/tags/v1") || !strings.Contains(refs, master+"\trefs/tags/v1^{}") {
			t.Fatalf("ls-remote after create:\n%s", refs)
		}

		runGitCmd(t, workDir, env, "push", "origin", ":refs/heads/feature", ":refs/tags/v1")
		want = []string{master + " " + receive.ZeroHash + " refs/heads/feature", tag + " " + receive.ZeroHash + " refs/tags/v1"}
		slices.Sort(want)
		if got := lastPost(); !slices.Equal(got, want) {
			t.Fatalf("post-receive after delete = %v, want %v", got, want)
		}
		if refs := runGitCmd(t, workDir, env, "ls-remote", "origin"); strings.Contains(refs, "refs/heads/feature") || strings.Contains(refs, "refs/tags/v1") {
			t.Fatalf("ls-remote after delete:\n%s", refs)
		}
	})

	t.Run("NonFastForward", func(t *testing.T) {
		workDir := filepath.Join(clientDir, "clone-empty")
		before := strings.TrimSpace(runGitCmd(t, repoPath, nil, "rev-parse", "master"))
		runGitCmd(t, workDir, env, "reset", "--hard", "HEAD~1")
		if err := os.WriteFile(filepath.Join(workDir, "diverged.txt"), []byte("diverged\n"), 0644); err != nil {
			t.Fatal(err)
		}
		runGitCmd(t, workDir, env, "add", "diverged.txt")
		runGitCmd(t, workDir, env, "commit", "-q", "-m", "diverged")
		diverged := strings.TrimSpace(runGitCmd(t, workDir, env, "rev-parse", "master"))
		hooks.mu.Lock()
		pre, post := hooks.pre, hooks.post
		hooks.mu.Unlock()

		cmd := exec.CommandContext(t.Context(), "git", "push", "origin", "master")
		cmd.Dir = workDir
		cmd.Env = append(os.Environ(), env...)
		output, err := cmd.CombinedOutput()
		if err == nil || !strings.Contains(string(output), "[rejected]") {
			t.Fatalf("non-fast-forward push: err = %v, want a rejection; output:\n%s", err, output)
		}
		if after := strings.TrimSpace(runGitCmd(t, repoPath, nil, "rev-parse", "master")); after != before {
			t.Fatalf("master moved on a rejected push: %s -> %s", before, after)
		}
		hooks.mu.Lock()
		if hooks.pre != pre || hooks.post != post {
			hooks.mu.Unlock()
			t.Fatalf("hooks ran for a push without commands: pre = %d, post = %d; want %d, %d", hooks.pre, hooks.post, pre, post)
		}
		hooks.mu.Unlock()

		runGitCmd(t, workDir, env, "push", "--force", "origin", "master")
		if got, want := lastPost(), []string{before + " " + diverged + " refs/heads/master"}; !slices.Equal(got, want) {
			t.Fatalf("post-receive after force push = %v, want %v", got, want)
		}
		if after := strings.TrimSpace(runGitCmd(t, repoPath, nil, "rev-parse", "master")); after != diverged {
			t.Fatalf("master after force push = %s, want %s", after, diverged)
		}
	})

	// git exits while parsing its config, before GIT_PROTOCOL matters, so one cell covers the mechanism.
	if native && protoVer == 0 {
		t.Run("ReceivePackExitsBeforeAdvertisement", func(t *testing.T) {
			// An invalid boolean kills git receive-pack before it advertises; the client keeps waiting with stdin open.
			runGitCmd(t, repoPath, nil, "config", "receive.advertisePushOptions", "not-a-bool")
			defer runGitCmd(t, repoPath, nil, "config", "--unset", "receive.advertisePushOptions")
			client, err := ssh.Dial("tcp", addr.String(), &ssh.ClientConfig{User: "git", HostKeyCallback: ssh.InsecureIgnoreHostKey()})
			if err != nil {
				t.Fatalf("Failed to dial SSH: %v", err)
			}
			defer client.Close()
			session, err := client.NewSession()
			if err != nil {
				t.Fatalf("Failed to create session: %v", err)
			}
			defer session.Close()
			stdin, err := session.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer stdin.Close()
			var stderr bytes.Buffer
			session.Stderr = &stderr
			if err := session.Start("git-receive-pack '" + repoName + "'"); err != nil {
				t.Fatalf("git-receive-pack: %v", err)
			}
			done := make(chan error, 1)
			go func() { done <- session.Wait() }()
			select {
			case err := <-done:
				if err == nil || !strings.Contains(stderr.String(), "not-a-bool") {
					t.Fatalf("session ended with %v, stderr %q; want a failure carrying git's message", err, stderr.String())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("session stayed open after git exited")
			}
			requireNoStdinFeeders(t)
		})
	}
}

func generateHostKey() (ssh.Signer, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating host key: %w", err)
	}
	return ssh.NewSignerFromKey(priv)
}

func generateClientKeyFile(path string) (ssh.PublicKey, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating client key: %w", err)
	}
	privKeyPEM, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		return nil, fmt.Errorf("marshaling private key: %w", err)
	}
	if err := os.WriteFile(path, pem.EncodeToMemory(privKeyPEM), 0600); err != nil {
		return nil, fmt.Errorf("writing private key: %w", err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		return nil, err
	}
	return signer.PublicKey(), nil
}

func TestSSHPublicKeyAuth(t *testing.T) {
	forEachMode(t, testSSHPublicKeyAuth)
}

func testSSHPublicKeyAuth(t *testing.T, native bool) {
	// Create a temporary directory for repositories
	repoDir, err := os.MkdirTemp("", "sshauth-test-repos")
	if err != nil {
		t.Fatalf("Failed to create temp repo dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(repoDir)
	}()

	clientDir, err := os.MkdirTemp("", "sshauth-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(clientDir)
	}()

	st := newStorage(t, repoDir, native)

	// Create a bare repository
	repoName := "auth-test-repo.git"
	repoPath := filepath.Join(repoDir, "repositories", repoName)
	runGitCmd(t, "", nil, "init", "--bare", repoPath)

	// Generate host key and authorized client key
	hostKey, err := generateHostKey()
	if err != nil {
		t.Fatalf("Failed to generate host key: %v", err)
	}

	goodKeyFile := filepath.Join(clientDir, "id_good")
	goodPubKey, err := generateClientKeyFile(goodKeyFile)
	if err != nil {
		t.Fatalf("Failed to generate good client key: %v", err)
	}

	// Start SSH server with public key auth
	server := backendssh.NewServer(backendssh.WithHostKey(hostKey), backendssh.WithStorage(st), backendssh.WithPublicKeyValidator(authenticate.NewSimplePublicKeyValidator(map[string]string{string(goodPubKey.Marshal()): ""})))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	go func() {
		_ = server.Serve(t.Context(), listener)
	}()

	addr := listener.Addr().(*net.TCPAddr)
	sshURL := "ssh://git@" + addr.String() + "/" + repoName
	port := strings.Split(addr.String(), ":")[1]

	goodSSHCmd := fmt.Sprintf("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s -p %s", goodKeyFile, port)
	goodEnv := []string{
		"GIT_TERMINAL_PROMPT=0",
		"GIT_SSH_COMMAND=" + goodSSHCmd,
	}

	t.Run("CloneWithAuthorizedKey", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-auth")
		runGitCmd(t, "", goodEnv, "clone", sshURL, cloneDir)

		hfdir := filepath.Join(cloneDir, ".git")
		if _, err := os.Stat(hfdir); os.IsNotExist(err) {
			t.Errorf(".git directory not found in cloned repository")
		}
	})

	t.Run("CloneWithUnauthorizedKeyFails", func(t *testing.T) {
		badKeyFile := filepath.Join(clientDir, "id_bad")
		_, err := generateClientKeyFile(badKeyFile)
		if err != nil {
			t.Fatalf("Failed to generate bad client key: %v", err)
		}

		badSSHCmd := fmt.Sprintf("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s -p %s", badKeyFile, port)
		badEnv := []string{
			"GIT_TERMINAL_PROMPT=0",
			"GIT_SSH_COMMAND=" + badSSHCmd,
		}

		cloneDir := filepath.Join(clientDir, "clone-bad-auth")
		cmd := exec.CommandContext(t.Context(), "git", "clone", sshURL, cloneDir)
		cmd.Env = append(os.Environ(), badEnv...)
		output, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("Expected clone to fail with unauthorized key, but it succeeded: %s", output)
		}
	})
}

func TestSSHPublicKeyIdentity(t *testing.T) {
	forEachMode(t, testSSHPublicKeyIdentity)
}

func testSSHPublicKeyIdentity(t *testing.T, native bool) {
	repoDir := t.TempDir()
	clientDir := t.TempDir()
	repoName := "identity.git"
	repoPath := filepath.Join(repoDir, "repositories", repoName)
	runGitCmd(t, "", nil, "init", "--bare", repoPath)
	hostKey, err := generateHostKey()
	if err != nil {
		t.Fatal(err)
	}
	boundKeyFile := filepath.Join(clientDir, "id_bound")
	boundKey, err := generateClientKeyFile(boundKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	freeKeyFile := filepath.Join(clientDir, "id_free")
	freeKey, err := generateClientKeyFile(freeKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	badKeyFile := filepath.Join(clientDir, "id_bad")
	if _, err := generateClientKeyFile(badKeyFile); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var names []string
	server := backendssh.NewServer(
		backendssh.WithHostKey(hostKey),
		backendssh.WithStorage(newStorage(t, repoDir, native)),
		backendssh.WithPublicKeyValidator(authenticate.NewSimplePublicKeyValidator(map[string]string{
			string(boundKey.Marshal()): "deploy",
			string(freeKey.Marshal()):  "",
		})),
		backendssh.WithPermissionHookFunc(func(ctx context.Context, _ permission.Operation, _ string, _ permission.Context) (bool, error) {
			mu.Lock()
			defer mu.Unlock()
			names = append(names, authenticate.IdentityFrom(ctx).Name())
			return true, nil
		}),
	)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() { _ = server.Serve(t.Context(), listener) }()
	addr := listener.Addr().(*net.TCPAddr)
	for _, test := range []struct {
		name    string
		keyFile string
		user    string
		want    string
	}{
		{name: "BoundIdentity", keyFile: boundKeyFile, user: "git", want: "deploy"},
		{name: "ClaimedIdentity", keyFile: freeKeyFile, user: "alice", want: "alice"},
		{name: "UnauthorizedKey", keyFile: badKeyFile, user: "git"},
	} {
		t.Run(test.name, func(t *testing.T) {
			mu.Lock()
			names = nil
			mu.Unlock()
			sshURL := "ssh://" + test.user + "@" + addr.String() + "/" + repoName
			sshCmd := fmt.Sprintf("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -o BatchMode=yes -i %s -p %d", test.keyFile, addr.Port)
			env := []string{"GIT_TERMINAL_PROMPT=0", "GIT_SSH_COMMAND=" + sshCmd}
			cloneDir := filepath.Join(clientDir, test.name)
			if test.want == "" {
				cmd := exec.CommandContext(t.Context(), "git", "clone", sshURL, cloneDir)
				cmd.Env = append(os.Environ(), env...)
				if output, err := cmd.CombinedOutput(); err == nil {
					t.Fatalf("Expected clone to fail with unauthorized key, but it succeeded: %s", output)
				}
			} else {
				runGitCmd(t, "", env, "clone", sshURL, cloneDir)
			}
			mu.Lock()
			defer mu.Unlock()
			if test.want == "" {
				if len(names) != 0 {
					t.Fatalf("unauthorized key reached permission hook: %v", names)
				}
				return
			}
			if len(names) == 0 {
				t.Fatal("permission hook was not called")
			}
			for _, name := range names {
				if name != test.want {
					t.Errorf("identity name = %q, want %q", name, test.want)
				}
			}
		})
	}
}

func TestSSHLFSAuthenticate(t *testing.T) {
	// Create a temporary directory for repositories
	repoDir, err := os.MkdirTemp("", "sshlfs-test-repos")
	if err != nil {
		t.Fatalf("Failed to create temp repo dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(repoDir)
	}()

	storage := newStorage(t, repoDir, true)

	// Create a bare repository
	repoName := "lfs-test-repo.git"
	repoPath := filepath.Join(repoDir, "repositories", repoName)
	runGitCmd(t, "", nil, "init", "--bare", repoPath)

	// Generate host key
	hostKey, err := generateHostKey()
	if err != nil {
		t.Fatalf("Failed to generate host key: %v", err)
	}

	httpURL := "http://localhost:8080"

	// Start SSH server with HTTP URL configured
	server := backendssh.NewServer(backendssh.WithHostKey(hostKey), backendssh.WithStorage(storage), backendssh.WithLFSURL(httpURL))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	go func() {
		_ = server.Serve(t.Context(), listener)
	}()

	addr := listener.Addr().String()

	t.Run("LFSAuthenticateDownload", func(t *testing.T) {
		config := &ssh.ClientConfig{
			User:            "git",
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		client, err := ssh.Dial("tcp", addr, config)
		if err != nil {
			t.Fatalf("Failed to dial SSH: %v", err)
		}
		defer client.Close()

		session, err := client.NewSession()
		if err != nil {
			t.Fatalf("Failed to create session: %v", err)
		}
		defer session.Close()

		output, err := session.Output("git-lfs-authenticate '/lfs-test-repo' download")
		if err != nil {
			t.Fatalf("git-lfs-authenticate failed: %v", err)
		}

		// Parse the JSON response
		var resp struct {
			Href      string            `json:"href"`
			Header    map[string]string `json:"header"`
			ExpiresIn int               `json:"expires_in"`
		}
		if err := json.Unmarshal(output, &resp); err != nil {
			t.Fatalf("Failed to parse LFS auth response: %v\nOutput: %s", err, output)
		}

		expectedHref := "http://localhost:8080/lfs-test-repo.git/info/lfs"
		if resp.Href != expectedHref {
			t.Errorf("href = %q, want %q", resp.Href, expectedHref)
		}
		if resp.ExpiresIn != 3600 {
			t.Errorf("expires_in = %d, want 3600", resp.ExpiresIn)
		}
	})

	t.Run("LFSAuthenticateUpload", func(t *testing.T) {
		config := &ssh.ClientConfig{
			User:            "git",
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		client, err := ssh.Dial("tcp", addr, config)
		if err != nil {
			t.Fatalf("Failed to dial SSH: %v", err)
		}
		defer client.Close()

		session, err := client.NewSession()
		if err != nil {
			t.Fatalf("Failed to create session: %v", err)
		}
		defer session.Close()

		output, err := session.Output("git-lfs-authenticate '/lfs-test-repo' upload")
		if err != nil {
			t.Fatalf("git-lfs-authenticate failed: %v", err)
		}

		var resp struct {
			Href string `json:"href"`
		}
		if err := json.Unmarshal(output, &resp); err != nil {
			t.Fatalf("Failed to parse LFS auth response: %v\nOutput: %s", err, output)
		}

		expectedHref := "http://localhost:8080/lfs-test-repo.git/info/lfs"
		if resp.Href != expectedHref {
			t.Errorf("href = %q, want %q", resp.Href, expectedHref)
		}
	})

	t.Run("LFSTransferReturnsError", func(t *testing.T) {
		config := &ssh.ClientConfig{
			User:            "git",
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		client, err := ssh.Dial("tcp", addr, config)
		if err != nil {
			t.Fatalf("Failed to dial SSH: %v", err)
		}
		defer client.Close()

		session, err := client.NewSession()
		if err != nil {
			t.Fatalf("Failed to create session: %v", err)
		}
		defer session.Close()

		err = session.Run("git-lfs-transfer '/lfs-test-repo' download")
		if err == nil {
			t.Fatal("Expected git-lfs-transfer to fail, but it succeeded")
		}
	})
}

func TestSSHLFSAuthenticateNoHTTPURL(t *testing.T) {
	// Create a temporary directory for repositories
	repoDir, err := os.MkdirTemp("", "sshlfs-nourl-test-repos")
	if err != nil {
		t.Fatalf("Failed to create temp repo dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(repoDir)
	}()

	storage := newStorage(t, repoDir, true)

	repoName := "lfs-test-repo.git"
	repoPath := filepath.Join(repoDir, "repositories", repoName)
	runGitCmd(t, "", nil, "init", "--bare", repoPath)

	hostKey, err := generateHostKey()
	if err != nil {
		t.Fatalf("Failed to generate host key: %v", err)
	}

	// Start SSH server WITHOUT HTTP URL configured
	server := backendssh.NewServer(backendssh.WithHostKey(hostKey), backendssh.WithStorage(storage))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	go func() {
		_ = server.Serve(t.Context(), listener)
	}()

	addr := listener.Addr().String()

	config := &ssh.ClientConfig{
		User:            "git",
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	client, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		t.Fatalf("Failed to dial SSH: %v", err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	defer session.Close()

	err = session.Run("git-lfs-authenticate '/lfs-test-repo' download")
	if err == nil {
		t.Fatal("Expected git-lfs-authenticate to fail when HTTP URL is not configured")
	}
}

func TestSSHPasswordAuth(t *testing.T) {
	forEachMode(t, testSSHPasswordAuth)
}

func testSSHPasswordAuth(t *testing.T, native bool) {
	// Create a temporary directory for repositories
	repoDir, err := os.MkdirTemp("", "sshpwdauth-test-repos")
	if err != nil {
		t.Fatalf("Failed to create temp repo dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(repoDir)
	}()

	clientDir, err := os.MkdirTemp("", "sshpwdauth-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(clientDir)
	}()

	st := newStorage(t, repoDir, native)

	// Create a bare repository
	repoName := "pwd-auth-test-repo.git"
	repoPath := filepath.Join(repoDir, "repositories", repoName)
	runGitCmd(t, "", nil, "init", "--bare", repoPath)

	// Generate host key
	hostKey, err := generateHostKey()
	if err != nil {
		t.Fatalf("Failed to generate host key: %v", err)
	}

	// Start SSH server with password auth
	auth := authenticate.NewSimpleBasicAuthValidator("testuser", "testpass")
	server := backendssh.NewServer(backendssh.WithHostKey(hostKey), backendssh.WithStorage(st), backendssh.WithBasicAuthValidator(auth))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	go func() {
		_ = server.Serve(t.Context(), listener)
	}()

	addr := listener.Addr().(*net.TCPAddr)

	t.Run("PasswordAuthViaDial", func(t *testing.T) {
		config := &ssh.ClientConfig{
			User: "testuser",
			Auth: []ssh.AuthMethod{
				ssh.Password("testpass"),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		adv := sshAdvertisement(t, addr.String(), config, 0, "git-upload-pack '"+repoName+"'")
		if !bytes.Contains(adv, []byte(wantAgent(native))) {
			t.Fatalf("advertisement lacks %q: %q", wantAgent(native), adv)
		}
	})

	t.Run("WrongPasswordViaDial", func(t *testing.T) {
		config := &ssh.ClientConfig{
			User: "testuser",
			Auth: []ssh.AuthMethod{
				ssh.Password("wrongpass"),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		_, err := ssh.Dial("tcp", addr.String(), config)
		if err == nil {
			t.Fatal("Expected SSH dial to fail with wrong password")
		}
	})
}

func TestSSHPublicKeyAuthViaAuthenticator(t *testing.T) {
	forEachMode(t, testSSHPublicKeyAuthViaAuthenticator)
}

func testSSHPublicKeyAuthViaAuthenticator(t *testing.T, native bool) {
	// Create a temporary directory for repositories
	repoDir, err := os.MkdirTemp("", "sshpkauth-test-repos")
	if err != nil {
		t.Fatalf("Failed to create temp repo dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(repoDir)
	}()

	clientDir, err := os.MkdirTemp("", "sshpkauth-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(clientDir)
	}()

	st := newStorage(t, repoDir, native)

	// Create a bare repository
	repoName := "pk-auth-test-repo.git"
	repoPath := filepath.Join(repoDir, "repositories", repoName)
	runGitCmd(t, "", nil, "init", "--bare", repoPath)

	// Generate host key and client key
	hostKey, err := generateHostKey()
	if err != nil {
		t.Fatalf("Failed to generate host key: %v", err)
	}

	goodKeyFile := filepath.Join(clientDir, "id_good")
	goodPubKey, err := generateClientKeyFile(goodKeyFile)
	if err != nil {
		t.Fatalf("Failed to generate good client key: %v", err)
	}

	// Start SSH server with public key auth
	auth := authenticate.NewSimplePublicKeyValidator(map[string]string{string(goodPubKey.Marshal()): ""})
	server := backendssh.NewServer(backendssh.WithHostKey(hostKey), backendssh.WithStorage(st), backendssh.WithPublicKeyValidator(auth))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	go func() {
		_ = server.Serve(t.Context(), listener)
	}()

	addr := listener.Addr().(*net.TCPAddr)
	sshURL := "ssh://git@" + addr.String() + "/" + repoName
	port := strings.Split(addr.String(), ":")[1]

	goodSSHCmd := fmt.Sprintf("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s -p %s", goodKeyFile, port)
	goodEnv := []string{
		"GIT_TERMINAL_PROMPT=0",
		"GIT_SSH_COMMAND=" + goodSSHCmd,
	}

	t.Run("CloneWithAuthorizedKeyViaAuthenticator", func(t *testing.T) {
		cloneDir := filepath.Join(clientDir, "clone-pk-auth")
		runGitCmd(t, "", goodEnv, "clone", sshURL, cloneDir)

		hfdir := filepath.Join(cloneDir, ".git")
		if _, err := os.Stat(hfdir); os.IsNotExist(err) {
			t.Errorf(".git directory not found in cloned repository")
		}
	})

	t.Run("CloneWithUnauthorizedKeyViaAuthenticatorFails", func(t *testing.T) {
		badKeyFile := filepath.Join(clientDir, "id_bad")
		_, err := generateClientKeyFile(badKeyFile)
		if err != nil {
			t.Fatalf("Failed to generate bad client key: %v", err)
		}

		badSSHCmd := fmt.Sprintf("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s -p %s", badKeyFile, port)
		badEnv := []string{
			"GIT_TERMINAL_PROMPT=0",
			"GIT_SSH_COMMAND=" + badSSHCmd,
		}

		cloneDir := filepath.Join(clientDir, "clone-bad-pk-auth")
		cmd := exec.CommandContext(t.Context(), "git", "clone", sshURL, cloneDir)
		cmd.Env = append(os.Environ(), badEnv...)
		output, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("Expected clone to fail with unauthorized key, but it succeeded: %s", output)
		}
	})
}

func TestSSHLFSAuthenticateWithAuthenticator(t *testing.T) {
	// Create a temporary directory for repositories
	repoDir, err := os.MkdirTemp("", "sshlfs-auth-test-repos")
	if err != nil {
		t.Fatalf("Failed to create temp repo dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(repoDir)
	}()

	storage := newStorage(t, repoDir, true)

	// Create a bare repository
	repoName := "lfs-auth-test-repo.git"
	repoPath := filepath.Join(repoDir, "repositories", repoName)
	runGitCmd(t, "", nil, "init", "--bare", repoPath)

	// Generate host key
	hostKey, err := generateHostKey()
	if err != nil {
		t.Fatalf("Failed to generate host key: %v", err)
	}

	httpURL := "http://localhost:8080"
	basicAuth := authenticate.NewSimpleBasicAuthValidator("admin", "secret")
	tokenSignValidator := authenticate.NewTokenSignValidator([]byte("secret"))

	// Start SSH server with authenticator and LFS URL
	server := backendssh.NewServer(backendssh.WithHostKey(hostKey), backendssh.WithStorage(storage),
		backendssh.WithLFSURL(httpURL),
		backendssh.WithBasicAuthValidator(basicAuth),
		backendssh.WithTokenSignValidator(tokenSignValidator),
	)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	go func() {
		_ = server.Serve(t.Context(), listener)
	}()

	addr := listener.Addr().String()

	t.Run("LFSAuthenticateIncludesAuthHeaders", func(t *testing.T) {
		config := &ssh.ClientConfig{
			User: "admin",
			Auth: []ssh.AuthMethod{
				ssh.Password("secret"),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		client, err := ssh.Dial("tcp", addr, config)
		if err != nil {
			t.Fatalf("Failed to dial SSH: %v", err)
		}
		defer client.Close()

		session, err := client.NewSession()
		if err != nil {
			t.Fatalf("Failed to create session: %v", err)
		}
		defer session.Close()

		output, err := session.Output("git-lfs-authenticate '/lfs-auth-test-repo' download")
		if err != nil {
			t.Fatalf("git-lfs-authenticate failed: %v", err)
		}

		// Parse the JSON response
		var resp struct {
			Href      string            `json:"href"`
			Header    map[string]string `json:"header"`
			ExpiresIn int               `json:"expires_in"`
		}
		if err := json.Unmarshal(output, &resp); err != nil {
			t.Fatalf("Failed to parse LFS auth response: %v\nOutput: %s", err, output)
		}

		expectedHref := "http://localhost:8080/lfs-auth-test-repo.git/info/lfs"
		if resp.Href != expectedHref {
			t.Errorf("href = %q, want %q", resp.Href, expectedHref)
		}

		// Verify auth headers are included
		authHeader, ok := resp.Header["Authorization"]
		if !ok {
			t.Fatal("Expected Authorization header in LFS auth response")
		}
		if !strings.HasPrefix(authHeader, "Bearer ") {
			t.Fatalf("Expected Bearer token, got %q", authHeader)
		}
		// Validate the signed token can be verified and contains the right subject
		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
		batchURL := expectedHref + "/objects/batch"
		user, next, ok, err := tokenSignValidator.Validate(context.Background(), http.MethodPost, batchURL, tokenStr)
		if err != nil || !ok || next {
			t.Fatal("Expected signed token to be valid")
		}
		if user != "admin" {
			t.Errorf("Expected user 'admin', got %q", user)
		}
	})
}

func TestSSHPreOpenHook(t *testing.T) {
	forEachMode(t, testSSHPreOpenHook)
}

func testSSHPreOpenHook(t *testing.T, native bool) {
	repoDir := t.TempDir()
	st := newStorage(t, repoDir, native)
	runGitCmd(t, "", nil, "init", "--bare", filepath.Join(repoDir, "repositories", "test-repo.git"))

	hostKey, err := generateHostKey()
	if err != nil {
		t.Fatalf("Failed to generate host key: %v", err)
	}

	type call struct {
		name  string
		write bool
	}
	var mu sync.Mutex
	var calls []call
	server := backendssh.NewServer(backendssh.WithHostKey(hostKey), backendssh.WithStorage(st),
		backendssh.WithPreOpenHookFunc(func(ctx context.Context, repoName string, write bool) error {
			mu.Lock()
			calls = append(calls, call{repoName, write})
			mu.Unlock()
			if repoName == "/late.git" {
				_, err := repository.Init(ctx, st.RepositoriesFS(), repository.ResolvePath(repoName), "main")
				return err
			}
			return nil
		}))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()
	go func() {
		_ = server.Serve(t.Context(), listener)
	}()

	for _, test := range []struct {
		name   string
		cmd    string
		exit   int
		stderr string
		calls  []call
	}{
		{"UploadPack", "git-upload-pack 'test-repo'", 0, "", []call{{"test-repo", false}}},
		{"ReceivePack", "git-receive-pack '/test-repo.git'", 0, "", []call{{"/test-repo.git", true}}},
		{"HookCreatesRepository", "git-upload-pack '/late.git'", 0, "", []call{{"/late.git", false}}},
		{"MissingRepository", "git-upload-pack '/missing.git'", 1, "repository not found\n", []call{{"/missing.git", false}}},
		{"InvalidName", "git-upload-pack '/org/../repo.git'", 1, "repository not found\n", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			mu.Lock()
			calls = nil
			mu.Unlock()
			client, err := ssh.Dial("tcp", listener.Addr().String(), &ssh.ClientConfig{User: "git", HostKeyCallback: ssh.InsecureIgnoreHostKey()})
			if err != nil {
				t.Fatalf("Failed to dial SSH: %v", err)
			}
			defer client.Close()
			session, err := client.NewSession()
			if err != nil {
				t.Fatalf("Failed to create session: %v", err)
			}
			defer session.Close()
			var stderr strings.Builder
			// A lone flush packet ends the service right after the advertisement.
			session.Stdin = strings.NewReader("0000")
			session.Stderr = &stderr
			exit := 0
			if err := session.Run(test.cmd); err != nil {
				var exitErr *ssh.ExitError
				if !errors.As(err, &exitErr) {
					t.Fatalf("%s: %v", test.cmd, err)
				}
				exit = exitErr.ExitStatus()
			}
			if exit != test.exit || stderr.String() != test.stderr {
				t.Errorf("exit = %d, stderr = %q; want %d, %q", exit, stderr.String(), test.exit, test.stderr)
			}
			mu.Lock()
			defer mu.Unlock()
			if !slices.Equal(calls, test.calls) {
				t.Errorf("hook calls = %v, want %v", calls, test.calls)
			}
		})
	}
}
