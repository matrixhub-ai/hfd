package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/permission"
)

// gitXetVersion pins the agent whose behavior the matrix asserts: uploads go
// through the xet CAS while its init_download answers not_supported, so clone
// and pull stay on git-lfs's basic transfer.
const gitXetVersion = "git-xet 0.2.1"

const (
	gitXetWriter     = "writer"
	gitXetWriterPass = "writer-secret"
	gitXetReader     = "reader"
	gitXetReaderPass = "reader-secret"
)

// requireGitXet returns the absolute git-xet path. Missing or mismatched tools
// skip locally and fail on CI, like requireUpDownMatrixTools.
func requireGitXet(t *testing.T) string {
	t.Helper()
	missing := func(format string, args ...any) {
		t.Helper()
		if os.Getenv("CI") != "" {
			t.Fatalf(format, args...)
		}
		t.Skipf(format, args...)
	}
	if _, err := exec.LookPath("git-lfs"); err != nil {
		missing("git-lfs not found; install git-lfs")
	}
	agent, err := exec.LookPath("git-xet")
	if err != nil {
		missing("git-xet not found; install %s", gitXetVersion)
	}
	out, err := exec.CommandContext(t.Context(), agent, "--version").CombinedOutput()
	if err != nil || strings.TrimSpace(string(out)) != gitXetVersion {
		missing("git-xet --version = %q (%v), want %q", strings.TrimSpace(string(out)), err, gitXetVersion)
	}
	return agent
}

// gitXetEnv builds a client environment from scratch: only PATH reaches the
// subprocess, HOME is a fresh directory (no gitconfig, netrc, or xet cache),
// system gitconfig is ignored, and the xet agent is registered through
// command-scope config so nothing on the host can pick the transfer.
func gitXetEnv(t *testing.T, agent string, config ...string) []string {
	t.Helper()
	config = append([]string{
		"lfs.customtransfer.xet.path", agent,
		"lfs.customtransfer.xet.args", "transfer",
		"lfs.customtransfer.xet.concurrent", "true",
		"lfs.basictransfersonly", "false",
	}, config...)
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + t.TempDir(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_COUNT=" + strconv.Itoa(len(config)/2),
	}
	for i := 0; i+1 < len(config); i += 2 {
		n := strconv.Itoa(i / 2)
		env = append(env, "GIT_CONFIG_KEY_"+n+"="+config[i], "GIT_CONFIG_VALUE_"+n+"="+config[i+1])
	}
	return env
}

// gitXet runs git with exactly env, returning combined output and the error;
// a watchdog kills wedged agents so hangs fail fast.
func gitXet(t *testing.T, dir string, env []string, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = env
	cmd.WaitDelay = 10 * time.Second
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("git %s timed out: %v\n%s", strings.Join(args, " "), ctx.Err(), out)
	}
	return string(out), err
}

func mustGitXet(t *testing.T, dir string, env []string, args ...string) {
	t.Helper()
	if out, err := gitXet(t, dir, env, args...); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

// randomData returns deterministic non-repeating bytes, so two versions never
// share xet chunks and every push must write new xorbs.
func randomData(t *testing.T, size int, seed byte) []byte {
	t.Helper()
	data := make([]byte, size)
	var key [32]byte
	key[0] = seed
	if _, err := rand.NewChaCha8(key).Read(data); err != nil {
		t.Fatal(err)
	}
	return data
}

// gitXetIdentity is one principal of the matrix; an empty user stays anonymous.
type gitXetIdentity struct {
	name, user, pass string
}

// hookUser is the name the permission hook must observe for this identity.
func (id gitXetIdentity) hookUser() string {
	if id.user == "" {
		return authenticate.AnonymousName
	}
	return id.user
}

// client returns a fresh client environment (with git-lfs filters installed
// in its private HOME) and the remote URL for repoID. Named identities embed
// their credentials in the remote: git-lfs and git-xet read them from there,
// and git presents them up front (http.proactiveAuth) because the server
// answers denials with 403, never a 401 challenge.
func (id gitXetIdentity) client(t *testing.T, s *e2eServer, agent, repoID string, config ...string) (env []string, remote string) {
	t.Helper()
	remote = s.httpURL + "/" + repoID + ".git"
	if id.user != "" {
		config = append([]string{"http.proactiveAuth", "basic"}, config...)
		remote = strings.Replace(remote, "http://", "http://"+id.user+":"+id.pass+"@", 1)
	}
	env = gitXetEnv(t, agent, config...)
	mustGitXet(t, "", env, "lfs", "install", "--skip-repo")
	return env, remote
}

// permRecorder keeps "<op> <user>" per permission check, proving which
// principal the server saw for each phase.
type permRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (pr *permRecorder) hook(ctx context.Context, op permission.Operation, _ string, _ permission.Context) (bool, error) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.calls = append(pr.calls, op.String()+" "+authenticate.IdentityFrom(ctx).Name())
	return true, nil
}

func (pr *permRecorder) reset() {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.calls = nil
}

// users returns the distinct principals seen since reset.
func (pr *permRecorder) users() []string {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	var users []string
	for _, call := range pr.calls {
		_, user, _ := strings.Cut(call, " ")
		if !slices.Contains(users, user) {
			users = append(users, user)
		}
	}
	return users
}

func (pr *permRecorder) saw(call string) bool {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return slices.Contains(pr.calls, call)
}

func (pr *permRecorder) dump() string {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return strings.Join(pr.calls, "\n")
}

// TestGitXetMatrix drives the real git-xet agent through git-lfs over HTTP
// against a server whose policy lets everyone read and only the writer
// update. Pushes must land through the xet CAS (xorb and shard writes, no
// basic PUT) and the Go xet client independently reconstructs the pushed
// bytes. git-xet 0.2.1 declines downloads by design, so clones and pulls are
// asserted to fetch exact bytes through the basic transfer, not the xet one.
// Denied identities must neither reach the CAS nor move the remote, and a
// basic-transfer control shows git-lfs's own upload path answers to the same
// policy. TestMain repeats the matrix for local and S3 storage.
func TestGitXetMatrix(t *testing.T) {
	agent := requireGitXet(t)
	rec, perms := &requestRecorder{}, &permRecorder{}
	writerOnly := func(ctx context.Context, _ permission.Operation, _ string, _ permission.Context) (bool, error) {
		return authenticate.IdentityFrom(ctx).Name() == gitXetWriter, nil
	}
	s := newE2EServer(t,
		withAuth(gitXetWriter, gitXetWriterPass),
		withBasicUsers(map[string]string{gitXetReader: gitXetReaderPass}),
		withPermissionHook(permission.All(perms.hook, permission.SplitReadWrite(nil, writerOnly))),
		withAPIHooks(),
		withWrap(rec.wrap))
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("requests:\n%s\npermission checks:\n%s", rec.dump(), perms.dump())
		}
	})

	const repoID = "xet-org/git-xet"
	// Only the writer may create the repository under this policy.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, s.httpURL+"/api/repos/create", strings.NewReader(`{"type":"model","name":"git-xet","organization":"xet-org"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(gitXetWriter, gitXetWriterPass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create repo status = %d, want 200", resp.StatusCode)
	}
	refsURL := s.httpURL + "/api/models/" + repoID + "/refs"
	getBody := func(t *testing.T, url string) (int, []byte) {
		t.Helper()
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, body
	}

	identities := []gitXetIdentity{
		{name: "Anonymous"},
		{name: "Reader", user: gitXetReader, pass: gitXetReaderPass},
		{name: "Writer", user: gitXetWriter, pass: gitXetWriterPass},
	}
	v1, v2 := randomData(t, 128*1024, 1), randomData(t, 128*1024, 2)

	// Clients live in the parent scope so their HOMEs and clones survive
	// across the sequential phases; basic pins git-lfs to its basic transfer.
	type client struct {
		env, basic  []string
		remote, dir string
	}
	clients := map[string]*client{}
	for _, id := range identities {
		env, remote := id.client(t, s, agent, repoID)
		basic, _ := id.client(t, s, agent, repoID, "lfs.basictransfersonly", "true")
		clients[id.name] = &client{env: env, basic: basic, remote: remote, dir: filepath.Join(t.TempDir(), "clone")}
	}
	writer := clients["Writer"]
	pushDir := filepath.Join(t.TempDir(), "push")

	commit := func(t *testing.T, dir string, env []string, data []byte, msg string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, transferMatrixFile), data, 0644); err != nil {
			t.Fatal(err)
		}
		mustGitXet(t, dir, env, "add", ".")
		mustGitXet(t, dir, env, "-c", "user.email=xet@test.com", "-c", "user.name=Xet Test", "commit", "-m", msg)
	}
	// pushXet pushes from dir; the bytes must land through the CAS as the writer.
	pushXet := func(t *testing.T, dir string, env []string) {
		t.Helper()
		rec.reset()
		perms.reset()
		mustGitXet(t, dir, env, "push", "origin", "main")
		if !rec.saw(http.MethodPost, "/xorbs/") || !rec.saw(http.MethodPost, "/shards") {
			t.Fatalf("git-xet push wrote no xorbs or shards; it did not use the xet transfer\n%s", rec.dump())
		}
		if rec.saw(http.MethodPut, "/objects/") {
			t.Fatalf("git-xet push used the basic PUT\n%s", rec.dump())
		}
		if users := perms.users(); !slices.Equal(users, []string{gitXetWriter}) {
			t.Fatalf("permission checks saw %v, want only %s\n%s", users, gitXetWriter, perms.dump())
		}
	}
	// readBasic asserts want reached dir through git-lfs's basic download as
	// user, the identity every permission check must have seen.
	readBasic := func(t *testing.T, dir string, want []byte, user string) {
		t.Helper()
		got, err := os.ReadFile(filepath.Join(dir, transferMatrixFile))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("checked-out bytes: %v (got %d bytes, want %d)", err, len(got), len(want))
		}
		if !rec.saw(http.MethodGet, "/objects/") || rec.saw(http.MethodGet, "reconstructions") {
			t.Fatalf("download did not use the basic transfer\n%s", rec.dump())
		}
		if users := perms.users(); !slices.Equal(users, []string{user}) {
			t.Fatalf("permission checks saw %v, want only %s\n%s", users, user, perms.dump())
		}
	}
	// reconstruct proves the pushed bytes through the xet protocol with the
	// Go client, independently of git-xet.
	reconstruct := func(t *testing.T, want []byte) {
		t.Helper()
		rec.reset()
		verifyHFResolveXet(t, s, repoID, want)
		if !rec.saw(http.MethodGet, "reconstructions") || rec.saw(http.MethodGet, "/xet-bridge/") {
			t.Fatalf("xet read did not reconstruct through the CAS\n%s", rec.dump())
		}
	}
	// pushDenied proves id is refused at the batch gate under env: nothing
	// reaches the data plane and the remote does not move.
	pushDenied := func(t *testing.T, id gitXetIdentity, env []string, seed byte) {
		t.Helper()
		c := clients[id.name]
		dir := filepath.Join(t.TempDir(), "denied")
		mustGitXet(t, "", env, "clone", c.remote, dir)
		denied := randomData(t, 128*1024, seed)
		commit(t, dir, env, denied, "denied push")
		_, refsBefore := getBody(t, refsURL)
		rec.reset()
		perms.reset()
		// git lfs push exercises the batch gate; git push stops at ref
		// discovery, before its pre-push hook.
		for _, attempt := range []struct{ args, want string }{
			{"lfs push origin main", "batch response: Authorization error"},
			{"push origin main", "403"},
		} {
			out, err := gitXet(t, dir, env, strings.Fields(attempt.args)...)
			if err == nil || !strings.Contains(out, attempt.want) {
				t.Fatalf("git %s: err = %v, want failure containing %q\n%s", attempt.args, err, attempt.want, out)
			}
		}
		if !rec.saw(http.MethodPost, "/info/lfs/objects/batch") || !perms.saw("update_repo "+id.hookUser()) {
			t.Fatalf("batch gate never denied %s\n%s\n%s", id.hookUser(), rec.dump(), perms.dump())
		}
		if rec.saw("", "/xorbs/") || rec.saw("", "/shards") || rec.saw(http.MethodPut, "/objects/") || rec.saw(http.MethodPost, "/git-receive-pack") {
			t.Fatalf("denied push reached the data plane\n%s", rec.dump())
		}
		if _, refsAfter := getBody(t, refsURL); !bytes.Equal(refsBefore, refsAfter) {
			t.Fatalf("refs changed: before=%s after=%s", refsBefore, refsAfter)
		}
		if status, _ := getBody(t, fmt.Sprintf("%s/objects/%x", s.httpURL, sha256.Sum256(denied))); status != http.StatusNotFound {
			t.Fatalf("denied object status = %d, want 404", status)
		}
	}

	t.Run("PushXetV1", func(t *testing.T) {
		mustGitXet(t, "", writer.env, "clone", writer.remote, pushDir)
		commit(t, pushDir, writer.env, v1, "add v1")
		pushXet(t, pushDir, writer.env)
		reconstruct(t, v1)
	})

	for _, id := range identities {
		t.Run("CloneBasic/"+id.name, func(t *testing.T) {
			c := clients[id.name]
			rec.reset()
			perms.reset()
			mustGitXet(t, "", c.env, "clone", c.remote, c.dir)
			readBasic(t, c.dir, v1, id.hookUser())
		})
	}

	for i, id := range identities[:2] {
		t.Run("PushDenied/"+id.name, func(t *testing.T) { pushDenied(t, id, clients[id.name].env, byte(10+i)) })
	}

	t.Run("PushXetV2", func(t *testing.T) {
		commit(t, pushDir, writer.env, v2, "update to v2")
		pushXet(t, pushDir, writer.env)
		reconstruct(t, v2)
	})

	for _, id := range identities {
		t.Run("PullBasic/"+id.name, func(t *testing.T) {
			c := clients[id.name]
			rec.reset()
			perms.reset()
			mustGitXet(t, c.dir, c.env, "pull")
			readBasic(t, c.dir, v2, id.hookUser())
		})
	}

	// Control: git-lfs's basic upload answers to the same policy and lands
	// through PUT without touching the CAS.
	for i, id := range identities[:2] {
		t.Run("PushBasicDenied/"+id.name, func(t *testing.T) { pushDenied(t, id, clients[id.name].basic, byte(20+i)) })
	}
	t.Run("PushBasicV3", func(t *testing.T) {
		v3 := randomData(t, 128*1024, 3)
		commit(t, pushDir, writer.basic, v3, "update to v3")
		rec.reset()
		perms.reset()
		mustGitXet(t, pushDir, writer.basic, "push", "origin", "main")
		if !rec.saw(http.MethodPut, "/objects/") || rec.saw("", "/xorbs/") || rec.saw("", "/shards") {
			t.Fatalf("basic push did not stay on the basic PUT\n%s", rec.dump())
		}
		if users := perms.users(); !slices.Equal(users, []string{gitXetWriter}) {
			t.Fatalf("permission checks saw %v, want only %s\n%s", users, gitXetWriter, perms.dump())
		}
		if got := mustGet(t, s.httpURL+"/"+repoID+"/resolve/main/"+transferMatrixFile); !bytes.Equal(got, v3) {
			t.Fatalf("resolved %d bytes, want v3 (%d bytes)", len(got), len(v3))
		}
	})
}
