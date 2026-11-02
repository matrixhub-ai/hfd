package e2e_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/receive"
)

// TestHarnessEngineOption checks withGoGit moves the local pass off native git while the default server stays native.
func TestHarnessEngineOption(t *testing.T) {
	const org, name = "harness-engine", "repo"
	repoID := org + "/" + name
	native := testS3Client == nil

	def := newE2EServer(t)
	def.createRepo(t, org, name)
	defURL, _ := def.httpRemote(repoID)
	requireEngine(t, defURL, native)

	goGit := newE2EServer(t, withGoGit())
	goGit.createRepo(t, org, name)
	goGitURL, _ := goGit.httpRemote(repoID)
	requireEngine(t, goGitURL, false)
}

// TestHarnessRequireEngineRejectsOtherEngine checks requireEngine fails a server running the other engine.
func TestHarnessRequireEngineRejectsOtherEngine(t *testing.T) {
	const org, name = "harness-engine-reject", "repo"
	s := newE2EServer(t)
	s.createRepo(t, org, name)
	repoURL, _ := s.httpRemote(org + "/" + name)

	otherEngine := testS3Client != nil
	rec := &recordingTB{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		requireEngine(rec, repoURL, otherEngine)
	}()
	<-done
	if !rec.failed {
		t.Fatalf("requireEngine(native=%t) accepted a server running the other engine", otherEngine)
	}
}

// recordingTB records failures instead of failing the test; FailNow ends the goroutine like testing does.
type recordingTB struct {
	testing.TB
	failed bool
}

func (r *recordingTB) Fail()                 { r.failed = true }
func (r *recordingTB) Error(...any)          { r.Fail() }
func (r *recordingTB) Errorf(string, ...any) { r.Fail() }
func (r *recordingTB) FailNow()              { r.Fail(); runtime.Goexit() }
func (r *recordingTB) Fatal(...any)          { r.FailNow() }
func (r *recordingTB) Fatalf(string, ...any) { r.FailNow() }

// TestHarnessTransportStats checks withTransportStats counts HTTP and SSH traffic in one pair of counters.
func TestHarnessTransportStats(t *testing.T) {
	const org, name = "harness-stats", "repo"
	repoID := org + "/" + name
	s := newE2EServer(t, withSSH(), withTransportStats())
	s.createRepo(t, org, name)
	httpURL, httpEnv := s.httpRemote(repoID)

	work := filepath.Join(t.TempDir(), "work")
	runGit(t, "", httpEnv, "clone", httpURL, work)
	runGit(t, work, httpEnv, "config", "user.email", "test@test.com")
	runGit(t, work, httpEnv, "config", "user.name", "Test User")
	if err := os.WriteFile(filepath.Join(work, "stats.txt"), []byte("stats\n"), 0o644); err != nil {
		t.Fatalf("write stats.txt: %v", err)
	}
	runGit(t, work, httpEnv, "add", "stats.txt")
	runGit(t, work, httpEnv, "commit", "-m", "Add stats.txt")
	runGit(t, work, httpEnv, "push", "origin", "HEAD:refs/heads/main")

	in, out1 := s.transportBytes()
	if in <= 0 || out1 <= 0 {
		t.Fatalf("transportBytes() after an HTTP push = (%d, %d), want both > 0", in, out1)
	}

	runGit(t, "", httpEnv, "clone", httpURL, filepath.Join(t.TempDir(), "http-clone"))
	_, out2 := s.transportBytes()
	if out2 <= out1 {
		t.Fatalf("transportBytes() out after an HTTP clone = %d, want > %d", out2, out1)
	}

	sshURL, sshEnv := s.sshRemote(repoID)
	runGit(t, "", sshEnv, "clone", sshURL, filepath.Join(t.TempDir(), "ssh-clone"))
	if _, out3 := s.transportBytes(); out3 <= out2 {
		t.Fatalf("transportBytes() out after an SSH clone = %d, want > %d: SSH traffic is not counted", out3, out2)
	}

	plain := newE2EServer(t)
	plain.createRepo(t, org, name)
	plainURL, plainEnv := plain.httpRemote(repoID)
	runGit(t, "", plainEnv, "clone", plainURL, filepath.Join(t.TempDir(), "plain-clone"))
	if in, out := plain.transportBytes(); in != 0 || out != 0 {
		t.Fatalf("transportBytes() without withTransportStats = (%d, %d), want (0, 0)", in, out)
	}
}

// TestHarnessRepoDir checks repoDir names the bare repository on the local pass, for either engine, and is empty on the S3 pass.
func TestHarnessRepoDir(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []e2eOption
	}{
		{"Default", nil},
		{"GoGit", []e2eOption{withGoGit()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const org, name = "harness-repodir", "repo"
			repoID := org + "/" + name
			s := newE2EServer(t, tc.opts...)
			s.createRepo(t, org, name)

			dir := s.repoDir(repoID)
			if testS3Client != nil {
				if dir != "" {
					t.Fatalf("repoDir(%q) = %q on the S3 pass, want \"\"", repoID, dir)
				}
				return
			}
			if !strings.HasSuffix(dir, ".git") {
				t.Fatalf("repoDir(%q) = %q on the local pass, want the bare repository directory ending in .git", repoID, dir)
			}
			if _, err := os.Stat(filepath.Join(dir, "HEAD")); err != nil {
				t.Fatalf("repoDir(%q) = %q has no HEAD: %v", repoID, dir, err)
			}
		})
	}
}

// TestHarnessMirrorDestination checks withMirrorDestination mirrors each push to the destination before the push returns.
func TestHarnessMirrorDestination(t *testing.T) {
	const org, name = "harness-mirror", "repo"
	repoID := org + "/" + name
	dest := newE2EServer(t)
	dest.createRepo(t, org, name)
	src := newE2EServer(t, withMirrorDestination(dest.httpURL))
	src.createRepo(t, org, name)
	srcURL, srcEnv := src.httpRemote(repoID)
	destURL, destEnv := dest.httpRemote(repoID)

	work := filepath.Join(t.TempDir(), "work")
	runGit(t, "", srcEnv, "clone", srcURL, work)
	runGit(t, work, srcEnv, "config", "user.email", "test@test.com")
	runGit(t, work, srcEnv, "config", "user.name", "Test User")
	for i, file := range []string{"first.txt", "second.txt"} {
		if err := os.WriteFile(filepath.Join(work, file), []byte(file+"\n"), 0o644); err != nil {
			t.Fatalf("write %s: %v", file, err)
		}
		runGit(t, work, srcEnv, "add", file)
		runGit(t, work, srcEnv, "commit", "-m", "Add "+file)
		runGit(t, work, srcEnv, "push", "origin", "HEAD:refs/heads/main")

		head, stderr, err := gitCmd(t, work, srcEnv, "rev-parse", "HEAD")
		if err != nil {
			t.Fatalf("git rev-parse HEAD: %v\n%s", err, stderr)
		}
		want := strings.TrimSpace(head) + "\trefs/heads/main"
		if refs := runGit(t, "", destEnv, "ls-remote", destURL); !strings.Contains(refs, want) {
			t.Fatalf("push %d: destination ls-remote = %q, want it to contain %q once the source push returns", i+1, refs, want)
		}
	}
}

// TestHarnessMirrorDestinationHookOrder checks withHooks' post-receive hook runs before the mirror push, on HTTP and SSH, and its error skips that push.
func TestHarnessMirrorDestinationHookOrder(t *testing.T) {
	const org, name = "harness-mirror-hooks", "repo"
	repoID := org + "/" + name
	dest := newE2EServer(t)
	dest.createRepo(t, org, name)
	destURL, destEnv := dest.httpRemote(repoID)

	var (
		mu     sync.Mutex
		saw    []bool // per user hook call: whether the destination already had a pushed head
		refuse bool
	)
	post := func(_ context.Context, _ string, updates []receive.RefUpdate) error {
		mu.Lock()
		defer mu.Unlock()
		refs, stderr, err := gitCmd(t, "", destEnv, "ls-remote", destURL)
		if err != nil {
			t.Errorf("destination ls-remote inside the user hook: %v\n%s", err, stderr)
		}
		had := false
		for _, u := range updates {
			had = had || strings.Contains(refs, u.NewRev())
		}
		saw = append(saw, had)
		if refuse {
			return errors.New("user hook refuses the push")
		}
		return nil
	}
	src := newE2EServer(t, withSSH(), withHooks(nil, post), withMirrorDestination(dest.httpURL))
	src.createRepo(t, org, name)
	httpURL, httpEnv := src.httpRemote(repoID)
	sshURL, sshEnv := src.sshRemote(repoID)

	work := filepath.Join(t.TempDir(), "work")
	runGit(t, "", httpEnv, "clone", httpURL, work)
	runGit(t, work, httpEnv, "config", "user.email", "test@test.com")
	runGit(t, work, httpEnv, "config", "user.name", "Test User")
	commit := func(file string) string {
		t.Helper()
		if err := os.WriteFile(filepath.Join(work, file), []byte(file+"\n"), 0o644); err != nil {
			t.Fatalf("write %s: %v", file, err)
		}
		runGit(t, work, httpEnv, "add", file)
		runGit(t, work, httpEnv, "commit", "-m", "Add "+file)
		head, stderr, err := gitCmd(t, work, httpEnv, "rev-parse", "HEAD")
		if err != nil {
			t.Fatalf("git rev-parse HEAD: %v\n%s", err, stderr)
		}
		return strings.TrimSpace(head)
	}
	destHas := func(head string) bool {
		t.Helper()
		return strings.Contains(runGit(t, "", destEnv, "ls-remote", destURL), head+"\trefs/heads/main")
	}

	h1 := commit("one.txt")
	runGit(t, work, httpEnv, "push", "origin", "HEAD:refs/heads/main")
	if !destHas(h1) {
		t.Fatalf("HTTP push 1: destination lacks %s once the push returns", h1)
	}

	mu.Lock()
	refuse = true
	mu.Unlock()
	h2 := commit("two.txt")
	// Post-receive runs once the refs moved, so its error cannot fail the client's push.
	runGit(t, work, httpEnv, "push", "origin", "HEAD:refs/heads/main")
	if destHas(h2) {
		t.Fatalf("HTTP push 2: destination has %s although the user hook returned an error; want the error to skip the mirror push", h2)
	}
	mu.Lock()
	refuse = false
	mu.Unlock()

	h3 := commit("three.txt")
	runGit(t, work, sshEnv, "push", sshURL, "HEAD:refs/heads/main")
	if !destHas(h3) {
		t.Fatalf("SSH push 3: destination lacks %s once the push returns; want SSH pushes mirrored too", h3)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(saw) != 3 {
		t.Fatalf("user hook ran %d times for 3 pushes, want 3", len(saw))
	}
	for i, had := range saw {
		if had {
			t.Errorf("push %d: the user hook saw the pushed head on the destination already; want it to run before the mirror push", i+1)
		}
	}
}

// TestHarnessSkipUnlessLocalPass checks skipUnlessLocalPass skips the S3 pass and only it.
func TestHarnessSkipUnlessLocalPass(t *testing.T) {
	wantSkip := testS3Client != nil
	t.Cleanup(func() {
		if t.Skipped() != wantSkip {
			t.Errorf("skipUnlessLocalPass skipped = %t, want %t: it must skip the S3 pass and only it", t.Skipped(), wantSkip)
		}
	})
	skipUnlessLocalPass(t)
}
