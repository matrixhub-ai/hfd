package repository

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/receive"
)

// policyConfig is what every native git command must run with: the hermetic base, then the serving policy repository config cannot override.
var policyConfig = []struct{ key, value string }{
	{"safe.directory", "*"},
	{"safe.bareRepository", "all"},
	{"gc.auto", "0"},
	{"credential.helper", ""},
	{"core.askPass", ""},
	{"uploadpack.allowAnySHA1InWant", "false"},
	{"uploadpack.allowReachableSHA1InWant", "true"},
	{"uploadpack.allowTipSHA1InWant", "true"},
	{"uploadpack.allowFilter", "false"},
	{"uploadpack.allowRefInWant", "false"},
	{"receive.shallowUpdate", "false"},
}

// policyRepo creates a plain OS-backed repository with one commit, served by native git.
func policyRepo(t *testing.T) *Repository {
	t.Helper()
	repo := initTestRepo(t)
	requireGitMode(t, repo, true)
	mustCommit(t, repo, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: []byte("policy\n")})
	return repo
}

// policyAdvertisement returns the pkt-lines of the upload-pack advertisement hfd serves for gitProtocol.
func policyAdvertisement(t *testing.T, repo *Repository, gitProtocol string) []string {
	t.Helper()
	var adv bytes.Buffer
	if err := repo.AdvertiseRefs(t.Context(), &adv, GitUploadPack, gitProtocol); err != nil {
		t.Fatalf("AdvertiseRefs(%q): %v", gitProtocol, err)
	}
	return readPktLines(t, adv.Bytes())
}

// policyRequireTokens checks the space-separated list holds every token in want and none in deny.
func policyRequireTokens(t *testing.T, what, list string, want, deny []string) {
	t.Helper()
	tokens := strings.Fields(list)
	for _, w := range want {
		if !slices.Contains(tokens, w) {
			t.Errorf("%s %q lacks %s", what, list, w)
		}
	}
	for _, d := range deny {
		if slices.Contains(tokens, d) {
			t.Errorf("%s %q advertises %s although gitEnv pins it off over repository config", what, list, d)
		}
	}
}

// policyRecorder returns a git wrapper that saves each invocation's arguments, then its environment, to a new file in dir before running git.
func policyRecorder(t *testing.T) (bin, dir string) {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	dir = t.TempDir()
	bin = filepath.Join(t.TempDir(), "git")
	script := fmt.Sprintf("#!/bin/sh\nf=$(mktemp '%s/call.XXXXXX') || exit 1\n{ printf '%%s\\n' \"$*\"; env; } > \"$f\"\nexec '%s' \"$@\"\n", dir, gitPath)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, dir
}

// policyCall is one recorded git invocation: its arguments and its GIT_* environment.
type policyCall struct {
	args string
	env  map[string]string
}

// policyCalls returns the invocations recorded in dir whose arguments include sub, failing when there is none.
func policyCalls(t *testing.T, dir, sub string) []policyCall {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var calls []policyCall
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		args, env, _ := strings.Cut(string(data), "\n")
		if !slices.Contains(strings.Fields(args), sub) {
			continue
		}
		call := policyCall{args: args, env: map[string]string{}}
		for line := range strings.Lines(env) {
			if k, v, ok := strings.Cut(strings.TrimSuffix(line, "\n"), "="); ok && strings.HasPrefix(k, "GIT_") {
				call.env[k] = v
			}
		}
		calls = append(calls, call)
	}
	if len(calls) == 0 {
		t.Fatalf("hfd ran no git %s", sub)
	}
	return calls
}

// config returns the last value of key among the call's GIT_CONFIG_* entries.
func (c policyCall) config(key string) (value string, ok bool) {
	n, _ := strconv.Atoi(c.env["GIT_CONFIG_COUNT"])
	for i := range n {
		if strings.EqualFold(c.env["GIT_CONFIG_KEY_"+strconv.Itoa(i)], key) {
			value, ok = c.env["GIT_CONFIG_VALUE_"+strconv.Itoa(i)], true
		}
	}
	return value, ok
}

// policyPack packs the objects revs reach in work as a pushing client does.
func policyPack(t *testing.T, work string, revs ...string) []byte {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", "-C", work, "pack-objects", "--stdout", "--revs", "-q")
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	cmd.Stdin = strings.NewReader(strings.Join(revs, "\n") + "\n")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	pack, err := cmd.Output()
	if err != nil {
		t.Fatalf("pack-objects: %v\n%s", err, stderr.String())
	}
	return pack
}

func TestGitPolicyPins(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not in PATH")
	}
	setGitBinary(t, gitPath)

	t.Run("Recorded", func(t *testing.T) {
		repo := policyRepo(t)
		for _, tc := range []struct {
			name, sub string
			op        func(ctx context.Context) error
		}{
			{"AdvertiseUploadPackV0", "upload-pack", func(ctx context.Context) error {
				return repo.AdvertiseRefs(ctx, io.Discard, GitUploadPack, "")
			}},
			{"AdvertiseUploadPackV2", "upload-pack", func(ctx context.Context) error {
				return repo.AdvertiseRefs(ctx, io.Discard, GitUploadPack, "version=2")
			}},
			{"AdvertiseReceivePack", "receive-pack", func(ctx context.Context) error {
				return repo.AdvertiseRefs(ctx, io.Discard, GitReceivePack, "")
			}},
			{"StatelessUploadPack", "upload-pack", func(ctx context.Context) error {
				return repo.Stateless(ctx, io.Discard, strings.NewReader("0000"), GitUploadPack, "", ReceivePackHooks{})
			}},
			{"StatelessReceivePack", "receive-pack", func(ctx context.Context) error {
				return repo.Stateless(ctx, io.Discard, strings.NewReader("0000"), GitReceivePack, "", ReceivePackHooks{})
			}},
			{"ServeUploadPack", "upload-pack", func(ctx context.Context) error {
				return repo.Serve(ctx, flushOnlyRW{strings.NewReader("0000"), io.Discard}, GitUploadPack, "", ReceivePackHooks{})
			}},
			{"ServeReceivePack", "receive-pack", func(ctx context.Context) error {
				return repo.Serve(ctx, flushOnlyRW{strings.NewReader("0000"), io.Discard}, GitReceivePack, "", ReceivePackHooks{})
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				bin, dir := policyRecorder(t)
				setGitBinary(t, bin)
				if err := tc.op(t.Context()); err != nil {
					t.Fatalf("hfd running git %s: %v", tc.sub, err)
				}
				for _, c := range policyCalls(t, dir, tc.sub) {
					for _, want := range policyConfig {
						if got, ok := c.config(want.key); !ok || got != want.value {
							t.Errorf("git %s: %s = %q (set %t), want %q from gitEnv", c.args, want.key, got, ok, want.value)
						}
					}
				}
			})
		}
	})

	t.Run("RepositoryUploadPackConfig", func(t *testing.T) {
		repo := policyRepo(t)
		// Each value is the opposite of its pin in gitEnv.
		for _, c := range []struct{ key, value string }{
			{"uploadpack.allowAnySHA1InWant", "true"},
			{"uploadpack.allowReachableSHA1InWant", "false"},
			{"uploadpack.allowTipSHA1InWant", "false"},
			{"uploadpack.allowFilter", "true"},
			{"uploadpack.allowRefInWant", "true"},
		} {
			runGit(t, repo.gitDir(), "config", c.key, c.value)
		}

		t.Run("V0", func(t *testing.T) {
			lines := policyAdvertisement(t, repo, "")
			i := slices.IndexFunc(lines, func(l string) bool { return l != "" && !strings.HasPrefix(l, "# service=") })
			if i < 0 {
				t.Fatalf("v0 advertisement %q has no ref line", lines)
			}
			_, caps, ok := strings.Cut(lines[i], "\x00")
			if !ok {
				t.Fatalf("first v0 ref line %q carries no capabilities", lines[i])
			}
			policyRequireTokens(t, "v0 capabilities", caps, []string{"multi_ack", "shallow", "allow-tip-sha1-in-want", "allow-reachable-sha1-in-want"}, []string{"filter"})
		})

		t.Run("V2", func(t *testing.T) {
			lines := policyAdvertisement(t, repo, "version=2")
			i := slices.IndexFunc(lines, func(l string) bool { return strings.HasPrefix(l, "fetch=") })
			if i < 0 {
				t.Fatalf("v2 advertisement %q has no fetch capability", lines)
			}
			policyRequireTokens(t, "v2 fetch capability", strings.TrimPrefix(lines[i], "fetch="), []string{"shallow"}, []string{"filter", "ref-in-want"})
		})
	})

	t.Run("RepositoryShallowUpdate", func(t *testing.T) {
		repo := policyRepo(t)
		runGit(t, repo.gitDir(), "config", "receive.shallowUpdate", "true")
		work := filepath.Join(t.TempDir(), "work")
		initParityWork(t, work)
		commitFile(t, work, "a.txt", "root\n", "root")
		commitFile(t, work, "a.txt", "tip\n", "tip")
		parent := strings.TrimSpace(gitOut(t, work, "rev-parse", "HEAD~1"))
		tip := strings.TrimSpace(gitOut(t, work, "rev-parse", "HEAD"))
		// Pushed without its parent, as from a depth-1 clone, tip can land only as a new shallow root.
		pack := policyPack(t, work, tip, "^"+parent)
		const ref = "refs/heads/shallow-branch"
		body := append(pktLines(t, "shallow "+tip+"\n", receive.ZeroHash+" "+tip+" "+ref+"\x00report-status\n", ""), pack...)
		var out bytes.Buffer
		if err := repo.Stateless(t.Context(), &out, bytes.NewReader(body), GitReceivePack, "", ReceivePackHooks{}); err != nil {
			t.Fatalf("receive-pack: %v", err)
		}
		if got, want := reportLines(t, &out, false), []string{"unpack ok", "ng " + ref + " shallow update not allowed"}; !slices.Equal(got, want) {
			t.Errorf("shallow push report = %q, want %q: gitEnv pins receive.shallowUpdate off over repository config", got, want)
		}
	})
}
