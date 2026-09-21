package repository

// Tests for the native git binary path: local directory detection, the
// hermetic command environment and the git/go-git selection in AdvertiseRefs.

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp/sideband"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	s3fs "github.com/wzshiming/go-billy-s3fs"

	"github.com/matrixhub-ai/hfd/pkg/receive"
)

// setGitBinary points GitBinary at bin for the test and restores it afterwards.
func setGitBinary(t *testing.T, bin string) {
	t.Helper()
	prev := gitBinary
	gitBinary = bin
	t.Cleanup(func() { gitBinary = prev })
}

// forEachGitMode runs fn with go-git selected and again with the real git binary.
func forEachGitMode(t *testing.T, fn func(t *testing.T, native bool)) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	for _, m := range []struct {
		name string
		bin  string
	}{{"go-git", ""}, {"native", gitPath}} {
		t.Run(m.name, func(t *testing.T) {
			setGitBinary(t, m.bin)
			fn(t, m.bin != "")
		})
	}
}

// forEachProtocol runs fn with each GIT_PROTOCOL request value; "" is the v0 default.
func forEachProtocol(t *testing.T, protocols []string, fn func(t *testing.T, gitProtocol string)) {
	for _, p := range protocols {
		t.Run("ProtocolV"+cmp.Or(strings.TrimPrefix(p, "version="), "0"), func(t *testing.T) { fn(t, p) })
	}
}

// requireGitMode asserts the OS-backed repo is served by the selected implementation.
func requireGitMode(t *testing.T, repo *Repository, native bool) {
	t.Helper()
	if (repo.gitDir() != "") != native {
		t.Fatalf("gitDir = %q, want native = %t", repo.gitDir(), native)
	}
}

// advertisedAgent returns the agent capability value of an advertisement.
func advertisedAgent(adv []byte) string {
	_, rest, ok := bytes.Cut(adv, []byte("agent="))
	if !ok {
		return ""
	}
	if end := bytes.IndexAny(rest, " \n"); end >= 0 {
		rest = rest[:end]
	}
	return string(rest)
}

// recordingGit returns a git wrapper that appends each invocation's arguments to log before running git.
func recordingGit(t *testing.T) (bin, log string) {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	dir := t.TempDir()
	log = filepath.Join(dir, "calls.log")
	bin = filepath.Join(dir, "git")
	script := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$*\" >> '%s'\nexec '%s' \"$@\"\n", log, gitPath)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, log
}

// recordedCalls returns the invocations logged by recordingGit, one per line.
func recordedCalls(t *testing.T, log string) string {
	t.Helper()
	data, err := os.ReadFile(log)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return string(data)
}

// appendConfig adds raw lines to a repository's config, for entries `git config` cannot write such as a bare key.
func appendConfig(t *testing.T, bare, lines string) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(bare, "config"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(lines); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// malformedChildRepo nests a HEAD-and-objects/ child.git inside a checkout: hfd opens it, git rejects it, discovery finds the checkout.
func malformedChildRepo(t *testing.T) (ancestor, child string) {
	t.Helper()
	ancestor = filepath.Join(t.TempDir(), "ancestor")
	initParityWork(t, ancestor)
	commitFile(t, ancestor, "file.txt", "one\n", "c1")
	child = filepath.Join(ancestor, "child.git")
	if err := os.MkdirAll(filepath.Join(child, "objects"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(child, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return ancestor, child
}

// flushOnlyRW is a stateful client that sends a lone flush packet after the advertisement.
type flushOnlyRW struct {
	io.Reader
	io.Writer
}

func TestServeGitBinarySelection(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	bare, _ := buildParityUpstream(t, root)
	local, err := Open(osfs.Default, bare)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	bin, log := recordingGit(t)

	for _, service := range []string{GitUploadPack, GitReceivePack} {
		t.Run(service, func(t *testing.T) {
			setGitBinary(t, bin)
			var out bytes.Buffer
			if err := local.Stateless(ctx, &out, strings.NewReader("0000"), service, "", ReceivePackHooks{}); err != nil {
				t.Fatalf("Stateless: %v", err)
			}
			if err := local.Serve(ctx, flushOnlyRW{strings.NewReader("0000"), &out}, service, "", ReceivePackHooks{}); err != nil {
				t.Fatalf("Serve: %v", err)
			}
			short := strings.TrimPrefix(service, "git-")
			calls := recordedCalls(t, log)
			for _, want := range []string{short + " --stateless-rpc " + bare, short + " " + bare} {
				if !strings.Contains(calls, want+"\n") {
					t.Errorf("native git not invoked as %q; calls:\n%s", want, calls)
				}
			}
			if agent := advertisedAgent(out.Bytes()); !strings.HasPrefix(agent, "git/") {
				t.Errorf("Serve advertisement agent = %q, want native git", agent)
			}
		})
	}

	bogus := filepath.Join(root, "no-such-git")
	t.Run("BogusBinaryFailsOnLocal", func(t *testing.T) {
		setGitBinary(t, bogus)
		for _, service := range []string{GitUploadPack, GitReceivePack} {
			var out bytes.Buffer
			if err := local.Stateless(ctx, &out, strings.NewReader("0000"), service, "", ReceivePackHooks{}); err == nil {
				t.Errorf("%s Stateless with bogus git should fail, got %q", service, out.Bytes())
			}
			if err := local.Serve(ctx, flushOnlyRW{strings.NewReader("0000"), &out}, service, "", ReceivePackHooks{}); err == nil {
				t.Errorf("%s Serve with bogus git should fail, got %q", service, out.Bytes())
			}
		}
	})

	t.Run("MemfsIgnoresBinary", func(t *testing.T) {
		setGitBinary(t, bogus)
		repo, err := Init(ctx, memfs.New(), "/mem.git", "main")
		if err != nil {
			t.Fatalf("init memfs repository: %v", err)
		}
		for _, service := range []string{GitUploadPack, GitReceivePack} {
			var out bytes.Buffer
			if err := repo.Stateless(ctx, &out, strings.NewReader("0000"), service, "", ReceivePackHooks{}); err != nil {
				t.Errorf("%s Stateless on memfs must use go-git: %v", service, err)
			}
			if err := repo.Serve(ctx, flushOnlyRW{strings.NewReader("0000"), &out}, service, "", ReceivePackHooks{}); err != nil {
				t.Errorf("%s Serve on memfs must use go-git: %v", service, err)
			}
		}
	})
}

// countingReader counts the bytes read through it.
type countingReader struct {
	io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.Reader.Read(p)
	c.n += n
	return n, err
}

// stalledReader is a client that never sends its pack: Read signals once, then waits for ctx and fails like a dropped connection.
type stalledReader struct {
	ctx     context.Context
	reading chan struct{}
}

func (r *stalledReader) Read([]byte) (int, error) {
	select {
	case r.reading <- struct{}{}:
	default:
	}
	<-r.ctx.Done()
	return 0, io.ErrUnexpectedEOF
}

func TestServeGitBinaryCancel(t *testing.T) {
	root := t.TempDir()
	bare, _ := buildParityUpstream(t, root)
	repo, err := Open(osfs.Default, bare)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	forEachGitMode(t, func(t *testing.T, native bool) {
		requireGitMode(t, repo, native)
		for _, service := range []string{GitUploadPack, GitReceivePack} {
			t.Run(service, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				inR, inW := io.Pipe()
				defer func() { _ = inW.Close() }()
				outR, outW := io.Pipe()
				done := make(chan error, 1)
				go func() {
					done <- repo.Serve(ctx, flushOnlyRW{inR, outW}, service, "", ReceivePackHooks{})
				}()
				// Consume the advertisement so the server is blocked reading the request when cancelled.
				rd := bufio.NewReader(outR)
				for {
					l, _, err := pktline.ReadLine(rd)
					if err != nil {
						t.Fatalf("reading advertisement: %v", err)
					}
					if l == pktline.Flush {
						break
					}
				}
				cancel()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("Serve = %v, want context.Canceled", err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("Serve did not return after cancellation")
				}
			})
		}
	})
}

// TestServeGitBinaryEarlyExit makes git die before its advertisement while the
// client keeps stdin open waiting for it; Serve must return git's error instead
// of waiting for commands the client will never send.
func TestServeGitBinaryEarlyExit(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	setGitBinary(t, gitPath)
	root := t.TempDir()
	bare, _ := buildParityUpstream(t, root)
	repo, err := Open(osfs.Default, bare)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	runGit(t, bare, "config", "receive.advertisePushOptions", "not-a-bool")
	inR, inW := io.Pipe()
	defer func() { _ = inW.Close() }()
	done := make(chan error, 1)
	go func() {
		done <- repo.Serve(t.Context(), flushOnlyRW{inR, io.Discard}, GitReceivePack, "", ReceivePackHooks{})
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "not-a-bool") {
			t.Fatalf("Serve = %v, want git's config error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve kept waiting for commands after git exited")
	}
}

// updateStrings renders ref updates as "old new ref" lines.
func updateStrings(updates []receive.RefUpdate) []string {
	out := make([]string, len(updates))
	for i, u := range updates {
		out[i] = u.OldRev() + " " + u.NewRev() + " " + u.RefName()
	}
	return out
}

// reportLines decodes the report-status filling out, demultiplexed when mux is set, as git's "unpack", "ok" and "ng" lines.
func reportLines(t *testing.T, out *bytes.Buffer, mux bool) []string {
	t.Helper()
	var rd io.Reader = out
	if mux {
		rd = sideband.NewDemuxer(sideband.Sideband64k, out)
	}
	var rs packp.ReportStatus
	if err := rs.Decode(rd); err != nil {
		t.Fatalf("decode report-status: %v", err)
	}
	if mux {
		if _, err := rd.Read(make([]byte, 1)); err != io.EOF {
			t.Fatalf("sideband stream should end with a flush, got %v", err)
		}
	}
	if out.Len() != 0 {
		t.Fatalf("unexpected trailing output %q", out.Bytes())
	}
	lines := []string{"unpack " + rs.UnpackStatus}
	for _, cs := range rs.CommandStatuses {
		if cs.Status == "ok" {
			lines = append(lines, "ok "+cs.ReferenceName.String())
		} else {
			lines = append(lines, "ng "+cs.ReferenceName.String()+" "+cs.Status)
		}
	}
	return lines
}

// Git 2.51 changed reference-transaction rejection messages.
var nativeReasons = map[string]string{
	"reference already exists":     "failed to update ref",
	"incorrect old value provided": "failed to update ref",
	"reference does not exist":     "failed to update ref",
	"failed to update ref":         "reference already exists",
}

func requireReport(t *testing.T, native bool, got, want []string) {
	t.Helper()
	got = slices.Clone(got)
	for i := range min(len(got), len(want)) {
		ref, reason, _ := strings.Cut(strings.TrimPrefix(want[i], "ng "), " ")
		if alt, ok := nativeReasons[reason]; native && ok && got[i] == "ng "+ref+" "+alt {
			got[i] = want[i]
		}
	}
	if !slices.Equal(got, want) {
		t.Errorf("report = %q, want %q", got, want)
	}
}

// Receive-pack uses v0 commands even when a client requests v2.
func TestReceivePackGitHooks(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		forEachProtocol(t, []string{"", "version=1", "version=2"}, func(t *testing.T, proto string) {
			testReceivePackHooks(t, native, proto)
		})
	})
}

func testReceivePackHooks(t *testing.T, native bool, proto string) {
	ctx := t.Context()
	root := t.TempDir()
	bare, work := buildParityUpstream(t, root)
	repo, err := Open(osfs.Default, bare)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	requireGitMode(t, repo, native)
	refs := gitLocalRefs(t, bare)
	main, v1, v2 := refs["refs/heads/main"], refs["refs/tags/v1"], refs["refs/tags/v2"]

	// A real incremental push: a commit the server lacks plus the pack carrying it.
	blob := make([]byte, 200<<10)
	_, _ = rand.Read(blob)
	commitFile(t, work, "blob.bin", string(blob), "pushed")
	pushed := strings.TrimSpace(gitOut(t, work, "rev-parse", "HEAD"))
	packCmd := exec.CommandContext(ctx, "git", "-C", work, "pack-objects", "--stdout", "--revs", "-q")
	packCmd.Stdin = strings.NewReader(pushed + "\n^" + main + "\n")
	pack, err := packCmd.Output()
	if err != nil {
		t.Fatalf("pack-objects: %v", err)
	}

	var pre, post [][]receive.RefUpdate
	var deny error
	hooks := ReceivePackHooks{
		PreReceive: func(_ context.Context, updates []receive.RefUpdate) error {
			pre = append(pre, updates)
			return deny
		},
		PostReceive: func(_ context.Context, updates []receive.RefUpdate) {
			post = append(post, updates)
		},
	}
	// request encodes commands (the first carrying caps) followed by pack.
	request := func(caps string, pack []byte, cmds ...string) []byte {
		lines := make([]string, 0, len(cmds)+1)
		for i, cmd := range cmds {
			if i == 0 {
				cmd += "\x00" + caps
			}
			lines = append(lines, cmd+"\n")
		}
		return append(pktLines(t, append(lines, "")...), pack...)
	}
	reset := func(denyWith error) {
		pre, post, deny = nil, nil, denyWith
	}
	denied := errors.New("policy says no")
	update := main + " " + pushed + " refs/heads/main"

	for _, tc := range []struct {
		name     string
		caps     string
		sideband bool
		cmds     []string
	}{
		{"DeniedSideband64k", "report-status side-band-64k", true, []string{update}},
		{"DeniedPlain", "report-status", false, []string{update}},
		{"DeniedReportStatusV2", "report-status-v2 side-band-64k", true, []string{update}},
		{"DeniedMultipleCommands", "report-status", false, []string{update, v1 + " " + receive.ZeroHash + " refs/tags/v1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reset(denied)
			before := gitLocalRefs(t, bare)
			body := request(tc.caps, pack, tc.cmds...)
			in := &countingReader{Reader: bytes.NewReader(body)}
			var out bytes.Buffer
			if err := repo.Stateless(ctx, &out, in, GitReceivePack, proto, hooks); !errors.Is(err, denied) {
				t.Fatalf("Stateless = %v, want the hook error", err)
			}
			if in.n != len(body) {
				t.Fatalf("request body not drained: %d of %d bytes read", in.n, len(body))
			}
			var report io.Reader = &out
			if tc.sideband {
				report = sideband.NewDemuxer(sideband.Sideband64k, &out)
			}
			var rs packp.ReportStatus
			if err := rs.Decode(report); err != nil {
				t.Fatalf("decode report-status: %v", err)
			}
			want := make(map[string]string, len(tc.cmds))
			for _, cmd := range tc.cmds {
				want[strings.Fields(cmd)[2]] = denied.Error()
			}
			got := make(map[string]string, len(rs.CommandStatuses))
			for _, cs := range rs.CommandStatuses {
				got[cs.ReferenceName.String()] = cs.Status
			}
			if rs.UnpackStatus != "ok" || len(rs.CommandStatuses) != len(want) || !maps.Equal(got, want) {
				t.Fatalf("report = unpack %q, %v; want every command refused with %q", rs.UnpackStatus, got, denied)
			}
			if tc.sideband {
				if _, err := report.Read(make([]byte, 1)); err != io.EOF {
					t.Fatalf("sideband stream should end with a flush, got %v", err)
				}
			}
			if out.Len() != 0 {
				t.Fatalf("unexpected trailing output %q", out.Bytes())
			}
			if len(pre) != 1 || strings.Join(updateStrings(pre[0]), "|") != strings.Join(tc.cmds, "|") || len(post) != 0 {
				t.Fatalf("pre-receive updates = %v, post calls = %d; want the denied updates and no post call", pre, len(post))
			}
			requireSameRefs(t, "refs after denial", gitLocalRefs(t, bare), before)
		})
	}

	t.Run("Malformed", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			body []byte
		}{
			{"NotACommand", pktLines(t, "not a command\x00report-status\n", "")},
			{"MissingFlush", pktLines(t, update+"\x00report-status\n")},
			{"TruncatedLine", pktLines(t, update+"\x00report-status\n")[:20]},
			{"BadLength", []byte("zzzz")},
		} {
			t.Run(tc.name, func(t *testing.T) {
				reset(nil)
				before := gitLocalRefs(t, bare)
				var out bytes.Buffer
				if err := repo.Stateless(ctx, &out, bytes.NewReader(tc.body), GitReceivePack, proto, hooks); err == nil {
					t.Fatalf("malformed commands accepted, output %q", out.Bytes())
				}
				if len(pre) != 0 || len(post) != 0 {
					t.Fatalf("hooks ran on malformed input: pre %v post %v", pre, post)
				}
				requireSameRefs(t, "refs after malformed input", gitLocalRefs(t, bare), before)
			})
		}
	})

	t.Run("FlushOnly", func(t *testing.T) {
		reset(nil)
		var out bytes.Buffer
		if err := repo.Stateless(ctx, &out, strings.NewReader("0000"), GitReceivePack, proto, hooks); err != nil {
			t.Fatalf("Stateless: %v", err)
		}
		if len(pre) != 0 || len(post) != 0 || out.Len() != 0 {
			t.Fatalf("flush-only request: pre %v post %v output %q", pre, post, out.Bytes())
		}
	})

	// restore puts the fixture refs back so the mutating rows below do not contaminate each other.
	restore := func(t *testing.T) {
		t.Helper()
		for name := range gitLocalRefs(t, bare) {
			if _, ok := refs[name]; !ok {
				runGit(t, bare, "update-ref", "-d", name)
			}
		}
		for name, hash := range refs {
			runGit(t, bare, "update-ref", name, hash)
		}
	}
	// applied returns the fixture refs after the given commands took effect.
	applied := func(cmds ...string) map[string]string {
		want := maps.Clone(refs)
		for _, cmd := range cmds {
			f := strings.Fields(cmd)
			if f[1] == receive.ZeroHash {
				delete(want, f[2])
			} else {
				want[f[2]] = f[1]
			}
		}
		return want
	}
	requireHooks := func(t *testing.T, wantPre, wantPost []string) {
		t.Helper()
		if len(pre) != 1 || !slices.Equal(updateStrings(pre[0]), wantPre) {
			t.Errorf("pre-receive calls = %v, want one with %v", pre, wantPre)
		}
		switch {
		case len(wantPost) == 0 && len(post) != 0:
			t.Errorf("post-receive calls = %v, want none", post)
		case len(wantPost) != 0 && (len(post) != 1 || !slices.Equal(updateStrings(post[0]), wantPost)):
			t.Errorf("post-receive calls = %v, want one with %v", post, wantPost)
		}
	}
	topic := refs["refs/heads/topic/nested"]
	create := receive.ZeroHash + " " + pushed + " refs/heads/new"
	delV1 := v1 + " " + receive.ZeroHash + " refs/tags/v1"
	delV2 := v2 + " " + receive.ZeroHash + " refs/tags/v2"
	delMain := main + " " + receive.ZeroHash + " refs/heads/main"
	delTopic := topic + " " + receive.ZeroHash + " refs/heads/topic/nested"
	staleDelV1 := v2 + " " + receive.ZeroHash + " refs/tags/v1"

	for _, tc := range []struct {
		name     string
		config   string
		rejected string
		sibling  string
		reason   string
	}{
		{"CreateExisting", "", receive.ZeroHash + " " + pushed + " refs/heads/main", delV1, "reference already exists"},
		// Someone pushed since the advertisement: the old value no longer matches.
		{"StaleOldValue", "", v1 + " " + pushed + " refs/heads/main", delV2, "incorrect old value provided"},
		{"StaleDelete", "", staleDelV1, update, "incorrect old value provided"},
		{"UpdateMissing", "", v1 + " " + pushed + " refs/heads/gone", delV1, "reference does not exist"},
		{"MissingObject", "", receive.ZeroHash + " " + strings.Repeat("1", 40) + " refs/heads/bogus", delV2, "missing necessary objects"},
		{"FunnyRefname", "", receive.ZeroHash + " " + pushed + " refs/heads/a..b", delV1, "funny refname"},
		// receive.denyDeletes protects branches only; the tag delete beside it applies.
		{"DenyDeletesBranch", "receive.denyDeletes=true", delTopic, delV1, "deletion prohibited"},
		{"DenyDeletesNumeric", "receive.denyDeletes=2", delTopic, delV1, "deletion prohibited"},
		{"DenyDeletesNumericSuffix", "receive.denyDeletes=1k", delTopic, delV1, "deletion prohibited"},
		{"DenyDeletesNegativeSuffix", "receive.denyDeletes=-1M", delTopic, delV1, "deletion prohibited"},
		{"DenyDeletesZeroSuffix", "receive.denyDeletes=0G", staleDelV1, delTopic, "incorrect old value provided"},
		{"DenyDeletesImplicit", "receive.denyDeletes", delTopic, delV1, "deletion prohibited"},
		{"DenyDeletesOff", "receive.denyDeletes=off", staleDelV1, delTopic, "incorrect old value provided"},
		{"DeleteCurrentBranch", "", delMain, delV2, "deletion of the current branch prohibited"},
		{"DeleteCurrentBranchIgnored", "receive.denyDeleteCurrent=ignore", staleDelV1, delMain, "incorrect old value provided"},
		{"DeleteCurrentBranchWarn", "receive.denyDeleteCurrent=warn", staleDelV1, delMain, "incorrect old value provided"},
		{"DeleteCurrentBranchEmpty", "receive.denyDeleteCurrent=", staleDelV1, delMain, "incorrect old value provided"},
	} {
		t.Run("PartialUpdate"+tc.name, func(t *testing.T) {
			restore(t)
			reset(nil)
			if tc.config != "" {
				key, value, ok := strings.Cut(tc.config, "=")
				if ok {
					runGit(t, bare, "config", key, value)
				} else {
					appendConfig(t, bare, "[receive]\n\t"+strings.TrimPrefix(key, "receive.")+"\n")
				}
				defer runGit(t, bare, "config", "--unset", key)
			}
			var out bytes.Buffer
			if err := repo.Stateless(ctx, &out, bytes.NewReader(request("report-status", pack, tc.rejected, tc.sibling)), GitReceivePack, proto, hooks); err != nil {
				t.Errorf("Stateless = %v, want nil: rejected commands are reported in-protocol", err)
			}
			rejectedRef, siblingRef := strings.Fields(tc.rejected)[2], strings.Fields(tc.sibling)[2]
			requireReport(t, native, reportLines(t, &out, false), []string{"unpack ok", "ng " + rejectedRef + " " + tc.reason, "ok " + siblingRef})
			requireHooks(t, []string{tc.rejected, tc.sibling}, []string{tc.sibling})
			requireSameRefs(t, "refs after partial push", gitLocalRefs(t, bare), applied(tc.sibling))
		})
	}

	t.Run("LockedRef", func(t *testing.T) {
		restore(t)
		reset(nil)
		lock := filepath.Join(bare, "refs/heads/main.lock")
		if err := os.WriteFile(lock, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = os.Remove(lock) }()
		var out bytes.Buffer
		if err := repo.Stateless(ctx, &out, bytes.NewReader(request("report-status", pack, update, delV1)), GitReceivePack, proto, hooks); err != nil {
			t.Errorf("Stateless = %v, want nil", err)
		}
		requireReport(t, native, reportLines(t, &out, false), []string{"unpack ok", "ng refs/heads/main failed to update ref", "ok refs/tags/v1"})
		requireHooks(t, []string{update, delV1}, []string{delV1})
		requireSameRefs(t, "refs after locked push", gitLocalRefs(t, bare), applied(delV1))
		if _, err := os.Stat(lock); err != nil {
			t.Errorf("foreign lock: %v, want it left in place", err)
		}
	})

	t.Run("InvalidBoolConfig", func(t *testing.T) {
		restore(t)
		reset(nil)
		runGit(t, bare, "config", "receive.denyDeletes", "maybe")
		defer runGit(t, bare, "config", "--unset", "receive.denyDeletes")
		var out bytes.Buffer
		if err := repo.Stateless(ctx, &out, bytes.NewReader(request("report-status", pack, update)), GitReceivePack, proto, hooks); err == nil || !strings.Contains(err.Error(), "maybe") {
			t.Errorf("Stateless = %v, want the config error naming the value", err)
		}
		if len(post) != 0 {
			t.Errorf("post-receive calls = %v, want none", post)
		}
		requireSameRefs(t, "refs after invalid config", gitLocalRefs(t, bare), refs)
	})

	for _, tc := range []struct {
		name     string
		caps     string
		deny     error
		cmds     []string
		wantPost []string
		wantOut  string
	}{
		{"Accepted", "", nil, []string{create, update, delV1}, []string{create, update, delV1}, ""},
		{"AcceptedSideband", "side-band-64k no-progress", nil, []string{create, update, delV1}, []string{create, update, delV1}, "0000"},
		{"Denied", "", denied, []string{update}, nil, ""},
		{"DeniedSideband", "side-band-64k", denied, []string{update}, nil, "0000"},
		{"Rejected", "", nil, []string{v1 + " " + pushed + " refs/heads/main"}, nil, ""},
	} {
		t.Run("NoReport"+tc.name, func(t *testing.T) {
			restore(t)
			reset(tc.deny)
			body := request(tc.caps, pack, tc.cmds...)
			in := &countingReader{Reader: bytes.NewReader(body)}
			var out bytes.Buffer
			if err := repo.Stateless(ctx, &out, in, GitReceivePack, proto, hooks); !errors.Is(err, tc.deny) {
				t.Errorf("Stateless = %v, want %v", err, tc.deny)
			}
			if in.n != len(body) || out.String() != tc.wantOut {
				t.Errorf("drained %d of %d bytes, output %q; want the body drained and output %q", in.n, len(body), out.Bytes(), tc.wantOut)
			}
			requireHooks(t, tc.cmds, tc.wantPost)
			requireSameRefs(t, "refs after no-report push", gitLocalRefs(t, bare), applied(tc.wantPost...))
		})
	}

	t.Run("NoReportCancelled", func(t *testing.T) {
		restore(t)
		reset(nil)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		stalled := &stalledReader{ctx: ctx, reading: make(chan struct{}, 1)}
		done := make(chan error, 1)
		go func() {
			done <- repo.Stateless(ctx, io.Discard, io.MultiReader(bytes.NewReader(request("", nil, update)), stalled), GitReceivePack, proto, hooks)
		}()
		select {
		case <-stalled.reading:
		case <-time.After(10 * time.Second):
			t.Fatal("the pack was never requested")
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Stateless = %v, want context.Canceled", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Stateless did not return after cancellation")
		}
		if len(post) != 0 {
			t.Errorf("post-receive calls = %v, want none", post)
		}
		requireSameRefs(t, "refs after cancelled push", gitLocalRefs(t, bare), refs)
	})

	// no-progress suppresses band 2, not sideband framing.
	for _, tc := range []struct {
		name string
		caps string
		mux  bool
	}{
		{"Plain", "report-status", false},
		{"SidebandNoProgress", "report-status side-band-64k no-progress", true},
	} {
		t.Run("Applied"+tc.name, func(t *testing.T) {
			restore(t)
			reset(nil)
			cmds := []string{create, update, delV1}
			var out bytes.Buffer
			if err := repo.Stateless(ctx, &out, bytes.NewReader(request(tc.caps, pack, cmds...)), GitReceivePack, proto, hooks); err != nil {
				t.Fatalf("Stateless: %v", err)
			}
			if got, want := reportLines(t, &out, tc.mux), []string{"unpack ok", "ok refs/heads/new", "ok refs/heads/main", "ok refs/tags/v1"}; !slices.Equal(got, want) {
				t.Errorf("report = %q, want %q", got, want)
			}
			requireHooks(t, cmds, cmds)
			requireSameRefs(t, "refs after push", gitLocalRefs(t, bare), applied(cmds...))
		})
	}

	t.Run("ValidRefNames", func(t *testing.T) {
		restore(t)
		reset(nil)
		names := []string{"refs/heads/-topic", "refs/tags/-release", "refs/heads/topic/@/nested", "refs/tags/@"}
		cmds := make([]string, 0, len(names))
		want := []string{"unpack ok"}
		for _, name := range names {
			cmds = append(cmds, receive.ZeroHash+" "+pushed+" "+name)
			want = append(want, "ok "+name)
		}
		var out bytes.Buffer
		if err := repo.Stateless(ctx, &out, bytes.NewReader(request("report-status", pack, cmds...)), GitReceivePack, proto, hooks); err != nil {
			t.Fatalf("Stateless: %v", err)
		}
		if got := reportLines(t, &out, false); !slices.Equal(got, want) {
			t.Errorf("report = %q, want %q", got, want)
		}
		requireHooks(t, cmds, cmds)
		requireSameRefs(t, "refs after valid names", gitLocalRefs(t, bare), applied(cmds...))
	})

	// Git reports malformed packs in-protocol instead of failing the service.
	for _, tc := range []struct {
		name string
		caps string
		mux  bool
		pack []byte
	}{
		{"Truncated", "report-status", false, pack[:len(pack)-40]},
		{"Garbage", "report-status side-band-64k", true, []byte("not a pack at all")},
		{"GarbageNoReport", "", false, []byte("not a pack at all")},
		{"GarbageNoReportSideband", "side-band-64k no-progress", true, []byte("not a pack at all")},
	} {
		t.Run("BadPack"+tc.name, func(t *testing.T) {
			restore(t)
			reset(nil)
			var out bytes.Buffer
			if err := repo.Stateless(ctx, &out, bytes.NewReader(request(tc.caps, tc.pack, update)), GitReceivePack, proto, hooks); err != nil {
				t.Errorf("Stateless = %v, want nil: the unpack failure is reported in-protocol", err)
			}
			if strings.Contains(tc.caps, "report-status") {
				if got := reportLines(t, &out, tc.mux); len(got) != 2 || got[0] == "unpack ok" || got[1] != "ng refs/heads/main unpacker error" {
					t.Errorf("report = %q, want an unpack error and main failed as unpacker error", got)
				}
			} else if want := map[bool]string{true: "0000"}[tc.mux]; out.String() != want {
				t.Errorf("output = %q, want %q without report-status", out.Bytes(), want)
			}
			// Native git sees the commands before the pack, so only there PreReceive already ran.
			wantPre := 0
			if native {
				wantPre = 1
			}
			if len(pre) != wantPre || len(post) != 0 {
				t.Errorf("pre-receive calls = %v, post-receive calls = %v; want %d and none", pre, post, wantPre)
			}
			requireSameRefs(t, "refs after bad pack", gitLocalRefs(t, bare), refs)
		})
	}
}

func TestGitBool(t *testing.T) {
	for _, value := range []string{
		"", "true", "off", "2", "0x10", "010", "08", "0o10", "1_0",
		"1k", "-1M", "0G", "-2G", "2G", "2097151k", "2097152k",
		"2147483647", "2147483648", "-2147483648", "-2147483649", "1kk", "maybe",
	} {
		t.Run(value, func(t *testing.T) {
			command := exec.CommandContext(t.Context(), "git", "-c", "hfd.parity="+value, "config", "--bool", "hfd.parity")
			command.Env = gitEnv("", nil)
			output, nativeErr := command.Output()
			got, err := gitBool(value, false)
			if nativeErr != nil {
				if err == nil {
					t.Fatalf("gitBool(%q) = %t, nil; git rejected it", value, got)
				}
				return
			}
			want := strings.TrimSpace(string(output)) == "true"
			if err != nil || got != want {
				t.Fatalf("gitBool(%q) = %t, %v; git returned %q", value, got, err, output)
			}
		})
	}
	// Native Git's binary-prefix acceptance varies with its C library.
	t.Run("BinaryPrefix", func(t *testing.T) {
		if got, err := gitBool("0b10", false); err == nil {
			t.Fatalf("gitBool(binary prefix) = %t, nil; want rejection", got)
		}
	})
	if got, err := gitBool("", true); err != nil || !got {
		t.Fatalf("bare config key = %t, %v; want true", got, err)
	}
}

func TestReceivePackGitDenialErrors(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	setGitBinary(t, gitPath)
	root := t.TempDir()
	bare, _ := buildParityUpstream(t, root)
	repo, err := Open(osfs.Default, bare)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	refs := gitLocalRefs(t, bare)
	denied := errors.New("policy says no")
	var post int
	hooks := ReceivePackHooks{
		PreReceive:  func(context.Context, []receive.RefUpdate) error { return denied },
		PostReceive: func(context.Context, []receive.RefUpdate) { post++ },
	}
	update := refs["refs/heads/main"] + " " + strings.Repeat("1", 40) + " refs/heads/main\x00"
	// The pack never reaches git, so any bytes stand in for it.
	body := func(caps string) []byte {
		return append(pktLines(t, update+caps+"\n", ""), bytes.Repeat([]byte("not a pack\n"), 1000)...)
	}
	check := func(t *testing.T, err error, want ...error) {
		t.Helper()
		for _, w := range want {
			if !errors.Is(err, w) {
				t.Errorf("Stateless = %v, want it to report %v", err, w)
			}
		}
		requireSameRefs(t, "refs after denial", gitLocalRefs(t, bare), refs)
		if post != 0 {
			t.Errorf("post-receive ran %d times", post)
		}
	}

	t.Run("NoReport", func(t *testing.T) {
		req := body("")
		in := &countingReader{Reader: bytes.NewReader(req)}
		var out bytes.Buffer
		check(t, repo.Stateless(t.Context(), &out, in, GitReceivePack, "", hooks), denied)
		if in.n != len(req) || out.Len() != 0 {
			t.Errorf("drained %d of %d bytes, output %q; want the body drained and no report", in.n, len(req), out.Bytes())
		}
	})

	t.Run("FailingOutput", func(t *testing.T) {
		pr, pw := io.Pipe()
		_ = pr.Close()
		check(t, repo.Stateless(t.Context(), pw, bytes.NewReader(body("report-status")), GitReceivePack, "", hooks), denied, io.ErrClosedPipe)
	})

	t.Run("CancelDuringReport", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		reportReader, reportWriter := io.Pipe()
		defer func() { _ = reportReader.Close() }()
		defer func() { _ = reportWriter.Close() }()
		readDone := make(chan error, 1)
		go func() {
			var firstByte [1]byte
			_, err := reportReader.Read(firstByte[:])
			cancel()
			_ = reportReader.Close()
			readDone <- err
		}()
		check(t, repo.Stateless(ctx, reportWriter, bytes.NewReader(body("report-status")), GitReceivePack, "", hooks), denied, io.ErrClosedPipe, context.Canceled)
		select {
		case err := <-readDone:
			if err != nil {
				t.Fatalf("read rejection report: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("rejection report was not received")
		}
	})

	t.Run("CancelDuringDrain", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		inR, inW := io.Pipe()
		defer func() { _ = inW.Close() }()
		var out bytes.Buffer
		done := make(chan error, 1)
		go func() { done <- repo.Stateless(ctx, &out, inR, GitReceivePack, "", hooks) }()
		// Write returns once the drain consumed the body; the drain's next read then waits for the cancellation.
		if _, err := inW.Write(body("report-status")); err != nil {
			t.Fatal(err)
		}
		cancel()
		select {
		case err := <-done:
			check(t, err, denied, context.Canceled)
			var rs packp.ReportStatus
			if err := rs.Decode(&out); err != nil || len(rs.CommandStatuses) != 1 || rs.CommandStatuses[0].Status != denied.Error() {
				t.Errorf("report = %v, %v; want the denial reported before draining", rs.CommandStatuses, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Stateless did not return after cancellation")
		}
	})
}

// lockRacer stands in for a git process that moves the reference just before receive-pack takes its <ref>.lock.
type lockRacer struct {
	billy.Filesystem
	race  func()
	races int
}

func (fs *lockRacer) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	if flag&os.O_EXCL != 0 && strings.HasSuffix(name, ".lock") && fs.races == 0 {
		fs.races++
		fs.race()
	}
	return fs.Filesystem.OpenFile(name, flag, perm)
}

// Another writer moves main, loose or packed, right before go-git locks it: the refusal must leave that value and no stray files behind.
func TestReceivePackGoGitRefRace(t *testing.T) {
	for _, packed := range []bool{false, true} {
		t.Run(fmt.Sprintf("packed=%t", packed), func(t *testing.T) {
			bare, _ := buildParityUpstream(t, t.TempDir())
			refs := gitLocalRefs(t, bare)
			main, topic, v1 := refs["refs/heads/main"], refs["refs/heads/topic/nested"], refs["refs/tags/v1"]
			// The objects are already present, so an empty pack carries the update.
			pack, err := exec.CommandContext(t.Context(), "git", "-C", bare, "pack-objects", "--stdout").Output()
			if err != nil {
				t.Fatalf("pack-objects: %v", err)
			}
			racer := &lockRacer{Filesystem: osfs.Default, race: func() {
				runGit(t, bare, "update-ref", "refs/heads/main", v1)
				if packed {
					runGit(t, bare, "pack-refs", "--all", "--prune")
				}
			}}
			if packed {
				runGit(t, bare, "pack-refs", "--all", "--prune")
			}
			// The wrapped filesystem is not OS-backed to Open, so go-git serves regardless of the binary.
			repo, err := Open(racer, bare)
			if err != nil {
				t.Fatalf("open repository: %v", err)
			}
			requireGitMode(t, repo, false)

			var post int
			hooks := ReceivePackHooks{PostReceive: func(context.Context, []receive.RefUpdate) { post++ }}
			body := append(pktLines(t, main+" "+topic+" refs/heads/main\x00report-status\n", ""), pack...)
			var out bytes.Buffer
			if err := repo.Stateless(t.Context(), &out, bytes.NewReader(body), GitReceivePack, "", hooks); err != nil {
				t.Fatalf("Stateless: %v", err)
			}
			if got, want := reportLines(t, &out, false), []string{"unpack ok", "ng refs/heads/main incorrect old value provided"}; !slices.Equal(got, want) {
				t.Errorf("report = %q, want %q", got, want)
			}
			if racer.races != 1 || post != 0 {
				t.Errorf("races = %d, post-receive calls = %d; want the race injected once and no post call", racer.races, post)
			}
			stray := []string{"refs/heads/main.lock"}
			if packed {
				stray = append(stray, "refs/heads/main")
			}
			for _, name := range stray {
				if _, err := os.Stat(filepath.Join(bare, name)); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("%s: stat = %v, want no such file after the refusal", name, err)
				}
			}
			refs["refs/heads/main"] = v1
			requireSameRefs(t, "refs after refused race", gitLocalRefs(t, bare), refs)
			gitFsck(t, bare)
		})
	}
}

func TestReadReceiveCommands(t *testing.T) {
	one, two := strings.Repeat("1", 40), strings.Repeat("2", 40)
	first := receive.ZeroHash + " " + one + " refs/heads/main\x00report-status side-band-64k push-options\n"
	second := one + " " + two + " refs/tags/v1\n"
	shallow := "shallow " + two + "\n"
	long := receive.ZeroHash + " " + one + " refs/heads/" + strings.Repeat("x", 5000) + "\x00report-status\n"
	rest := append(pktLines(t, "opt=1\n", ""), "PACK\x00\x00\x00\x02rest"...)
	for _, tc := range []struct {
		name           string
		hdr            []byte
		cmds, shallows int
	}{
		{"Commands", pktLines(t, first, second, ""), 2, 0},
		{"Shallow", pktLines(t, shallow, first, ""), 1, 1},
		{"ShallowOnly", pktLines(t, shallow, ""), 0, 1},
		{"FlushOnly", []byte("0000"), 0, 0},
		{"LongFirstLine", pktLines(t, long, ""), 1, 0},
	} {
		for _, fragmented := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fragmented=%t", tc.name, fragmented), func(t *testing.T) {
				var in io.Reader = bytes.NewReader(slices.Concat(tc.hdr, rest))
				if fragmented {
					in = iotest.OneByteReader(in)
				}
				rd := bufio.NewReader(in)
				req, hdr, err := readReceiveCommands(rd)
				if err != nil {
					t.Fatalf("readReceiveCommands: %v", err)
				}
				if len(req.Commands) != tc.cmds || len(req.Shallows) != tc.shallows {
					t.Errorf("commands = %d, shallows = %d; want %d and %d", len(req.Commands), len(req.Shallows), tc.cmds, tc.shallows)
				}
				if !bytes.Equal(hdr, tc.hdr) {
					t.Errorf("header = %q, want %q", hdr, tc.hdr)
				}
				if unread, _ := io.ReadAll(rd); !bytes.Equal(unread, rest) {
					t.Errorf("unread = %q, want %q", unread, rest)
				}
			})
		}
	}

	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"Empty", nil},
		{"ShortLength", []byte("00")},
		{"BadLength", []byte("zzzz")},
		{"ReservedLength", []byte("0003")},
		{"TruncatedLine", pktLines(t, first)[:20]},
		{"MissingFlush", pktLines(t, first, second)},
		{"NotACommand", pktLines(t, "not a command\x00report-status\n", "")},
		{"BadShallow", pktLines(t, "shallow nothex\n", first, "")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := readReceiveCommands(bufio.NewReader(bytes.NewReader(tc.body))); err == nil {
				t.Error("malformed commands accepted")
			}
		})
	}

	t.Run("CommandLimit", func(t *testing.T) {
		line := pktLines(t, one+" "+two+" refs/heads/"+strings.Repeat("y", 65000)+"\n")
		in := &countingReader{Reader: io.MultiReader(bytes.NewReader(pktLines(t, first)), bytes.NewReader(bytes.Repeat(line, maxReceiveCommandsSize/len(line)+2)))}
		rd := bufio.NewReader(in)
		if _, _, err := readReceiveCommands(rd); err == nil {
			t.Fatal("command list beyond the cap accepted")
		}
		if in.n > maxReceiveCommandsSize+rd.Size() {
			t.Errorf("read %d bytes past the %d cap", in.n-maxReceiveCommandsSize, maxReceiveCommandsSize)
		}
	})
}

func TestLocalDir(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	osRoot, err := os.OpenRoot(root)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	t.Cleanup(func() { _ = osRoot.Close() })
	rootFS, err := osfs.FromRoot(osRoot)
	if err != nil {
		t.Fatalf("FromRoot: %v", err)
	}
	rootChroot, err := rootFS.Chroot("repositories")
	if err != nil {
		t.Fatalf("RootOS chroot: %v", err)
	}
	boundChroot, err := osfs.New(root).Chroot("/repositories")
	if err != nil {
		t.Fatalf("BoundOS chroot: %v", err)
	}

	positive := []struct {
		name string
		fs   billy.Filesystem
		path string
		want string
	}{
		{"Default", osfs.Default, filepath.Join(root, "default.git"), filepath.Join(root, "default.git")},
		{"BoundRootedName", osfs.New(root), "/org/repo.git", filepath.Join(root, "org/repo.git")},
		{"BoundRelativeName", osfs.New(root), "rel.git", filepath.Join(root, "rel.git")},
		{"BoundAbsoluteInsideBase", osfs.New(root), filepath.Join(root, "inside.git"), filepath.Join(root, "inside.git")},
		{"BoundAbsoluteOutsideBase", osfs.New(root), "/outside/repo.git", filepath.Join(root, "outside/repo.git")},
		{"BoundChroot", boundChroot, "/Qwen/Qwen2.git", filepath.Join(root, "repositories/Qwen/Qwen2.git")},
		{"NestedChrootHelper", chroot.New(chroot.New(osfs.New(root), "/repositories"), "/org"), "/nested.git", filepath.Join(root, "repositories/org/nested.git")},
		{"RootOS", rootFS, "/rootos.git", filepath.Join(root, "rootos.git")},
		{"RootOSChroot", rootChroot, "/chrooted.git", filepath.Join(root, "repositories/chrooted.git")},
	}
	for _, tc := range positive {
		t.Run(tc.name, func(t *testing.T) {
			repo, err := Init(ctx, tc.fs, tc.path, "main")
			if err != nil {
				t.Fatalf("init: %v", err)
			}
			if repo.localDir != tc.want {
				t.Fatalf("localDir = %q, want %q", repo.localDir, tc.want)
			}
			if _, err := os.Stat(filepath.Join(repo.localDir, "HEAD")); err != nil {
				t.Fatalf("repository not at host path: %v", err)
			}
		})
	}

	t.Run("RelativeRoot", func(t *testing.T) {
		t.Chdir(root)
		repo, err := Init(ctx, osfs.New("./data"), "/relative.git", "main")
		if err != nil {
			t.Fatalf("init: %v", err)
		}
		if want := filepath.Join(root, "data/relative.git"); repo.localDir != want {
			t.Fatalf("localDir = %q, want %q", repo.localDir, want)
		}
		if _, err := os.Stat(filepath.Join(repo.localDir, "HEAD")); err != nil {
			t.Fatalf("repository not at host path: %v", err)
		}
	})

	negative := []struct {
		name string
		fs   billy.Basic
		path string
	}{
		{"Memfs", memfs.New(), "/repo.git"},
		{"ChrootOverMemfs", chroot.New(memfs.New(), "/repositories"), "/repo.git"},
		{"EmbeddingWrapper", struct{ billy.Filesystem }{osfs.New(root)}, "/repo.git"},
		{"BoundEscape", osfs.New(root), "/../escape.git"},
		{"BoundRelativeEscape", osfs.New(root), "../escape.git"},
		{"ChrootEscape", chroot.New(osfs.New(root), "/repositories"), "/../escape.git"},
	}
	for _, tc := range negative {
		t.Run(tc.name, func(t *testing.T) {
			if got := localDir(tc.fs, tc.path); got != "" {
				t.Fatalf("localDir = %q, want go-git fallback", got)
			}
		})
	}
}

// advertisedRefs parses v0 "<hash> <name>[\0caps]" lines up to the closing flush.
func advertisedRefs(t *testing.T, lines []string) map[string]string {
	t.Helper()
	refs := map[string]string{}
	for _, line := range lines {
		if line == "" {
			break
		}
		line, _, _ = strings.Cut(line, "\x00")
		hash, name, ok := strings.Cut(line, " ")
		if !ok {
			t.Fatalf("malformed ref line %q", line)
		}
		refs[name] = hash
	}
	return refs
}

func TestAdvertiseRefsProtocols(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	bare, _ := buildParityUpstream(t, root)
	repo, err := Open(osfs.Default, bare)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	want := gitLocalRefs(t, bare)

	cases := []struct {
		service, proto string
		header         bool
		version        string
		v2             bool
	}{
		{GitUploadPack, "", true, "", false},
		{GitUploadPack, "version=1", true, "version 1", false},
		{GitUploadPack, "version=2", false, "version 2", true},
		{GitReceivePack, "", true, "", false},
		{GitReceivePack, "version=1", true, "version 1", false},
		// http-backend drops the service line for any v2 request; receive-pack still answers v0.
		{GitReceivePack, "version=2", false, "", false},
	}
	forEachGitMode(t, func(t *testing.T, native bool) {
		for _, tc := range cases {
			t.Run(tc.service+"/"+tc.proto, func(t *testing.T) {
				var out bytes.Buffer
				if err := repo.AdvertiseRefs(ctx, &out, tc.service, tc.proto); err != nil {
					t.Fatalf("AdvertiseRefs: %v", err)
				}
				wantAgent := "go-git/"
				if native {
					wantAgent = "git/"
				}
				if agent := advertisedAgent(out.Bytes()); !strings.HasPrefix(agent, wantAgent) {
					t.Fatalf("agent = %q, want prefix %q", agent, wantAgent)
				}
				lines := readPktLines(t, out.Bytes())
				if tc.header {
					if len(lines) < 2 || lines[0] != "# service="+tc.service || lines[1] != "" {
						t.Fatalf("missing service line, got %q", lines[:min(2, len(lines))])
					}
					lines = lines[2:]
				} else if len(lines) > 0 && strings.HasPrefix(lines[0], "# service=") {
					t.Fatalf("unexpected service line for %s %s", tc.service, tc.proto)
				}
				if tc.version != "" {
					if len(lines) == 0 || lines[0] != tc.version {
						t.Fatalf("version line = %q, want %q", lines[:min(1, len(lines))], tc.version)
					}
					lines = lines[1:]
				}
				if len(lines) == 0 || lines[len(lines)-1] != "" {
					t.Fatalf("advertisement must end with flush, got %q", lines)
				}
				if tc.v2 {
					caps := strings.Join(lines, "|")
					if !strings.Contains(caps, "|ls-refs") || !strings.Contains(caps, "|fetch") || strings.Contains(caps, "refs/heads/") {
						t.Fatalf("v2 capability advertisement = %q", lines)
					}
					return
				}
				got := advertisedRefs(t, lines)
				for name, hash := range want {
					if got[name] != hash {
						t.Errorf("%s = %q, want %q", name, got[name], hash)
					}
				}
				if tc.service == GitUploadPack {
					if got["HEAD"] != want["refs/heads/main"] || got["refs/tags/v2^{}"] != want["refs/heads/main"] {
						t.Errorf("HEAD/peeled tag advertisement = %q", got)
					}
				}
			})
		}
	})
}

// TestGitCmdHermetic poisons every inherited git input (GIT_DIR, GIT_CONFIG_*,
// global/system config, askpass, repo-local credential helper) and checks the
// command still serves the addressed repository and never answers or prompts
// for credentials.
func TestGitCmdHermetic(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	setGitBinary(t, gitPath)
	ctx := t.Context()
	root := t.TempDir()
	bare, _ := buildParityUpstream(t, root)
	repo, err := Open(osfs.Default, bare)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	want := gitLocalRefs(t, bare)

	helper := "!f() { echo username=u; echo password=p; }; f"
	other := filepath.Join(root, "other.git")
	runGit(t, "", "init", "--bare", "--initial-branch=other", other)
	runGit(t, other, "config", "credential.helper", helper)
	poison := "[transfer]\n\thideRefs = refs/heads/main\n[credential]\n\thelper = \"" + helper + "\"\n"
	home := filepath.Join(root, "home")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{filepath.Join(home, ".gitconfig"), filepath.Join(root, "poison.gitconfig")} {
		if err := os.WriteFile(name, []byte(poison), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	askpass := filepath.Join(root, "askpass.sh")
	if err := os.WriteFile(askpass, []byte("#!/bin/sh\necho hunter2\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	localConfig := filepath.Join(other, "config")
	configContents, err := os.ReadFile(localConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("GIT_DIR", other)
	t.Setenv("GIT_CONFIG_GLOBAL", filepath.Join(root, "poison.gitconfig"))
	t.Setenv("GIT_CONFIG_SYSTEM", filepath.Join(root, "poison.gitconfig"))
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "transfer.hideRefs")
	t.Setenv("GIT_CONFIG_VALUE_0", "refs/heads/main")
	t.Setenv("GIT_ASKPASS", askpass)

	for _, service := range []string{GitUploadPack, GitReceivePack} {
		var out bytes.Buffer
		if err := repo.AdvertiseRefs(ctx, &out, service, ""); err != nil {
			t.Fatalf("%s: %v", service, err)
		}
		lines := readPktLines(t, out.Bytes())
		if got := advertisedRefs(t, lines[2:]); got["refs/heads/main"] != want["refs/heads/main"] {
			t.Errorf("%s: refs/heads/main = %q, want %q", service, got["refs/heads/main"], want["refs/heads/main"])
		}
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for _, source := range []string{"GIT_ASKPASS", "SSH_ASKPASS", "core.askPass"} {
		t.Run(source, func(t *testing.T) {
			if source == "core.askPass" {
				poisoned := string(configContents) + "\n[core]\n\taskPass = " + askpass + "\n"
				if err := os.WriteFile(localConfig, []byte(poisoned), 0o644); err != nil {
					t.Fatal(err)
				}
			} else {
				t.Setenv(source, askpass)
			}
			cmd := gitCmd(ctx, "", nil, "credential", "fill")
			cmd.Dir = other
			cmd.Stdin = strings.NewReader("protocol=https\nhost=example.com\n\n")
			if err := runGitCmd(cmd); err == nil || !strings.Contains(err.Error(), "terminal prompts disabled") {
				t.Fatalf("credential fill = %v, want refusal without prompting", err)
			}
		})
	}
}

// TestRunGitCmdError checks appended config entries reach git and only the stderr tail is reported.
func TestRunGitCmdError(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	setGitBinary(t, gitPath)
	spew := "alias.spew=!head -c 10000 /dev/zero | tr '\\0' z >&2; exit 3"
	err = runGitCmd(gitCmd(t.Context(), "", []string{spew}, "spew"))
	if err == nil {
		t.Fatal("spew alias should fail")
	}
	msg := err.Error()
	if !strings.HasPrefix(msg, "git spew: exit status 3: ") || strings.Count(msg, "z") != stderrTailSize {
		t.Fatalf("error = %.60q... (len %d), want exit status 3 with %d-byte stderr tail", msg, len(msg), stderrTailSize)
	}
}

func TestGitBinaryS3Fallback(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	backend := s3mem.New()
	const bucket = "git-binary-test"
	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(server.Close)
	client := s3.New(s3.Options{
		Region:                     "us-east-1",
		BaseEndpoint:               aws.String(server.URL),
		UsePathStyle:               true,
		Credentials:                credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	remoteFS := s3fs.New(bucket, s3fs.WithClient(client))
	repositoriesFS, err := remoteFS.Chroot("/repositories")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	bare, _ := buildParityUpstream(t, root)
	upstream := gitLocalRefs(t, bare)
	mirrored := map[string]string{"refs/heads/main": upstream["refs/heads/main"], "refs/tags/v2": upstream["refs/tags/v2"]}
	// A real push through the fallback: the topic commit the mirrored repository lacks, plus the pack carrying it.
	topic := upstream["refs/heads/topic/nested"]
	packCmd := exec.CommandContext(t.Context(), "git", "-C", bare, "pack-objects", "--stdout", "--revs", "-q")
	packCmd.Stdin = strings.NewReader(topic + "\n^" + upstream["refs/heads/main"] + "\n")
	topicPack, err := packCmd.Output()
	if err != nil {
		t.Fatalf("pack-objects: %v", err)
	}
	push := append(pktLines(t, receive.ZeroHash+" "+topic+" refs/heads/topic/nested\x00report-status\n", ""), topicPack...)
	pushed := maps.Clone(mirrored)
	pushed["refs/heads/topic/nested"] = topic
	for _, filesystem := range []struct {
		name   string
		fs     billy.Filesystem
		prefix string
	}{
		{"Direct", remoteFS, "repo.git/"},
		{"RepositoriesChroot", repositoriesFS, "repositories/repo.git/"},
		{"HelperChroot", chroot.New(remoteFS, "/helper"), "helper/repo.git/"},
	} {
		t.Run(filesystem.name, func(t *testing.T) {
			repo, err := Init(t.Context(), filesystem.fs, "/repo.git", "main")
			if err != nil {
				t.Fatal(err)
			}
			for _, binary := range []struct {
				name string
				path string
			}{
				{"InstalledGit", gitPath},
				{"InvalidGit", filepath.Join(t.TempDir(), "missing-git")},
			} {
				t.Run(binary.name, func(t *testing.T) {
					setGitBinary(t, binary.path)
					if dir := repo.gitDir(); dir != "" {
						t.Fatalf("S3 selected native Git directory %q", dir)
					}
					for _, service := range []string{GitUploadPack, GitReceivePack} {
						var output bytes.Buffer
						if err := repo.AdvertiseRefs(t.Context(), &output, service, ""); err != nil {
							t.Fatalf("S3 %s must use go-git: %v", service, err)
						}
						if agent := advertisedAgent(output.Bytes()); !strings.HasPrefix(agent, "go-git/") {
							t.Fatalf("S3 %s agent = %q, want go-git", service, agent)
						}
						if err := repo.Stateless(t.Context(), &output, strings.NewReader("0000"), service, "", ReceivePackHooks{}); err != nil {
							t.Fatalf("S3 %s Stateless must use go-git: %v", service, err)
						}
						output.Reset()
						if err := repo.Serve(t.Context(), flushOnlyRW{strings.NewReader("0000"), &output}, service, "", ReceivePackHooks{}); err != nil {
							t.Fatalf("S3 %s Serve must use go-git: %v", service, err)
						}
						if agent := advertisedAgent(output.Bytes()); !strings.HasPrefix(agent, "go-git/") {
							t.Fatalf("S3 %s Serve agent = %q, want go-git", service, agent)
						}
					}
					if err := repo.PullMirrorRefs(t.Context(), bare, []string{"refs/heads/main", "refs/tags/v2"}, nil); err != nil {
						t.Fatalf("S3 PullMirrorRefs must use go-git: %v", err)
					}
					got, err := repo.Refs()
					if err != nil {
						t.Fatal(err)
					}
					requireSameRefs(t, "S3 pulled refs", got, mirrored)
					objects, err := backend.ListBucket(bucket, &gofakes3.Prefix{HasPrefix: true, Prefix: filesystem.prefix + "objects/"}, gofakes3.ListBucketPage{})
					if err != nil {
						t.Fatal(err)
					}
					if len(objects.Contents) == 0 {
						t.Fatalf("no objects stored under %sobjects/ in the bucket", filesystem.prefix)
					}
					dest := filepath.Join(t.TempDir(), "dest.git")
					runGit(t, "", "init", "--bare", "--initial-branch=main", dest)
					if err := repo.PushMirrorRefs(t.Context(), dest, []string{"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"}, true, nil); err != nil {
						t.Fatalf("S3 PushMirrorRefs must use go-git: %v", err)
					}
					requireSameRefs(t, "S3 pushed refs", gitLocalRefs(t, dest), mirrored)
					gitFsck(t, dest)
					var out bytes.Buffer
					if err := repo.Stateless(t.Context(), &out, bytes.NewReader(push), GitReceivePack, "", ReceivePackHooks{}); err != nil {
						t.Fatalf("S3 receive-pack must use go-git: %v", err)
					}
					if got, want := reportLines(t, &out, false), []string{"unpack ok", "ok refs/heads/topic/nested"}; !slices.Equal(got, want) {
						t.Fatalf("S3 receive-pack report = %q, want %q", got, want)
					}
					if got, err = repo.Refs(); err != nil {
						t.Fatal(err)
					}
					requireSameRefs(t, "S3 refs after receive-pack", got, pushed)
					if _, err := repo.Blob("topic/nested", "file.txt"); err != nil {
						t.Fatalf("pushed commit not readable from S3: %v", err)
					}
				})
			}
		})
	}
}

func TestAdvertiseRefsGitBinarySelection(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	bare, _ := buildParityUpstream(t, root)
	local, err := Open(osfs.Default, bare)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	bogus := filepath.Join(root, "no-such-git")

	t.Run("BogusBinaryFailsOnLocal", func(t *testing.T) {
		setGitBinary(t, bogus)
		var out bytes.Buffer
		if err := local.AdvertiseRefs(ctx, &out, GitUploadPack, ""); err == nil {
			t.Fatalf("advertisement with bogus GitBinary should fail, got %q", out.Bytes())
		}
	})

	t.Run("EmptyBinaryUsesGoGitOnLocal", func(t *testing.T) {
		setGitBinary(t, "")
		var out bytes.Buffer
		if err := local.AdvertiseRefs(ctx, &out, GitUploadPack, ""); err != nil {
			t.Fatalf("AdvertiseRefs: %v", err)
		}
		if agent := advertisedAgent(out.Bytes()); !strings.HasPrefix(agent, "go-git/") {
			t.Fatalf("agent = %q, want go-git", agent)
		}
	})

	t.Run("MemfsIgnoresBinary", func(t *testing.T) {
		setGitBinary(t, bogus)
		repo, err := Init(ctx, memfs.New(), "/mem.git", "main")
		if err != nil {
			t.Fatalf("init memfs repository: %v", err)
		}
		var out bytes.Buffer
		if err := repo.AdvertiseRefs(ctx, &out, GitUploadPack, ""); err != nil {
			t.Fatalf("memfs advertisement must not use GitBinary: %v", err)
		}
	})

	t.Run("EmbeddingWrapperIgnoresBinary", func(t *testing.T) {
		setGitBinary(t, bogus)
		repo, err := Open(struct{ billy.Filesystem }{osfs.New(root)}, "/upstream.git")
		if err != nil {
			t.Fatalf("open wrapped repository: %v", err)
		}
		var out bytes.Buffer
		if err := repo.AdvertiseRefs(ctx, &out, GitUploadPack, ""); err != nil {
			t.Fatalf("unknown wrapper advertisement must not use GitBinary: %v", err)
		}
	})
}

// TestMirrorGitBinarySelection checks PullMirrorRefs and PushMirrorRefs run
// native git for local repositories, keeping URLs off argv, fail without
// fallback on a bogus binary, and stay on go-git off the OS filesystem.
func TestMirrorGitBinarySelection(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	bare, _ := buildParityUpstream(t, root)
	upstream := gitLocalRefs(t, bare)
	refs := []string{"refs/heads/main", "refs/tags/v2"}
	want := map[string]string{"refs/heads/main": upstream["refs/heads/main"], "refs/tags/v2": upstream["refs/tags/v2"]}
	wildcard := []string{"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"}
	bin, log := recordingGit(t)
	bogus := filepath.Join(root, "no-such-git")
	mirror := filepath.Join(root, "mirror.git")
	repo, err := InitMirror(ctx, osfs.Default, mirror, bare)
	if err != nil {
		t.Fatalf("init mirror: %v", err)
	}
	// Repository-stored mappings must not leak into the one-off remote git is given.
	runGit(t, mirror, "remote", "add", "origin", bare)
	runGit(t, mirror, "config", "push.followTags", "true")

	t.Run("RecordingBinaryOnLocal", func(t *testing.T) {
		setGitBinary(t, bin)
		var progress bytes.Buffer
		if err := repo.PullMirrorRefs(ctx, bare, refs, &progress); err != nil {
			t.Fatalf("PullMirrorRefs: %v", err)
		}
		got, err := repo.Refs()
		if err != nil {
			t.Fatal(err)
		}
		requireSameRefs(t, "pulled refs", got, want)
		dest := filepath.Join(root, "dest.git")
		runGit(t, "", "init", "--bare", "--initial-branch=main", dest)
		if err := repo.PushMirrorRefs(ctx, dest, wildcard, true, &progress); err != nil {
			t.Fatalf("PushMirrorRefs: %v", err)
		}
		requireSameRefs(t, "pushed refs", gitLocalRefs(t, dest), want)
		if progress.Len() == 0 {
			t.Error("no progress emitted")
		}
		if head, _ := os.ReadFile(filepath.Join(mirror, "HEAD")); string(head) != "ref: refs/heads/main\n" {
			t.Errorf("HEAD = %q after mirroring", head)
		}
		mainOnly := filepath.Join(root, "main-only.git")
		runGit(t, "", "init", "--bare", "--initial-branch=main", mainOnly)
		if err := repo.PushMirrorRefs(ctx, mainOnly, []string{"+refs/heads/main:refs/heads/main"}, false, nil); err != nil {
			t.Fatalf("PushMirrorRefs main only: %v", err)
		}
		requireSameRefs(t, "push must not follow tags", gitLocalRefs(t, mainOnly), map[string]string{"refs/heads/main": want["refs/heads/main"]})
		calls := recordedCalls(t, log)
		for _, call := range []string{
			"--git-dir " + mirror + " --bare fetch --no-tags --progress --no-write-fetch-head --stdin ",
			"--git-dir " + mirror + " --bare push --progress ",
			" " + strings.Join(wildcard, " ") + "\n",
		} {
			if !strings.Contains(calls, call) {
				t.Errorf("native git not invoked with %q; calls:\n%s", call, calls)
			}
		}
		if strings.Contains(calls, bare) || strings.Contains(calls, dest) {
			t.Errorf("remote URL passed on argv; calls:\n%s", calls)
		}
	})

	t.Run("BogusBinaryFailsOnLocal", func(t *testing.T) {
		setGitBinary(t, bogus)
		if err := repo.PullMirrorRefs(ctx, bare, refs, nil); err == nil {
			t.Error("PullMirrorRefs with bogus git should fail")
		}
		dest := filepath.Join(root, "bogus-dest.git")
		runGit(t, "", "init", "--bare", "--initial-branch=main", dest)
		if err := repo.PushMirrorRefs(ctx, dest, wildcard, true, nil); err == nil {
			t.Error("PushMirrorRefs with bogus git should fail")
		}
		if got := gitLocalRefs(t, dest); len(got) != 0 {
			t.Errorf("bogus push wrote refs %v", got)
		}
	})

	t.Run("MemfsIgnoresBinary", func(t *testing.T) {
		setGitBinary(t, bogus)
		repo, err := Init(ctx, memfs.New(), "/mem.git", "main")
		if err != nil {
			t.Fatalf("init memfs repository: %v", err)
		}
		if err := repo.PullMirrorRefs(ctx, bare, refs, nil); err != nil {
			t.Fatalf("memfs PullMirrorRefs must use go-git: %v", err)
		}
		got, err := repo.Refs()
		if err != nil {
			t.Fatal(err)
		}
		requireSameRefs(t, "memfs pulled refs", got, want)
		dest := filepath.Join(root, "mem-dest.git")
		runGit(t, "", "init", "--bare", "--initial-branch=main", dest)
		if err := repo.PushMirrorRefs(ctx, dest, wildcard, true, nil); err != nil {
			t.Fatalf("memfs PushMirrorRefs must use go-git: %v", err)
		}
		requireSameRefs(t, "memfs pushed refs", gitLocalRefs(t, dest), want)
	})
}

// Mirror transfers on a repository git rejects must not fetch into, or push from, the checkout enclosing it.
func TestMirrorMalformedRepositoryFailsClosed(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	setGitBinary(t, gitPath)
	ctx := t.Context()
	root := t.TempDir()
	upstream, _ := buildParityUpstream(t, root)
	ancestor, child := malformedChildRepo(t)
	// packed-refs gives go-git a push source while refs/ stays missing, so git still rejects the directory.
	packedRefs := fmt.Sprintf("# pack-refs with: peeled fully-peeled sorted \n%s refs/heads/main\n", revParse(t, ancestor, "HEAD"))
	if err := os.WriteFile(filepath.Join(child, "packed-refs"), []byte(packedRefs), 0o644); err != nil {
		t.Fatal(err)
	}
	repo, err := Open(osfs.Default, child)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	requireGitMode(t, repo, true)
	dest := filepath.Join(root, "dest.git")
	runGit(t, "", "init", "--bare", "--initial-branch=main", dest)
	before := snapshotFiles(t, osfs.Default, ancestor)

	if err := repo.PushMirrorRefs(ctx, dest, []string{"+refs/heads/main:refs/heads/main"}, false, nil); err == nil {
		t.Error("PushMirrorRefs from a repository git rejects succeeded")
	}
	if got := gitLocalRefs(t, dest); len(got) != 0 {
		t.Errorf("push sent the enclosing repository's refs %v", got)
	}
	// The ancestor has topic/nested neither checked out nor present, so a fetch reaching it would succeed.
	if err := repo.PullMirrorRefs(ctx, upstream, []string{"refs/heads/topic/nested"}, nil); err == nil {
		t.Error("PullMirrorRefs into a repository git rejects succeeded")
	}
	if !maps.Equal(before, snapshotFiles(t, osfs.Default, ancestor)) {
		t.Fatal("mirror transfer changed the enclosing repository or the malformed one")
	}
}

// Relative remote paths resolve from the repository directory, as they did when git ran with -C.
func TestMirrorGitBinaryRelativeURL(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	setGitBinary(t, gitPath)
	ctx := t.Context()
	root := t.TempDir()
	bare, _ := buildParityUpstream(t, root)
	want := map[string]string{"refs/heads/main": gitLocalRefs(t, bare)["refs/heads/main"]}
	repo, err := InitMirror(ctx, osfs.Default, filepath.Join(root, "mirror.git"), bare)
	if err != nil {
		t.Fatalf("init mirror: %v", err)
	}
	requireGitMode(t, repo, true)
	if err := repo.PullMirrorRefs(ctx, "../upstream.git", []string{"refs/heads/main"}, nil); err != nil {
		t.Fatalf("PullMirrorRefs from a relative path: %v", err)
	}
	got, err := repo.Refs()
	if err != nil {
		t.Fatal(err)
	}
	requireSameRefs(t, "pulled refs", got, want)
	dest := filepath.Join(root, "dest.git")
	runGit(t, "", "init", "--bare", "--initial-branch=main", dest)
	if err := repo.PushMirrorRefs(ctx, "../dest.git", []string{"+refs/heads/main:refs/heads/main"}, false, nil); err != nil {
		t.Fatalf("PushMirrorRefs to a relative path: %v", err)
	}
	requireSameRefs(t, "pushed refs", gitLocalRefs(t, dest), want)
}

func TestPullMirrorRefsGitBinaryReindex(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		ctx := t.Context()
		root := t.TempDir()
		bare, work := buildParityUpstream(t, root)
		mirror := filepath.Join(root, "mirror.git")
		repo, err := InitMirror(ctx, osfs.Default, mirror, bare)
		if err != nil {
			t.Fatalf("init mirror: %v", err)
		}
		requireGitMode(t, repo, native)
		// Every fetch lands as a pack instead of loose objects.
		runGit(t, mirror, "config", "fetch.unpackLimit", "1")
		main := []string{"refs/heads/main"}
		if err := repo.PullMirrorRefs(ctx, bare, main, nil); err != nil {
			t.Fatalf("first PullMirrorRefs: %v", err)
		}
		if _, err := repo.Blob("main", "file.txt"); err != nil {
			t.Fatalf("read after first fetch: %v", err)
		}

		commitFile(t, work, "file.txt", "three\n", "c3")
		runGit(t, work, "push", "origin", "main")
		if err := repo.PullMirrorRefs(ctx, bare, main, nil); err != nil {
			t.Fatalf("second PullMirrorRefs: %v", err)
		}
		if packs, _ := filepath.Glob(filepath.Join(mirror, "objects/pack/*.pack")); len(packs) < 2 {
			t.Fatalf("fixture did not store the second fetch as a pack: %v", packs)
		}
		got, err := repo.Refs()
		if err != nil {
			t.Fatal(err)
		}
		if want := gitLocalRefs(t, bare)["refs/heads/main"]; got["refs/heads/main"] != want {
			t.Fatalf("refs/heads/main = %q, want %q", got["refs/heads/main"], want)
		}
		blob, err := repo.Blob("main", "file.txt")
		if err != nil {
			t.Fatalf("read after second fetch: %v", err)
		}
		if blob.Size() != int64(len("three\n")) {
			t.Fatalf("blob size = %d, want %d", blob.Size(), len("three\n"))
		}
	})
}

// TestMirrorGitBinaryHTTPAuth mirrors over basic-auth smart HTTP in both
// modes: URL credentials are honoured, poisoned host git configuration is
// ignored, and neither argv nor the repository retains the secret.
func TestMirrorGitBinaryHTTPAuth(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	bare, _ := buildParityUpstream(t, root)
	upstream := gitLocalRefs(t, bare)
	refs := []string{"refs/heads/main", "refs/heads/topic/nested", "refs/tags/v1", "refs/tags/v2"}
	wildcard := []string{"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"}
	const user, password = "alice", "s3cret"
	backend := gitHTTPBackend(t, root)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u, p, ok := r.BasicAuth(); !ok || u != user || p != password {
			w.Header().Set("WWW-Authenticate", `Basic realm="mirror"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		backend.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	authed := func(name string) string {
		u, err := url.Parse(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		u.User = url.UserPassword(user, password)
		return u.String() + "/" + name
	}

	// Host-level inputs git would honour when not hermetic: a rewrite to a dead port and another repository.
	home := filepath.Join(root, "home")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatal(err)
	}
	poison := "[url \"http://127.0.0.1:1/\"]\n\tinsteadOf = " + srv.URL + "/\n"
	for _, name := range []string{filepath.Join(home, ".gitconfig"), filepath.Join(root, "poison.gitconfig")} {
		if err := os.WriteFile(name, []byte(poison), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	other := filepath.Join(root, "other.git")
	runGit(t, "", "init", "--bare", "--initial-branch=other", other)

	forEachGitMode(t, func(t *testing.T, native bool) {
		bin, log := recordingGit(t)
		if native {
			setGitBinary(t, bin)
		}
		mirror := filepath.Join(t.TempDir(), "mirror.git")
		repo, err := InitMirror(ctx, osfs.Default, mirror, authed("upstream.git"))
		if err != nil {
			t.Fatalf("init mirror: %v", err)
		}
		dest := filepath.Join(root, fmt.Sprintf("dest-native-%t.git", native))
		runGit(t, "", "init", "--bare", "--initial-branch=main", dest)
		runGit(t, dest, "config", "http.receivepack", "true")
		if err := repo.PullMirrorRefs(ctx, srv.URL+"/upstream.git", refs, nil); err == nil {
			t.Fatal("PullMirrorRefs without credentials succeeded")
		}

		var progress bytes.Buffer
		t.Run("Poisoned", func(t *testing.T) {
			t.Setenv("HOME", home)
			t.Setenv("GIT_DIR", other)
			t.Setenv("GIT_CONFIG_GLOBAL", filepath.Join(root, "poison.gitconfig"))
			t.Setenv("GIT_CONFIG_SYSTEM", filepath.Join(root, "poison.gitconfig"))
			t.Setenv("GIT_CONFIG_COUNT", "1")
			t.Setenv("GIT_CONFIG_KEY_0", "url.http://127.0.0.1:1/.insteadOf")
			t.Setenv("GIT_CONFIG_VALUE_0", srv.URL+"/")
			if err := repo.PullMirrorRefs(ctx, authed("upstream.git"), refs, &progress); err != nil {
				t.Fatalf("PullMirrorRefs: %v", err)
			}
			if err := repo.PushMirrorRefs(ctx, authed(filepath.Base(dest)), wildcard, true, &progress); err != nil {
				t.Fatalf("PushMirrorRefs: %v", err)
			}
		})

		got, err := repo.Refs()
		if err != nil {
			t.Fatal(err)
		}
		requireSameRefs(t, "pulled refs", got, upstream)
		requireSameRefs(t, "pushed refs", gitLocalRefs(t, dest), upstream)
		gitFsck(t, dest)
		if got := gitLocalRefs(t, other); len(got) != 0 {
			t.Errorf("GIT_DIR repository received refs %v", got)
		}
		calls := recordedCalls(t, log)
		if native {
			if progress.Len() == 0 {
				t.Error("no progress emitted")
			}
			if !strings.Contains(calls, " fetch ") || !strings.Contains(calls, " push ") {
				t.Errorf("native git not invoked for fetch and push; calls:\n%s", calls)
			}
		}
		if strings.Contains(calls, password) || strings.Contains(calls, srv.URL) {
			t.Errorf("secret URL reached argv; calls:\n%s", calls)
		}
		err = filepath.WalkDir(mirror, func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if bytes.Contains(data, []byte(password)) {
				t.Errorf("%s retains the password", path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}
