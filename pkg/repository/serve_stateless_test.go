package repository

// Tests for the v0/v1 stateless-RPC upload-pack round handling in serve.go:
// each negotiation round arrives as an independent request and must be
// answered with ACK/NAK without waiting for "done" in the same body.

import (
	"bytes"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
)

// unpackGitPack feeds a raw packfile to `git unpack-objects` in repoPath.
func unpackGitPack(t *testing.T, repoPath string, pack []byte) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", "unpack-objects", "-q")
	cmd.Dir = repoPath
	cmd.Stdin = bytes.NewReader(pack)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git unpack-objects: %v\n%s", err, out)
	}
}

// pktLines encodes pkt-lines; an empty string encodes a flush-pkt.
func pktLines(t *testing.T, lines ...string) []byte {
	t.Helper()
	var buf bytes.Buffer
	for _, line := range lines {
		if line == "" {
			if err := pktline.WriteFlush(&buf); err != nil {
				t.Fatalf("write flush: %v", err)
			}
			continue
		}
		if _, err := pktline.WriteString(&buf, line); err != nil {
			t.Fatalf("write pkt-line: %v", err)
		}
	}
	return buf.Bytes()
}

// readPktLines decodes all pkt-lines of a response; flush-pkts decode as "".
func readPktLines(t *testing.T, data []byte) []string {
	t.Helper()
	rd := bytes.NewReader(data)
	var lines []string
	for rd.Len() > 0 {
		l, line, err := pktline.ReadLine(rd)
		if err != nil {
			t.Fatalf("read pkt-line from %q: %v", data, err)
		}
		if l == pktline.Flush {
			lines = append(lines, "")
			continue
		}
		lines = append(lines, strings.TrimSuffix(string(line), "\n"))
	}
	return lines
}

func TestUploadPackRequestHasDone(t *testing.T) {
	want := "want 0123456789012345678901234567890123456789 multi_ack_detailed\n"
	cases := []struct {
		name string
		body []byte
		want bool
	}{
		{"FlushOnlyProbe", pktLines(t, ""), false},
		{"WantsAndHavesWithoutDone", pktLines(t, want, "", "have 89012345678901234567890123456789abcdef01\n", ""), false},
		{"WantsWithDone", pktLines(t, want, "", "done\n"), true},
		{"HavesThenDone", pktLines(t, want, "", "have 89012345678901234567890123456789abcdef01\n", "done\n"), true},
		{"DoneWithoutNewline", pktLines(t, want, "", "done"), true},
		{"Truncated", []byte("0012want incomplete"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := uploadPackRequestHasDone(tc.body); got != tc.want {
				t.Fatalf("uploadPackRequestHasDone(%q) = %v, want %v", tc.body, got, tc.want)
			}
		})
	}
}

func TestStatelessUploadPackRequestSizeLimit(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()

	bare, _ := buildParityUpstream(t, root)
	repo, err := Open(osfs.Default, bare)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}

	oversized := bytes.NewReader(make([]byte, maxUploadPackRequestSize+1))
	var out bytes.Buffer
	err = repo.Stateless(ctx, &out, oversized, GitUploadPack, "", ReceivePackHooks{})
	if err == nil || !strings.Contains(err.Error(), "maximum size") {
		t.Fatalf("oversized request should be rejected, got err=%v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("oversized request should produce no output, got %d bytes", out.Len())
	}
}

func TestStatelessUploadPackNegotiationRound(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()

	bare, work := buildParityUpstream(t, root)
	repo, err := Open(osfs.Default, bare)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	refs := gitLocalRefs(t, bare)
	mainHash := refs["refs/heads/main"]

	// A hash the server cannot have: a commit created only in the work repo
	// after the last push.
	commitFile(t, work, "file.txt", "local only\n", "local only")
	localOnly := strings.TrimSpace(gitOut(t, work, "rev-parse", "HEAD"))

	serve := func(t *testing.T, body []byte) []string {
		t.Helper()
		var out bytes.Buffer
		if err := repo.Stateless(ctx, &out, bytes.NewReader(body), GitUploadPack, "", ReceivePackHooks{}); err != nil {
			t.Fatalf("Stateless: %v", err)
		}
		return readPktLines(t, out.Bytes())
	}

	wantLine := fmt.Sprintf("want %s multi_ack_detailed\n", mainHash)

	t.Run("FlushOnlyProbe", func(t *testing.T) {
		if got := serve(t, pktLines(t, "")); len(got) != 0 {
			t.Fatalf("probe request should produce no output, got %q", got)
		}
	})

	t.Run("RoundWithUnknownHaves", func(t *testing.T) {
		body := pktLines(t, wantLine, "",
			fmt.Sprintf("have %s\n", localOnly), "")
		got := serve(t, body)
		want := []string{"NAK"}
		if strings.Join(got, "|") != strings.Join(want, "|") {
			t.Fatalf("round response = %q, want %q", got, want)
		}
	})

	t.Run("RoundWithCommonHaves", func(t *testing.T) {
		body := pktLines(t, wantLine, "",
			fmt.Sprintf("have %s\n", localOnly),
			fmt.Sprintf("have %s\n", mainHash), "")
		got := serve(t, body)
		want := []string{
			fmt.Sprintf("ACK %s common", mainHash),
			"NAK",
		}
		if strings.Join(got, "|") != strings.Join(want, "|") {
			t.Fatalf("round response = %q, want %q", got, want)
		}
	})

	t.Run("MultiAckRound", func(t *testing.T) {
		body := pktLines(t,
			fmt.Sprintf("want %s multi_ack\n", mainHash), "",
			fmt.Sprintf("have %s\n", mainHash), "")
		got := serve(t, body)
		want := []string{
			fmt.Sprintf("ACK %s continue", mainHash),
			"NAK",
		}
		if strings.Join(got, "|") != strings.Join(want, "|") {
			t.Fatalf("round response = %q, want %q", got, want)
		}
	})

	t.Run("SingleAckRound", func(t *testing.T) {
		body := pktLines(t,
			fmt.Sprintf("want %s\n", mainHash), "",
			fmt.Sprintf("have %s\n", mainHash), "")
		got := serve(t, body)
		want := []string{fmt.Sprintf("ACK %s", mainHash)}
		if strings.Join(got, "|") != strings.Join(want, "|") {
			t.Fatalf("round response = %q, want %q", got, want)
		}
	})

	t.Run("FinalRoundWithDoneSendsPack", func(t *testing.T) {
		var out bytes.Buffer
		body := pktLines(t, wantLine, "", "done\n")
		if err := repo.Stateless(ctx, &out, bytes.NewReader(body), GitUploadPack, "", ReceivePackHooks{}); err != nil {
			t.Fatalf("Stateless final round: %v", err)
		}
		resp := out.Bytes()
		lines := readPktLines(t, resp[:8]) // first pkt-line only: "0008NAK\n"
		if len(lines) == 0 || lines[0] != "NAK" {
			t.Fatalf("final round should start with NAK, got %q", resp[:min(16, len(resp))])
		}
		if !bytes.Contains(resp, []byte("PACK")) {
			t.Fatalf("final round response should contain a packfile")
		}
	})
}

// TestStatelessUploadPackFullNegotiation drives a complete multi-round
// stateless negotiation the way a real smart-HTTP client does: rounds are
// separate requests replaying the accumulated state, ending with done.
func TestStatelessUploadPackFullNegotiation(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()

	bare, work := buildParityUpstream(t, root)
	repo, err := Open(osfs.Default, bare)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}

	// The client has the old main plus local-only history the server lacks.
	oldMain := gitLocalRefs(t, bare)["refs/heads/main"]
	commitFile(t, work, "file.txt", "new upstream\n", "new upstream")
	runGit(t, work, "push", "origin", "main")
	newMain := gitLocalRefs(t, bare)["refs/heads/main"]

	wantLine := fmt.Sprintf("want %s multi_ack_detailed\n", newMain)
	unknown := strings.Repeat("ab", 20)

	// Round 1: only haves the server does not know -> NAK, keep negotiating.
	round1 := pktLines(t, wantLine, "", fmt.Sprintf("have %s\n", unknown), "")
	var out1 bytes.Buffer
	if err := repo.Stateless(ctx, &out1, bytes.NewReader(round1), GitUploadPack, "", ReceivePackHooks{}); err != nil {
		t.Fatalf("round 1: %v", err)
	}
	if got := readPktLines(t, out1.Bytes()); len(got) != 1 || got[0] != "NAK" {
		t.Fatalf("round 1 response = %q, want [NAK]", got)
	}

	// Round 2: replayed state plus a common have -> ACK common, NAK.
	round2 := pktLines(t, wantLine, "",
		fmt.Sprintf("have %s\n", unknown),
		fmt.Sprintf("have %s\n", oldMain), "")
	var out2 bytes.Buffer
	if err := repo.Stateless(ctx, &out2, bytes.NewReader(round2), GitUploadPack, "", ReceivePackHooks{}); err != nil {
		t.Fatalf("round 2: %v", err)
	}
	got2 := readPktLines(t, out2.Bytes())
	want2 := []string{fmt.Sprintf("ACK %s common", oldMain), "NAK"}
	if strings.Join(got2, "|") != strings.Join(want2, "|") {
		t.Fatalf("round 2 response = %q, want %q", got2, want2)
	}

	// Final round: full state plus done -> ACK/pack.
	final := pktLines(t, wantLine, "",
		fmt.Sprintf("have %s\n", oldMain),
		"done\n")
	var out3 bytes.Buffer
	if err := repo.Stateless(ctx, &out3, bytes.NewReader(final), GitUploadPack, "", ReceivePackHooks{}); err != nil {
		t.Fatalf("final round: %v", err)
	}
	if !bytes.Contains(out3.Bytes(), []byte("PACK")) {
		t.Fatalf("final response should contain a packfile")
	}

	// The pack must be usable by the git binary: index it into a scratch
	// repository and verify the wanted commit becomes readable.
	resp := out3.Bytes()
	packStart := bytes.Index(resp, []byte("PACK"))
	scratch := filepath.Join(root, "scratch.git")
	runGit(t, "", "init", "--bare", "--initial-branch=main", scratch)
	unpackGitPack(t, scratch, resp[packStart:])
	if typ := strings.TrimSpace(gitOut(t, scratch, "cat-file", "-t", newMain)); typ != "commit" {
		t.Fatalf("wanted commit not usable from negotiated pack, cat-file -t = %q", typ)
	}
}
