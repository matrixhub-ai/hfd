package repository

// Tests for the v0/v1 stateless-RPC upload-pack round handling in serve.go:
// each negotiation round arrives as an independent request and must be
// answered with ACK/NAK without waiting for "done" in the same body.

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"slices"
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
	setGitBinary(t, "")
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

func uploadPackRequest(t *testing.T, want, caps string, haves []string, done bool) []byte {
	t.Helper()
	if caps != "" {
		want += " " + caps
	}
	lines := []string{"want " + want + "\n", ""}
	for _, h := range haves {
		lines = append(lines, "have "+h+"\n")
	}
	if done {
		return pktLines(t, append(lines, "done\n")...)
	}
	return pktLines(t, append(lines, "")...)
}

// multi_ack_detailed permits optional ready hints for advertised haves.
func withoutReadyACKs(t *testing.T, lines []string, detailed bool, haves []string) []string {
	t.Helper()
	var out []string
	for _, line := range lines {
		if hash, ok := strings.CutSuffix(strings.TrimPrefix(line, "ACK "), " ready"); ok && strings.HasPrefix(line, "ACK ") {
			if !detailed || !slices.Contains(haves, hash) {
				t.Fatalf("unexpected %q (multi_ack_detailed=%t, haves %v)", line, detailed, haves)
			}
			continue
		}
		out = append(out, line)
	}
	return out
}

// splitPack separates the pkt-lines answering "done" from the raw packfile.
func splitPack(t *testing.T, resp []byte) (lines []string, pack []byte) {
	t.Helper()
	packStart := bytes.Index(resp, []byte("PACK"))
	if packStart < 0 {
		t.Fatalf("response %q carries no packfile", resp[:min(64, len(resp))])
	}
	return readPktLines(t, resp[:packStart]), resp[packStart:]
}

// requireUsablePack unpacks pack with the git binary and checks commit is readable from it.
func requireUsablePack(t *testing.T, pack []byte, commit string) {
	t.Helper()
	scratch := filepath.Join(t.TempDir(), "scratch.git")
	runGit(t, "", "init", "--bare", "--initial-branch=main", scratch)
	unpackGitPack(t, scratch, pack)
	if typ := strings.TrimSpace(gitOut(t, scratch, "cat-file", "-t", commit)); typ != "commit" {
		t.Fatalf("wanted commit not usable from pack, cat-file -t = %q", typ)
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
	mainHash := gitLocalRefs(t, bare)["refs/heads/main"]

	// A hash the server cannot have: a commit created only in the work repo
	// after the last push.
	commitFile(t, work, "file.txt", "local only\n", "local only")
	localOnly := strings.TrimSpace(gitOut(t, work, "rev-parse", "HEAD"))

	cases := []struct {
		name  string
		caps  string
		haves []string
		want  []string
	}{
		{"RoundWithUnknownHaves", "multi_ack_detailed", []string{localOnly}, []string{"NAK"}},
		{"RoundWithCommonHaves", "multi_ack_detailed", []string{localOnly, mainHash}, []string{"ACK " + mainHash + " common", "NAK"}},
		// Every have common: git adds an "ACK <hash> ready" hint here that go-git does not.
		{"RoundWithOnlyCommonHaves", "multi_ack_detailed", []string{mainHash}, []string{"ACK " + mainHash + " common", "NAK"}},
		{"MultiAckRound", "multi_ack", []string{mainHash}, []string{"ACK " + mainHash + " continue", "NAK"}},
		{"SingleAckRound", "", []string{mainHash}, []string{"ACK " + mainHash}},
	}

	forEachGitMode(t, func(t *testing.T, native bool) {
		requireGitMode(t, repo, native)
		forEachProtocol(t, []string{"", "version=1"}, func(t *testing.T, proto string) {
			serve := func(t *testing.T, body []byte) []byte {
				t.Helper()
				var out bytes.Buffer
				if err := repo.Stateless(ctx, &out, bytes.NewReader(body), GitUploadPack, proto, ReceivePackHooks{}); err != nil {
					t.Fatalf("Stateless: %v", err)
				}
				return out.Bytes()
			}

			t.Run("FlushOnlyProbe", func(t *testing.T) {
				if got := serve(t, pktLines(t, "")); len(got) != 0 {
					t.Fatalf("probe request should produce no output, got %q", got)
				}
			})

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					lines := readPktLines(t, serve(t, uploadPackRequest(t, mainHash, tc.caps, tc.haves, false)))
					if got := withoutReadyACKs(t, lines, tc.caps == "multi_ack_detailed", tc.haves); !slices.Equal(got, tc.want) {
						t.Fatalf("round response = %q, want %q", got, tc.want)
					}
				})
			}

			t.Run("FinalRoundWithDoneSendsPack", func(t *testing.T) {
				lines, pack := splitPack(t, serve(t, uploadPackRequest(t, mainHash, "multi_ack_detailed", nil, true)))
				if !slices.Equal(lines, []string{"NAK"}) {
					t.Fatalf("final round without haves = %q, want [NAK]", lines)
				}
				requireUsablePack(t, pack, mainHash)
			})
		})
	})
}

// TestStatelessUploadPackFullNegotiation drives a complete multi-round
// stateless negotiation the way a real smart-HTTP client does: rounds are
// separate requests replaying the accumulated state, ending with done.
func TestStatelessUploadPackFullNegotiation(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		forEachProtocol(t, []string{"", "version=1"}, func(t *testing.T, proto string) {
			testStatelessUploadPackFullNegotiation(t, native, proto)
		})
	})
}

func testStatelessUploadPackFullNegotiation(t *testing.T, native bool, proto string) {
	ctx := t.Context()
	root := t.TempDir()

	bare, work := buildParityUpstream(t, root)
	repo, err := Open(osfs.Default, bare)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	requireGitMode(t, repo, native)

	// The client has the old main plus local-only history the server lacks.
	oldMain := gitLocalRefs(t, bare)["refs/heads/main"]
	commitFile(t, work, "file.txt", "new upstream\n", "new upstream")
	runGit(t, work, "push", "origin", "main")
	newMain := gitLocalRefs(t, bare)["refs/heads/main"]
	unknown := strings.Repeat("ab", 20)

	serve := func(t *testing.T, haves []string, done bool) []byte {
		t.Helper()
		var out bytes.Buffer
		if err := repo.Stateless(ctx, &out, bytes.NewReader(uploadPackRequest(t, newMain, "multi_ack_detailed", haves, done)), GitUploadPack, proto, ReceivePackHooks{}); err != nil {
			t.Fatalf("Stateless(haves %v, done %t): %v", haves, done, err)
		}
		return out.Bytes()
	}

	// Round 1: only haves the server does not know -> NAK, keep negotiating.
	haves := []string{unknown}
	if got := withoutReadyACKs(t, readPktLines(t, serve(t, haves, false)), true, haves); !slices.Equal(got, []string{"NAK"}) {
		t.Fatalf("round 1 response = %q, want [NAK]", got)
	}

	// Round 2: replayed state plus a common have -> ACK common, NAK.
	haves = []string{unknown, oldMain}
	if got, want := withoutReadyACKs(t, readPktLines(t, serve(t, haves, false)), true, haves), []string{"ACK " + oldMain + " common", "NAK"}; !slices.Equal(got, want) {
		t.Fatalf("round 2 response = %q, want %q", got, want)
	}

	// Final round: the acknowledged common have plus done -> ACK common, final ACK, pack.
	haves = []string{oldMain}
	lines, pack := splitPack(t, serve(t, haves, true))
	if got, want := withoutReadyACKs(t, lines, true, haves), []string{"ACK " + oldMain + " common", "ACK " + oldMain}; !slices.Equal(got, want) {
		t.Fatalf("final round response = %q, want %q", got, want)
	}
	requireUsablePack(t, pack, newMain)
}
