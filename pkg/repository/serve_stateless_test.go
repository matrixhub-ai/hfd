package repository

// Tests for the v0/v1 stateless-RPC upload-pack round handling in serve.go:
// each negotiation round arrives as an independent request and must be
// answered with ACK/NAK without waiting for "done" in the same body.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp/sideband"
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

// wantTestRepos holds /a/x.git, whose main runs c1, parent, ca with annotated tag v1 on c1, and cb, a commit of /b/y.git in the same object pool.
type wantTestRepos struct {
	a                       *Repository
	c1, parent, ca, tag, cb string
}

func wantTestFixture(t *testing.T) wantTestRepos {
	t.Helper()
	bound := sharedTestBind(osfs.New(t.TempDir()))
	f := wantTestRepos{a: sharedTestInit(t, bound, "/a/x.git")}
	f.c1 = mustCommit(t, f.a, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: []byte("tagged\n")})
	f.parent = mustCommit(t, f.a, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: []byte("parent\n")})
	f.ca = mustCommit(t, f.a, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: []byte("tip\n")})
	f.tag = wantTestTag(t, f.a, "v1", plumbing.CommitObject, f.c1)
	wantTestSetRef(t, f.a, plumbing.NewHashReference(plumbing.NewTagReferenceName("v1"), plumbing.NewHash(f.tag)))
	f.cb = mustCommit(t, sharedTestInit(t, bound, "/b/y.git"), "main", "", CommitOperation{Type: CommitOperationAdd, Path: "b.txt", Content: []byte("private\n")})
	return f
}

// wantTestTag stores an annotated tag object named name on target, without a ref.
func wantTestTag(t *testing.T, repo *Repository, name string, targetType plumbing.ObjectType, target string) string {
	t.Helper()
	tag := &object.Tag{
		Name:       name,
		Tagger:     object.Signature{Name: "Test", Email: "test@example.com", When: time.Unix(1700000000, 0).UTC()},
		Message:    name + "\n",
		TargetType: targetType,
		Target:     plumbing.NewHash(target),
	}
	obj := repo.repo.Storer.NewEncodedObject()
	if err := tag.Encode(obj); err != nil {
		t.Fatalf("encode tag %s: %v", name, err)
	}
	h, err := repo.repo.Storer.SetEncodedObject(obj)
	if err != nil {
		t.Fatalf("store tag %s: %v", name, err)
	}
	return h.String()
}

func wantTestSetRef(t *testing.T, repo *Repository, ref *plumbing.Reference) {
	t.Helper()
	if err := repo.repo.Storer.SetReference(ref); err != nil {
		t.Fatalf("set %s: %v", ref.Name(), err)
	}
}

// wantTestUnreferenced commits on branch and then deletes it, leaving a commit no ref reaches.
func wantTestUnreferenced(t *testing.T, repo *Repository, branch string) string {
	t.Helper()
	commit := mustCommit(t, repo, branch, "", CommitOperation{Type: CommitOperationAdd, Path: branch + ".txt", Content: []byte(branch + "\n")})
	if err := repo.repo.Storer.RemoveReference(plumbing.NewBranchReferenceName(branch)); err != nil {
		t.Fatalf("remove branch %s: %v", branch, err)
	}
	return commit
}

// wantTestServe plays an SSH client against Serve: read the advertisement, send request, return the rest and Serve's error.
func wantTestServe(t *testing.T, repo *Repository, gitProtocol string, request []byte) ([]byte, error) {
	t.Helper()
	ctx := t.Context()
	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	if err := client.SetDeadline(time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() {
		err := repo.Serve(ctx, server, GitUploadPack, gitProtocol, ReceivePackHooks{})
		_ = server.Close()
		served <- err
	}()
	for {
		l, _, err := pktline.ReadLine(client)
		if err != nil {
			t.Fatalf("read advertisement: %v", err)
		}
		if l == pktline.Flush {
			break
		}
	}
	// A refused request is left partly unread, so this write may only end when Serve closes the pipe.
	go func() { _, _ = client.Write(request) }()
	resp, err := io.ReadAll(client)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp, <-served
}

// wantTestRequireRefused checks a want of oid failed with errNotOurRef and its ERR line, without ACK/NAK or a packfile.
func wantTestRequireRefused(t *testing.T, resp []byte, err error, oid string) {
	t.Helper()
	if !errors.Is(err, errNotOurRef) {
		t.Errorf("error = %v, want errNotOurRef", err)
	}
	if bytes.Contains(resp, []byte("PACK")) {
		t.Fatalf("want %s was served a packfile", oid)
	}
	lines := wantTestPktLines(t, resp)
	if want := wantTestNotOurRef + oid; len(lines) == 0 || lines[len(lines)-1] != want {
		t.Errorf("response = %q, want it to end with %q", lines, want)
	}
	for _, line := range lines {
		if strings.HasPrefix(line, "ACK") || strings.HasPrefix(line, "NAK") {
			t.Errorf("refused request was negotiated: %q", lines)
			break
		}
	}
}

// wantTestV2Pack returns the packfile a v2 fetch response multiplexes on sideband 1 after its packfile section header.
func wantTestV2Pack(t *testing.T, resp []byte) []byte {
	t.Helper()
	rd := bytes.NewReader(resp)
	if _, header, err := pktline.ReadLine(rd); err != nil || string(header) != "packfile\n" {
		t.Fatalf("v2 response begins with %q (%v), want the packfile section", header, err)
	}
	var pack []byte
	for {
		l, frame, err := pktline.ReadLine(rd)
		if err != nil {
			t.Fatalf("read packfile section: %v", err)
		}
		if l == pktline.Flush {
			return pack
		}
		if len(frame) > 0 && sideband.Channel(frame[0]) == sideband.PackData {
			pack = append(pack, frame[1:]...)
		}
	}
}

// TestUploadPackWantGate checks each go-git upload-pack entry point refuses another repository's pooled commit.
func TestUploadPackWantGate(t *testing.T) {
	setGitBinary(t, "")
	f := wantTestFixture(t)
	const caps = "multi_ack_detailed"
	unknown := strings.Repeat("ab", 20)
	stateless := func(t *testing.T, gitProtocol string, body []byte) ([]byte, error) {
		t.Helper()
		var out bytes.Buffer
		err := f.a.Stateless(t.Context(), &out, bytes.NewReader(body), GitUploadPack, gitProtocol, ReceivePackHooks{})
		return out.Bytes(), err
	}
	v0Done := func(oid string) []byte { return uploadPackRequest(t, oid, caps, nil, true) }
	v2Fetch := func(oid string) []byte {
		return wantTestPkts(t, "command=fetch\n", wantTestDelim, "want "+oid+"\n", "done\n", "")
	}
	requireV0Pack := func(t *testing.T, resp []byte, commit string) {
		t.Helper()
		lines, pack := splitPack(t, resp)
		if !slices.Equal(lines, []string{"NAK"}) {
			t.Fatalf("lines before the pack = %q, want [NAK]", lines)
		}
		requireUsablePack(t, pack, commit)
	}

	t.Run("V0FinalRoundRefusesOtherRepositoryCommit", func(t *testing.T) {
		resp, err := stateless(t, "", v0Done(f.cb))
		wantTestRequireRefused(t, resp, err, f.cb)
	})

	for _, c := range []struct{ name, want, commit string }{
		{"BranchTip", f.ca, f.ca},
		{"TagObject", f.tag, f.c1},
		{"PeeledTagTarget", f.c1, f.c1},
	} {
		t.Run("V0FinalRoundServes"+c.name, func(t *testing.T) {
			resp, err := stateless(t, "", v0Done(c.want))
			if err != nil {
				t.Fatalf("Stateless: %v", err)
			}
			requireV0Pack(t, resp, c.commit)
		})
	}

	t.Run("V0ServesReachableNonTip", func(t *testing.T) {
		resp, err := stateless(t, "", v0Done(f.parent))
		if err != nil {
			t.Fatalf("Stateless: %v", err)
		}
		requireV0Pack(t, resp, f.parent)
	})

	t.Run("V0NegotiationRoundRefusesOtherRepositoryCommit", func(t *testing.T) {
		resp, err := stateless(t, "", uploadPackRequest(t, f.cb, caps, []string{unknown}, false))
		wantTestRequireRefused(t, resp, err, f.cb)
	})

	t.Run("V0NegotiationRoundAnswersAdvertisedWant", func(t *testing.T) {
		resp, err := stateless(t, "", uploadPackRequest(t, f.ca, caps, []string{unknown}, false))
		if err != nil {
			t.Fatalf("Stateless: %v", err)
		}
		if lines := readPktLines(t, resp); !slices.Equal(lines, []string{"NAK"}) {
			t.Fatalf("round response = %q, want [NAK]", lines)
		}
	})

	t.Run("V0RefusesUnadvertisedAmongWants", func(t *testing.T) {
		resp, err := stateless(t, "", pktLines(t, "want "+f.ca+" "+caps+"\n", "want "+f.cb+"\n", "", "done\n"))
		wantTestRequireRefused(t, resp, err, f.cb)
	})

	t.Run("V2RefusesOtherRepositoryCommit", func(t *testing.T) {
		resp, err := stateless(t, "version=2", v2Fetch(f.cb))
		wantTestRequireRefused(t, resp, err, f.cb)
	})

	t.Run("V2ServesAdvertisedWant", func(t *testing.T) {
		resp, err := stateless(t, "version=2", v2Fetch(f.ca))
		if err != nil {
			t.Fatalf("Stateless: %v", err)
		}
		requireUsablePack(t, wantTestV2Pack(t, resp), f.ca)
	})

	t.Run("V2ServesReachableNonTip", func(t *testing.T) {
		resp, err := stateless(t, "version=2", v2Fetch(f.parent))
		if err != nil {
			t.Fatalf("Stateless: %v", err)
		}
		requireUsablePack(t, wantTestV2Pack(t, resp), f.parent)
	})

	t.Run("StatefulV0RefusesOtherRepositoryCommit", func(t *testing.T) {
		resp, err := wantTestServe(t, f.a, "", v0Done(f.cb))
		wantTestRequireRefused(t, resp, err, f.cb)
	})

	t.Run("StatefulV0ServesAdvertisedWant", func(t *testing.T) {
		resp, err := wantTestServe(t, f.a, "", v0Done(f.ca))
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
		requireV0Pack(t, resp, f.ca)
	})

	t.Run("StatefulV2RefusesOtherRepositoryCommit", func(t *testing.T) {
		resp, err := wantTestServe(t, f.a, "version=2", v2Fetch(f.cb))
		wantTestRequireRefused(t, resp, err, f.cb)
	})

	t.Run("StatefulV2ServesAdvertisedWant", func(t *testing.T) {
		resp, err := wantTestServe(t, f.a, "version=2", v2Fetch(f.ca))
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
		requireUsablePack(t, wantTestV2Pack(t, resp), f.ca)
	})
}

// TestWantAllowed checks the want gate admits the commits and tag objects the refs reach, each predicate answering from one snapshot.
func TestWantAllowed(t *testing.T) {
	setGitBinary(t, "")
	ctx := t.Context()
	f := wantTestFixture(t)
	target := wantTestUnreferenced(t, f.a, "nested")
	nested := wantTestTag(t, f.a, "v2", plumbing.TagObject, wantTestTag(t, f.a, "inner", plumbing.CommitObject, target))
	wantTestSetRef(t, f.a, plumbing.NewHashReference(plumbing.NewTagReferenceName("v2"), plumbing.NewHash(nested)))
	ask := func(t *testing.T, allowed func(plumbing.Hash) (bool, error), oid string) bool {
		t.Helper()
		ok, err := allowed(plumbing.NewHash(oid))
		if err != nil {
			t.Fatalf("wantAllowed(%s): %v", oid, err)
		}
		return ok
	}
	newTip := func(t *testing.T, branch string) string {
		t.Helper()
		return mustCommit(t, f.a, branch, "", CommitOperation{Type: CommitOperationAdd, Path: branch + ".txt", Content: []byte(branch + "\n")})
	}

	for _, c := range []struct {
		name string
		oid  string
		want bool
	}{
		{"BranchTip", f.ca, true},
		{"ReachableNonTip", f.parent, true},
		{"TagObject", f.tag, true},
		{"NestedTagObject", nested, true},
		{"NestedTagTarget", target, true},
		{"OtherRepositoryCommit", f.cb, false},
		{"UnknownHash", strings.Repeat("ab", 20), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := ask(t, f.a.wantAllowed(ctx), c.oid); got != c.want {
				t.Errorf("wantAllowed(%s) = %t, want %t", c.oid, got, c.want)
			}
		})
	}

	t.Run("NewPredicateSeesNewBranch", func(t *testing.T) {
		ask(t, f.a.wantAllowed(ctx), f.ca)
		tip := newTip(t, "feature")
		if !ask(t, f.a.wantAllowed(ctx), tip) {
			t.Errorf("a predicate made after branch feature moved to %s refuses it", tip)
		}
	})

	t.Run("PredicateKeepsItsSnapshot", func(t *testing.T) {
		allowed := f.a.wantAllowed(ctx)
		if !ask(t, allowed, f.ca) {
			t.Fatalf("wantAllowed refuses the main tip %s", f.ca)
		}
		tip := newTip(t, "later")
		if ask(t, allowed, tip) {
			t.Errorf("predicate admits %s, which branch later reached only after its first answer", tip)
		}
	})

	t.Run("CancelledContextFails", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := f.a.wantAllowed(cancelled)(plumbing.NewHash(f.ca)); !errors.Is(err, context.Canceled) {
			t.Fatalf("wantAllowed with a cancelled context = %v, want context.Canceled", err)
		}
	})

	t.Run("UnbornHEAD", func(t *testing.T) {
		wantTestSetRef(t, f.a, plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("unborn")))
		if !ask(t, f.a.wantAllowed(ctx), f.ca) {
			t.Errorf("wantAllowed refuses the main tip %s while HEAD is unborn", f.ca)
		}
	})

	t.Run("DetachedHEAD", func(t *testing.T) {
		detached := wantTestUnreferenced(t, f.a, "detached")
		wantTestSetRef(t, f.a, plumbing.NewHashReference(plumbing.HEAD, plumbing.NewHash(detached)))
		if !ask(t, f.a.wantAllowed(ctx), detached) {
			t.Errorf("wantAllowed refuses %s, the detached HEAD", detached)
		}
	})
}

// TestUploadPackWantGateNative checks native git upload-pack, whose v2 serves any object the pool lends it, gets the same want gate.
func TestUploadPackWantGateNative(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not in PATH")
	}
	setGitBinary(t, gitPath)
	f := wantTestFixture(t)
	requireGitMode(t, f.a, true)
	stateless := func(t *testing.T, gitProtocol string, body []byte) ([]byte, error) {
		t.Helper()
		var out bytes.Buffer
		err := f.a.Stateless(t.Context(), &out, bytes.NewReader(body), GitUploadPack, gitProtocol, ReceivePackHooks{})
		return out.Bytes(), err
	}
	v0Done := func(oid string) []byte { return uploadPackRequest(t, oid, "multi_ack_detailed", nil, true) }
	v2Fetch := func(oid string) []byte {
		return wantTestPkts(t, "command=fetch\n", wantTestDelim, "want "+oid+"\n", "done\n", "")
	}
	// Native v0 refuses with the same ERR line itself; only errNotOurRef shows the gate did.
	requireRefused := func(t *testing.T, resp []byte, err error) {
		t.Helper()
		if !errors.Is(err, errNotOurRef) {
			t.Errorf("error = %v, want errNotOurRef", err)
		}
		if bytes.Contains(resp, []byte("PACK")) {
			t.Fatalf("want %s was served a packfile", f.cb)
		}
		if want := wantTestNotOurRef + f.cb; !bytes.Contains(resp, []byte(want)) {
			t.Errorf("response %q lacks %q", resp, want)
		}
	}

	t.Run("V2RefusesOtherRepositoryCommit", func(t *testing.T) {
		resp, err := stateless(t, "version=2", v2Fetch(f.cb))
		requireRefused(t, resp, err)
	})

	t.Run("V0RefusesOtherRepositoryCommit", func(t *testing.T) {
		resp, err := stateless(t, "", v0Done(f.cb))
		requireRefused(t, resp, err)
	})

	t.Run("V2ServesBranchTip", func(t *testing.T) {
		resp, err := stateless(t, "version=2", v2Fetch(f.ca))
		if err != nil {
			t.Fatalf("Stateless: %v", err)
		}
		requireUsablePack(t, wantTestV2Pack(t, resp), f.ca)
	})

	t.Run("V0ServesBranchTip", func(t *testing.T) {
		resp, err := stateless(t, "", v0Done(f.ca))
		if err != nil {
			t.Fatalf("Stateless: %v", err)
		}
		lines, pack := splitPack(t, resp)
		if !slices.Equal(lines, []string{"NAK"}) {
			t.Fatalf("lines before the pack = %q, want [NAK]", lines)
		}
		requireUsablePack(t, pack, f.ca)
	})

	t.Run("V2ServesReachableNonTip", func(t *testing.T) {
		resp, err := stateless(t, "version=2", v2Fetch(f.parent))
		if err != nil {
			t.Fatalf("Stateless: %v", err)
		}
		requireUsablePack(t, wantTestV2Pack(t, resp), f.parent)
	})

	t.Run("StatefulV2RefusesOtherRepositoryCommit", func(t *testing.T) {
		// The extra flush-pkt ends the session, so a git that served the want exits instead of awaiting another command.
		resp, err := wantTestServe(t, f.a, "version=2", append(v2Fetch(f.cb), pktLines(t, "")...))
		requireRefused(t, resp, err)
	})
}
