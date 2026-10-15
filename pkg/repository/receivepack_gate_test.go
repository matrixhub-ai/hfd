package repository

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage/memory"

	"github.com/matrixhub-ai/hfd/pkg/receive"
)

// gateTestReadme is README.md as c1 adds it.
const gateTestReadme = "c1\n"

// gateTestData is data.txt as c2 adds it, long enough for a thin pack to deltify against.
var gateTestData = strings.Repeat("a line the pushed version keeps, so its blob deltifies against this one\n", 64)

// gateTestSecret is secret.txt as cb adds it to b, long enough for a delta in a's push to copy from.
var gateTestSecret = strings.Repeat("b's secret, which a delta in a's push could copy from\n", 64)

// gateTestFixture is two repositories sharing one pool: a, whose main went c1 then c2 and whose side branch is at c2, and b, whose main alone reaches cb.
type gateTestFixture struct {
	dataFS       billy.Filesystem
	aHost, bHost string
	a, b         *Repository
	c1, c2, cb   plumbing.Hash
}

func gateTestSetup(t *testing.T, native bool) *gateTestFixture {
	t.Helper()
	root := t.TempDir()
	f := &gateTestFixture{
		dataFS: osfs.New(root),
		aHost:  filepath.Join(root, "repositories", "org", "a.git"),
		bHost:  filepath.Join(root, "repositories", "org", "b.git"),
	}
	bound := sharedTestBind(f.dataFS)
	f.a, f.b = sharedTestInit(t, bound, "/org/a.git"), sharedTestInit(t, bound, "/org/b.git")
	requireGitMode(t, f.a, native)
	f.c1 = plumbing.NewHash(mustCommit(t, f.a, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "README.md", Content: []byte(gateTestReadme)}))
	f.c2 = plumbing.NewHash(mustCommit(t, f.a, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "data.txt", Content: []byte(gateTestData)}))
	if err := f.a.CreateBranch("side", "main"); err != nil {
		t.Fatalf("create side: %v", err)
	}
	f.cb = plumbing.NewHash(mustCommit(t, f.b, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "secret.txt", Content: []byte(gateTestSecret)}))
	if !slices.Contains(sharedTestPoolObjects(t, f.dataFS), f.cb.String()) {
		t.Fatalf("fixture: b's commit %s is not loose in the pool a borrows from", f.cb)
	}
	return f
}

// gateTestPack collects the objects a pushing client packs.
type gateTestPack struct {
	t      *testing.T
	st     *memory.Storage
	hashes []plumbing.Hash
}

func gateTestNewPack(t *testing.T) *gateTestPack {
	return &gateTestPack{t: t, st: memory.NewStorage()}
}

func (p *gateTestPack) add(obj interface {
	Encode(plumbing.EncodedObject) error
}) plumbing.Hash {
	p.t.Helper()
	o := p.st.NewEncodedObject()
	if err := obj.Encode(o); err != nil {
		p.t.Fatalf("encode: %v", err)
	}
	h, err := p.st.SetEncodedObject(o)
	if err != nil {
		p.t.Fatalf("store: %v", err)
	}
	p.hashes = append(p.hashes, h)
	return h
}

func (p *gateTestPack) blob(content string) plumbing.Hash {
	p.t.Helper()
	h := storeBlob(p.t, p.st, content)
	p.hashes = append(p.hashes, h)
	return h
}

func (p *gateTestPack) tree(entries ...object.TreeEntry) plumbing.Hash {
	p.t.Helper()
	sort.Sort(object.TreeEntrySorter(entries))
	return p.add(&object.Tree{Entries: entries})
}

func (p *gateTestPack) commit(tree plumbing.Hash, parents ...plumbing.Hash) plumbing.Hash {
	p.t.Helper()
	return p.add(&object.Commit{Author: reachSig, Committer: reachSig, Message: "pushed\n", TreeHash: tree, ParentHashes: parents})
}

// encode packs every object added so far.
func (p *gateTestPack) encode() []byte {
	p.t.Helper()
	var buf bytes.Buffer
	if _, err := packfile.NewEncoder(&buf, p.st, false).Encode(p.hashes, 10); err != nil {
		p.t.Fatalf("encode pack: %v", err)
	}
	return buf.Bytes()
}

// thin packs every object added so far whole, then target as a REF_DELTA against base, a blob the pack leaves out.
func (p *gateTestPack) thin(base, target string) []byte {
	p.t.Helper()
	pack := bytes.NewBuffer(binary.BigEndian.AppendUint32([]byte("PACK\x00\x00\x00\x02"), uint32(len(p.hashes)+1)))
	for _, h := range p.hashes {
		obj, err := p.st.EncodedObject(plumbing.AnyObject, h)
		if err != nil {
			p.t.Fatalf("read %s: %v", h, err)
		}
		r, err := obj.Reader()
		if err != nil {
			p.t.Fatalf("read %s: %v", h, err)
		}
		data, err := io.ReadAll(r)
		if err := errors.Join(err, r.Close()); err != nil {
			p.t.Fatalf("read %s: %v", h, err)
		}
		p.entry(pack, obj.Type(), nil, data)
	}
	p.entry(pack, plumbing.REFDeltaObject, plumbing.NewHash(sharedTestBlobHash([]byte(base))).Bytes(), packfile.DiffDelta([]byte(base), []byte(target)))
	sum := sha1.Sum(pack.Bytes())
	return append(pack.Bytes(), sum[:]...)
}

// entry appends one pack entry: its type and size, a REF_DELTA's base, then data deflated.
func (p *gateTestPack) entry(pack *bytes.Buffer, typ plumbing.ObjectType, base, data []byte) {
	p.t.Helper()
	size := len(data)
	b := byte(typ)<<4 | byte(size&0x0f)
	for size >>= 4; size > 0; size >>= 7 {
		pack.WriteByte(b | 0x80)
		b = byte(size & 0x7f)
	}
	pack.WriteByte(b)
	pack.Write(base)
	zw := zlib.NewWriter(pack)
	_, err := zw.Write(data)
	if err := errors.Join(err, zw.Close()); err != nil {
		p.t.Fatalf("deflate: %v", err)
	}
}

// gateTestEmptyPack is what a client sends when the server holds every object already: a header counting none, then its SHA-1.
func gateTestEmptyPack() []byte {
	pack := []byte("PACK\x00\x00\x00\x02\x00\x00\x00\x00")
	sum := sha1.Sum(pack)
	return append(pack, sum[:]...)
}

// gateTestPush sends cmds, the first asking for report-status unless it lists its capabilities itself, then pack through receive-pack, stateless or stateful, and returns the report lines with the service's error.
func gateTestPush(t *testing.T, repo *Repository, stateless bool, hooks ReceivePackHooks, pack []byte, cmds ...string) ([]string, error) {
	t.Helper()
	lines := make([]string, 0, len(cmds)+1)
	for i, cmd := range cmds {
		if i == 0 && !strings.Contains(cmd, "\x00") {
			cmd += "\x00report-status"
		}
		lines = append(lines, cmd+"\n")
	}
	body := append(pktLines(t, append(lines, "")...), pack...)
	var out bytes.Buffer
	var err error
	if stateless {
		err = repo.Stateless(t.Context(), &out, bytes.NewReader(body), GitReceivePack, "", hooks)
	} else {
		// Like a real client, it sends the request only once the advertisement arrived: a refusing native server kills git as soon as it read the request.
		inR, inW := io.Pipe()
		outR, outW := io.Pipe()
		done := make(chan error, 1)
		go func() {
			err := repo.Serve(t.Context(), struct {
				io.Reader
				io.Writer
			}{inR, outW}, GitReceivePack, "", hooks)
			_ = inR.Close()
			_ = outW.CloseWithError(err)
			done <- err
		}()
		rd := bufio.NewReader(outR)
		for {
			l, _, rerr := pktline.ReadLine(rd)
			if rerr != nil {
				t.Fatalf("read advertisement: %v", rerr)
			}
			if l == pktline.Flush {
				break
			}
		}
		go func() {
			_, _ = inW.Write(body)
			_ = inW.Close()
		}()
		// The read ends with the service's error, which done hands over.
		_, _ = out.ReadFrom(rd)
		err = <-done
	}
	if out.Len() == 0 {
		t.Fatalf("receive-pack wrote no report: %v", err)
	}
	return reportLines(t, &out, false), err
}

// gateTestRequireApplied pushes cmds with pack to repo, served from host, and checks every command applied.
func gateTestRequireApplied(t *testing.T, repo *Repository, host string, stateless bool, pack []byte, cmds ...string) {
	t.Helper()
	want := gitLocalRefs(t, host)
	report := []string{"unpack ok"}
	for _, cmd := range cmds {
		fields := strings.Fields(cmd)
		if fields[1] == receive.ZeroHash {
			delete(want, fields[2])
		} else {
			want[fields[2]] = fields[1]
		}
		report = append(report, "ok "+fields[2])
	}
	if got, err := gateTestPush(t, repo, stateless, ReceivePackHooks{}, pack, cmds...); err != nil || !slices.Equal(got, report) {
		t.Fatalf("receive-pack = %v with report %q; want nil and %q", err, got, report)
	}
	requireSameRefs(t, "refs after the push", gitLocalRefs(t, host), want)
}

// requireRefused pushes cmds with pack to a and checks the gate failed every one, moving no ref and running no post-receive.
func (f *gateTestFixture) requireRefused(t *testing.T, stateless bool, pack []byte, cmds ...string) {
	t.Helper()
	before := gitLocalRefs(t, f.aHost)
	var post [][]string
	hooks := ReceivePackHooks{PostReceive: func(_ context.Context, updates []receive.RefUpdate) {
		post = append(post, updateStrings(updates))
	}}
	report := []string{"unpack ok"}
	for _, cmd := range cmds {
		report = append(report, "ng "+strings.Fields(cmd)[2]+" missing necessary objects")
	}
	// The contract leaves the service's error open; the report is what the client sees.
	if got, _ := gateTestPush(t, f.a, stateless, hooks, pack, cmds...); !slices.Equal(got, report) {
		t.Errorf("report = %q, want %q", got, report)
	}
	requireSameRefs(t, "refs after the refused push", gitLocalRefs(t, f.aHost), before)
	if len(post) != 0 {
		t.Errorf("post-receive calls = %q, want none", post)
	}
}

// mixedPush packs two commits sharing a delivered tree, good on c2 and bad on b's cb, and returns the pack with the commands creating refs/heads/good and refs/heads/bad.
func (f *gateTestFixture) mixedPush(t *testing.T) (pack []byte, good, bad string) {
	t.Helper()
	p := gateTestNewPack(t)
	tree := p.tree(reachEntry("new.txt", filemode.Regular, p.blob("new\n")))
	goodHash, badHash := p.commit(tree, f.c2), p.commit(tree, f.cb)
	return p.encode(), receive.ZeroHash + " " + goodHash.String() + " refs/heads/good", receive.ZeroHash + " " + badHash.String() + " refs/heads/bad"
}

// A push may point refs only at what it delivered or what the repository's refs already reach, never at what another repository left in the pool.
func TestReceivePackGate(t *testing.T) {
	forEachGitMode(t, func(t *testing.T, native bool) {
		for _, mode := range []struct {
			name      string
			stateless bool
		}{{"Stateless", true}, {"Stateful", false}} {
			// a reads cb through the pool, so an empty pack alone would adopt it.
			t.Run("AdoptsPoolCommitRefused/"+mode.name, func(t *testing.T) {
				f := gateTestSetup(t, native)
				before := sharedTestPoolObjects(t, f.dataFS)
				f.requireRefused(t, mode.stateless, gateTestEmptyPack(), receive.ZeroHash+" "+f.cb.String()+" refs/heads/stolen")
				if after := sharedTestPoolObjects(t, f.dataFS); !slices.Equal(after, before) {
					t.Errorf("the refused push changed the pooled objects from %q to %q", before, after)
				}
			})

			t.Run("EmptyPackToReachableCommit/"+mode.name, func(t *testing.T) {
				f := gateTestSetup(t, native)
				gateTestRequireApplied(t, f.a, f.aHost, mode.stateless, gateTestEmptyPack(), receive.ZeroHash+" "+f.c1.String()+" refs/heads/again")
			})

			// Both new commits are delivered, so the one walk fails on bad's parent and only judging each alone lets good apply.
			t.Run("MixedDeliveryIsPerCommand/"+mode.name, func(t *testing.T) {
				f := gateTestSetup(t, native)
				pack, good, bad := f.mixedPush(t)
				var post [][]string
				hooks := ReceivePackHooks{PostReceive: func(_ context.Context, updates []receive.RefUpdate) {
					post = append(post, updateStrings(updates))
				}}
				want := []string{"unpack ok", "ok refs/heads/good", "ng refs/heads/bad missing necessary objects"}
				if got, err := gateTestPush(t, f.a, mode.stateless, hooks, pack, good, bad); err != nil || !slices.Equal(got, want) {
					t.Errorf("receive-pack = %v with report %q; want nil and %q", err, got, want)
				}
				requireSameRefs(t, "refs after the push", gitLocalRefs(t, f.aHost), map[string]string{"refs/heads/main": f.c2.String(), "refs/heads/side": f.c2.String(), "refs/heads/good": strings.Fields(good)[1]})
				if len(post) != 1 || !slices.Equal(post[0], []string{good}) {
					t.Errorf("post-receive calls = %q, want one with %q", post, good)
				}
			})

			// git applies the surviving commands even under atomic, since connectivity errors are set before the atomic transaction.
			t.Run("AtomicStillRefusesPerCommand/"+mode.name, func(t *testing.T) {
				f := gateTestSetup(t, native)
				pack, good, bad := f.mixedPush(t)
				deleteSide := f.c2.String() + " " + receive.ZeroHash + " refs/heads/side"
				var post [][]string
				hooks := ReceivePackHooks{PostReceive: func(_ context.Context, updates []receive.RefUpdate) {
					post = append(post, updateStrings(updates))
				}}
				want := []string{"unpack ok", "ok refs/heads/good", "ng refs/heads/bad missing necessary objects", "ok refs/heads/side"}
				if got, err := gateTestPush(t, f.a, mode.stateless, hooks, pack, good+"\x00report-status atomic", bad, deleteSide); err != nil || !slices.Equal(got, want) {
					t.Errorf("receive-pack = %v with report %q; want nil and %q", err, got, want)
				}
				requireSameRefs(t, "refs after the push", gitLocalRefs(t, f.aHost), map[string]string{"refs/heads/main": f.c2.String(), "refs/heads/good": strings.Fields(good)[1]})
				if applied := []string{good, deleteSide}; len(post) != 1 || !slices.Equal(post[0], applied) {
					t.Errorf("post-receive calls = %q, want one with %q", post, applied)
				}
			})
		}

		// Like git, the gate fails only the commands naming objects the push may not adopt; the others still apply.
		t.Run("RefusalIsPerCommand", func(t *testing.T) {
			f := gateTestSetup(t, native)
			stolen := receive.ZeroHash + " " + f.cb.String() + " refs/heads/stolen"
			again := receive.ZeroHash + " " + f.c1.String() + " refs/heads/again"
			deleteSide := f.c2.String() + " " + receive.ZeroHash + " refs/heads/side"
			var post [][]string
			hooks := ReceivePackHooks{PostReceive: func(_ context.Context, updates []receive.RefUpdate) {
				post = append(post, updateStrings(updates))
			}}
			want := []string{"unpack ok", "ng refs/heads/stolen missing necessary objects", "ok refs/heads/again", "ok refs/heads/side"}
			if got, err := gateTestPush(t, f.a, true, hooks, gateTestEmptyPack(), stolen, again, deleteSide); err != nil || !slices.Equal(got, want) {
				t.Errorf("receive-pack = %v with report %q; want nil and %q", err, got, want)
			}
			requireSameRefs(t, "refs after the push", gitLocalRefs(t, f.aHost), map[string]string{"refs/heads/main": f.c2.String(), "refs/heads/again": f.c1.String()})
			if applied := []string{again, deleteSide}; len(post) != 1 || !slices.Equal(post[0], applied) {
				t.Errorf("post-receive calls = %q, want one with %q", post, applied)
			}
		})

		t.Run("ForeignParentRefused", func(t *testing.T) {
			f := gateTestSetup(t, native)
			p := gateTestNewPack(t)
			n := p.commit(p.tree(reachEntry("new.txt", filemode.Regular, p.blob("new\n"))), f.cb)
			f.requireRefused(t, true, p.encode(), receive.ZeroHash+" "+n.String()+" refs/heads/x")
		})

		t.Run("ForeignTreeRefused", func(t *testing.T) {
			f := gateTestSetup(t, native)
			foreign, err := f.b.repo.CommitObject(f.cb)
			if err != nil {
				t.Fatalf("read b's commit: %v", err)
			}
			p := gateTestNewPack(t)
			n := p.commit(foreign.TreeHash, f.c2)
			f.requireRefused(t, true, p.encode(), receive.ZeroHash+" "+n.String()+" refs/heads/x")
		})

		t.Run("MissingBlobRefused", func(t *testing.T) {
			f := gateTestSetup(t, native)
			p := gateTestNewPack(t)
			ghost := plumbing.NewHash(sharedTestBlobHash([]byte("stored nowhere\n")))
			n := p.commit(p.tree(reachEntry("ghost.txt", filemode.Regular, ghost)), f.c2)
			f.requireRefused(t, true, p.encode(), receive.ZeroHash+" "+n.String()+" refs/heads/x")
		})

		t.Run("MissingParentRefused", func(t *testing.T) {
			f := gateTestSetup(t, native)
			p := gateTestNewPack(t)
			n := p.commit(p.tree(reachEntry("new.txt", filemode.Regular, p.blob("new\n"))), plumbing.NewHash(strings.Repeat("e", 40)))
			f.requireRefused(t, true, p.encode(), receive.ZeroHash+" "+n.String()+" refs/heads/x")
		})

		// README.md and data.txt keep c2's blobs, which the pack does not carry.
		t.Run("NewCommitLandsInPool", func(t *testing.T) {
			f := gateTestSetup(t, native)
			p := gateTestNewPack(t)
			blob := p.blob("new\n")
			tree := p.tree(
				reachEntry("README.md", filemode.Regular, plumbing.NewHash(sharedTestBlobHash([]byte(gateTestReadme)))),
				reachEntry("data.txt", filemode.Regular, plumbing.NewHash(sharedTestBlobHash([]byte(gateTestData)))),
				reachEntry("new.txt", filemode.Regular, blob))
			n := p.commit(tree, f.c2)
			gateTestRequireApplied(t, f.a, f.aHost, true, p.encode(), f.c2.String()+" "+n.String()+" refs/heads/main")
			pool := sharedTestPoolObjects(t, f.dataFS)
			for _, h := range []plumbing.Hash{blob, tree, n} {
				if !slices.Contains(pool, h.String()) {
					t.Errorf("pushed object %s is not loose in the pool", h)
				}
			}
			if got := sharedTestLocalFiles(t, f.dataFS, "/org/a.git"); !slices.Equal(got, sharedTestOnlyAlternates) {
				t.Errorf("a's objects = %q, want %q: the push stored objects outside the pool", got, sharedTestOnlyAlternates)
			}
		})

		// The second pack carries only objects the pool already holds, which still count as delivered.
		t.Run("SameContentFromTwoRepositories", func(t *testing.T) {
			f := gateTestSetup(t, native)
			p := gateTestNewPack(t)
			same := p.commit(p.tree(reachEntry("same.txt", filemode.Regular, p.blob("same content\n"))))
			pack := p.encode()
			create := receive.ZeroHash + " " + same.String() + " refs/heads/same"
			gateTestRequireApplied(t, f.a, f.aHost, true, pack, create)
			gateTestRequireApplied(t, f.b, f.bHost, true, pack, create)
		})

		t.Run("ThinPackOnOwnHistory", func(t *testing.T) {
			f := gateTestSetup(t, native)
			clone := filepath.Join(t.TempDir(), "clone")
			sharedTestGit(t, sharedTestGitEnv(t), "", "clone", "--quiet", "--no-local", f.aHost, clone)
			head := redirectTestCommit(t, clone, "data.txt", gateTestData+"a line only the pushed version has\n")
			thin := redirectTestPack(t, clone, true, head, "^"+f.c2.String())
			if !slices.Contains(redirectTestRefDeltaBases(t, thin), sharedTestBlobHash([]byte(gateTestData))) {
				t.Fatal("fixture pack is not thin: no delta against c2's data.txt")
			}
			gateTestRequireApplied(t, f.a, f.aHost, true, thin, f.c2.String()+" "+head+" refs/heads/main")
		})

		// A delta against a blob only b reaches would rebuild b's content in a.
		t.Run("ForeignDeltaBaseRefused", func(t *testing.T) {
			f := gateTestSetup(t, native)
			target := gateTestSecret + "a line of a's own\n"
			p := gateTestNewPack(t)
			n := p.commit(p.tree(reachEntry("copy.txt", filemode.Regular, plumbing.NewHash(sharedTestBlobHash([]byte(target))))), f.c2)
			f.requireRefused(t, true, p.thin(gateTestSecret, target), receive.ZeroHash+" "+n.String()+" refs/heads/x")
		})

		// Copied whole, the base comes out of the pack as b's blob itself, which the push still did not deliver.
		t.Run("ForeignDeltaBaseRebuiltRefused", func(t *testing.T) {
			f := gateTestSetup(t, native)
			p := gateTestNewPack(t)
			n := p.commit(p.tree(reachEntry("copy.txt", filemode.Regular, plumbing.NewHash(sharedTestBlobHash([]byte(gateTestSecret))))), f.c2)
			f.requireRefused(t, true, p.thin(gateTestSecret, gateTestSecret), receive.ZeroHash+" "+n.String()+" refs/heads/x")
		})

		t.Run("AnnotatedTagPush", func(t *testing.T) {
			f := gateTestSetup(t, native)
			p := gateTestNewPack(t)
			tag := p.add(&object.Tag{Name: "v9", Tagger: reachSig, Message: "v9\n", TargetType: plumbing.CommitObject, Target: f.c2})
			gateTestRequireApplied(t, f.a, f.aHost, true, p.encode(), receive.ZeroHash+" "+tag.String()+" refs/tags/v9")
		})

		t.Run("DeleteOnly", func(t *testing.T) {
			f := gateTestSetup(t, native)
			gateTestRequireApplied(t, f.a, f.aHost, true, nil, f.c2.String()+" "+receive.ZeroHash+" refs/heads/side")
		})

		// Push options come between the commands and the pack; native git reads them only where it advertises them.
		t.Run("PushOptionsBeforePack", func(t *testing.T) {
			f := gateTestSetup(t, native)
			runGit(t, f.aHost, "config", "receive.advertisePushOptions", "true")
			p := gateTestNewPack(t)
			n := p.commit(p.tree(reachEntry("new.txt", filemode.Regular, p.blob("new\n"))), f.c2)
			// git send-pack writes each option without a newline.
			body := slices.Concat(pktLines(t, f.c2.String()+" "+n.String()+" refs/heads/main\x00report-status push-options\n", "", "ci.skip", ""), p.encode())
			var out bytes.Buffer
			if err := f.a.Stateless(t.Context(), &out, bytes.NewReader(body), GitReceivePack, "", ReceivePackHooks{}); err != nil {
				t.Fatalf("receive-pack: %v", err)
			}
			if got, want := reportLines(t, &out, false), []string{"unpack ok", "ok refs/heads/main"}; !slices.Equal(got, want) {
				t.Fatalf("report = %q, want %q", got, want)
			}
			requireSameRefs(t, "refs after the push", gitLocalRefs(t, f.aHost), map[string]string{"refs/heads/main": n.String(), "refs/heads/side": f.c2.String()})
		})

		// Every repository reads the pool, so a denied push must not leave its objects there.
		t.Run("PreReceiveDenyLeavesPoolUntouched", func(t *testing.T) {
			f := gateTestSetup(t, native)
			p := gateTestNewPack(t)
			n := p.commit(p.tree(reachEntry("denied.txt", filemode.Regular, p.blob("denied\n"))), f.c2)
			before := sharedTestFiles(t, f.dataFS, sharedTestPool)
			denied := errors.New("policy says no")
			hooks := ReceivePackHooks{PreReceive: func(context.Context, []receive.RefUpdate) error { return denied }}
			if got, _ := gateTestPush(t, f.a, true, hooks, p.encode(), f.c2.String()+" "+n.String()+" refs/heads/main"); !slices.Equal(got, []string{"unpack ok", "ng refs/heads/main " + denied.Error()}) {
				t.Errorf("report = %q, want main refused with %q", got, denied)
			}
			if after := sharedTestFiles(t, f.dataFS, sharedTestPool); !slices.Equal(after, before) {
				t.Errorf("the denied push changed the pool files from %q to %q", before, after)
			}
		})
	})
}

// Without the pool, native git unpacks and checks the push itself, so a thin pack still lands.
func TestReceivePackGateLeavesUnboundToGit(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	setGitBinary(t, gitPath)
	root := t.TempDir()
	repo := sharedTestInit(t, osfs.New(root), "/org/a.git")
	requireGitMode(t, repo, true)
	host := filepath.Join(root, "org", "a.git")
	old := mustCommit(t, repo, "main", "", CommitOperation{Type: CommitOperationAdd, Path: "data.txt", Content: []byte(gateTestData)})
	clone := filepath.Join(t.TempDir(), "clone")
	sharedTestGit(t, sharedTestGitEnv(t), "", "clone", "--quiet", "--no-local", host, clone)
	head := redirectTestCommit(t, clone, "data.txt", gateTestData+"a line only the pushed version has\n")
	thin := redirectTestPack(t, clone, true, head, "^"+old)
	if !slices.Contains(redirectTestRefDeltaBases(t, thin), sharedTestBlobHash([]byte(gateTestData))) {
		t.Fatal("fixture pack is not thin: no delta against the first data.txt")
	}
	gateTestRequireApplied(t, repo, host, true, thin, old+" "+head+" refs/heads/main")
}
