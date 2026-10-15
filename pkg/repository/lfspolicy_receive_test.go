package repository

// Tests for the LFS policy on git-receive-pack: every push runs through both
// engines, and the client must see huggingface.co's report and band-2 bytes
// from each.

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/storage/filesystem"

	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/receive"
)

// privatePolicyHooks makes the policy hook native git runs during the test live, and go, with the test's temporary directory.
func privatePolicyHooks(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	reset := func() {
		policyHooks.Lock()
		policyHooks.dir = ""
		policyHooks.Unlock()
	}
	reset()
	t.Cleanup(reset)
}

// lfsPolicyFixture is a work clone whose commits the policy tests push, and the bare repository every push sequence starts from a copy of.
type lfsPolicyFixture struct {
	work, base string
	main       string // the base's refs/heads/main
}

func newLFSPolicyFixture(t *testing.T) *lfsPolicyFixture {
	t.Helper()
	root := t.TempDir()
	f := &lfsPolicyFixture{work: filepath.Join(root, "work"), base: filepath.Join(root, "base.git")}
	runGit(t, "", "init", "--bare", "--initial-branch=main", f.base)
	initParityWork(t, f.work)
	commitFile(t, f.work, "file.txt", "base\n", "base")
	runGit(t, f.work, "push", "-q", f.base, "main:refs/heads/main", "main:refs/heads/old")
	f.main = strings.TrimSpace(gitOut(t, f.work, "rev-parse", "HEAD"))
	return f
}

// commit commits files over parent in the work clone, a nil content removing the path, and returns the commit.
func (f *lfsPolicyFixture) commit(t *testing.T, parent string, files map[string][]byte) string {
	t.Helper()
	runGit(t, f.work, "checkout", "-q", "--detach", parent)
	for name, data := range files {
		if data == nil {
			runGit(t, f.work, "rm", "-q", "--", name)
			continue
		}
		path := filepath.Join(f.work, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	runGit(t, f.work, "add", "-A")
	runGit(t, f.work, "commit", "-q", "-m", "push")
	return strings.TrimSpace(gitOut(t, f.work, "rev-parse", "HEAD"))
}

// pack is the pack a client sends for revs, rev-list arguments such as a tip and "^" + what the server has.
func (f *lfsPolicyFixture) pack(t *testing.T, revs ...string) []byte {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", "-C", f.work, "pack-objects", "--stdout", "--revs", "-q")
	cmd.Stdin = strings.NewReader(strings.Join(revs, "\n") + "\n")
	pack, err := cmd.Output()
	if err != nil {
		t.Fatalf("pack-objects %q: %v", revs, err)
	}
	return pack
}

// packObjects is the pack a hand-built client may send: exactly objects, whatever they reach.
func (f *lfsPolicyFixture) packObjects(t *testing.T, objects ...string) []byte {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", "-C", f.work, "pack-objects", "--stdout", "-q")
	cmd.Stdin = strings.NewReader(strings.Join(objects, "\n") + "\n")
	pack, err := cmd.Output()
	if err != nil {
		t.Fatalf("pack-objects %q: %v", objects, err)
	}
	return pack
}

// server copies the base repository for one push sequence and opens it.
func (f *lfsPolicyFixture) server(t *testing.T, native bool) (string, *Repository) {
	t.Helper()
	bare := filepath.Join(t.TempDir(), "server.git")
	if err := os.CopyFS(bare, os.DirFS(f.base)); err != nil {
		t.Fatal(err)
	}
	repo, err := Open(osfs.Default, bare)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	requireGitMode(t, repo, native)
	return bare, repo
}

// emptyPack is the pack a client sends when the server already has every object.
func emptyPack() []byte {
	pack := []byte("PACK\x00\x00\x00\x02\x00\x00\x00\x00")
	sum := sha1.Sum(pack)
	return append(pack, sum[:]...)
}

// sameFirstByteBlobs returns two binary blobs of n bytes whose object IDs share a pack index fanout bucket.
func sameFirstByteBlobs(n int) ([]byte, []byte) {
	seen := make(map[byte][]byte)
	for i := 0; ; i++ {
		b := bytes.Repeat([]byte{'a'}, n)
		b[0] = 0
		copy(b[1:], strconv.Itoa(i))
		id := sha1.Sum(append(fmt.Appendf(nil, "blob %d\x00", n), b...))
		if prev, ok := seen[id[0]]; ok {
			return prev, b
		}
		seen[id[0]] = b
	}
}

// packets records every band-2 packet a demultiplexer relays.
type packets [][]byte

func (p *packets) Write(b []byte) (int, error) {
	*p = append(*p, bytes.Clone(b))
	return len(b), nil
}

// bannerPackets is perr's banner as git relays a hook's output: band-2 packets of at most 128 bytes.
func bannerPackets(perr *lfs.PolicyError) [][]byte {
	if perr == nil {
		return nil
	}
	var b bytes.Buffer
	_ = perr.WriteBanner(&b)
	return slices.Collect(slices.Chunk(b.Bytes(), 128))
}

// policyPush is one push and what its client must see: the report, and perr's banner on band 2 (none when nil).
type policyPush struct {
	policy lfs.Policy
	caps   string
	deny   error // PreReceive's answer, which Stateless returns as well
	pack   []byte
	cmds   []string
	want   []string
	perr   *lfs.PolicyError
}

// run pushes p to repo and checks the report, the band-2 packets, the refs and post-receive.
func (p policyPush) run(t *testing.T, bare string, repo *Repository, proto string) {
	t.Helper()
	before := gitLocalRefs(t, bare)
	var post int
	hooks := ReceivePackHooks{Policy: p.policy, PostReceive: func(context.Context, []receive.RefUpdate) { post++ }}
	if p.deny != nil {
		hooks.PreReceive = func(context.Context, []receive.RefUpdate) error { return p.deny }
	}
	var out bytes.Buffer
	if err := repo.Stateless(t.Context(), &out, bytes.NewReader(receiveRequest(t, p.caps, p.pack, p.cmds...)), GitReceivePack, proto, hooks); !errors.Is(err, p.deny) {
		t.Fatalf("Stateless = %v, want %v: a refused push is reported in-protocol", err, p.deny)
	}
	var band2 packets
	if got := reportLinesProgress(t, &out, strings.Contains(p.caps, "side-band-64k"), &band2); !slices.Equal(got, p.want) {
		t.Errorf("report = %q, want %q", got, p.want)
	}
	if want := bannerPackets(p.perr); !slices.EqualFunc(band2, want, bytes.Equal) {
		t.Errorf("band 2 = %q in %d packets, want %q in %d", bytes.Join(band2, nil), len(band2), bytes.Join(want, nil), len(want))
	}
	wantRefs, applied := maps.Clone(before), 0
	for i, cmd := range p.cmds {
		if !strings.HasPrefix(p.want[i+1], "ok ") {
			continue
		}
		applied++
		if f := strings.Fields(cmd); f[1] == receive.ZeroHash {
			delete(wantRefs, f[2])
		} else {
			wantRefs[f[2]] = f[1]
		}
	}
	requireSameRefs(t, "refs after push", gitLocalRefs(t, bare), wantRefs)
	if (post > 0) != (applied > 0) {
		t.Errorf("post-receive ran %d times for %d applied commands", post, applied)
	}
}

// TestReceivePackLFSPolicy pins huggingface.co's git push rules (large beats
// binary across the push, one line per blob, every ref declined) on both engines.
func TestReceivePackLFSPolicy(t *testing.T) {
	privatePolicyHooks(t)
	f := newLFSPolicyFixture(t)
	const large, binary = 10 << 20, 100000
	const caps = "report-status side-band-64k quiet"
	def := lfs.DefaultPolicy()
	fill := func(c byte, n int) []byte { return bytes.Repeat([]byte{c}, n) }
	nulAt := func(n, i int) []byte { b := fill('a', n); b[i] = 0; return b }
	controls := func(n int) []byte { return append(fill(1, n), fill('a', binary+1-n)...) }
	create := func(commit, ref string) string { return receive.ZeroHash + " " + commit + " " + ref }
	report := func(status string, refs ...string) []string {
		lines := []string{"unpack ok"}
		for _, ref := range refs {
			if status == "ok" {
				lines = append(lines, "ok "+ref)
			} else {
				lines = append(lines, "ng "+ref+" "+status)
			}
		}
		return lines
	}
	const declined = "pre-receive hook declined"
	denied := errors.New("policy says no")
	tooLarge := func(limit int64, files ...lfs.OffendingFile) *lfs.PolicyError {
		return &lfs.PolicyError{Kind: lfs.ViolationLarge, Files: files, Limit: limit}
	}
	tooBinary := func(files ...lfs.OffendingFile) *lfs.PolicyError {
		return &lfs.PolicyError{Kind: lfs.ViolationBinary, Files: files}
	}
	file := func(path, ref string) lfs.OffendingFile { return lfs.OffendingFile{Path: path, Ref: ref} }

	type row struct {
		name   string
		pushes []policyPush
	}
	// branch is the row creating refs/heads/<name> with files over main under policy, refused with perr when set.
	branch := func(name string, policy lfs.Policy, files map[string][]byte, perr *lfs.PolicyError) row {
		ref := "refs/heads/" + name
		commit := f.commit(t, f.main, files)
		push := policyPush{policy: policy, caps: caps, pack: f.pack(t, commit, "^"+f.main), cmds: []string{create(commit, ref)}, want: report("ok", ref)}
		if perr != nil {
			push.want, push.perr = report(declined, ref), perr
		}
		return row{name, []policyPush{push}}
	}
	big := fill('a', large+1)
	nulBlob := nulAt(binary+1, 0)
	pointer := []byte("version https://git-lfs.github.com/spec/v1\noid sha256:" + strings.Repeat("ab", 32) + "\nsize 12345\n")
	twinA, twinB := sameFirstByteBlobs(binary + 1)
	rows := []row{
		branch("Large", def, map[string][]byte{"big.txt": big}, tooLarge(large, file("big.txt", "refs/heads/Large"))),
		branch("LargeAtLimit", def, map[string][]byte{"big.txt": fill('a', large)}, nil),
		branch("Binary", def, map[string][]byte{"blob.bin": nulBlob}, tooBinary(file("blob.bin", "refs/heads/Binary"))),
		branch("BinaryAtLimit", def, map[string][]byte{"zeros.bin": make([]byte, binary)}, nil),
		branch("UTF16BOM", def, map[string][]byte{"utf16.txt": append([]byte{0xff, 0xfe}, fill('a', binary)...)}, tooBinary(file("utf16.txt", "refs/heads/UTF16BOM"))),
		branch("ControlBytes", def, map[string][]byte{"ctl.txt": controls(63)}, tooBinary(file("ctl.txt", "refs/heads/ControlBytes"))),
		branch("ControlBytesBelowRatio", def, map[string][]byte{"ctl.txt": controls(62)}, nil),
		branch("NULInSniffWindow", def, map[string][]byte{"nul.txt": nulAt(binary+1, 7999)}, tooBinary(file("nul.txt", "refs/heads/NULInSniffWindow"))),
		branch("NULPastSniffWindow", def, map[string][]byte{"nul.txt": nulAt(binary+1, 8000)}, nil),
		branch("SameBlobTwoPaths", def, map[string][]byte{"a.txt": big, "b.txt": big}, tooLarge(large, file("a.txt", "refs/heads/SameBlobTwoPaths"))),
		branch("TreeOrder", def, map[string][]byte{"big.txt": big, "big2.txt": fill('b', large+1), "dir/sub/big3.txt": fill('c', large+1)},
			tooLarge(large, file("big.txt", "refs/heads/TreeOrder"), file("big2.txt", "refs/heads/TreeOrder"), file("dir/sub/big3.txt", "refs/heads/TreeOrder"))),
		// A subtree's blobs come before the later entries of its parent, and twins sharing a fanout bucket are both found.
		branch("DepthFirst", def, map[string][]byte{"a/twin.bin": twinA, "b.bin": twinB}, tooBinary(file("a/twin.bin", "refs/heads/DepthFirst"), file("b.bin", "refs/heads/DepthFirst"))),
		branch("Pointer", def, map[string][]byte{"model.bin": pointer}, nil),
		branch("PointerUnderTinyLimit", lfs.Policy{MaxRegularFileSize: 100}, map[string][]byte{"model.bin": pointer, "notes.txt": fill('n', 200)}, tooLarge(100, file("notes.txt", "refs/heads/PointerUnderTinyLimit"))),
		branch("PolicyOff", lfs.Policy{}, map[string][]byte{"big.txt": big}, nil),
		branch("Limit5MiB", lfs.Policy{MaxRegularFileSize: 5 << 20}, map[string][]byte{"big.txt": fill('f', 5<<20+1)}, tooLarge(5<<20, file("big.txt", "refs/heads/Limit5MiB"))),
	}

	bad := f.commit(t, f.main, map[string][]byte{"big.txt": big})
	badPack := f.pack(t, bad, "^"+f.main)
	binaryCommit := f.commit(t, f.main, map[string][]byte{"blob.bin": nulBlob})
	clean := f.commit(t, f.main, map[string][]byte{"small.txt": []byte("small\n")})
	tagged := f.commit(t, f.main, map[string][]byte{"big.txt": fill('t', large+1)})
	runGit(t, f.work, "tag", "-a", "bad", tagged, "-m", "bad")
	tag := strings.TrimSpace(gitOut(t, f.work, "rev-parse", "refs/tags/bad"))
	added := f.commit(t, f.main, map[string][]byte{"big.txt": fill('i', large+1)})
	removed := f.commit(t, added, map[string][]byte{"big.txt": nil})
	legacy := f.commit(t, f.main, map[string][]byte{"legacy.bin": fill('L', large+1)})
	afterLegacy := f.commit(t, legacy, map[string][]byte{"small.txt": []byte("small\n")})
	refusedBad := func(ref string) policyPush {
		return policyPush{policy: def, caps: caps, pack: badPack, cmds: []string{create(bad, ref)}, want: report(declined, ref), perr: tooLarge(large, file("big.txt", ref))}
	}
	bypass := f.commit(t, f.main, map[string][]byte{"big.txt": fill('s', large+1)})
	stray := strings.TrimSpace(gitOut(t, f.work, "rev-parse", bypass+":big.txt"))
	// clean's objects and a blob nothing reaches.
	strayPack := f.packObjects(t, append(strings.Fields(gitOut(t, f.work, "rev-list", "--objects", "--no-object-names", clean, "^"+f.main)), stray)...)
	// bypass's commit and tree, leaving its blob to the server.
	bypassPack := f.packObjects(t, bypass, strings.TrimSpace(gitOut(t, f.work, "rev-parse", bypass+"^{tree}")))
	// writeObject writes stdin with hash-object or mktree, so no commit or ref holds the object.
	writeObject := func(stdin []byte, args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "git", append([]string{"-C", f.work}, args...)...)
		cmd.Stdin = bytes.NewReader(stdin)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("git %q: %v", args, err)
		}
		return strings.TrimSpace(string(out))
	}
	noise := make([]byte, 200000)
	_, _ = rand.NewChaCha8([32]byte{}).Read(noise)
	bigBlob := writeObject(noise, "hash-object", "-w", "--stdin")
	bigTree := writeObject([]byte("100644 blob "+bigBlob+"\tbig.bin\n"), "mktree")
	tinyBlob := writeObject([]byte("tiny\n"), "hash-object", "-w", "--stdin")
	tinyTree := writeObject([]byte("100644 blob "+tinyBlob+"\ttiny.txt\n"), "mktree")
	replaced := strings.TrimSpace(gitOut(t, f.work, "commit-tree", bigTree, "-p", f.main, "-m", "replaced"))
	// Refused for the binary blob stored, whatever refs/replace names in place of its tree or of the blob.
	replacedPush := policyPush{
		policy: def, caps: caps, pack: f.packObjects(t, replaced, bigTree, bigBlob), cmds: []string{create(replaced, "refs/heads/replaced")},
		want: report(declined, "refs/heads/replaced"), perr: tooBinary(file("big.bin", "refs/heads/replaced")),
	}
	replaceTree := policyPush{policy: def, caps: caps, pack: f.packObjects(t, tinyTree, tinyBlob), cmds: []string{create(tinyTree, "refs/replace/"+bigTree)}, want: report("ok", "refs/replace/"+bigTree)}
	// 1000 commits adding a file each: 3000 new objects that 50 new tags share.
	var history strings.Builder
	for i := range 1000 {
		content := strconv.Itoa(i) + "\n"
		fmt.Fprintf(&history, "commit refs/heads/history\ncommitter C <c@example.com> %d +0000\ndata 2\nc\n", 1700000000+i)
		if i == 0 {
			fmt.Fprintf(&history, "from %s\n", f.main)
		}
		fmt.Fprintf(&history, "M 644 inline f%d.txt\ndata %d\n%s\n", i, len(content), content)
	}
	fastImport := exec.CommandContext(t.Context(), "git", "-C", f.work, "fast-import", "--quiet")
	fastImport.Stdin = strings.NewReader(history.String())
	if out, err := fastImport.CombinedOutput(); err != nil {
		t.Fatalf("fast-import: %v\n%s", err, out)
	}
	historyTip := strings.TrimSpace(gitOut(t, f.work, "rev-parse", "refs/heads/history"))
	historyObjects := len(strings.Fields(gitOut(t, f.work, "rev-list", "--objects", "--no-object-names", historyTip, "^"+f.main)))
	manyRefs := policyPush{policy: def, caps: caps, pack: f.pack(t, historyTip, "^"+f.main)}
	var tags []string
	for i := range 50 {
		tag := fmt.Sprintf("refs/tags/v%d", i)
		manyRefs.cmds = append(manyRefs.cmds, create(historyTip, tag))
		tags = append(tags, tag)
	}
	manyRefs.want = report("ok", tags...)
	rows = append(rows,
		// The large rule outranks the binary one across refs, whatever the command order.
		row{"LargeOutranksBinary", []policyPush{{
			policy: def, caps: caps, pack: f.pack(t, bad, binaryCommit, "^"+f.main),
			cmds: []string{create(binaryCommit, "refs/heads/B"), create(bad, "refs/heads/A")},
			want: report(declined, "refs/heads/B", "refs/heads/A"), perr: tooLarge(large, file("big.txt", "refs/heads/A")),
		}}},
		row{"AnnotatedTag", []policyPush{{
			policy: def, caps: caps, pack: f.pack(t, tag, "^"+f.main), cmds: []string{create(tag, "refs/tags/bad")},
			want: report(declined, "refs/tags/bad"), perr: tooLarge(large, file("big.txt", "refs/tags/bad")),
		}}},
		row{"CleanBesideBad", []policyPush{{
			policy: def, caps: caps, pack: f.pack(t, clean, bad, "^"+f.main),
			cmds: []string{create(clean, "refs/heads/clean"), create(bad, "refs/heads/bad")},
			want: report(declined, "refs/heads/clean", "refs/heads/bad"), perr: tooLarge(large, file("big.txt", "refs/heads/bad")),
		}}},
		// One line per blob across refs too, under the first ref reaching it.
		row{"SameBlobTwoRefs", []policyPush{{
			policy: def, caps: caps, pack: badPack,
			cmds: []string{create(bad, "refs/heads/one"), create(bad, "refs/heads/two")},
			want: report(declined, "refs/heads/one", "refs/heads/two"), perr: tooLarge(large, file("big.txt", "refs/heads/one")),
		}}},
		// Every pushed commit counts, not only the tip.
		row{"IntermediateCommit", []policyPush{{
			policy: def, caps: caps, pack: f.pack(t, removed, "^"+f.main), cmds: []string{create(removed, "refs/heads/removed")},
			want: report(declined, "refs/heads/removed"), perr: tooLarge(large, file("big.txt", "refs/heads/removed")),
		}}},
		row{"NoSideband", []policyPush{{
			policy: def, caps: "report-status", pack: badPack, cmds: []string{create(bad, "refs/heads/bad")},
			want: report(declined, "refs/heads/bad"),
		}}},
		// Without side-band: native git's connectivity check prints the missing object on band 2 itself.
		row{"MissingTipBesideClean", []policyPush{{
			policy: def, caps: "report-status", pack: f.pack(t, clean, "^"+f.main),
			cmds: []string{create(strings.Repeat("1", 40), "refs/heads/bogus"), create(clean, "refs/heads/clean")},
			want: []string{"unpack ok", "ng refs/heads/bogus missing necessary objects", "ok refs/heads/clean"},
		}}},
		row{"DeleteOnly", []policyPush{{
			policy: def, caps: caps, cmds: []string{f.main + " " + receive.ZeroHash + " refs/heads/old"}, want: report("ok", "refs/heads/old"),
		}}},
		row{"ExistingCommitEmptyPack", []policyPush{{
			policy: def, caps: caps, pack: emptyPack(), cmds: []string{create(f.main, "refs/heads/copy")}, want: report("ok", "refs/heads/copy"),
		}}},
		// A blob the refs already reach is not judged again.
		row{"LegacyBlobStays", []policyPush{
			{caps: caps, pack: f.pack(t, legacy, "^"+f.main), cmds: []string{f.main + " " + legacy + " refs/heads/main"}, want: report("ok", "refs/heads/main")},
			{policy: def, caps: caps, pack: f.pack(t, afterLegacy, "^"+legacy), cmds: []string{legacy + " " + afterLegacy + " refs/heads/main"}, want: report("ok", "refs/heads/main")},
		}},
		// A refused push leaves nothing a later push could point a ref at.
		row{"RefusedPushLeavesNothing", []policyPush{
			refusedBad("refs/heads/bad"),
			refusedBad("refs/heads/bad"),
			{policy: def, caps: "report-status", pack: emptyPack(), cmds: []string{create(bad, "refs/heads/bad")}, want: report("missing necessary objects", "refs/heads/bad")},
		}},
		// A pack identical to one already stored is judged again, and the refusal keeps the stored objects.
		row{"StoredPackJudgedAgain", []policyPush{
			{caps: caps, pack: badPack, cmds: []string{create(bad, "refs/heads/kept")}, want: report("ok", "refs/heads/kept")},
			{caps: caps, cmds: []string{bad + " " + receive.ZeroHash + " refs/heads/kept"}, want: report("ok", "refs/heads/kept")},
			refusedBad("refs/heads/again"),
			{caps: caps, pack: emptyPack(), cmds: []string{create(bad, "refs/heads/again")}, want: report("ok", "refs/heads/again")},
		}},
		// PreReceive refuses before the policy judges, so no banner.
		row{"DeniedBeforePolicy", []policyPush{{
			policy: def, caps: caps, deny: denied, pack: badPack, cmds: []string{create(bad, "refs/heads/bad")}, want: report(denied.Error(), "refs/heads/bad"),
		}}},
		// A denied push leaves nothing a later push could point a ref at.
		row{"DeniedPackThenEmptyPack", []policyPush{
			{policy: def, caps: caps, deny: denied, pack: badPack, cmds: []string{create(bad, "refs/heads/protected")}, want: report(denied.Error(), "refs/heads/protected")},
			{policy: def, caps: "report-status", pack: emptyPack(), cmds: []string{create(bad, "refs/heads/open")}, want: report("missing necessary objects", "refs/heads/open")},
		}},
		// The hook measures the blob stored, not the one refs/replace/<blob> names for it.
		row{"ReplacedBlob", []policyPush{
			{policy: def, caps: caps, pack: f.packObjects(t, tinyBlob), cmds: []string{create(tinyBlob, "refs/replace/"+bigBlob)}, want: report("ok", "refs/replace/"+bigBlob)},
			replacedPush,
		}},
	)

	forEachGitMode(t, func(t *testing.T, native bool) {
		forEachProtocol(t, []string{"", "version=1", "version=2"}, func(t *testing.T, proto string) {
			for _, tc := range rows {
				t.Run(tc.name, func(t *testing.T) {
					bare, repo := f.server(t, native)
					for _, push := range tc.pushes {
						push.run(t, bare, repo, proto)
					}
				})
			}
			// go-git judges a blob no tip reaches with its pack; native git stores it and judges the push that reaches it.
			t.Run("StrayBlobThenReference", func(t *testing.T) {
				bare, repo := f.server(t, native)
				first := policyPush{policy: def, caps: caps, pack: strayPack, cmds: []string{create(clean, "refs/heads/clean")}, want: report(declined, "refs/heads/clean"), perr: tooLarge(large, file(stray, "refs/heads/clean"))}
				// On go-git the blob went with the refused pack, so the push reaches an object it lacks.
				second := policyPush{policy: def, caps: caps, pack: bypassPack, cmds: []string{create(bypass, "refs/heads/bypass")}, want: report(declined, "refs/heads/bypass")}
				if native {
					first.want, first.perr = report("ok", "refs/heads/clean"), nil
					second.perr = tooLarge(large, file("big.txt", "refs/heads/bypass"))
				}
				first.run(t, bare, repo, proto)
				second.run(t, bare, repo, proto)
				if reached := gitOut(t, bare, "rev-list", "--objects", "--all"); strings.Contains(reached, stray) {
					t.Errorf("a ref reaches the oversized blob %s:\n%s", stray, reached)
				}
			})
			// The hook walks the tree stored, not the one refs/replace/<tree> names for it.
			t.Run("ReplacedTree", func(t *testing.T) {
				bare, repo := f.server(t, native)
				replaceTree.run(t, bare, repo, proto)
				replacedPush.run(t, bare, repo, proto)
				// Without replacements, which would walk the harmless tree in place of big.bin's.
				if reached := gitOut(t, bare, "--no-replace-objects", "rev-list", "--objects", "--all"); strings.Contains(reached, bigBlob) {
					t.Errorf("a ref reaches the binary blob %s:\n%s", bigBlob, reached)
				}
			})
			// One walk serves every ref, where a walk per ref took seconds.
			t.Run("ManyRefs", func(t *testing.T) {
				bare, repo := f.server(t, native)
				start := time.Now()
				manyRefs.run(t, bare, repo, proto)
				t.Logf("%d refs over %d new objects: %s", len(manyRefs.cmds), historyObjects, time.Since(start).Round(time.Millisecond))
			})
		})
	})
}

// TestReceivePackLFSPolicyFailsClosed pushes an object go-git cannot decode: the push is refused, never accepted unchecked.
func TestReceivePackLFSPolicyFailsClosed(t *testing.T) {
	setGitBinary(t, "")
	f := newLFSPolicyFixture(t)
	// Packed object by object, so git never parses the tree.
	garbage := filepath.Join(t.TempDir(), "tree")
	if err := os.WriteFile(garbage, []byte("not a tree"), 0o644); err != nil {
		t.Fatal(err)
	}
	tree := strings.TrimSpace(gitOut(t, f.work, "hash-object", "-t", "tree", "-w", "--literally", garbage))
	commit := strings.TrimSpace(gitOut(t, f.work, "commit-tree", tree, "-p", f.main, "-m", "broken"))
	packCmd := exec.CommandContext(t.Context(), "git", "-C", f.work, "pack-objects", "--stdout", "-q")
	packCmd.Stdin = strings.NewReader(commit + "\n" + tree + "\n")
	pack, err := packCmd.Output()
	if err != nil {
		t.Fatalf("pack-objects: %v", err)
	}
	bare, repo := f.server(t, false)
	policyPush{
		policy: lfs.DefaultPolicy(), caps: "report-status side-band-64k quiet", pack: pack,
		cmds: []string{receive.ZeroHash + " " + commit + " refs/heads/broken"},
		want: []string{"unpack ok", "ng refs/heads/broken pre-receive hook declined"},
	}.run(t, bare, repo, "")
}

// packDirFiles lists every file in objects/pack of the repository at path, a pack still being written included.
func packDirFiles(t *testing.T, path string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(path, "objects", "pack", "*"))
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// unlistedPacks is storage that writes packs but cannot list them.
type unlistedPacks struct {
	*filesystem.Storage
	err error
}

func (s unlistedPacks) ObjectPacks() ([]plumbing.Hash, error) { return nil, s.err }

// TestReceivePackLFSPolicyUnlistedPacks unpacks a push into storage whose packs cannot be listed: discard could not
// drop the pack were the push refused, so none is stored and the unpack fails.
func TestReceivePackLFSPolicyUnlistedPacks(t *testing.T) {
	f := newLFSPolicyFixture(t)
	commit := f.commit(t, f.main, map[string][]byte{"small.txt": []byte("small\n")})
	errList := errors.New("cannot list packs")
	pushed := newPushedPack(unlistedPacks{newStorer(osfs.Default, f.base, cache.NewObjectLRUDefault()), errList})
	before := packDirFiles(t, f.base)
	// As applyPush unpacks a push.
	if err := packfile.UpdateObjectStorage(pushed, bytes.NewReader(f.pack(t, commit, "^"+f.main))); !errors.Is(err, errList) {
		t.Errorf("UpdateObjectStorage = %v, want PackfileWriter's %v", err, errList)
	}
	if after := packDirFiles(t, f.base); !slices.Equal(after, before) {
		t.Errorf("objects/pack holds %q, want %q", after, before)
	}
}

// untrackedPacks is storage that writes packs through an untrackedWriter.
type untrackedPacks struct{ *filesystem.Storage }

func (s untrackedPacks) PackfileWriter() (io.WriteCloser, error) {
	w, err := s.Storage.PackfileWriter()
	if err != nil {
		return nil, err
	}
	return untrackedWriter{w}, nil
}

// untrackedWriter hides the pack writer it wraps from pushedPack, which then cannot learn the pack's hash and index.
type untrackedWriter struct{ io.WriteCloser }

// TestReceivePackLFSPolicyUntrackedWriter unpacks a push into storage whose pack writer pushedPack cannot see into:
// discard could not drop the pack were the push refused, so none is stored and the unpack fails.
func TestReceivePackLFSPolicyUntrackedWriter(t *testing.T) {
	f := newLFSPolicyFixture(t)
	commit := f.commit(t, f.main, map[string][]byte{"small.txt": []byte("small\n")})
	st := untrackedPacks{newStorer(osfs.Default, f.base, cache.NewObjectLRUDefault())}
	writer := fmt.Sprintf("%T", untrackedWriter{})
	before := packDirFiles(t, f.base)
	w, err := newPushedPack(st).PackfileWriter()
	if w != nil {
		_ = w.Close()
		t.Errorf("PackfileWriter returned a %T, want none", w)
	}
	if err == nil || !strings.Contains(err.Error(), writer) {
		t.Errorf("PackfileWriter = %v, want an error naming %s", err, writer)
	}
	// As applyPush unpacks a push.
	if err := packfile.UpdateObjectStorage(newPushedPack(st), bytes.NewReader(f.pack(t, commit, "^"+f.main))); err == nil || !strings.Contains(err.Error(), writer) {
		t.Errorf("UpdateObjectStorage = %v, want PackfileWriter's error naming %s", err, writer)
	}
	// Neither a pack nor the temporary file of a writer left open.
	if after := packDirFiles(t, f.base); !slices.Equal(after, before) {
		t.Errorf("objects/pack holds %q, want %q", after, before)
	}
}

// TestReceivePackLFSPolicyConcurrentPushes pushes to one repository two at a time through go-git: under the policy the
// pushes take turns, so a refused push never deletes a pack an accepted one relies on.
func TestReceivePackLFSPolicyConcurrentPushes(t *testing.T) {
	setGitBinary(t, "")
	f := newLFSPolicyFixture(t)
	clean := f.commit(t, f.main, map[string][]byte{"small.txt": []byte("small\n")})
	cleanPack := f.pack(t, clean, "^"+f.main)
	bad := f.commit(t, f.main, map[string][]byte{"big.txt": bytes.Repeat([]byte{'a'}, 1<<10+1)})
	badPack := f.pack(t, bad, "^"+f.main)
	denied := errors.New("policy says no")
	hooks := ReceivePackHooks{Policy: lfs.Policy{MaxRegularFileSize: 1 << 10}, PreReceive: func(_ context.Context, updates []receive.RefUpdate) error {
		if updates[0].RefName() == "refs/heads/protected" {
			return denied
		}
		return nil
	}}
	type push struct {
		pack        []byte
		commit, ref string
		status      string // "ok", or why the ref is refused
	}
	for _, tc := range []struct {
		name   string
		pushes [2]push
	}{
		// The later push finds the pack stored already.
		{"SamePack", [2]push{{cleanPack, clean, "refs/heads/a", "ok"}, {cleanPack, clean, "refs/heads/b", "ok"}}},
		// The denied push drops the very pack the accepted one stores.
		{"SamePackDenied", [2]push{{cleanPack, clean, "refs/heads/protected", denied.Error()}, {cleanPack, clean, "refs/heads/open", "ok"}}},
		{"RefusedBesideAccepted", [2]push{{badPack, bad, "refs/heads/bad", preReceiveDeclined}, {cleanPack, clean, "refs/heads/clean", "ok"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reqs [2][]byte
			for i, p := range tc.pushes {
				reqs[i] = receiveRequest(t, "report-status", p.pack, receive.ZeroHash+" "+p.commit+" "+p.ref)
			}
			for round := range 20 {
				bare, repo := f.server(t, false)
				want := gitLocalRefs(t, bare)
				var outs [2]bytes.Buffer
				var errs [2]error
				var wg sync.WaitGroup
				for i := range reqs {
					wg.Go(func() {
						errs[i] = repo.Stateless(t.Context(), &outs[i], bytes.NewReader(reqs[i]), GitReceivePack, "", hooks)
					})
				}
				wg.Wait()
				for i, p := range tc.pushes {
					wantErr, line := error(nil), "ok "+p.ref
					if p.status == denied.Error() {
						wantErr = denied
					}
					if p.status != "ok" {
						line = "ng " + p.ref + " " + p.status
					}
					if !errors.Is(errs[i], wantErr) {
						t.Errorf("round %d: %s: Stateless = %v, want %v", round, p.ref, errs[i], wantErr)
					}
					if got := reportLines(t, &outs[i], false); !slices.Equal(got, []string{"unpack ok", line}) {
						t.Errorf("round %d: report = %q, want %q", round, got, line)
					}
					if p.status != "ok" {
						continue
					}
					want[p.ref] = p.commit
					if err := repo.repo.Storer.HasEncodedObject(plumbing.NewHash(p.commit)); err != nil {
						t.Errorf("round %d: the commit %s points at: %v", round, p.ref, err)
					}
					// The pack's trailing checksum names its file.
					if _, err := os.Stat(filepath.Join(bare, "objects", "pack", fmt.Sprintf("pack-%x.pack", p.pack[len(p.pack)-20:]))); err != nil {
						t.Errorf("round %d: the pack of %s: %v", round, p.ref, err)
					}
				}
				requireSameRefs(t, fmt.Sprintf("round %d", round), gitLocalRefs(t, bare), want)
				gitFsck(t, bare)
			}
		})
	}
}

func TestLFSPolicyHooks(t *testing.T) {
	privatePolicyHooks(t)
	ctx := t.Context()
	requireHook := func(t *testing.T, dir string) {
		t.Helper()
		if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o700 {
			t.Fatalf("hooks directory %s: %v; want it private", dir, err)
		}
		hook := filepath.Join(dir, "pre-receive")
		fi, err := os.Stat(hook)
		if err != nil || fi.Mode().Perm() != 0o755 {
			t.Fatalf("hook %s: %v; want mode 0755", hook, err)
		}
		if data, err := os.ReadFile(hook); err != nil || !bytes.Equal(data, preReceiveScript) {
			t.Fatalf("hook %s: %v; want the embedded script", hook, err)
		}
	}
	dir, err := lfsPolicyHooks(ctx)
	if err != nil {
		t.Fatalf("lfsPolicyHooks: %v", err)
	}
	requireHook(t, dir)
	if again, err := lfsPolicyHooks(ctx); err != nil || again != dir {
		t.Fatalf("lfsPolicyHooks again = %q, %v; want %q", again, err, dir)
	}
	// Whatever altered or removed the hook, a fresh directory replaces it.
	for _, spoil := range []struct {
		name string
		fn   func(hook string) error
	}{
		{"Altered", func(hook string) error { return os.WriteFile(hook, []byte("#!/bin/sh\nexit 0\n"), 0o755) }},
		{"NotExecutable", func(hook string) error { return os.Chmod(hook, 0o644) }},
		{"Removed", func(hook string) error { return os.RemoveAll(filepath.Dir(hook)) }},
	} {
		t.Run(spoil.name, func(t *testing.T) {
			if err := spoil.fn(filepath.Join(dir, "pre-receive")); err != nil {
				t.Fatal(err)
			}
			fresh, err := lfsPolicyHooks(ctx)
			if err != nil || fresh == dir {
				t.Fatalf("lfsPolicyHooks = %q, %v; want a new directory", fresh, err)
			}
			requireHook(t, fresh)
			dir = fresh
		})
	}
	t.Run("RelativeTMPDIR", func(t *testing.T) {
		policyHooks.Lock()
		policyHooks.dir = ""
		policyHooks.Unlock()
		t.Chdir(t.TempDir())
		if err := os.Mkdir("tmp", 0o700); err != nil {
			t.Fatal(err)
		}
		t.Setenv("TMPDIR", "tmp")
		got, err := lfsPolicyHooks(ctx)
		if err != nil || !filepath.IsAbs(got) {
			t.Fatalf("lfsPolicyHooks = %q, %v; want an absolute directory, as git resolves a relative hooks path against the repository", got, err)
		}
		requireHook(t, got)
	})
	t.Run("CannotRun", func(t *testing.T) {
		policyHooks.Lock()
		policyHooks.dir = ""
		policyHooks.Unlock()
		t.Setenv("PATH", "")
		if got, err := lfsPolicyHooks(ctx); err == nil {
			t.Fatalf("lfsPolicyHooks = %q, nil; want an error for a hook that cannot run", got)
		}
	})
}

// TestLFSPolicyHookEnv pins what native receive-pack gets: core.hooksPath and the HFD_* variables, only under a policy.
func TestLFSPolicyHookEnv(t *testing.T) {
	privatePolicyHooks(t)
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	dir := t.TempDir()
	log, bin := filepath.Join(dir, "receive-pack.env"), filepath.Join(dir, "git")
	script := `#!/bin/sh
if [ "$1" = receive-pack ]; then
	{
		i=0
		while [ "$i" -lt "${GIT_CONFIG_COUNT:-0}" ]; do
			eval "printf 'config %s=%s\n' \"\$GIT_CONFIG_KEY_$i\" \"\$GIT_CONFIG_VALUE_$i\""
			i=$((i + 1))
		done
		for v in HFD_GIT HFD_LFS_MAX_REGULAR_SIZE HFD_LFS_MAX_BINARY_SIZE HFD_LFS_LARGE_BANNER HFD_LFS_BINARY_BANNER; do
			eval "printf '%s=%s\n' \"\$v\" \"\${$v-unset}\""
		done
	} >'` + log + `'
fi
exec '` + gitPath + `' "$@"
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	setGitBinary(t, bin)
	f := newLFSPolicyFixture(t)
	commit := f.commit(t, f.main, map[string][]byte{"small.txt": []byte("small\n")})
	push := policyPush{caps: "report-status", pack: f.pack(t, commit, "^"+f.main), cmds: []string{receive.ZeroHash + " " + commit + " refs/heads/small"}, want: []string{"unpack ok", "ok refs/heads/small"}}

	t.Run("Policy", func(t *testing.T) {
		push.policy = lfs.Policy{MaxRegularFileSize: 5 << 20, MaxBinaryFileSize: 1000}
		bare, repo := f.server(t, true)
		push.run(t, bare, repo, "")
		env, err := os.ReadFile(log)
		if err != nil {
			t.Fatal(err)
		}
		policyHooks.Lock()
		hooks := policyHooks.dir
		policyHooks.Unlock()
		large := (&lfs.PolicyError{Kind: lfs.ViolationLarge, Limit: 5 << 20}).Banner()
		binary := (&lfs.PolicyError{Kind: lfs.ViolationBinary}).Banner()
		for _, want := range []string{
			"config core.hooksPath=" + hooks + "\n",
			"HFD_GIT=" + bin + "\n",
			"HFD_LFS_MAX_REGULAR_SIZE=5242880\n",
			"HFD_LFS_MAX_BINARY_SIZE=1000\n",
			"HFD_LFS_LARGE_BANNER=" + strings.Join(large, "\n") + "\n",
			"HFD_LFS_BINARY_BANNER=" + strings.Join(binary, "\n") + "\n",
		} {
			if hooks == "" || !strings.Contains(string(env), want) {
				t.Errorf("receive-pack environment lacks %q:\n%s", want, env)
			}
		}
	})
	t.Run("NoPolicy", func(t *testing.T) {
		push.policy = lfs.Policy{}
		bare, repo := f.server(t, true)
		push.run(t, bare, repo, "")
		env, err := os.ReadFile(log)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(env), "core.hooksPath") || strings.Count(string(env), "=unset\n") != 5 {
			t.Errorf("receive-pack environment without a policy:\n%s\nwant no hooks path and no HFD_* variable", env)
		}
	})
}

// TestLFSPolicyHookToolFailureDeclines fails one git command the native hook runs: the push is declined, never let through unjudged.
func TestLFSPolicyHookToolFailureDeclines(t *testing.T) {
	privatePolicyHooks(t)
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	f := newLFSPolicyFixture(t)
	small := f.commit(t, f.main, map[string][]byte{"small.txt": []byte("small\n")})
	// Text over the binary limit, which the hook reads to judge.
	text := f.commit(t, f.main, map[string][]byte{"text.txt": bytes.Repeat([]byte{'t'}, 100001)})
	big := f.commit(t, f.main, map[string][]byte{"big.txt": bytes.Repeat([]byte{'b'}, 10<<20+1)})
	for _, tc := range []struct {
		name   string
		fails  string // a case pattern matching the arguments of the git command that fails
		commit string
	}{
		{"TipCheck", "'cat-file --batch-check=%(objectname)'", small},
		{"Walk", "'rev-list --objects --stdin --not --all'", small},
		{"TypeCheck", "'cat-file --batch-check=%(objectname) %(objecttype) %(objectsize)'", small},
		{"BlobRead", "'cat-file blob '*", text},
		// Naming the refused blob's ref walks each ref on its own.
		{"RefWalk", "'rev-list --objects '[0-9a-f]*", big},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bin := filepath.Join(t.TempDir(), "git")
			script := "#!/bin/sh\ncase \"$*\" in " + tc.fails + ") exit 1 ;; esac\nexec '" + gitPath + "' \"$@\"\n"
			if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			setGitBinary(t, bin)
			bare, repo := f.server(t, true)
			ref := "refs/heads/" + tc.name
			policyPush{
				policy: lfs.DefaultPolicy(), caps: "report-status side-band-64k quiet", pack: f.pack(t, tc.commit, "^"+f.main),
				cmds: []string{receive.ZeroHash + " " + tc.commit + " " + ref}, want: []string{"unpack ok", "ng " + ref + " pre-receive hook declined"},
			}.run(t, bare, repo, "")
		})
	}
}
