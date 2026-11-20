package repository

// Tests for Repack, NeedsRepack and ReusablePack and the pack layout GC now keeps.

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/revlist"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	s3fs "github.com/wzshiming/go-billy-s3fs"

	"github.com/matrixhub-ai/hfd/pkg/receive"
)

// rpFixture is a go-git repository whose refs reach known objects.
type rpFixture struct {
	fs     billy.Filesystem
	path   string
	repo   *Repository
	main   plumbing.Hash   // tip of refs/heads/main
	tips   []plumbing.Hash // targets of HEAD, refs/heads/* and refs/tags/*, ascending
	others []plumbing.Hash // targets of every other ref
}

// rpCommit adds name with content on branch through the hfd API and returns the new tip.
func rpCommit(t *testing.T, repo *Repository, branch, name string, content []byte) plumbing.Hash {
	t.Helper()
	commit, err := repo.CreateCommit(t.Context(), branch, "add "+name, "Test", "test@example.com",
		[]CommitOperation{{Type: CommitOperationAdd, Path: name, Content: content}}, "")
	if err != nil {
		t.Fatalf("commit %s on %s: %v", name, branch, err)
	}
	return plumbing.NewHash(commit)
}

func rpSetRef(t *testing.T, repo *Repository, name plumbing.ReferenceName, h plumbing.Hash) {
	t.Helper()
	if err := repo.repo.Storer.SetReference(plumbing.NewHashReference(name, h)); err != nil {
		t.Fatalf("set %s: %v", name, err)
	}
}

// rpDangling commits name on top of base and leaves no ref reaching the commit.
func rpDangling(t *testing.T, repo *Repository, base plumbing.Hash, name string) plumbing.Hash {
	t.Helper()
	tmp := plumbing.NewBranchReferenceName("rp-tmp")
	rpSetRef(t, repo, tmp, base)
	commit := rpCommit(t, repo, "rp-tmp", name, randomBytes(t, 16<<10))
	if err := repo.repo.Storer.RemoveReference(tmp); err != nil {
		t.Fatalf("remove %s: %v", tmp, err)
	}
	return commit
}

// rpTag creates the annotated tag name on target and returns the tag object.
func rpTag(t *testing.T, repo *Repository, name string, target plumbing.Hash) plumbing.Hash {
	t.Helper()
	ref, err := repo.repo.CreateTag(name, target, &git.CreateTagOptions{
		Tagger:  &object.Signature{Name: "Test", Email: "test@example.com", When: time.Now()},
		Message: name,
	})
	if err != nil {
		t.Fatalf("create annotated tag %s: %v", name, err)
	}
	return ref.Hash()
}

// rpMixedRepo builds main, dev, annotated tag v1 and, if pr, refs/pr/1, all loose.
func rpMixedRepo(t *testing.T, fs billy.Filesystem, path string, pr bool) *rpFixture {
	t.Helper()
	repo, err := Init(t.Context(), fs, path, "main")
	if err != nil {
		t.Fatalf("init repository: %v", err)
	}
	first := rpCommit(t, repo, "main", "file.txt", []byte("one\n"))
	shared := []byte("held by main's history, by no tip's tree\n")
	rpCommit(t, repo, "main", "shared.txt", shared)
	mustCommit(t, repo, "main", "", CommitOperation{Type: CommitOperationDelete, Path: "shared.txt"})
	f := &rpFixture{fs: fs, path: path, repo: repo, main: rpCommit(t, repo, "main", "main.bin", randomBytes(t, 32<<10))}
	rpSetRef(t, repo, plumbing.NewBranchReferenceName("dev"), first)
	f.tips = []plumbing.Hash{f.main, rpCommit(t, repo, "dev", "dev.bin", randomBytes(t, 16<<10)), rpTag(t, repo, "v1", first)}
	plumbing.HashesSort(f.tips)
	if pr {
		// The tips reach copy.txt's blob only through an older main commit, so it pins B minus A.
		tmp := plumbing.NewBranchReferenceName("rp-tmp")
		rpSetRef(t, repo, tmp, f.main)
		rpCommit(t, repo, "rp-tmp", "pr.bin", randomBytes(t, 16<<10))
		f.others = []plumbing.Hash{rpCommit(t, repo, "rp-tmp", "copy.txt", shared)}
		if err := repo.repo.Storer.RemoveReference(tmp); err != nil {
			t.Fatalf("remove %s: %v", tmp, err)
		}
		rpSetRef(t, repo, "refs/pr/1", f.others[0])
	}
	return f
}

// rpCommitObjects returns commit, its root tree and the blob at file.
func rpCommitObjects(t *testing.T, repo *Repository, commit plumbing.Hash, file string) []plumbing.Hash {
	t.Helper()
	c, err := repo.repo.CommitObject(commit)
	if err != nil {
		t.Fatalf("read commit %s: %v", commit, err)
	}
	tree, err := c.Tree()
	if err != nil {
		t.Fatalf("read the tree of %s: %v", commit, err)
	}
	blob, err := tree.File(file)
	if err != nil {
		t.Fatalf("read %s of %s: %v", file, commit, err)
	}
	return []plumbing.Hash{c.Hash, tree.Hash, blob.Hash}
}

// rpRefusedPush pushes a root commit that pre-receive refuses after receive-pack stored its pack.
func rpRefusedPush(t *testing.T, repo *Repository) (pack plumbing.Hash, objects []plumbing.Hash) {
	t.Helper()
	src, err := Init(t.Context(), memfs.New(), "/src.git", "main")
	if err != nil {
		t.Fatalf("init the push source: %v", err)
	}
	commit := rpCommit(t, src, "main", "refused.bin", randomBytes(t, 16<<10))
	objects = rpCommitObjects(t, src, commit, "refused.bin")
	var data bytes.Buffer
	if pack, err = packfile.NewEncoder(&data, src.repo.Storer, false).Encode(objects, 10); err != nil {
		t.Fatalf("encode the pushed pack: %v", err)
	}
	refused := errors.New("refused by policy")
	hooks := ReceivePackHooks{PreReceive: func(context.Context, []receive.RefUpdate) error { return refused }}
	body := append(pktLines(t, receive.ZeroHash+" "+commit.String()+" refs/heads/refused\x00report-status\n", ""), data.Bytes()...)
	var out bytes.Buffer
	if err := repo.Stateless(t.Context(), &out, bytes.NewReader(body), GitReceivePack, "", hooks); !errors.Is(err, refused) {
		t.Fatalf("receive-pack = %v, want the pre-receive refusal", err)
	}
	return pack, objects
}

// rpManifest is objects/info/hfd-packs as the contract's format reads it.
type rpManifest struct {
	raw             []byte
	reusable, rest  plumbing.Hash
	reusableSize    int64
	reusableObjects uint32
	restSize        int64
	tips            []string
}

// rpReadManifest parses the manifest of f, failing the test on anything the format does not allow.
func rpReadManifest(t *testing.T, f *rpFixture) rpManifest {
	t.Helper()
	raw, err := util.ReadFile(f.fs, filepath.Join(f.path, packManifestPath))
	if err != nil {
		t.Fatalf("read the Repack manifest %s: %v", packManifestPath, err)
	}
	m := rpManifest{raw: raw}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	if lines[0] != "hfd-packs 1" {
		t.Fatalf("manifest %q lacks the hfd-packs 1 header", raw)
	}
	count := map[string]int{}
	for _, line := range lines[1:] {
		fields := strings.Split(line, " ")
		count[fields[0]]++
		var err error
		switch {
		case fields[0] == "reusable" && len(fields) == 4:
			m.reusable = rpHex(t, fields[1])
			var n uint64
			if m.reusableSize, err = strconv.ParseInt(fields[2], 10, 64); err == nil {
				n, err = strconv.ParseUint(fields[3], 10, 32)
				m.reusableObjects = uint32(n)
			}
		case fields[0] == "rest" && len(fields) == 3:
			m.rest = rpHex(t, fields[1])
			m.restSize, err = strconv.ParseInt(fields[2], 10, 64)
		case fields[0] == "others" && len(fields) == 2:
			if sum, decodeErr := hex.DecodeString(fields[1]); decodeErr != nil || len(sum) != 32 {
				err = fmt.Errorf("not a sha256 digest: %v", decodeErr)
			}
		case fields[0] == "tip" && len(fields) == 2:
			m.tips = append(m.tips, rpHex(t, fields[1]).String())
		default:
			err = errors.New("not a manifest line")
		}
		if err != nil {
			t.Fatalf("manifest line %q: %v", line, err)
		}
	}
	if count["reusable"] > 1 || count["rest"] > 1 || count["others"] != 1 {
		t.Fatalf("manifest %q: want at most one reusable and one rest line and exactly one others line", raw)
	}
	return m
}

// rpHex parses a full object hash.
func rpHex(t *testing.T, s string) plumbing.Hash {
	t.Helper()
	h, ok := plumbing.FromHex(s)
	if !ok || len(s) != 40 {
		t.Fatalf("%q is not an object hash", s)
	}
	return h
}

// rpLayout requires objects/pack to hold exactly the manifest's packs plus extra, each recorded at its size on disk.
func rpLayout(t *testing.T, f *rpFixture, extra ...plumbing.Hash) rpManifest {
	t.Helper()
	m := rpReadManifest(t, f)
	recorded := map[string]int64{}
	if !m.reusable.IsZero() {
		recorded[m.reusable.String()] = m.reusableSize
	}
	if !m.rest.IsZero() {
		recorded[m.rest.String()] = m.restSize
	}
	var want []string
	for h := range recorded {
		want = append(want, "pack-"+h+".pack")
	}
	for _, h := range extra {
		want = append(want, "pack-"+h.String()+".pack")
	}
	slices.Sort(want)
	got := packFiles(t, f.fs, f.path)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("objects/pack holds %v, want the manifest's packs and those left behind: %v", got, want)
	}
	for h, size := range recorded {
		info, err := f.fs.Stat(filepath.Join(f.path, "objects", "pack", "pack-"+h+".pack"))
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != size {
			t.Errorf("manifest records pack-%s.pack at %d bytes, the file has %d", h, size, info.Size())
		}
	}
	return m
}

// rpPackObjects lists the objects the index of pack names, as sorted hex.
func rpPackObjects(t *testing.T, f *rpFixture, pack plumbing.Hash) []string {
	t.Helper()
	objects, err := packIndexObjects(f.fs, filepath.Join(f.path, "objects", "pack", "pack-"+pack.String()+".idx"), pack)
	if err != nil {
		t.Fatalf("read the index of pack %s: %v", pack, err)
	}
	return rpHexes(objects)
}

// rpReachable lists what revlist reaches from wants through a fresh storer, as sorted hex.
func rpReachable(t *testing.T, f *rpFixture, wants []plumbing.Hash) []string {
	t.Helper()
	st := newStorer(f.fs, f.path, cache.NewObjectLRUDefault())
	defer func() { _ = st.Close() }()
	objects, err := revlist.Objects(st, wants, nil)
	if err != nil {
		t.Fatalf("revlist from %v: %v", rpStrings(wants), err)
	}
	return rpHexes(objects)
}

// rpPackFiles records the files under objects/pack whose names start with prefix.
func rpPackFiles(t *testing.T, f *rpFixture, prefix string) map[string]fileState {
	t.Helper()
	files := snapshotFiles(t, f.fs, filepath.Join(f.path, "objects", "pack"))
	maps.DeleteFunc(files, func(name string, s fileState) bool {
		return s.mode.IsDir() || !strings.HasPrefix(filepath.Base(name), prefix)
	})
	return files
}

// rpChanged names the files added, removed or altered between two snapshots.
func rpChanged(before, after map[string]fileState) []string {
	var changed []string
	for name, s := range before {
		if a, ok := after[name]; !ok || a != s {
			changed = append(changed, name)
		}
	}
	for name := range after {
		if _, ok := before[name]; !ok {
			changed = append(changed, name)
		}
	}
	slices.Sort(changed)
	return changed
}

func rpStrings(hs []plumbing.Hash) []string {
	s := make([]string, len(hs))
	for i, h := range hs {
		s[i] = h.String()
	}
	return s
}

func rpHexes(hs []plumbing.Hash) []string {
	s := rpStrings(hs)
	slices.Sort(s)
	return slices.Compact(s)
}

// rpRepack runs Repack without grace.
func rpRepack(t *testing.T, repo *Repository) {
	t.Helper()
	if err := repo.Repack(t.Context(), time.Time{}); err != nil {
		t.Fatalf("Repack: %v", err)
	}
}

func rpNeedsRepack(t *testing.T, repo *Repository, want bool, when string) {
	t.Helper()
	if got, err := repo.NeedsRepack(t.Context()); err != nil || got != want {
		t.Fatalf("NeedsRepack %s = %t, %v; want %t, nil", when, got, err, want)
	}
}

// rpWant is the GC result for deleting exactly objects, sized through a fresh storer.
func rpWant(t *testing.T, f *rpFixture, objects []plumbing.Hash) GCResult {
	t.Helper()
	st := newStorer(f.fs, f.path, cache.NewObjectLRUDefault())
	defer func() { _ = st.Close() }()
	want := GCResult{DeletedObjects: objects}
	for _, h := range objects {
		size, err := st.EncodedObjectSize(h)
		if err != nil {
			t.Fatalf("size of %s: %v", h, err)
		}
		want.DeletedBytes += size
	}
	return want
}

// rpRequireDeleted compares the objects and bytes a GC reports deleted with want.
func rpRequireDeleted(t *testing.T, label string, got GCResult, err error, want GCResult) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if g, w := rpHexes(got.DeletedObjects), rpHexes(want.DeletedObjects); !slices.Equal(g, w) || got.DeletedBytes != want.DeletedBytes {
		t.Errorf("%s deleted %v (%d bytes), want %v (%d bytes)", label, g, got.DeletedBytes, w, want.DeletedBytes)
	}
}

// rpRequireReusable checks every ReusablePack field and both files it opens against the disk.
func rpRequireReusable(t *testing.T, f *rpFixture, rp *ReusablePack, err error) {
	t.Helper()
	if err != nil || rp == nil {
		t.Fatalf("ReusablePack = %v, %v; want the manifest's reusable pack", rp, err)
	}
	if m := rpReadManifest(t, f); rp.Hash.String() != m.reusable.String() {
		t.Fatalf("ReusablePack.Hash = %s, want the manifest's reusable pack %s", rp.Hash, m.reusable)
	}
	base := filepath.Join(f.path, "objects", "pack", "pack-"+rp.Hash.String())
	pack, err := util.ReadFile(f.fs, base+".pack")
	if err != nil {
		t.Fatalf("read the reusable pack: %v", err)
	}
	idx, err := util.ReadFile(f.fs, base+".idx")
	if err != nil {
		t.Fatalf("read the reusable pack's index: %v", err)
	}
	if len(pack) < 12+rp.Hash.Size() || string(pack[:4]) != "PACK" {
		t.Fatalf("pack-%s.pack is not a pack", rp.Hash)
	}
	count := binary.BigEndian.Uint32(pack[8:12])
	if rp.Size != int64(len(pack)) || rp.Objects != count || len(rpPackObjects(t, f, rp.Hash)) != int(count) {
		t.Errorf("ReusablePack Size = %d, Objects = %d; want the file's %d bytes and the %d objects its header and index count", rp.Size, rp.Objects, len(pack), count)
	}
	if got, want := rpStrings(rp.Tips), rpStrings(f.tips); !slices.Equal(got, want) {
		t.Errorf("ReusablePack.Tips = %v, want R's targets ascending %v", got, want)
	}
	file, err := rp.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = file.Close() }()
	if got, err := io.ReadAll(file); err != nil || !bytes.Equal(got, pack) {
		t.Errorf("Open read %d bytes, %v; want the %d-byte pack through its trailer", len(got), err, len(pack))
	}
	trailer := make([]byte, rp.Hash.Size())
	if n, err := file.ReadAt(trailer, rp.Size-int64(len(trailer))); n != len(trailer) || (err != nil && !errors.Is(err, io.EOF)) || !bytes.Equal(trailer, rp.Hash.Bytes()) {
		t.Errorf("ReadAt of the trailer through Open = %x, %v; want %s", trailer[:n], err, rp.Hash)
	}
	index, err := rp.OpenIndex()
	if err != nil {
		t.Fatalf("OpenIndex: %v", err)
	}
	defer func() { _ = index.Close() }()
	if got, err := io.ReadAll(index); err != nil || !bytes.Equal(got, idx) {
		t.Errorf("OpenIndex read %d bytes, %v; want the %d-byte index", len(got), err, len(idx))
	}
}

// Repack leaves exactly the reusable pack, plus a rest pack when another ref reaches more, and drops the loose objects they cover.
func TestReusablePackLayout(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pr    bool
		packs int
	}{{"MixedRefs", true, 2}, {"BranchesAndTagsOnly", false, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			f := rpMixedRepo(t, memfs.New(), "/repo.git", tc.pr)
			rpRepack(t, f.repo)
			rpLayout(t, f)
			if packs := packFiles(t, f.fs, f.path); len(packs) != tc.packs {
				t.Errorf("Repack left packs %v, want %d", packs, tc.packs)
			}
			var loose []plumbing.Hash
			st := newStorer(f.fs, f.path, cache.NewObjectLRUDefault())
			defer func() { _ = st.Close() }()
			if err := st.ForEachObjectHash(func(h plumbing.Hash) error {
				loose = append(loose, h)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(loose) != 0 {
				t.Errorf("Repack left loose objects %v its packs cover", rpHexes(loose))
			}
		})
	}
}

// rpRequireObjectSets repacks f and checks both packs' objects and the manifest's tips against revlist.
func rpRequireObjectSets(t *testing.T, f *rpFixture) {
	t.Helper()
	reusable := rpReachable(t, f, f.tips)
	all := rpReachable(t, f, append(slices.Clone(f.tips), f.others...))
	rpRepack(t, f.repo)
	m := rpLayout(t, f)
	if got, want := m.tips, rpStrings(f.tips); !slices.Equal(got, want) {
		t.Errorf("manifest tips = %v, want R's targets ascending %v", got, want)
	}
	rp, err := f.repo.ReusablePack(t.Context())
	rpRequireReusable(t, f, rp, err)
	a := rpPackObjects(t, f, m.reusable)
	if !slices.Equal(a, reusable) {
		t.Errorf("reusable pack holds %v, want exactly what R reaches %v", a, reusable)
	}
	var b []string
	if !m.rest.IsZero() {
		b = rpPackObjects(t, f, m.rest)
	}
	for _, h := range b {
		if _, found := slices.BinarySearch(a, h); found {
			t.Errorf("object %s is in both the reusable and the rest pack", h)
		}
	}
	union := append(slices.Clone(a), b...)
	slices.Sort(union)
	if union = slices.Compact(union); !slices.Equal(union, all) {
		t.Errorf("reusable and rest packs hold %v, want exactly what every ref reaches %v", union, all)
	}
}

// The reusable pack holds what R reaches, the rest pack the remainder; the tips are R's targets, tags unpeeled.
func TestReusablePackObjectSets(t *testing.T) {
	t.Run("EveryRefKind", func(t *testing.T) {
		f := rpMixedRepo(t, memfs.New(), "/repo.git", true)
		blob, err := f.repo.storeBlob(randomBytes(t, 8<<10))
		if err != nil {
			t.Fatal(err)
		}
		rpSetRef(t, f.repo, plumbing.NewTagReferenceName("blob"), blob)
		tag := rpTag(t, f.repo, "v2", rpDangling(t, f.repo, f.main, "tagged.bin"))
		head := rpDangling(t, f.repo, f.main, "head.bin")
		rpSetRef(t, f.repo, plumbing.HEAD, head)
		f.tips = append(f.tips, blob, tag, head)
		plumbing.HashesSort(f.tips)
		rpRequireObjectSets(t, f)
	})
	t.Run("UnbornHEAD", func(t *testing.T) {
		fs := memfs.New()
		repo, err := Init(t.Context(), fs, "/repo.git", "main")
		if err != nil {
			t.Fatalf("init repository: %v", err)
		}
		dev := rpCommit(t, repo, "dev", "dev.bin", randomBytes(t, 8<<10))
		pr := rpDangling(t, repo, dev, "pr.bin")
		rpSetRef(t, repo, "refs/pr/1", pr)
		rpRequireObjectSets(t, &rpFixture{fs: fs, path: "/repo.git", repo: repo, tips: []plumbing.Hash{dev}, others: []plumbing.Hash{pr}})
	})
}

// NeedsRepack holds until Repack runs; repacking unchanged refs again rewrites no pack file nor the manifest's content.
func TestReusablePackRepackIsStable(t *testing.T) {
	f := rpMixedRepo(t, memfs.New(), "/repo.git", true)
	rpNeedsRepack(t, f.repo, true, "before the first Repack")
	rpRepack(t, f.repo)
	m := rpLayout(t, f)
	rpNeedsRepack(t, f.repo, false, "after Repack")
	packs := rpPackFiles(t, f, "")
	rpRepack(t, f.repo)
	if changed := rpChanged(packs, rpPackFiles(t, f, "")); len(changed) != 0 {
		t.Errorf("second Repack over unchanged refs rewrote %v", changed)
	}
	if got := rpReadManifest(t, f); !bytes.Equal(got.raw, m.raw) {
		t.Errorf("second Repack wrote manifest %q, want %q", got.raw, m.raw)
	}
	rpNeedsRepack(t, f.repo, false, "after the second Repack")
}

// When only refs/pr/1 moves, NeedsRepack turns true and Repack keeps the reusable pack's files while it rebuilds the rest pack.
func TestReusablePackOthersChange(t *testing.T) {
	f := rpMixedRepo(t, memfs.New(), "/repo.git", true)
	rpRepack(t, f.repo)
	before := rpLayout(t, f)
	prefix := "pack-" + before.reusable.String()
	reusable := rpPackFiles(t, f, prefix)
	f.others = []plumbing.Hash{rpDangling(t, f.repo, f.others[0], "pr2.bin")}
	rpSetRef(t, f.repo, "refs/pr/1", f.others[0])
	rpNeedsRepack(t, f.repo, true, "after only refs/pr/1 moved")
	inReusable := rpReachable(t, f, f.tips)
	rest := slices.DeleteFunc(rpReachable(t, f, append(slices.Clone(f.tips), f.others...)), func(h string) bool {
		_, found := slices.BinarySearch(inReusable, h)
		return found
	})
	rpRepack(t, f.repo)
	after := rpLayout(t, f)
	if after.reusable.String() != before.reusable.String() {
		t.Errorf("reusable pack = %s after only refs/pr/1 moved, want %s kept", after.reusable, before.reusable)
	}
	if changed := rpChanged(reusable, rpPackFiles(t, f, prefix)); len(changed) != 0 {
		t.Errorf("Repack rewrote %v although no branch or tag moved", changed)
	}
	if after.rest.IsZero() || after.rest.String() == before.rest.String() {
		t.Fatalf("rest pack = %s after refs/pr/1 moved, want one rebuilt in place of %s", after.rest, before.rest)
	}
	if got := rpPackObjects(t, f, after.rest); !slices.Equal(got, rest) {
		t.Errorf("rebuilt rest pack holds %v, want %v", got, rest)
	}
	rpNeedsRepack(t, f.repo, false, "after Repack")
}

// A refused push's pack survives Repack and NeedsRepack; GC deletes it, with a cutoff only once the pack is older.
func TestReusablePackRefusedPush(t *testing.T) {
	// setup lays out the reusable and rest packs, then lets a refused push leave its pack beside them.
	setup := func(t *testing.T) (*rpFixture, plumbing.Hash, []plumbing.Hash) {
		t.Helper()
		f := rpMixedRepo(t, memfs.New(), "/repo.git", true)
		rpRepack(t, f.repo)
		rpLayout(t, f)
		pack, objects := rpRefusedPush(t, f.repo)
		rpLayout(t, f, pack)
		return f, pack, objects
	}
	t.Run("RepackKeepsIt", func(t *testing.T) {
		f, pack, objects := setup(t)
		rpNeedsRepack(t, f.repo, false, "with only a refused push's pack added")
		rpRepack(t, f.repo)
		rpLayout(t, f, pack)
		for _, h := range objects {
			if !hasObject(t, f.fs, f.path, h.String()) {
				t.Errorf("Repack without a cutoff deleted %s of the refused push", h)
			}
		}
		rpNeedsRepack(t, f.repo, false, "after Repack")
	})
	t.Run("GCWithoutCutoffDeletesIt", func(t *testing.T) {
		f, _, objects := setup(t)
		want := rpWant(t, f, objects)
		for _, dry := range []bool{true, false} {
			res, err := f.repo.GC(t.Context(), time.Time{}, dry)
			rpRequireDeleted(t, fmt.Sprintf("GC(dryRun=%t)", dry), res, err, want)
		}
		rpLayout(t, f)
	})
	t.Run("GCCutoffByMtime", func(t *testing.T) {
		f, pack, objects := setup(t)
		packs, err := packInfos(t.Context(), f.fs, f.path)
		if err != nil {
			t.Fatal(err)
		}
		i := slices.IndexFunc(packs, func(p packInfo) bool { return p.hash.String() == pack.String() })
		if i < 0 {
			t.Fatalf("packInfos lists no pack-%s.pack", pack)
		}
		mtime := packs[i].mtime
		for _, step := range []struct {
			cutoff time.Time
			want   GCResult
			left   []plumbing.Hash
		}{
			{mtime, GCResult{}, []plumbing.Hash{pack}},
			{mtime.Add(time.Second), rpWant(t, f, objects), nil},
		} {
			for _, dry := range []bool{true, false} {
				res, err := f.repo.GC(t.Context(), step.cutoff, dry)
				rpRequireDeleted(t, fmt.Sprintf("GC(cutoff=pack mtime+%s, dryRun=%t)", step.cutoff.Sub(mtime), dry), res, err, step.want)
			}
			rpLayout(t, f, step.left...)
		}
	})
}

// ReusablePack describes the manifest's pack exactly, and returns nil, nil once the manifest or the pack no longer matches.
func TestReusablePackManifestValidation(t *testing.T) {
	// setup lays out the packs and requires ReusablePack to describe them before anything is corrupted.
	setup := func(t *testing.T) (*rpFixture, rpManifest) {
		t.Helper()
		f := rpMixedRepo(t, memfs.New(), "/repo.git", true)
		rpRepack(t, f.repo)
		m := rpLayout(t, f)
		rp, err := f.repo.ReusablePack(t.Context())
		rpRequireReusable(t, f, rp, err)
		return f, m
	}
	t.Run("Valid", func(t *testing.T) { setup(t) })
	remove := func(t *testing.T, f *rpFixture, name string) {
		t.Helper()
		if err := f.fs.Remove(filepath.Join(f.path, name)); err != nil {
			t.Fatal(err)
		}
	}
	write := func(t *testing.T, f *rpFixture, name string, data []byte) {
		t.Helper()
		if err := util.WriteFile(f.fs, filepath.Join(f.path, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	pack := func(m rpManifest, ext string) string {
		return filepath.Join("objects", "pack", "pack-"+m.reusable.String()+ext)
	}
	for _, tc := range []struct {
		name    string
		corrupt func(t *testing.T, f *rpFixture, m rpManifest)
	}{
		{"NoManifest", func(t *testing.T, f *rpFixture, _ rpManifest) { remove(t, f, packManifestPath) }},
		{"DeletedPack", func(t *testing.T, f *rpFixture, m rpManifest) { remove(t, f, pack(m, ".pack")) }},
		{"DeletedIndex", func(t *testing.T, f *rpFixture, m rpManifest) { remove(t, f, pack(m, ".idx")) }},
		{"AlteredPackLength", func(t *testing.T, f *rpFixture, m rpManifest) {
			data, err := util.ReadFile(f.fs, filepath.Join(f.path, pack(m, ".pack")))
			if err != nil {
				t.Fatal(err)
			}
			write(t, f, pack(m, ".pack"), append(data, 0))
		}},
		{"GarbledManifest", func(t *testing.T, f *rpFixture, _ rpManifest) {
			write(t, f, packManifestPath, []byte("not a manifest\n"))
		}},
		{"UnknownManifestVersion", func(t *testing.T, f *rpFixture, m rpManifest) {
			write(t, f, packManifestPath, bytes.Replace(m.raw, []byte("hfd-packs 1"), []byte("hfd-packs 2"), 1))
		}},
		{"TruncatedManifest", func(t *testing.T, f *rpFixture, m rpManifest) {
			write(t, f, packManifestPath, m.raw[:bytes.Index(m.raw, []byte("reusable "))+len("reusable ")+8])
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, m := setup(t)
			tc.corrupt(t, f, m)
			rp, err := f.repo.ReusablePack(t.Context())
			if err != nil {
				t.Fatalf("ReusablePack = %v, want nil, nil", err)
			}
			if rp != nil {
				t.Fatalf("ReusablePack = pack %s, want nil once the manifest no longer matches the disk", rp.Hash)
			}
		})
	}
}

// Repack and NeedsRepack leave native-engine and alternates repositories untouched, though go-git alone would repack them.
func TestReusablePackSkipsNativeAndAlternates(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skipf("git binary required: %v", err)
	}
	for _, tc := range []struct {
		name   string
		native bool
	}{{"Native", true}, {"Alternates", false}} {
		t.Run(tc.name, func(t *testing.T) {
			setGitBinary(t, "")
			f := buildGCFixture(t)
			rpNeedsRepack(t, f.repo, true, "under go-git")
			if tc.native {
				setGitBinary(t, gitPath)
			} else {
				other := filepath.Join(t.TempDir(), "other.git")
				runGit(t, "", "init", "--bare", other)
				if err := os.WriteFile(filepath.Join(f.bare, "objects", "info", "alternates"), []byte(filepath.Join(other, "objects")+"\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			requireGitMode(t, f.repo, tc.native)
			before := snapshotFiles(t, osfs.Default, f.bare)
			rpNeedsRepack(t, f.repo, false, "with "+tc.name)
			rpRepack(t, f.repo)
			if !maps.Equal(before, snapshotFiles(t, osfs.Default, f.bare)) {
				t.Fatalf("Repack with %s changed repository files", tc.name)
			}
		})
	}
}

// rpAlternates adds an alternate with no objects, so only the file's presence matters.
func rpAlternates(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(path, "objects", "info", "alternates"), []byte(t.TempDir()+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// With objects/info/alternates the go-git GC only prunes: expired loose garbage goes, every pack file stays and no manifest appears.
func TestReusablePackAlternatesGCOnlyPrunes(t *testing.T) {
	setGitBinary(t, "")
	f := buildGCFixture(t)
	requireGitMode(t, f.repo, false)
	rpAlternates(t, f.bare)
	ageObjects(t, f.bare, time.Now().Add(-2*time.Hour))
	if packs := packFiles(t, osfs.Default, f.bare); len(packs) < 2 {
		t.Fatalf("fixture holds packs %v, want at least two for a repack to merge", packs)
	}
	disk := &rpFixture{fs: osfs.Default, path: f.bare}
	packs := rpPackFiles(t, disk, "")
	if _, err := f.repo.GC(t.Context(), time.Now().Add(-time.Hour), false); err != nil {
		t.Fatalf("GC with objects/info/alternates: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.bare, packManifestPath)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("GC with objects/info/alternates wrote %s (stat: %v), want no manifest", packManifestPath, err)
	}
	if changed := rpChanged(packs, rpPackFiles(t, disk, "")); len(changed) != 0 {
		t.Errorf("GC with objects/info/alternates repacked, changing %v; want the pack files untouched", changed)
	}
	for label, h := range f.loose {
		if hasObject(t, osfs.Default, f.bare, h) {
			t.Errorf("GC with objects/info/alternates kept the expired loose %s %s, want Prune to delete it", label, h)
		}
	}
}

// ReusablePack returns nil, nil once objects/info/alternates appears, though the manifest Repack wrote still matches the disk.
func TestReusablePackAlternatesDisableReuse(t *testing.T) {
	setGitBinary(t, "")
	path := filepath.Join(t.TempDir(), "repo.git")
	f := rpMixedRepo(t, osfs.Default, path, true)
	rpRepack(t, f.repo)
	if rp, err := f.repo.ReusablePack(t.Context()); err != nil || rp == nil {
		t.Fatalf("ReusablePack after Repack = %v, %v; want the manifest's pack before alternates exist", rp, err)
	}
	rpAlternates(t, path)
	rp, err := f.repo.ReusablePack(t.Context())
	if err != nil {
		t.Fatalf("ReusablePack with objects/info/alternates = %v, want nil, nil", err)
	}
	if rp != nil {
		t.Errorf("ReusablePack with objects/info/alternates = pack %s, want nil, nil although the manifest is valid", rp.Hash)
	}
}

// rpSameReusable requires repo to serve the same reusable pack as want, read in full from repo's own files.
func rpSameReusable(t *testing.T, label string, repo *Repository, want *ReusablePack) {
	t.Helper()
	got, err := repo.ReusablePack(t.Context())
	if err != nil || got == nil {
		t.Fatalf("ReusablePack after %s = %v, %v; want pack %s", label, got, err, want.Hash)
	}
	if got.Hash.String() != want.Hash.String() || got.Size != want.Size || got.Objects != want.Objects || !slices.Equal(rpStrings(got.Tips), rpStrings(want.Tips)) {
		t.Errorf("ReusablePack after %s = %s, %d bytes, %d objects, tips %v; want %s, %d, %d, %v",
			label, got.Hash, got.Size, got.Objects, rpStrings(got.Tips), want.Hash, want.Size, want.Objects, rpStrings(want.Tips))
	}
	file, err := got.Open()
	if err != nil {
		t.Fatalf("Open after %s: %v", label, err)
	}
	defer func() { _ = file.Close() }()
	if n, err := io.Copy(io.Discard, file); err != nil || n != want.Size {
		t.Errorf("Open after %s read %d bytes, %v; want %d", label, n, err, want.Size)
	}
}

// The manifest carries no paths, so a duplicated or moved repository still serves the same reusable pack.
func TestReusablePackSurvivesDuplicateAndMove(t *testing.T) {
	fs := memfs.New()
	f := rpMixedRepo(t, fs, "/src.git", true)
	rpRepack(t, f.repo)
	want, err := f.repo.ReusablePack(t.Context())
	rpRequireReusable(t, f, want, err)
	dup, err := f.repo.Duplicate(t.Context(), "/copy.git")
	if err != nil {
		t.Fatalf("Duplicate: %v", err)
	}
	if err := f.repo.Move("/moved.git"); err != nil {
		t.Fatalf("Move: %v", err)
	}
	moved, err := Open(fs, "/moved.git")
	if err != nil {
		t.Fatalf("open the moved repository: %v", err)
	}
	rpSameReusable(t, "Duplicate", dup, want)
	rpSameReusable(t, "Move", moved, want)
}

// rpFlow runs Repack, ReusablePack, NeedsRepack, check and GC over a mixed-ref repository with loose garbage.
func rpFlow(t *testing.T, fs billy.Filesystem, path string, check func(t *testing.T)) {
	ctx := t.Context()
	f := rpMixedRepo(t, fs, path, true)
	requireGitMode(t, f.repo, false)
	orphan := rpCommitObjects(t, f.repo, rpDangling(t, f.repo, f.main, "orphan.bin"), "orphan.bin")
	want := rpWant(t, f, orphan)
	all := rpReachable(t, f, append(slices.Clone(f.tips), f.others...))
	rpNeedsRepack(t, f.repo, true, "before Repack")
	rpRepack(t, f.repo)
	m := rpLayout(t, f)
	if m.rest.IsZero() {
		t.Fatal("manifest names no rest pack although refs/pr/1 reaches objects")
	}
	rp, err := f.repo.ReusablePack(ctx)
	rpRequireReusable(t, f, rp, err)
	rpNeedsRepack(t, f.repo, false, "after Repack")
	if check != nil {
		check(t)
	}
	for _, dry := range []bool{true, false} {
		res, err := f.repo.GC(ctx, time.Time{}, dry)
		rpRequireDeleted(t, fmt.Sprintf("GC(dryRun=%t)", dry), res, err, want)
	}
	if got := rpLayout(t, f); got.reusable.String() != m.reusable.String() || got.rest.String() != m.rest.String() {
		t.Errorf("packs after GC = %s, %s; want the layout %s, %s", got.reusable, got.rest, m.reusable, m.rest)
	}
	if got := rpReachable(t, f, append(slices.Clone(f.tips), f.others...)); !slices.Equal(got, all) {
		t.Errorf("refs reach %d objects after GC, want %d", len(got), len(all))
	}
	requireBlob(t, f.repo, "main", "file.txt", []byte("one\n"))
}

// The whole cycle works on every storage the go-git engine serves, and native git accepts what it wrote.
func TestReusablePackFilesystems(t *testing.T) {
	setGitBinary(t, "")
	t.Run("osfs", func(t *testing.T) {
		if _, err := exec.LookPath("git"); err != nil {
			t.Skipf("git binary required for fsck: %v", err)
		}
		path := filepath.Join(t.TempDir(), "repo.git")
		rpFlow(t, osfs.Default, path, func(t *testing.T) { gitFsck(t, path) })
	})
	t.Run("gofakes3", func(t *testing.T) {
		backend := s3mem.New()
		const bucket = "reusable-pack"
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
		rpFlow(t, s3fs.New(bucket, s3fs.WithClient(client)), "/repo.git", nil)
	})
	t.Run("memfs", func(t *testing.T) { rpFlow(t, memfs.New(), "/repo.git", nil) })
}

// GC's preview names exactly what GC then deletes from a mixed-ref repository, which GC leaves with the reusable and the rest pack.
func TestReusablePackGCPreviewMatchesLive(t *testing.T) {
	f := rpMixedRepo(t, memfs.New(), "/repo.git", true)
	orphan := rpCommitObjects(t, f.repo, rpDangling(t, f.repo, f.main, "orphan.bin"), "orphan.bin")
	_, refused := rpRefusedPush(t, f.repo)
	want := rpWant(t, f, append(orphan, refused...))
	for _, dry := range []bool{true, false} {
		res, err := f.repo.GC(t.Context(), time.Time{}, dry)
		rpRequireDeleted(t, fmt.Sprintf("GC(dryRun=%t)", dry), res, err, want)
	}
	if m := rpLayout(t, f); m.rest.IsZero() {
		t.Error("GC left only the reusable pack, want the rest pack for refs/pr/1 beside it")
	}
	rpNeedsRepack(t, f.repo, false, "after GC")
}

// GC, NeedsRepack and Repack read the refs on disk, not a ref snapshot that a writer bypassing invalidation left stale.
func TestReusablePackReadsRefsLive(t *testing.T) {
	setGitBinary(t, "")
	fixture := func(t *testing.T) *rpFixture {
		t.Helper()
		f := rpMixedRepo(t, osfs.Default, filepath.Join(t.TempDir(), "repo.git"), false)
		requireGitMode(t, f.repo, false)
		return f
	}
	// stale puts the stamps back so the ref snapshot cannot tell main moved.
	stale := func(t *testing.T, f *rpFixture) {
		t.Helper()
		tip := rpDangling(t, f.repo, f.main, "live.bin")
		hourAgo := time.Now().Add(-time.Hour)
		rcAgeStamps(t, f.path, hourAgo)
		rcWarm(t, f.repo)
		if err := os.WriteFile(filepath.Join(f.path, "refs", "heads", "main"), []byte(tip.String()+"\n"), 0o644); err != nil {
			t.Fatalf("write main: %v", err)
		}
		rcAgeStamps(t, f.path, hourAgo)
		if ref, err := f.repo.repo.Storer.Reference("refs/heads/main"); err != nil || ref.Hash().String() != f.main.String() {
			t.Fatalf("Reference(refs/heads/main) = %v, %v; want the snapshot's stale %s, without which a snapshot read cannot show", ref, err, f.main)
		}
		f.tips[slices.Index(f.tips, f.main)] = tip
		plumbing.HashesSort(f.tips)
		f.main = tip
	}
	t.Run("GC", func(t *testing.T) {
		f := fixture(t)
		want := rpWant(t, f, rpCommitObjects(t, f.repo, rpDangling(t, f.repo, f.main, "orphan.bin"), "orphan.bin"))
		stale(t, f)
		live := rpCommitObjects(t, f.repo, f.main, "live.bin")
		for _, dry := range []bool{true, false} {
			res, err := f.repo.GC(t.Context(), time.Time{}, dry)
			rpRequireDeleted(t, fmt.Sprintf("GC(dryRun=%t)", dry), res, err, want)
		}
		for _, h := range live {
			if !hasObject(t, f.fs, f.path, h.String()) {
				t.Errorf("GC deleted %s, which main reaches on disk though not in the ref snapshot", h)
			}
		}
	})
	t.Run("NeedsRepack", func(t *testing.T) {
		f := fixture(t)
		rpRepack(t, f.repo)
		rpNeedsRepack(t, f.repo, false, "after Repack")
		stale(t, f)
		rpNeedsRepack(t, f.repo, true, "once main moved on disk behind the ref snapshot")
	})
	t.Run("Repack", func(t *testing.T) {
		f := fixture(t)
		stale(t, f)
		rpRepack(t, f.repo)
		rp, err := f.repo.ReusablePack(t.Context())
		rpRequireReusable(t, f, rp, err)
	})
}
