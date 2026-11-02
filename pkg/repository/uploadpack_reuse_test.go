package repository

// Tests for the upload-pack fast path: a clone covering A's tips is answered with the reusable pack, anything else by go-git as before.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/utils/ioutil"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	s3fs "github.com/wzshiming/go-billy-s3fs"
)

// ruCaps are the v0/v1 capabilities git sends for a clone, side-band-64k included.
const ruCaps = "multi_ack_detailed no-done side-band-64k thin-pack include-tag ofs-delta agent=git/2.47.0"

// ruUnknown names an object no fixture holds.
var ruUnknown = plumbing.NewHash(strings.Repeat("ab", 20))

// ruFraming is a clone's wire framing: protocol, v0/v1 capabilities and the pkt-lines around the pack, flushes as "0000".
type ruFraming struct {
	name, proto, caps string
	lines             []string
}

// ruSideband is the framing git uses for a v0 clone.
var ruSideband = ruFraming{"V0Sideband64k", "", ruCaps, []string{"NAK", "0000"}}

// ruFramings are the framings a clone arrives in.
var ruFramings = []ruFraming{
	{"V0", "", strings.Replace(ruCaps, " side-band-64k", "", 1), []string{"NAK"}},
	ruSideband,
	{"V1", "version=1", ruCaps, []string{"NAK", "0000"}},
	{"V2", "version=2", "", []string{"packfile", "0000"}},
}

// request encodes a clone of wants in fr's framing.
func (fr ruFraming) request(t *testing.T, wants []plumbing.Hash) []byte {
	t.Helper()
	if fr.proto == "version=2" {
		return ruV2Request(t, ruV2Clone(wants)...)
	}
	return ruV0Request(t, wants, fr.caps, nil, nil)
}

// ruV0Request encodes a v0/v1 request: wants, caps on the first, extra lines, a flush, haves, then done.
func ruV0Request(t *testing.T, wants []plumbing.Hash, caps string, extra []string, haves []plumbing.Hash) []byte {
	t.Helper()
	var lines []string
	for i, h := range wants {
		line := "want " + h.String()
		if i == 0 && caps != "" {
			line += " " + caps
		}
		lines = append(lines, line+"\n")
	}
	for _, l := range extra {
		lines = append(lines, l+"\n")
	}
	lines = append(lines, "")
	for _, h := range haves {
		lines = append(lines, "have "+h.String()+"\n")
	}
	return pktLines(t, append(lines, "done\n")...)
}

// ruV2Request encodes a v2 fetch command with args.
func ruV2Request(t *testing.T, args ...string) []byte {
	t.Helper()
	lines := make([]string, 0, len(args)+1)
	for _, a := range args {
		lines = append(lines, a+"\n")
	}
	// 0001 is the delim-pkt ending the command's capabilities.
	return slices.Concat(pktLines(t, "command=fetch\n", "agent=git/2.47.0\n", "object-format=sha1\n"), []byte("0001"), pktLines(t, append(lines, "")...))
}

// ruV2Clone lists a v2 clone's fetch arguments for wants, extra going before done.
func ruV2Clone(wants []plumbing.Hash, extra ...string) []string {
	args := []string{"thin-pack", "no-progress", "include-tag", "ofs-delta"}
	for _, h := range wants {
		args = append(args, "want "+h.String())
	}
	return append(append(args, extra...), "done")
}

// ruRepo is rpMixedRepo with refs/pr/1, its main tip adding a blob that spans several sideband frames.
func ruRepo(t *testing.T, fs billy.Filesystem, path string) *rpFixture {
	t.Helper()
	f := rpMixedRepo(t, fs, path, true)
	ruAdvance(f, rpCommit(t, f.repo, "main", "big.bin", randomBytes(t, 192<<10)))
	return f
}

// ruAdvance records main's new tip in f.
func ruAdvance(f *rpFixture, main plumbing.Hash) {
	f.tips[slices.Index(f.tips, f.main)] = main
	plumbing.HashesSort(f.tips)
	f.main = main
}

// ruGrow commits two near-identical blobs on main past A, so what A lacks holds a delta whose base it holds too.
func ruGrow(t *testing.T, f *rpFixture) {
	t.Helper()
	blob := randomBytes(t, 24<<10)
	edited := slices.Clone(blob)
	edited[len(edited)/2] ^= 0xff
	ruAdvance(f, plumbing.NewHash(mustCommit(t, f.repo, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "grow/a.bin", Content: blob},
		CommitOperation{Type: CommitOperationAdd, Path: "grow/b.bin", Content: edited})))
}

// ruLinear builds and repacks main, depth commits above dev, with no tags.
func ruLinear(t *testing.T, depth int) (f *rpFixture, dev plumbing.Hash) {
	t.Helper()
	repo, err := Init(t.Context(), memfs.New(), "/repo.git", "main")
	if err != nil {
		t.Fatalf("init repository: %v", err)
	}
	dev = rpCommit(t, repo, "main", "dev.bin", randomBytes(t, 16<<10))
	rpSetRef(t, repo, plumbing.NewBranchReferenceName("dev"), dev)
	f = &rpFixture{fs: repo.fs, path: repo.repoPath, repo: repo, main: dev}
	for i := range depth {
		f.main = rpCommit(t, repo, "main", fmt.Sprintf("main-%d.txt", i), fmt.Appendf(nil, "commit %d\n", i))
	}
	f.tips = []plumbing.Hash{f.main, dev}
	plumbing.HashesSort(f.tips)
	rpRepack(t, repo)
	return f, dev
}

// ruParent returns commit's first parent.
func ruParent(t *testing.T, repo *Repository, commit plumbing.Hash) plumbing.Hash {
	t.Helper()
	c, err := repo.repo.CommitObject(commit)
	if err != nil || len(c.ParentHashes) == 0 {
		t.Fatalf("parent of %s: %v", commit, err)
	}
	return c.ParentHashes[0]
}

// ruRef resolves name in repo.
func ruRef(t *testing.T, repo *Repository, name plumbing.ReferenceName) plumbing.Hash {
	t.Helper()
	ref, err := repo.repo.Storer.Reference(name)
	if err != nil {
		t.Fatalf("resolve %s: %v", name, err)
	}
	return ref.Hash()
}

// ruS3 is a bucket served by gofakes3, as TestReusablePackFilesystems builds one.
func ruS3(t *testing.T) billy.Filesystem {
	t.Helper()
	backend := s3mem.New()
	const bucket = "upload-pack-reuse"
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
	return s3fs.New(bucket, s3fs.WithClient(client))
}

// ruServe answers body through Stateless, returning the decisions uploadPackReuseHook saw meanwhile.
func ruServe(t *testing.T, repo *Repository, proto string, body []byte) (resp []byte, decisions []uploadPackReuse, err error) {
	t.Helper()
	prev := uploadPackReuseHook
	t.Cleanup(func() { uploadPackReuseHook = prev })
	uploadPackReuseHook = func(d uploadPackReuse) { decisions = append(decisions, d) }
	var out bytes.Buffer
	err = repo.Stateless(t.Context(), &out, bytes.NewReader(body), GitUploadPack, proto, ReceivePackHooks{})
	return out.Bytes(), decisions, err
}

// ruSlowPath answers body with transport.UploadPack alone, as Stateless did before the fast path.
func ruSlowPath(t *testing.T, repo *Repository, proto string, body []byte) ([]byte, error) {
	t.Helper()
	var out bytes.Buffer
	err := transport.UploadPack(t.Context(), repo.repo.Storer, io.NopCloser(bytes.NewReader(body)), ioutil.WriteNopCloser(&out),
		&transport.UploadPackRequest{GitProtocol: proto, StatelessRPC: true})
	return out.Bytes(), err
}

// ruSplit separates a response into its pkt-lines, flushes as "0000", and the pack it carries raw or on sideband band 1.
func ruSplit(t *testing.T, resp []byte) (lines []string, pack []byte) {
	t.Helper()
	rd := bytes.NewReader(resp)
	for rd.Len() > 0 {
		if rest := resp[len(resp)-rd.Len():]; bytes.HasPrefix(rest, []byte("PACK")) {
			return lines, append(pack, rest...)
		}
		l, p, err := pktline.ReadLine(rd)
		switch {
		case err != nil:
			t.Fatalf("pkt-line ending at byte %d of the response: %v", len(resp)-rd.Len(), err)
		case l == pktline.Flush:
			lines = append(lines, "0000")
		case l == pktline.Delim:
			lines = append(lines, "0001")
		case len(p) > 0 && p[0] == 1:
			pack = append(pack, p[1:]...)
		default:
			lines = append(lines, strings.TrimSuffix(string(p), "\n"))
		}
	}
	return lines, pack
}

// ruReusable returns repo's reusable pack and its bytes, read through Open.
func ruReusable(t *testing.T, repo *Repository) (*ReusablePack, []byte) {
	t.Helper()
	rp, err := repo.ReusablePack(t.Context())
	if err != nil || rp == nil {
		t.Fatalf("ReusablePack = %v, %v; want the pack Repack wrote", rp, err)
	}
	file, err := rp.Open()
	if err != nil {
		t.Fatalf("open the reusable pack: %v", err)
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("read the reusable pack: %v", err)
	}
	return rp, data
}

// ruIndexPack has git index-pack --strict check pack, duplicate entries included, and lists its objects as sorted hex.
func ruIndexPack(t *testing.T, pack []byte) []string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "served.pack"), pack, 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, dir, "index-pack", "--strict", "-o", "served.idx", "served.pack")
	objects, err := packIndexObjects(osfs.Default, filepath.Join(dir, "served.idx"), plumbing.ZeroHash)
	if err != nil {
		t.Fatalf("read the index git wrote: %v", err)
	}
	return rpHexes(objects)
}

// ruRequireReused requires decisions to be one reuse of rp with fresh objects encoded after it.
func ruRequireReused(t *testing.T, decisions []uploadPackReuse, rp *ReusablePack, fresh int) {
	t.Helper()
	if len(decisions) != 1 || !decisions[0].Reused || decisions[0].PackObjects != rp.Objects || decisions[0].FreshObjects != fresh {
		t.Errorf("uploadPackReuseHook saw %+v; want one decision, Reused with PackObjects %d and FreshObjects %d", decisions, rp.Objects, fresh)
	}
}

// ruRequireFellBack requires decisions to be one fallback whose Reason is among reasons.
func ruRequireFellBack(t *testing.T, decisions []uploadPackReuse, reasons ...string) {
	t.Helper()
	if len(decisions) != 1 || decisions[0].Reused || !slices.Contains(reasons, decisions[0].Reason) {
		t.Errorf("uploadPackReuseHook saw %+v; want one decision, a fallback with Reason in %q", decisions, reasons)
	}
}

// ruRequireSlowPath requires resp and err to be exactly what transport.UploadPack answers to body.
func ruRequireSlowPath(t *testing.T, repo *Repository, proto string, body, resp []byte, err error) {
	t.Helper()
	want, wantErr := ruSlowPath(t, repo, proto, body)
	if !bytes.Equal(resp, want) || (err == nil) != (wantErr == nil) {
		t.Errorf("response = %d bytes %.40q, error %v; want transport.UploadPack's %d bytes %.40q, error %v",
			len(resp), resp, err, len(want), want, wantErr)
	}
}

// ruRequireVerbatim requires a clone of wants framed as fr to be answered with repo's reusable pack byte for byte.
func ruRequireVerbatim(t *testing.T, repo *Repository, fr ruFraming, wants []plumbing.Hash) {
	t.Helper()
	rp, a := ruReusable(t, repo)
	resp, decisions, err := ruServe(t, repo, fr.proto, fr.request(t, wants))
	if err != nil {
		t.Fatalf("Stateless: %v", err)
	}
	ruRequireReused(t, decisions, rp, 0)
	lines, pack := ruSplit(t, resp)
	if !bytes.HasPrefix(resp, pktLines(t, fr.lines[0]+"\n")) || !slices.Equal(lines, fr.lines) {
		t.Errorf("response pkt-lines = %q, want exactly %q around the pack", lines, fr.lines)
	}
	if !bytes.Equal(pack, a) {
		t.Errorf("served pack of %d bytes differs from the %d bytes of reusable pack %s", len(pack), len(a), rp.Hash)
	}
}

// ruRequireSpliced requires a clone of f's tips framed as fr to carry A's entries verbatim, then what A lacks, in one pack git accepts.
func ruRequireSpliced(t *testing.T, f *rpFixture, fr ruFraming) {
	t.Helper()
	rp, a := ruReusable(t, f.repo)
	inA := rpPackObjects(t, f, rp.Hash)
	fresh := slices.DeleteFunc(rpReachable(t, f, f.tips), func(h string) bool {
		_, found := slices.BinarySearch(inA, h)
		return found
	})
	if len(fresh) == 0 {
		t.Fatal("A holds every object the tips reach; the fixture needs objects past it")
	}
	body := fr.request(t, f.tips)
	resp, decisions, err := ruServe(t, f.repo, fr.proto, body)
	if err != nil {
		t.Fatalf("Stateless: %v", err)
	}
	ruRequireReused(t, decisions, rp, len(fresh))
	lines, pack := ruSplit(t, resp)
	if !slices.Equal(lines, fr.lines) {
		t.Errorf("response pkt-lines = %q, want exactly %q around the pack", lines, fr.lines)
	}
	var count uint32
	if len(pack) >= 12 {
		count = binary.BigEndian.Uint32(pack[8:12])
	}
	if want := rp.Objects + uint32(len(fresh)); count != want {
		t.Errorf("pack header counts %d objects, want A's %d plus the %d it lacks", count, rp.Objects, len(fresh))
	}
	if end := rp.Size - int64(rp.Hash.Size()); int64(len(pack)) < end || !bytes.Equal(pack[12:end], a[12:end]) {
		t.Errorf("pack does not carry A's entries verbatim from byte 12 on")
	}
	slow, err := ruSlowPath(t, f.repo, fr.proto, body)
	if err != nil {
		t.Fatalf("transport.UploadPack: %v", err)
	}
	_, slowPack := ruSplit(t, slow)
	if got, want := ruIndexPack(t, pack), ruIndexPack(t, slowPack); !slices.Equal(got, want) {
		t.Errorf("pack holds %d objects %v, want transport.UploadPack's %d %v", len(got), got, len(want), want)
	}
}

// A clone of every branch and tag after Repack is answered with A's bytes verbatim, in every framing.
func TestUploadPackReuseFullClone(t *testing.T) {
	f := ruRepo(t, memfs.New(), "/repo.git")
	rpRepack(t, f.repo)
	for _, fr := range ruFramings {
		t.Run(fr.name, func(t *testing.T) { ruRequireVerbatim(t, f.repo, fr, f.tips) })
	}
}

// Once main moves past A, a clone splices A's entries with the freshly encoded rest into one pack.
func TestUploadPackReuseIncrement(t *testing.T) {
	f := ruRepo(t, memfs.New(), "/repo.git")
	rpRepack(t, f.repo)
	ruGrow(t, f)
	for _, fr := range ruFramings {
		t.Run(fr.name, func(t *testing.T) { ruRequireSpliced(t, f, fr) })
	}
}

// A tip that is an ancestor of a want needs no want of its own; one neither wanted nor below a want falls back.
func TestUploadPackReuseAncestorTip(t *testing.T) {
	f, dev := ruLinear(t, 2)
	t.Run("WantMain", func(t *testing.T) { ruRequireVerbatim(t, f.repo, ruSideband, []plumbing.Hash{f.main}) })
	t.Run("WantDev", func(t *testing.T) {
		body := ruSideband.request(t, []plumbing.Hash{dev})
		resp, decisions, err := ruServe(t, f.repo, ruSideband.proto, body)
		ruRequireFellBack(t, decisions, reuseTipNotWanted)
		ruRequireSlowPath(t, f.repo, ruSideband.proto, body, resp, err)
	})
}

// ruFallback is a request the fast path must hand to go-git: the repository serving it, the Reasons allowed, whether go-git refuses it.
type ruFallback struct {
	name, proto, repo string
	reasons           []string
	refused           bool
	request           func(t *testing.T, f *rpFixture) []byte
}

// ruBranches are main and dev without v1, an annotated tag among A's tips.
func ruBranches(t *testing.T, f *rpFixture) []plumbing.Hash {
	return []plumbing.Hash{f.main, ruRef(t, f.repo, "refs/heads/dev")}
}

// ruFallbacks are requests that each miss one condition of the fast path.
var ruFallbacks = []ruFallback{
	{name: "V0/Haves", repo: "repacked", reasons: []string{reuseNegotiation}, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV0Request(t, f.tips, ruCaps, nil, []plumbing.Hash{f.main})
	}},
	{name: "V0/Deepen", repo: "repacked", reasons: []string{reuseShape, reuseNegotiation}, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV0Request(t, f.tips, ruCaps, []string{"deepen 1"}, nil)
	}},
	{name: "V0/Shallow", repo: "repacked", reasons: []string{reuseShape, reuseNegotiation}, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV0Request(t, f.tips, ruCaps, []string{"shallow " + f.main.String()}, nil)
	}},
	// go-git's UploadRequest.Decode rejects a filter line.
	{name: "V0/Filter", repo: "repacked", reasons: []string{reuseShape}, refused: true, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV0Request(t, f.tips, ruCaps, []string{"filter blob:none"}, nil)
	}},
	{name: "V0/NoOfsDelta", repo: "repacked", reasons: []string{reuseCapabilities}, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV0Request(t, f.tips, strings.Replace(ruCaps, " ofs-delta", "", 1), nil, nil)
	}},
	{name: "V0/SidebandOnly", repo: "repacked", reasons: []string{reuseCapabilities}, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV0Request(t, f.tips, strings.Replace(ruCaps, "side-band-64k", "side-band", 1), nil, nil)
	}},
	{name: "V0/UnknownWant", repo: "repacked", reasons: []string{reuseWantMissing}, refused: true, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV0Request(t, append(slices.Clone(f.tips), ruUnknown), ruCaps, nil, nil)
	}},
	{name: "V0/NotRepacked", repo: "loose", reasons: []string{reuseNoPack}, request: func(t *testing.T, f *rpFixture) []byte {
		return ruSideband.request(t, f.tips)
	}},
	{name: "V0/Alternates", repo: "alternates", reasons: []string{reuseNoPack}, request: func(t *testing.T, f *rpFixture) []byte {
		return ruSideband.request(t, f.tips)
	}},
	{name: "V0/TagNotWanted", repo: "repacked", reasons: []string{reuseTipNotWanted}, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV0Request(t, ruBranches(t, f), ruCaps, nil, nil)
	}},
	{name: "V2/Haves", proto: "version=2", repo: "repacked", reasons: []string{reuseNegotiation, reuseShape}, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV2Request(t, ruV2Clone(f.tips, "have "+f.main.String())...)
	}},
	{name: "V2/Deepen", proto: "version=2", repo: "repacked", reasons: []string{reuseShape, reuseNegotiation}, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV2Request(t, ruV2Clone(f.tips, "deepen 1")...)
	}},
	{name: "V2/Shallow", proto: "version=2", repo: "repacked", reasons: []string{reuseShape, reuseNegotiation}, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV2Request(t, ruV2Clone(f.tips, "shallow "+f.main.String())...)
	}},
	// go-git's FetchArgs.Decode accepts the filter and serveFetchV2 ignores it.
	{name: "V2/Filter", proto: "version=2", repo: "repacked", reasons: []string{reuseShape}, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV2Request(t, ruV2Clone(f.tips, "filter blob:none")...)
	}},
	{name: "V2/NoOfsDelta", proto: "version=2", repo: "repacked", reasons: []string{reuseCapabilities}, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV2Request(t, slices.DeleteFunc(ruV2Clone(f.tips), func(a string) bool { return a == "ofs-delta" })...)
	}},
	{name: "V2/UnknownWant", proto: "version=2", repo: "repacked", reasons: []string{reuseWantMissing}, refused: true, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV2Request(t, ruV2Clone(append(slices.Clone(f.tips), ruUnknown))...)
	}},
	// go-git's FetchArgs.Decode skips a line it does not know.
	{name: "V2/UnknownLine", proto: "version=2", repo: "repacked", reasons: []string{reuseShape}, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV2Request(t, ruV2Clone(f.tips, "unknown-argument")...)
	}},
	{name: "V2/NotRepacked", proto: "version=2", repo: "loose", reasons: []string{reuseNoPack}, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV2Request(t, ruV2Clone(f.tips)...)
	}},
	{name: "V2/Alternates", proto: "version=2", repo: "alternates", reasons: []string{reuseNoPack}, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV2Request(t, ruV2Clone(f.tips)...)
	}},
	{name: "V2/TagNotWanted", proto: "version=2", repo: "repacked", reasons: []string{reuseTipNotWanted}, request: func(t *testing.T, f *rpFixture) []byte {
		return ruV2Request(t, ruV2Clone(ruBranches(t, f))...)
	}},
}

// ruFallbackRepos serves ruFallbacks: repositories repacked and never repacked in memory, and one repacked on disk given alternates.
func ruFallbackRepos(t *testing.T) map[string]*rpFixture {
	t.Helper()
	setGitBinary(t, "")
	repacked := ruRepo(t, memfs.New(), "/repo.git")
	rpRepack(t, repacked.repo)
	path := filepath.Join(t.TempDir(), "repo.git")
	alternates := ruRepo(t, osfs.Default, path)
	rpRepack(t, alternates.repo)
	rpAlternates(t, path)
	return map[string]*rpFixture{"repacked": repacked, "loose": ruRepo(t, memfs.New(), "/repo.git"), "alternates": alternates}
}

// Each request the fast path may not serve is answered exactly as transport.UploadPack answers it.
func TestUploadPackReuseFallbackServesSlowPath(t *testing.T) {
	repos := ruFallbackRepos(t)
	for _, tc := range ruFallbacks {
		t.Run(tc.name, func(t *testing.T) {
			f := repos[tc.repo]
			body := tc.request(t, f)
			resp, _, err := ruServe(t, f.repo, tc.proto, body)
			if (err != nil) != tc.refused {
				t.Errorf("Stateless = %v; want go-git to refuse the request: %t", err, tc.refused)
			}
			ruRequireSlowPath(t, f.repo, tc.proto, body, resp, err)
		})
	}
}

// Each request the fast path may not serve reports one fallback naming why.
func TestUploadPackReuseFallbackReason(t *testing.T) {
	repos := ruFallbackRepos(t)
	for _, tc := range ruFallbacks {
		t.Run(tc.name, func(t *testing.T) {
			f := repos[tc.repo]
			_, decisions, _ := ruServe(t, f.repo, tc.proto, tc.request(t, f))
			ruRequireFellBack(t, decisions, tc.reasons...)
		})
	}
}

// A force-push leaving A's main tip unreachable falls back until Repack makes the clone reusable again.
func TestUploadPackReuseForcePush(t *testing.T) {
	f := ruRepo(t, memfs.New(), "/repo.git")
	rpRepack(t, f.repo)
	ruAdvance(f, rpDangling(t, f.repo, ruParent(t, f.repo, f.main), "rewritten.bin"))
	rpSetRef(t, f.repo, plumbing.NewBranchReferenceName("main"), f.main)
	body := ruSideband.request(t, f.tips)
	resp, decisions, err := ruServe(t, f.repo, ruSideband.proto, body)
	ruRequireFellBack(t, decisions, reuseTipNotWanted)
	ruRequireSlowPath(t, f.repo, ruSideband.proto, body, resp, err)
	rpRepack(t, f.repo)
	ruRequireVerbatim(t, f.repo, ruSideband, f.tips)
}

// The same clone that reuses A falls back with reuseBudget once dev lies more than reuseAncestorBudget commits below main.
func TestUploadPackReuseAncestorBudget(t *testing.T) {
	f, _ := ruLinear(t, 16)
	wants := []plumbing.Hash{f.main}
	ruRequireVerbatim(t, f.repo, ruSideband, wants)
	prev := reuseAncestorBudget
	t.Cleanup(func() { reuseAncestorBudget = prev })
	reuseAncestorBudget = 8
	body := ruSideband.request(t, wants)
	resp, decisions, err := ruServe(t, f.repo, ruSideband.proto, body)
	ruRequireFellBack(t, decisions, reuseBudget)
	ruRequireSlowPath(t, f.repo, ruSideband.proto, body, resp, err)
}

// Verbatim and spliced clones work on every storage go-git serves.
func TestUploadPackReuseFilesystems(t *testing.T) {
	setGitBinary(t, "")
	for _, tc := range []struct {
		name string
		fs   func(t *testing.T) (billy.Filesystem, string)
	}{
		{"osfs", func(t *testing.T) (billy.Filesystem, string) {
			return osfs.Default, filepath.Join(t.TempDir(), "repo.git")
		}},
		{"memfs", func(t *testing.T) (billy.Filesystem, string) { return memfs.New(), "/repo.git" }},
		{"gofakes3", func(t *testing.T) (billy.Filesystem, string) { return ruS3(t), "/repo.git" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, path := tc.fs(t)
			f := ruRepo(t, fs, path)
			requireGitMode(t, f.repo, false)
			rpRepack(t, f.repo)
			t.Run("Verbatim", func(t *testing.T) {
				for _, fr := range ruFramings {
					t.Run(fr.name, func(t *testing.T) { ruRequireVerbatim(t, f.repo, fr, f.tips) })
				}
			})
			ruGrow(t, f)
			t.Run("Spliced", func(t *testing.T) {
				for _, fr := range ruFramings {
					t.Run(fr.name, func(t *testing.T) { ruRequireSpliced(t, f, fr) })
				}
			})
		})
	}
}

// Native git serves a repacked repository without calling uploadPackReuseHook, which go-git calls for the same request.
func TestUploadPackReuseNativeSkipsHook(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("git binary required: %v", err)
	}
	setGitBinary(t, "")
	f := ruRepo(t, osfs.Default, filepath.Join(t.TempDir(), "repo.git"))
	rpRepack(t, f.repo)
	for _, fr := range ruFramings {
		t.Run(fr.name, func(t *testing.T) {
			body := fr.request(t, f.tips)
			if _, decisions, err := ruServe(t, f.repo, fr.proto, body); err != nil || len(decisions) != 1 {
				t.Fatalf("go-git Stateless = %v with decisions %+v; want one decision", err, decisions)
			}
			setGitBinary(t, gitPath)
			requireGitMode(t, f.repo, true)
			resp, decisions, err := ruServe(t, f.repo, fr.proto, body)
			if err != nil {
				t.Fatalf("native Stateless: %v", err)
			}
			if len(decisions) != 0 {
				t.Errorf("uploadPackReuseHook saw %+v under native git, want no call", decisions)
			}
			if !bytes.Contains(resp, []byte("PACK")) {
				t.Errorf("native response of %d bytes carries no pack", len(resp))
			}
		})
	}
}
