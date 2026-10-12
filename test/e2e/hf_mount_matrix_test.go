package e2e_test

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	xetclient "github.com/wzshiming/xet/client"
)

// hfMountVersion pins the hf-mount release whose Hub client the matrix asserts.
const hfMountVersion = "0.13.1"

const (
	hfMountAuthUser  = "admin"
	hfMountAuthToken = "secret123"
	// hfMountLFSSize spans many xet chunks, so ranged reads reconstruct mid-file.
	hfMountLFSSize = 2 << 20
	hfMountReadme  = `# hf-mount fixture

TestHFMountMatrix seeds this repository through the Hub API and mounts it
with the real hf-mount daemons. Every listing, size and byte read through
the mounted filesystem is compared with what the test committed here, so
this text only has to be plain and longer than a handful of bytes.
`
)

// hfMountFiles are the regular files every fixture commits first.
var hfMountFiles = []hubFile{
	{path: "README.md", content: hfMountReadme},
	{path: "config.json", content: `{"architectures": ["HFMountProbe"], "hidden_size": 8}` + "\n"},
	{path: "empty.txt", content: ""},
	{path: "nested/dir/inner.txt", content: "two directories down\n"},
}

// hfMountBackend is the kernel interface axis: hf-mount ships one daemon per transport.
type hfMountBackend struct {
	name, binary string
	fuse         bool
}

var hfMountBackends = []hfMountBackend{
	{name: "NFS", binary: "hf-mount-nfs"},
	{name: "FUSE", binary: "hf-mount-fuse", fuse: true},
}

// TestHFMountMatrix mounts hfd-served repos with the real hf-mount daemons and
// checks through the filesystem what huggingface.co would serve, while the
// request recorder proves which hfd routes served it.
func TestHFMountMatrix(t *testing.T) {
	typeOf := func(arg string) hubRepoType {
		i := slices.IndexFunc(hubRepoTypes, func(rt hubRepoType) bool { return rt.arg == arg })
		if i < 0 {
			t.Fatalf("hubRepoTypes has no %q entry", arg)
		}
		return hubRepoTypes[i]
	}
	model, dataset := typeOf("model"), typeOf("dataset")
	rows := []struct {
		name string
		rt   hubRepoType
		run  func(t *testing.T, b hfMountBackend, rt hubRepoType)
	}{
		{name: "Model", rt: model, run: hfMountParityRow},
		{name: "Dataset", rt: dataset, run: hfMountParityRow},
		{name: "AuthToken", rt: model, run: hfMountAuthTokenRow},
		{name: "AuthWrongToken", rt: model, run: hfMountAuthWrongTokenRow},
		{name: "PinnedRevision", rt: model, run: hfMountPinnedRevisionRow},
		{name: "Poll", rt: model, run: hfMountPollRow},
	}
	for _, b := range hfMountBackends {
		t.Run(b.name, func(t *testing.T) {
			// Rows run the binary whose version requireHFMount vetted.
			b.binary = requireHFMount(t, b)
			for _, row := range rows {
				t.Run(row.name, func(t *testing.T) { row.run(t, b, row.rt) })
			}
		})
	}
}

// hfMountParityRow mounts a seeded repo of type rt and runs every parity check.
func hfMountParityRow(t *testing.T, b hfMountBackend, rt hubRepoType) {
	rec := &requestRecorder{}
	s := newE2EServer(t, withWrap(rec.wrap))
	seed := seedHFMountRepo(t, s, rt, hfMountRepoName(t), true)
	rec.reset()
	m := mountHF(t, b, s, rec, hfMountRepoID(rt, seed.repoID), hfMountOptions{metadataTTLms: 60000})
	assertHubParityThroughMount(t, s, m, seed)
}

// hfMountAuthTokenRow mounts from an hfd that authenticates the token hf-mount sends.
func hfMountAuthTokenRow(t *testing.T, b hfMountBackend, rt hubRepoType) {
	rec := &requestRecorder{}
	s := newE2EServer(t, withWrap(rec.wrap), withAuth(hfMountAuthUser, hfMountAuthToken))
	// withAuth lets anonymous requests through, so seeding needs no credentials.
	seed := seedHFMountRepo(t, s, rt, hfMountRepoName(t), true)
	rec.reset()
	m := mountHF(t, b, s, rec, hfMountRepoID(rt, seed.repoID), hfMountOptions{token: hfMountAuthToken, metadataTTLms: 60000})
	checkMountStartup(t, m, seed)
	checkMountListing(t, m, seed)
	checkMountLFSReads(t, m, seed)
}

// hfMountAuthWrongTokenRow expects hf-mount to give up before mounting once
// hfd answers its unknown token with 401.
func hfMountAuthWrongTokenRow(t *testing.T, b hfMountBackend, rt hubRepoType) {
	rec := &requestRecorder{}
	s := newE2EServer(t, withWrap(rec.wrap), withAuth(hfMountAuthUser, hfMountAuthToken))
	seed := seedHFMountRepo(t, s, rt, hfMountRepoName(t), false)
	rec.reset()
	m := mountHF(t, b, s, rec, hfMountRepoID(rt, seed.repoID), hfMountOptions{token: "wrong-token", metadataTTLms: 60000, expectExit: true})
	out, exited, exitErr := m.waitExit(30 * time.Second)
	if !exited {
		m.kill(t)
		t.Fatalf("hf-mount with a wrong token still ran after 30s, want it to exit on hfd's 401\n%s", m.report())
	}
	if exitErr == nil || !strings.Contains(out, "401") {
		t.Errorf("hf-mount with a wrong token exited with %v, want a non-zero exit naming 401\n%s", exitErr, m.report())
	}
	if isMounted(t, m.dir) {
		t.Errorf("hf-mount with a wrong token left %s mounted\n%s", m.dir, m.report())
	}
	if rec.saw(http.MethodGet, "/xet-read-token/") {
		t.Errorf("hf-mount requested a CAS read token despite the 401\n%s", m.report())
	}
}

// hfMountPinnedRevisionRow mounts the seeded commit while main moves on.
func hfMountPinnedRevisionRow(t *testing.T, b hfMountBackend, rt hubRepoType) {
	rec := &requestRecorder{}
	s := newE2EServer(t, withWrap(rec.wrap))
	seed := seedHFMountRepo(t, s, rt, hfMountRepoName(t), false)
	pinned := seed.commit
	head := hubCommit(t, s, rt, seed.repoID, "Add later.txt", hubFileOp("later.txt", "committed after the pinned revision\n"))
	rec.reset()
	m := mountHF(t, b, s, rec, hfMountRepoID(rt, seed.repoID), hfMountOptions{revision: pinned, metadataTTLms: 60000})

	if names, err := readDirNames(m.dir); err != nil || !slices.Equal(names, seed.dirs[""]) {
		t.Errorf("root listing pinned to %s = %v (%v), want %v", pinned, names, err, seed.dirs[""])
	}
	if _, err := os.Stat(filepath.Join(m.dir, "later.txt")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("stat later.txt pinned to %s: %v, want ENOENT", pinned, err)
	}
	if got, err := os.ReadFile(filepath.Join(m.dir, "README.md")); err != nil || string(got) != hfMountReadme {
		t.Errorf("read README.md pinned to %s = %q (%v), want %q", pinned, got, err, hfMountReadme)
	}
	for _, frag := range []string{"/tree/" + pinned, "/xet-read-token/" + pinned, "/resolve/" + pinned + "/README.md"} {
		if !rec.saw(http.MethodGet, frag) {
			t.Errorf("pinned mount never sent GET ...%s\n%s", frag, rec.dump())
		}
	}
	status, body := httpGetContent(t, hubAPIRepoURL(s, rt, seed.repoID))
	var info struct {
		SHA string `json:"sha"`
	}
	if err := json.Unmarshal([]byte(body), &info); status != http.StatusOK || err != nil || info.SHA != head {
		t.Errorf("repo info sha = %q (status %d, %v), want main at %s while the mount stays pinned to %s", info.SHA, status, err, head, pinned)
	}
}

// hfMountPollRow commits after mounting and waits for the poll loop to surface
// the new file. Nothing lists the root or looks up added.txt before the
// commit: the Linux NFS client keeps a cached root listing while the directory
// ctime is unchanged, and hf-mount never changes it.
func hfMountPollRow(t *testing.T, b hfMountBackend, rt hubRepoType) {
	rec := &requestRecorder{}
	s := newE2EServer(t, withWrap(rec.wrap))
	seed := seedHFMountRepo(t, s, rt, hfMountRepoName(t), false)
	rec.reset()
	m := mountHF(t, b, s, rec, hfMountRepoID(rt, seed.repoID), hfMountOptions{
		pollSecs: 1, metadataTTLms: 1000, negativeTTLms: 200,
		// Poll debug lines name each probe's sha in the failure report.
		rustLog: "hf_mount=info,hf_mount::virtual_fs::poll=debug",
	})
	// An exact line: saw's substring match would also hit the tree URL.
	repoInfo := "GET /api/" + rt.apiPrefix + "/" + seed.repoID

	rec.reset()
	// The seed probe may reach hfd only after the commit on slow storage; a second probe proves the first returned the old sha.
	m.waitFor(t, time.Now().Add(15*time.Second), "two revision probes from hf-mount's poll loop before committing", func() bool {
		probes := 0
		for _, l := range strings.Split(rec.dump(), "\n") {
			if l == repoInfo {
				probes++
			}
		}
		return probes >= 2
	})

	const added = "committed while mounted\n"
	rec.reset()
	hubCommit(t, s, rt, seed.repoID, "Add added.txt", hubFileOp("added.txt", added))
	deadline := time.Now().Add(30 * time.Second)
	m.waitFor(t, deadline, "the poll loop to re-list the root (GET .../tree/main) after the new sha", func() bool {
		return rec.saw(http.MethodGet, "/tree/main")
	})
	m.waitFor(t, deadline, "the next poll round ("+repoInfo+") after the re-list", func() bool {
		lines := strings.Split(rec.dump(), "\n")
		relist := slices.IndexFunc(lines, func(l string) bool { return strings.HasPrefix(l, "GET ") && strings.Contains(l, "/tree/main") })
		return relist >= 0 && slices.Contains(lines[relist+1:], repoInfo)
	})
	m.waitFor(t, deadline, "the root listing to show added.txt; the Linux NFS client may serve a cached root listing while the directory ctime is unchanged", func() bool {
		names, err := readDirNames(m.dir)
		return err == nil && slices.Contains(names, "added.txt")
	})
	if got, err := os.ReadFile(filepath.Join(m.dir, "added.txt")); err != nil || string(got) != added {
		t.Errorf("read added.txt through the mount = %q (%v), want %q", got, err, added)
	}
}

// requireHFMount returns the absolute path of b's daemon once it and the
// platform mount tooling it drives are usable. Missing or mismatched
// prerequisites skip locally and fail on CI, like requireGitXet.
func requireHFMount(t *testing.T, b hfMountBackend) string {
	t.Helper()
	missing := func(format string, args ...any) {
		t.Helper()
		if os.Getenv("CI") != "" {
			t.Fatalf(format, args...)
		}
		t.Skipf(format, args...)
	}
	install := "brew install hf-mount"
	if runtime.GOOS == "linux" {
		install = "download " + b.binary + " from the hf-mount v" + hfMountVersion + " release on github.com/huggingface/hf-mount onto PATH"
	}
	bin, err := exec.LookPath(b.binary)
	if err != nil {
		missing("%s %s not found on PATH; %s", b.binary, hfMountVersion, install)
	}
	out, err := runTool(10*time.Second, []string{"PATH=" + os.Getenv("PATH")}, bin, "--version")
	version := strings.TrimSpace(string(out))
	t.Logf("%s --version: %s", bin, version)
	if err != nil || !strings.HasSuffix(version, hfMountVersion) {
		missing("%s --version = %q (%v), want hf-mount %s; %s", bin, version, err, hfMountVersion, install)
	}
	switch {
	case runtime.GOOS == "darwin" && b.fuse:
		const macFUSE = "/Library/Filesystems/macfuse.fs/Contents/Resources/mount_macfuse"
		if _, err := os.Stat(macFUSE); err != nil {
			missing("%s needs macFUSE (%v); brew install --cask macfuse", b.binary, err)
		}
	case runtime.GOOS == "darwin":
		if !findTool("mount_nfs", "/sbin") {
			missing("%s needs mount_nfs, not found on PATH or in /sbin", b.binary)
		}
	case runtime.GOOS == "linux" && b.fuse:
		if !findTool("fusermount3") && !findTool("fusermount") {
			missing("%s needs fusermount3, not found on PATH; sudo apt-get install fuse3", b.binary)
		}
		if _, err := os.Stat("/dev/fuse"); err != nil {
			missing("%s needs /dev/fuse (%v); sudo modprobe fuse", b.binary, err)
		}
		if os.Geteuid() != 0 {
			conf, _ := os.ReadFile("/etc/fuse.conf")
			active := func(line string) bool { return strings.TrimSpace(line) == "user_allow_other" }
			if !slices.ContainsFunc(strings.Split(string(conf), "\n"), active) {
				missing("/etc/fuse.conf lacks user_allow_other (hf-mount mounts with allow_other); add it")
			}
		}
	case runtime.GOOS == "linux":
		if !findTool("mount.nfs", "/sbin", "/usr/sbin", "/bin", "/usr/bin") {
			missing("%s needs mount.nfs, not found on PATH or in /sbin, /usr/sbin, /bin, /usr/bin; sudo apt-get install nfs-common", b.binary)
		}
		if os.Geteuid() != 0 {
			if _, err := exec.LookPath("sudo"); err != nil {
				missing("%s runs mount.nfs through sudo -n when not root, but sudo is not on PATH; run as root or install sudo with NOPASSWD", b.binary)
			}
			if out, err := runTool(10*time.Second, nil, "sudo", "-n", "true"); err != nil {
				missing("%s runs mount.nfs through sudo -n when not root, but sudo -n true failed (%v): %s; grant NOPASSWD sudo or run as root", b.binary, err, out)
			}
		}
	default:
		missing("the hf-mount matrix runs on linux and darwin, not %s", runtime.GOOS)
	}
	return bin
}

// findTool reports whether name is on PATH or in one of dirs.
func findTool(name string, dirs ...string) bool {
	if _, err := exec.LookPath(name); err == nil {
		return true
	}
	return slices.ContainsFunc(dirs, func(dir string) bool {
		_, err := exec.LookPath(filepath.Join(dir, name))
		return err == nil
	})
}

// runTool runs a short-lived command under a watchdog; a nil env inherits the test's.
func runTool(timeout time.Duration, env []string, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = env
	cmd.WaitDelay = time.Second
	return cmd.CombinedOutput()
}

// hfMountRepoName is unique per backend, row and storage pass, e.g. hfmount/nfs-model-s3.
func hfMountRepoName(t *testing.T) string {
	pass := "local"
	if testS3Client != nil {
		pass = "s3"
	}
	_, row, _ := strings.Cut(t.Name(), "/")
	return "hfmount/" + strings.ToLower(strings.ReplaceAll(row, "/", "-")) + "-" + pass
}

// hfMountRepoID is hf-mount's repo_id: org/name for models, datasets/org/name for datasets.
func hfMountRepoID(rt hubRepoType, repoID string) string {
	return strings.TrimPrefix(rt.resolvePrefix+"/"+repoID, "/")
}

// hfMountSeed is what seedHFMountRepo committed, to compare through the mount.
type hfMountSeed struct {
	rt        hubRepoType
	repoID    string              // org/name as the Hub API addresses it
	files     []hubFile           // regular files of the first commit
	dirs      map[string][]string // each directory ("" is the root) to its sorted child names
	commit    string              // the regular files' commit
	lfs       []byte              // model.bin; nil without LFS
	lfsOID    string
	lfsCommit string
}

// seedHFMountRepo creates repoID through the Hub API with hfMountFiles and,
// withLFS, a model.bin ingested through the CAS. It fails fast unless hfd
// already serves what hf-mount relies on: a root tree of exactly the seeded
// names plus .gitattributes, and model.bin resolving with an X-Xet-Hash that
// its tree entry repeats.
func seedHFMountRepo(t *testing.T, s *e2eServer, rt hubRepoType, repoID string, withLFS bool) hfMountSeed {
	t.Helper()
	seed := hfMountSeed{rt: rt, repoID: repoID, files: hfMountFiles}
	seed.commit = hubSeedFiles(t, s, rt, repoID, seed.files, "Seed the hf-mount fixture")
	paths := []string{".gitattributes"}
	for _, f := range seed.files {
		paths = append(paths, f.path)
	}
	if withLFS {
		seed.lfs = randomData(t, hfMountLFSSize, 77)
		sum := sha256.Sum256(seed.lfs)
		seed.lfsOID = hex.EncodeToString(sum[:])
		casURL, token := mintXetWriteToken(t, s, rt, repoID)
		xc, err := xetclient.NewClient(
			xetclient.WithCache(xetclient.NewCache(t.TempDir(), 0, 0)),
			xetclient.WithUpstreamProvider(xetclient.StaticUpstreamProvider(casURL, token)),
		)
		if err != nil {
			t.Fatalf("create xet client: %v", err)
		}
		if _, err := xc.UploadFile(t.Context(), bytes.NewReader(seed.lfs)); err != nil {
			t.Fatalf("xet upload of model.bin: %v", err)
		}
		seed.lfsCommit = hubCommit(t, s, rt, repoID, "Add model.bin", map[string]any{"key": "lfsFile", "value": map[string]any{
			"path": "model.bin", "algo": "sha256", "oid": seed.lfsOID, "size": hfMountLFSSize,
		}})
		paths = append(paths, "model.bin")
	}
	seed.dirs = hfMountDirs(paths)

	entries := hubTree(t, s, rt, repoID)
	listed := make([]string, 0, len(entries))
	for _, e := range entries {
		listed = append(listed, e.Path)
	}
	slices.Sort(listed)
	if !slices.Equal(listed, seed.dirs[""]) {
		t.Fatalf("precondition: the tree API lists %v at the root of %s, want the seeded names plus .gitattributes %v", listed, repoID, seed.dirs[""])
	}
	if !withLFS {
		return seed
	}
	url := hubResolveURL(s, rt, repoID, "main", "model.bin")
	req, err := http.NewRequestWithContext(t.Context(), http.MethodHead, url, nil)
	if err != nil {
		t.Fatalf("build HEAD %s: %v", url, err)
	}
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := noRedirect.Do(req)
	if err != nil {
		t.Fatalf("HEAD %s: %v", url, err)
	}
	resp.Body.Close()
	xetHash := resp.Header.Get("X-Xet-Hash")
	if resp.StatusCode != http.StatusOK || xetHash == "" {
		t.Fatalf("precondition: HEAD %s = %d with X-Xet-Hash %q, want 200 naming the ingested object's xet hash", url, resp.StatusCode, xetHash)
	}
	e := entries[slices.IndexFunc(entries, func(entry hubTreeEntry) bool { return entry.Path == "model.bin" })]
	if e.XetHash != xetHash || e.LFS == nil || e.LFS.Size != hfMountLFSSize {
		t.Fatalf("precondition: the tree API lists model.bin with xetHash %q and lfs %+v, want xetHash %q and lfs.size %d", e.XetHash, e.LFS, xetHash, hfMountLFSSize)
	}
	return seed
}

// hfMountDirs maps every directory of paths ("" is the root) to its sorted child names.
func hfMountDirs(paths []string) map[string][]string {
	dirs := map[string][]string{}
	for _, p := range paths {
		parent := ""
		for name := range strings.SplitSeq(p, "/") {
			if !slices.Contains(dirs[parent], name) {
				dirs[parent] = append(dirs[parent], name)
			}
			parent = path.Join(parent, name)
		}
	}
	for _, names := range dirs {
		slices.Sort(names)
	}
	return dirs
}

// mintXetWriteToken mints a CAS write token through the Hub route clients use.
func mintXetWriteToken(t *testing.T, s *e2eServer, rt hubRepoType, repoID string) (casURL, token string) {
	t.Helper()
	url := hubAPIRepoURL(s, rt, repoID) + "/xet-write-token/main"
	status, body := httpGetContent(t, url)
	var tok struct {
		CasURL      string `json:"casUrl"`
		AccessToken string `json:"accessToken"`
	}
	if err := json.Unmarshal([]byte(body), &tok); status != http.StatusOK || err != nil || tok.CasURL == "" || tok.AccessToken == "" {
		t.Fatalf("GET %s = %d (%v), want a CAS write token\nbody: %s", url, status, err, body)
	}
	return tok.CasURL, tok.AccessToken
}

// hubCommit posts ops as one NDJSON commit to main of an existing repo and
// returns the new commit oid.
func hubCommit(t *testing.T, s *e2eServer, rt hubRepoType, repoID, summary string, ops ...map[string]any) string {
	t.Helper()
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, op := range append([]map[string]any{{"key": "header", "value": map[string]any{"summary": summary}}}, ops...) {
		if err := enc.Encode(op); err != nil {
			t.Fatalf("encode commit %q: %v", summary, err)
		}
	}
	url := hubAPIRepoURL(s, rt, repoID) + "/commit/main"
	resp, err := http.Post(url, "application/x-ndjson", &body)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	var result struct {
		CommitOid string `json:"commitOid"`
	}
	if resp.StatusCode != http.StatusOK || err != nil || json.Unmarshal(raw, &result) != nil || len(result.CommitOid) < 40 {
		t.Fatalf("POST %s %q = %d (%v), want 200 with a commitOid\nbody: %s", url, summary, resp.StatusCode, err, raw)
	}
	return result.CommitOid
}

// hubFileOp is the NDJSON commit operation adding a regular file.
func hubFileOp(name, content string) map[string]any {
	return map[string]any{"key": "file", "value": map[string]any{"path": name, "content": content, "encoding": "utf-8"}}
}

// hubTreeEntry is the part of a tree API entry hf-mount consumes.
type hubTreeEntry struct {
	Path    string `json:"path"`
	XetHash string `json:"xetHash"`
	LFS     *struct {
		Size int64 `json:"size"`
	} `json:"lfs"`
}

// hubTree lists the root of main through the tree API.
func hubTree(t *testing.T, s *e2eServer, rt hubRepoType, repoID string) []hubTreeEntry {
	t.Helper()
	url := hubAPIRepoURL(s, rt, repoID) + "/tree/main"
	status, body := httpGetContent(t, url)
	if status != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200\nbody: %s", url, status, body)
	}
	var entries []hubTreeEntry
	if err := json.Unmarshal([]byte(body), &entries); err != nil {
		t.Fatalf("decode GET %s: %v\nbody: %s", url, err, body)
	}
	return entries
}

// hfMountOptions are a row's daemon flags.
type hfMountOptions struct {
	token         string
	revision      string
	pollSecs      int
	metadataTTLms int
	negativeTTLms int    // 0 omits --negative-ttl-ms
	rustLog       string // RUST_LOG; "" means hf_mount=info
	expectExit    bool   // the daemon must exit on its own: skip readiness
}

// hfMount is one running hf-mount daemon and the mountpoint it serves.
type hfMount struct {
	backend hfMountBackend
	rec     *requestRecorder
	dir     string // the mountpoint, outside t.TempDir
	cmdline string
	cmd     *exec.Cmd
	cancel  context.CancelFunc
	out     *lockedBuffer
	done    chan struct{} // closed once Wait returned and waitErr is set
	waitErr error
	probe   string // the readiness probe's last finding
}

// lockedBuffer collects a daemon's stdout and stderr while the test reads them.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// mountHF starts b's daemon mounting repoID (hf-mount's repo_id) from s and,
// unless opts.expectExit, waits until README.md is served through the mount.
// Callers reset rec first, so startup requests are attributable.
func mountHF(t *testing.T, b hfMountBackend, s *e2eServer, rec *requestRecorder, repoID string, opts hfMountOptions) *hfMount {
	t.Helper()
	cacheDir, home := t.TempDir(), t.TempDir()
	// Outside t.TempDir: a stuck mount must not break TempDir's cleanup.
	dir, err := os.MkdirTemp("", "hfd-hfmount-")
	if err != nil {
		t.Fatalf("create mountpoint: %v", err)
	}

	var args []string
	if opts.token != "" {
		args = append(args, "--hf-token", opts.token)
	}
	args = append(args,
		"--hub-endpoint", s.httpURL,
		"--cache-dir", cacheDir,
		"--poll-interval-secs", strconv.Itoa(opts.pollSecs),
		"--metadata-ttl-ms", strconv.Itoa(opts.metadataTTLms),
	)
	if opts.negativeTTLms > 0 {
		args = append(args, "--negative-ttl-ms", strconv.Itoa(opts.negativeTTLms))
	}
	// Owner-only mounts returned EACCES to the mounting user on the GitHub runner; allow_other is what hf-mount's own CI exercises.
	if b.fuse && runtime.GOOS == "darwin" {
		args = append(args, "--fuse-owner-only")
	}
	args = append(args, "repo", repoID, dir)
	if opts.revision != "" {
		args = append(args, "--revision", opts.revision)
	}

	// Not t.Context: it is canceled before cleanups, which still need the daemon.
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, b.binary, args...)
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + home,
		"NO_PROXY=*",
		"no_proxy=*",
		"RUST_LOG=" + cmp.Or(opts.rustLog, "hf_mount=info"),
	}
	out := &lockedBuffer{}
	cmd.Stdout, cmd.Stderr = out, out
	// SIGTERM lets the daemon unmount itself; WaitDelay escalates to SIGKILL.
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 10 * time.Second
	m := &hfMount{
		backend: b,
		rec:     rec,
		dir:     dir,
		cmdline: b.binary + " " + strings.Join(args, " "),
		cmd:     cmd,
		cancel:  cancel,
		out:     out,
		done:    make(chan struct{}),
	}
	if err := cmd.Start(); err != nil {
		cancel()
		_ = os.Remove(dir)
		t.Fatalf("start %s: %v", m.cmdline, err)
	}
	go func() {
		m.waitErr = cmd.Wait()
		close(m.done)
	}()
	t.Cleanup(func() { m.teardown(t) })

	if opts.expectExit {
		return m
	}
	readme := filepath.Join(dir, "README.md")
	m.waitFor(t, time.Now().Add(30*time.Second), "hf-mount to serve "+readme, func() bool {
		mounted := isMounted(t, dir)
		// A lookup, not a root listing the NFS client could keep cached.
		err, returned := within(hfMountProbeBudget, func() error {
			_, err := os.Stat(readme)
			return err
		})
		stat := statOutcome(err)
		if !returned {
			stat = fmt.Sprintf("hung for %v", hfMountProbeBudget)
		}
		m.probe = fmt.Sprintf("isMounted=%t, stat: %s", mounted, stat)
		return mounted && returned && err == nil
	})
	return m
}

func (m *hfMount) exited() bool {
	select {
	case <-m.done:
		return true
	default:
		return false
	}
}

// waitExit waits up to d for the daemon to exit, returning its output, whether
// it exited, and its exit error.
func (m *hfMount) waitExit(d time.Duration) (output string, exited bool, exitErr error) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-m.done:
		return m.out.String(), true, m.waitErr
	case <-timer.C:
		return m.out.String(), false, nil
	}
}

// waitFor polls cond every 100 ms until deadline, failing early once the
// daemon exits: its mount is then gone or dead, so waiting cannot help.
func (m *hfMount) waitFor(t *testing.T, deadline time.Time, desc string, cond func() bool) {
	t.Helper()
	for {
		if m.exited() {
			t.Fatalf("hf-mount exited while waiting for %s\n%s", desc, m.report())
		}
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s\n%s\n%s", desc, m.mountState(), m.report())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// kill force-unmounts the mountpoint if still mounted, then SIGKILLs the
// daemon and waits up to 10 s for it.
func (m *hfMount) kill(t *testing.T) {
	t.Helper()
	forceUnmount(t, m.backend, m.dir)
	if !m.exited() {
		_ = m.cmd.Process.Kill()
		m.waitExit(10 * time.Second)
	}
}

// teardown SIGTERMs the daemon so it unmounts itself, forces the unmount and
// SIGKILL only when it does not, and reports a mount left behind.
func (m *hfMount) teardown(t *testing.T) {
	// Should teardown abort, cancel still SIGTERMs and WaitDelay kills.
	defer m.cancel()
	if !m.exited() {
		_ = m.cmd.Process.Signal(syscall.SIGTERM)
		m.waitExit(15 * time.Second)
	}
	if !m.exited() || isMounted(t, m.dir) {
		m.kill(t)
	}
	if line, err := mountLine(m.dir); err != nil || line != "" {
		t.Errorf("hf-mount left %s mounted after teardown (%v); unmount it by hand:\n%s\n%s", m.dir, err, line, m.report())
		return
	}
	// Only once unmounted: touching a dead NFS mountpoint could hang.
	_ = os.Remove(m.dir)
	if t.Failed() {
		t.Logf("hf-mount report:\n%s", m.report())
	}
}

// report is what an hf-mount failure needs: command line, exit status, output
// tail and the requests hfd recorded.
func (m *hfMount) report() string {
	status := "still running"
	if m.exited() {
		status = "exit status 0"
		if m.waitErr != nil {
			status = m.waitErr.Error()
		}
	}
	lines := strings.Split(strings.TrimRight(m.out.String(), "\n"), "\n")
	if len(lines) > 40 {
		lines = lines[len(lines)-40:]
	}
	return fmt.Sprintf("command: %s\nstatus: %s\noutput (last %d lines):\n%s\nrequests:\n%s",
		m.cmdline, status, len(lines), strings.Join(lines, "\n"), m.rec.dump())
}

// mountState is what the test process sees at the mountpoint.
func (m *hfMount) mountState() string {
	var b strings.Builder
	fmt.Fprintf(&b, "readiness probe, last attempt: %s\n", m.probe)
	resolved, err := mountpointPath(m.dir)
	fmt.Fprintf(&b, "mountpoint as matched in the mount table: %s (%v)\n", resolved, err)
	table, err := mountTable()
	var named []string
	for line := range strings.Lines(table) {
		if strings.Contains(line, "hfd-hfmount") {
			named = append(named, strings.TrimSpace(line))
		}
	}
	fmt.Fprintf(&b, "%d mount-table lines name hfd-hfmount (%v)\n", len(named), err)
	for _, line := range named[max(0, len(named)-20):] {
		fmt.Fprintf(&b, "\t%s\n", line)
	}
	fmt.Fprintf(&b, "test process: uid=%d gid=%d euid=%d egid=%d\n", os.Getuid(), os.Getgid(), os.Geteuid(), os.Getegid())
	status, _ := os.ReadFile("/proc/self/status") // none off linux
	for line := range strings.Lines(string(status)) {
		if strings.HasPrefix(line, "Uid:") || strings.HasPrefix(line, "Gid:") {
			fmt.Fprintf(&b, "test process /proc/self/status %s\n", strings.TrimSpace(line))
		}
	}
	ls, returned := within(hfMountProbeBudget, func() string {
		out, err := exec.Command("ls", "-la", m.dir).CombinedOutput()
		return fmt.Sprintf("(%v):\n%s", err, out)
	})
	if !returned {
		ls = fmt.Sprintf("hung for %v", hfMountProbeBudget)
	}
	fmt.Fprintf(&b, "ls -la %s %s", m.dir, ls)
	return strings.TrimRight(b.String(), "\n")
}

// hfMountProbeBudget bounds a stat or ls through a mount whose daemon may never answer.
const hfMountProbeBudget = 5 * time.Second

// within waits at most d for f; f stuck on a hung mount returns once teardown unmounts.
func within[T any](d time.Duration, f func() T) (result T, returned bool) {
	done := make(chan T, 1)
	go func() { done <- f() }()
	select {
	case result = <-done:
		return result, true
	case <-time.After(d):
		return result, false
	}
}

// statOutcome describes a stat result, naming the errnos a broken mount answers with.
func statOutcome(err error) string {
	if err == nil {
		return "ok"
	}
	class := "other"
	for name, errno := range map[string]syscall.Errno{"EACCES": syscall.EACCES, "ENOENT": syscall.ENOENT, "ENOTCONN": syscall.ENOTCONN, "EIO": syscall.EIO} {
		if errors.Is(err, errno) {
			class = name
		}
	}
	return fmt.Sprintf("%s (%v)", class, err)
}

// mountFieldUnescaper undoes the octal escapes /proc/self/mounts applies to paths.
var mountFieldUnescaper = strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)

// mountLine returns dir's mount-table entry, or "" when nothing is mounted
// there. It never touches dir, as a hard NFS mount whose server died would
// hang.
func mountLine(dir string) (string, error) {
	resolved, err := mountpointPath(dir)
	if err != nil {
		return "", err
	}
	table, err := mountTable()
	if err != nil {
		return "", err
	}
	switch runtime.GOOS {
	case "linux":
		for line := range strings.Lines(table) {
			if fields := strings.Fields(line); len(fields) > 1 && mountFieldUnescaper.Replace(fields[1]) == resolved {
				return strings.TrimSpace(line), nil
			}
		}
	case "darwin":
		for line := range strings.Lines(table) {
			if strings.Contains(line, " on "+resolved+" (") {
				return strings.TrimSpace(line), nil
			}
		}
	}
	return "", nil
}

// mountpointPath is dir as mount tables print it, without touching dir.
func mountpointPath(dir string) (string, error) {
	parent, err := filepath.EvalSymlinks(filepath.Dir(dir))
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, filepath.Base(dir)), nil
}

// mountTable is /proc/self/mounts on linux and /sbin/mount's listing on darwin.
func mountTable() (string, error) {
	switch runtime.GOOS {
	case "linux":
		table, err := os.ReadFile("/proc/self/mounts")
		return string(table), err
	case "darwin":
		out, err := runTool(10*time.Second, nil, "/sbin/mount")
		if err != nil {
			return "", fmt.Errorf("mount: %w: %s", err, out)
		}
		return string(out), nil
	}
	return "", nil
}

// isMounted reports whether dir is a mountpoint without touching it.
func isMounted(t *testing.T, dir string) bool {
	t.Helper()
	line, err := mountLine(dir)
	if err != nil {
		t.Fatalf("read the mount table for %s: %v", dir, err)
	}
	return line != ""
}

// forceUnmount detaches dir while it stays mounted, trying the platform's
// forced unmount tools in turn.
func forceUnmount(t *testing.T, b hfMountBackend, dir string) {
	t.Helper()
	var cmds [][]string
	switch {
	case runtime.GOOS == "darwin":
		cmds = [][]string{{"/sbin/umount", "-f", dir}, {"/usr/sbin/diskutil", "unmount", "force", dir}}
	case b.fuse:
		cmds = [][]string{{"fusermount3", "-u", "-z", dir}, {"fusermount", "-u", "-z", dir}}
	case os.Geteuid() == 0:
		cmds = [][]string{{"umount", "-f", "-l", dir}}
	default:
		cmds = [][]string{{"sudo", "-n", "umount", "-f", "-l", dir}}
	}
	for _, args := range cmds {
		if !isMounted(t, dir) {
			return
		}
		out, err := runTool(30*time.Second, nil, args[0], args[1:]...)
		t.Logf("forced unmount: %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

// readDirNames lists dir's names in order.
func readDirNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, err
}

// assertHubParityThroughMount checks through the mount what huggingface.co
// would serve for an LFS seed, with hfd's request log proving the routes.
func assertHubParityThroughMount(t *testing.T, s *e2eServer, m *hfMount, seed hfMountSeed) {
	t.Helper()
	checkMountStartup(t, m, seed)
	checkMountListing(t, m, seed)
	checkMountSizes(t, m, seed)
	checkMountRegularReads(t, m, seed)
	checkMountLFSReads(t, m, seed)
	checkMountMissing(t, m)
	checkMountReadOnly(t, s, m, seed)
}

// checkMountStartup proves the mount came up through hfd's repo info, CAS read
// token and tree routes.
func checkMountStartup(t *testing.T, m *hfMount, seed hfMountSeed) {
	t.Helper()
	for _, frag := range []string{"/api/" + seed.rt.apiPrefix + "/" + seed.repoID, "/xet-read-token/", "/tree/main"} {
		if !m.rec.saw(http.MethodGet, frag) {
			t.Errorf("mount startup never sent GET ...%s\n%s", frag, m.rec.dump())
		}
	}
}

// checkMountListing lists every seeded directory through the mount: names in
// order, directories as directories and files as regular files.
func checkMountListing(t *testing.T, m *hfMount, seed hfMountSeed) {
	t.Helper()
	for _, dir := range slices.Sorted(maps.Keys(seed.dirs)) {
		entries, err := os.ReadDir(filepath.Join(m.dir, filepath.FromSlash(dir)))
		if err != nil {
			t.Errorf("list %q through the mount: %v", dir, err)
			continue
		}
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
			p := path.Join(dir, e.Name())
			_, wantDir := seed.dirs[p]
			kind := "regular file"
			if wantDir {
				kind = "directory"
			}
			info, err := e.Info()
			if err != nil {
				t.Errorf("stat %s through the mount: %v", p, err)
				continue
			}
			if e.IsDir() != wantDir || info.IsDir() != wantDir || (!wantDir && !info.Mode().IsRegular()) {
				t.Errorf("%s through the mount has mode %v (DirEntry.IsDir %v), want a %s", p, info.Mode(), e.IsDir(), kind)
			}
		}
		if !slices.Equal(names, seed.dirs[dir]) {
			t.Errorf("list %q through the mount = %v, want %v", dir, names, seed.dirs[dir])
		}
	}
}

// checkMountSizes stats every file: regular sizes from the tree, model.bin's
// from its LFS pointer.
func checkMountSizes(t *testing.T, m *hfMount, seed hfMountSeed) {
	t.Helper()
	sizes := map[string]int64{"model.bin": int64(len(seed.lfs))}
	for _, f := range seed.files {
		sizes[f.path] = int64(len(f.content))
	}
	for _, p := range slices.Sorted(maps.Keys(sizes)) {
		info, err := os.Stat(filepath.Join(m.dir, filepath.FromSlash(p)))
		if err != nil {
			t.Errorf("stat %s through the mount: %v", p, err)
		} else if info.Size() != sizes[p] {
			t.Errorf("stat %s through the mount: size %d, want %d", p, info.Size(), sizes[p])
		}
	}
}

// checkMountRegularReads reads every regular file byte-exact; hf-mount
// downloads files without a xet hash whole through resolve.
func checkMountRegularReads(t *testing.T, m *hfMount, seed hfMountSeed) {
	t.Helper()
	m.rec.reset()
	for _, f := range seed.files {
		got, err := os.ReadFile(filepath.Join(m.dir, filepath.FromSlash(f.path)))
		if err != nil || string(got) != f.content {
			t.Errorf("read %s through the mount = %q (%v), want %q", f.path, got, err, f.content)
		}
	}
	if !m.rec.saw(http.MethodGet, "/resolve/main/README.md") {
		t.Errorf("reading README.md never sent GET .../resolve/main/README.md\n%s", m.rec.dump())
	}
}

// checkMountLFSReads reads model.bin cold, ranged then whole, and proves the
// bytes came through CAS reconstructions, not the resolve redirect to /xet-bridge.
func checkMountLFSReads(t *testing.T, m *hfMount, seed hfMountSeed) {
	t.Helper()
	m.rec.reset()
	name := filepath.Join(m.dir, "model.bin")
	f, err := os.Open(name)
	if err != nil {
		t.Fatalf("open model.bin through the mount: %v", err)
	}
	defer f.Close()
	for _, r := range []struct{ off, n int }{{1000003, 131072}, {hfMountLFSSize - 70001, 70001}} {
		buf := make([]byte, r.n)
		if n, err := f.ReadAt(buf, int64(r.off)); err != nil || !bytes.Equal(buf, seed.lfs[r.off:r.off+r.n]) {
			t.Errorf("read %d bytes at offset %d of model.bin through the mount: got %d (%v), want the seeded bytes", r.n, r.off, n, err)
		}
	}
	got, err := os.ReadFile(name)
	sum := sha256.Sum256(got)
	if err != nil || !bytes.Equal(got, seed.lfs) || hex.EncodeToString(sum[:]) != seed.lfsOID {
		t.Errorf("read model.bin through the mount: %d bytes with sha256 %x (%v), want %d bytes with sha256 %s", len(got), sum, err, len(seed.lfs), seed.lfsOID)
	}
	if !m.rec.saw(http.MethodGet, "reconstructions") {
		t.Errorf("model.bin reads never queried CAS reconstructions\n%s", m.rec.dump())
	}
	for _, frag := range []string{"/xet-bridge/", "/resolve/main/model.bin"} {
		if m.rec.saw(http.MethodGet, frag) {
			t.Errorf("model.bin reads sent GET ...%s, want CAS reconstructions only\n%s", frag, m.rec.dump())
		}
	}
}

// checkMountMissing looks up names hfd does not have, at the root and below.
func checkMountMissing(t *testing.T, m *hfMount) {
	t.Helper()
	for _, p := range []string{"missing.txt", "nested/missing"} {
		if _, err := os.Stat(filepath.Join(m.dir, filepath.FromSlash(p))); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("stat %s through the mount: %v, want ENOENT", p, err)
		}
	}
}

// checkMountReadOnly expects writes to fail and to leave the mount and the repo unchanged.
func checkMountReadOnly(t *testing.T, s *e2eServer, m *hfMount, seed hfMountSeed) {
	t.Helper()
	for _, w := range []struct {
		desc  string
		write func() error
	}{
		{"create new.txt", func() error { return os.WriteFile(filepath.Join(m.dir, "new.txt"), []byte("rejected\n"), 0o644) }},
		{"mkdir newdir", func() error { return os.Mkdir(filepath.Join(m.dir, "newdir"), 0o755) }},
		{"remove README.md", func() error { return os.Remove(filepath.Join(m.dir, "README.md")) }},
	} {
		if err := w.write(); err == nil {
			t.Errorf("%s through the read-only mount succeeded, want an error", w.desc)
		}
	}
	if names, err := readDirNames(m.dir); err != nil || !slices.Equal(names, seed.dirs[""]) {
		t.Errorf("root listing after the rejected writes = %v (%v), want %v", names, err, seed.dirs[""])
	}
	if slices.ContainsFunc(hubTree(t, s, seed.rt, seed.repoID), func(e hubTreeEntry) bool { return e.Path == "new.txt" }) {
		t.Errorf("the tree API lists new.txt after a write through the read-only mount")
	}
}
