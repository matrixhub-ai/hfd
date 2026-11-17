package mirror_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixhub-ai/hfd/pkg/mirror"
)

// lfsFile is an LFS object's content and the path its pointer is committed at.
type lfsFile struct {
	path string
	data []byte
	oid  string
}

func newLFSFile(path string) lfsFile {
	data := bytes.Repeat([]byte(path+" bytes. "), 2048)
	sum := sha256.Sum256(data)
	return lfsFile{path: path, data: data, oid: hex.EncodeToString(sum[:])}
}

// lfsSource commits a pointer to each file to main of org/repo under root and returns the tip.
func lfsSource(t *testing.T, root string, files ...lfsFile) string {
	t.Helper()
	src, _ := initSourceRepo(t, root, "org/repo")
	var tip string
	for _, f := range files {
		tip = addCommit(t, src, "main", f.path, lfsPointerText(f.oid, len(f.data)))
	}
	return tip
}

// hubFiles answers hub resolve requests for the files of org/repo as fakeHub does for one.
func hubFiles(files ...lfsFile) http.Handler {
	mux := http.NewServeMux()
	for _, f := range files {
		mux.Handle("/org/repo/resolve/{rev}/"+f.path, fakeHubHandler(f.path, f.data, f.oid))
	}
	return mux
}

// lfsHub serves the repositories under root to git and hands hub resolve requests to resolve, recording every request path.
func lfsHub(t *testing.T, root string, resolve http.Handler, paths *recorder) string {
	t.Helper()
	git := gitHTTPBackend(t, root)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths.add(r.URL.Path)
		if strings.Contains(r.URL.Path, "/resolve/") {
			resolve.ServeHTTP(w, r)
			return
		}
		git.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// refSyncGate holds the first ref sync of a mirror at its ref filter until released, counting every ref sync.
type refSyncGate struct {
	syncs   atomic.Int32
	held    chan struct{}
	release chan struct{}
}

func newRefSyncGate() *refSyncGate {
	return &refSyncGate{held: make(chan struct{}), release: make(chan struct{})}
}

// option installs the gate as a ref filter that passes every ref.
func (g *refSyncGate) option() mirror.Option {
	return mirror.WithMirrorRefFilterFunc(func(ctx context.Context, repoName string, refs []string) ([]string, error) {
		if g.syncs.Add(1) == 1 {
			close(g.held)
			<-g.release
		}
		return refs, nil
	})
}

// overlapPulls starts a pull of org/repo with first, joins a pull with second to its held ref sync, then releases both.
func overlapPulls(t *testing.T, m *mirror.Mirror, gate *refSyncGate, first, second *mirror.PullOptions) {
	t.Helper()
	release := sync.OnceFunc(func() { close(gate.release) })
	t.Cleanup(release)
	repoPath := filepath.Join(t.TempDir(), "org", "repo.git")
	errs := make(chan error, 2)
	pull := func(opts *mirror.PullOptions) {
		errs <- m.PullFromRemote(context.Background(), repoPath, "org/repo", opts)
	}
	go pull(first)
	select {
	case <-gate.held:
	case err := <-errs:
		t.Fatalf("first pull ended before its ref sync: %v", err)
	}
	go pull(second)
	waitJoined(t)
	release()
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("pull from remote: %v", err)
		}
	}
	if n := gate.syncs.Load(); n != 1 {
		t.Fatalf("ref syncs = %d, want 1 shared by both pulls", n)
	}
}

// waitJoined waits for a pull parked on another pull's shared ref sync, which only the goroutine stacks show.
func waitJoined(t *testing.T) {
	t.Helper()
	buf := make([]byte, 4<<20)
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		for _, g := range strings.Split(string(buf[:runtime.Stack(buf, true)]), "\n\n") {
			if strings.Contains(g, "mirror.(*Mirror).PullFromRemote(") && strings.Contains(g, "singleflight.(*Group).Do(") && strings.Contains(g, "sync.(*WaitGroup).Wait(") {
				return
			}
		}
	}
	t.Fatal("the joining pull was not found in the goroutine dump; finding it depends on x/sync and runtime frame names")
}

// downloadPeak holds every object download a while and records the most that ran at once.
type downloadPeak struct {
	mu             sync.Mutex
	inflight, peak int
}

func (d *downloadPeak) hold(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			next.ServeHTTP(w, r)
			return
		}
		d.mu.Lock()
		d.inflight++
		d.peak = max(d.peak, d.inflight)
		d.mu.Unlock()
		defer func() {
			d.mu.Lock()
			d.inflight--
			d.mu.Unlock()
		}()
		// Long enough for a download of a second prefetch batch to overlap.
		time.Sleep(500 * time.Millisecond)
		next.ServeHTTP(w, r)
	})
}

// Full pulls sharing one scan prefetch it in one batch, so no more of its objects ingest at once than WithPrefetchConcurrency allows.
func TestOverlappingPullsPrefetchOneBatch(t *testing.T) {
	root := t.TempDir()
	files := []lfsFile{newLFSFile("model-a.bin"), newLFSFile("model-b.bin"), newLFSFile("model-c.bin")}
	lfsSource(t, root, files...)
	var downloads downloadPeak
	gate := newRefSyncGate()
	m := newMirror(t, lfsHub(t, root, downloads.hold(hubFiles(files...)), new(recorder)), mirror.WithPrefetchConcurrency(1), gate.option())

	overlapPulls(t, m, gate, nil, nil)
	m.Wait()

	for _, f := range files {
		if !m.HasObject(context.Background(), f.oid) {
			t.Errorf("object %s was not prefetched", f.path)
		}
	}
	downloads.mu.Lock()
	defer downloads.mu.Unlock()
	if downloads.peak != 1 {
		t.Errorf("peak concurrent object downloads = %d, want 1", downloads.peak)
	}
}

// One prefetch batch per shared scan, not per mirror: a later full pull still prefetches an object added upstream since the first.
func TestLaterPullPrefetchesObjectAddedUpstream(t *testing.T) {
	root := t.TempDir()
	first, added := newLFSFile("model-a.bin"), newLFSFile("model-b.bin")
	src, _ := initSourceRepo(t, root, "org/repo")
	addCommit(t, src, "main", first.path, lfsPointerText(first.oid, len(first.data)))
	m := newMirror(t, lfsHub(t, root, hubFiles(first, added), new(recorder)))
	ctx := context.Background()
	repoPath := filepath.Join(t.TempDir(), "org", "repo.git")
	pull := func() {
		t.Helper()
		if err := m.PullFromRemote(ctx, repoPath, "org/repo", nil); err != nil {
			t.Fatalf("pull from remote: %v", err)
		}
		m.Wait()
	}

	pull()
	if !m.HasObject(ctx, first.oid) {
		t.Fatalf("object %s was not prefetched by the first pull", first.path)
	}
	addCommit(t, src, "main", added.path, lfsPointerText(added.oid, len(added.data)))
	pull()
	if !m.HasObject(ctx, added.oid) {
		t.Errorf("object %s added upstream was not prefetched by the later pull", added.path)
	}
}

// objectRequests keeps the paths that fetch LFS objects: hub resolves and git-lfs batches.
func objectRequests(paths []string) []string {
	var objects []string
	for _, p := range paths {
		if strings.Contains(p, "/resolve/") || strings.Contains(p, "/info/lfs/") {
			objects = append(objects, p)
		}
	}
	return objects
}

// pullTwice pulls org/repo into a new mirror path twice: cold, creating it, then warm, with no ref to change.
func pullTwice(t *testing.T, m *mirror.Mirror, opts *mirror.PullOptions) {
	t.Helper()
	repoPath := filepath.Join(t.TempDir(), "org", "repo.git")
	for range 2 {
		if err := m.PullFromRemote(context.Background(), repoPath, "org/repo", opts); err != nil {
			t.Fatalf("pull from remote: %v", err)
		}
	}
}

// Every pull registers the LFS objects at the ref tips and fetches them unless its policy is PullOnDemand; a pull's own policy outranks the mirror's.
func TestPullPolicy(t *testing.T) {
	onDemand := mirror.WithPullPolicy(mirror.PullOnDemand)
	for _, tc := range []struct {
		name     string
		mirror   []mirror.Option
		opts     *mirror.PullOptions
		prefetch bool
	}{
		{name: "Unset", prefetch: true},
		{name: "MirrorZero", mirror: []mirror.Option{mirror.WithPullPolicy("")}, prefetch: true},
		{name: "MirrorUnknown", mirror: []mirror.Option{mirror.WithPullPolicy("lazy")}, prefetch: true},
		{name: "MirrorOnDemand", mirror: []mirror.Option{onDemand}},
		{name: "MirrorOnDemandZeroPullPolicy", mirror: []mirror.Option{onDemand}, opts: &mirror.PullOptions{}},
		{name: "MirrorOnDemandPullFull", mirror: []mirror.Option{onDemand}, opts: &mirror.PullOptions{Policy: mirror.PullFull}, prefetch: true},
		{name: "MirrorOnDemandPullUnknown", mirror: []mirror.Option{onDemand}, opts: &mirror.PullOptions{Policy: "lazy"}, prefetch: true},
		{name: "PullOnDemand", opts: &mirror.PullOptions{Policy: mirror.PullOnDemand}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			files := []lfsFile{newLFSFile("model-a.bin"), newLFSFile("model-b.bin")}
			lfsSource(t, root, files...)
			var paths recorder
			m := newMirror(t, lfsHub(t, root, hubFiles(files...), &paths), tc.mirror...)

			pullTwice(t, m, tc.opts)
			m.Wait()

			ctx := context.Background()
			for _, f := range files {
				if !m.KnowsObject(ctx, f.oid) {
					t.Errorf("KnowsObject(%s) = false, want the pull scan to register it", f.path)
				}
				if got := m.HasObject(ctx, f.oid); got != tc.prefetch {
					t.Errorf("HasObject(%s) = %v, want %v", f.path, got, tc.prefetch)
				}
			}
			if got := objectRequests(paths.all()); !tc.prefetch && len(got) > 0 {
				t.Errorf("upstream object requests = %q, want none", got)
			}
		})
	}
}

// Under PullOnDemand the first read of an object fetches that object from the upstream and no other.
func TestPullOnDemandFetchesOnFirstRead(t *testing.T) {
	root := t.TempDir()
	read, other := newLFSFile("model-a.bin"), newLFSFile("model-b.bin")
	tip := lfsSource(t, root, read, other)
	var paths recorder
	m := newMirror(t, lfsHub(t, root, hubFiles(read, other), &paths), mirror.WithPullPolicy(mirror.PullOnDemand))
	pullTwice(t, m, nil)
	m.Wait()

	rec := httptest.NewRecorder()
	served := m.ServeOID(rec, httptest.NewRequest(http.MethodGet, "/anything", nil), read.oid)
	if !served || rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), read.data) {
		t.Fatalf("ServeOID = %v, status %d, body %d bytes, want 200 with %d bytes", served, rec.Code, rec.Body.Len(), len(read.data))
	}
	ctx := context.Background()
	deadline := time.Now().Add(30 * time.Second)
	for !m.HasObject(ctx, read.oid) {
		if time.Now().After(deadline) {
			t.Fatalf("object %s was never ingested after its first read", read.path)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if m.HasObject(ctx, other.oid) {
		t.Errorf("HasObject(%s) = true, want only the read object ingested", other.path)
	}
	wantOnly(t, "upstream object requests", objectRequests(paths.all()), "/org/repo/resolve/"+tip+"/"+read.path)
}

// A PullFull pull that joins a PullOnDemand pull's ref sync still prefetches what their shared scan found.
func TestPullFullJoiningOnDemandPullPrefetches(t *testing.T) {
	root := t.TempDir()
	files := []lfsFile{newLFSFile("model-a.bin"), newLFSFile("model-b.bin")}
	lfsSource(t, root, files...)
	gate := newRefSyncGate()
	m := newMirror(t, lfsHub(t, root, hubFiles(files...), new(recorder)), gate.option())

	overlapPulls(t, m, gate, &mirror.PullOptions{Policy: mirror.PullOnDemand}, &mirror.PullOptions{Policy: mirror.PullFull})
	m.Wait()

	for _, f := range files {
		if !m.HasObject(context.Background(), f.oid) {
			t.Errorf("object %s was not prefetched for the joining PullFull pull", f.path)
		}
	}
}
