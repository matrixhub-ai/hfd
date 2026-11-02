package repository

// Tests for the serve lock: go-git upload-pack serves share a per-repository lock that Repack, live GC and maintain take exclusively.

import (
	"bytes"
	"context"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
)

// slTimeout bounds every wait for what the lock must let happen; generous so a loaded -race run never flakes.
const slTimeout = 30 * time.Second

// slOp is a call running in the background.
type slOp struct {
	done chan struct{}
	err  error
}

// slGo runs fn in the background with the test's context, which is cancelled before the cleanup waiting for fn.
func slGo(t *testing.T, fn func(ctx context.Context) error) *slOp {
	ctx := t.Context()
	o := &slOp{done: make(chan struct{})}
	go func() {
		defer close(o.done)
		o.err = fn(ctx)
	}()
	t.Cleanup(func() {
		select {
		case <-o.done:
		case <-time.After(slTimeout):
			t.Errorf("a background call outlived its test by %v", slTimeout)
		}
	})
	return o
}

// wait requires o to return nil within slTimeout.
func (o *slOp) wait(t *testing.T, what string) {
	t.Helper()
	select {
	case <-o.done:
	case <-time.After(slTimeout):
		t.Fatalf("%s did not return within %v", what, slTimeout)
	}
	if o.err != nil {
		t.Fatalf("%s: %v", what, o.err)
	}
}

// slWriter discards a serve's response, blocking each write holding mark until release closes; an empty mark matches every write.
type slWriter struct {
	ctx     context.Context
	mark    []byte
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *slWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, w.mark) {
		w.once.Do(func() { close(w.reached) })
		select {
		case <-w.release:
		case <-w.ctx.Done():
			return 0, w.ctx.Err()
		}
	}
	return len(p), nil
}

// slStall reads nothing until release closes, then reports EOF; reached closes at its first Read.
type slStall struct {
	ctx     context.Context
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *slStall) Read([]byte) (int, error) {
	s.once.Do(func() { close(s.reached) })
	select {
	case <-s.release:
		return 0, io.EOF
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	}
}

// slServe is an upload-pack serve whose response blocks at its mark until finish.
type slServe struct {
	*slOp
	out *slWriter
}

// slStartServe runs serve in the background, its response going to an slWriter blocking at mark.
func slStartServe(t *testing.T, mark string, serve func(ctx context.Context, w io.Writer) error) *slServe {
	out := &slWriter{ctx: t.Context(), mark: []byte(mark), reached: make(chan struct{}), release: make(chan struct{})}
	return &slServe{slOp: slGo(t, func(ctx context.Context) error { return serve(ctx, out) }), out: out}
}

// slUploadPack starts a stateless protocol v0 clone of want from repo whose response blocks at its pack header.
func slUploadPack(t *testing.T, repo *Repository, want plumbing.Hash) *slServe {
	t.Helper()
	body := uploadPackRequest(t, want.String(), "", nil, true)
	return slStartServe(t, "PACK", func(ctx context.Context, w io.Writer) error {
		return repo.Stateless(ctx, w, bytes.NewReader(body), GitUploadPack, "", ReceivePackHooks{})
	})
}

// inFlight waits until the serve blocks on its response, inside whatever lock it holds.
func (s *slServe) inFlight(t *testing.T, what string) {
	t.Helper()
	select {
	case <-s.out.reached:
	case <-s.done:
		t.Fatalf("%s returned %v before blocking on its response", what, s.err)
	case <-time.After(slTimeout):
		t.Fatalf("%s did not block on its response within %v", what, slTimeout)
	}
}

// finish releases the serve's response and requires the serve to succeed.
func (s *slServe) finish(t *testing.T) {
	t.Helper()
	close(s.out.release)
	s.wait(t, "the released upload-pack serve")
}

// slQueue starts op and requires it to queue on held's serve lock, neither returning nor writing to f's repository while a serve is in flight.
func slQueue(t *testing.T, f *rpFixture, held *Repository, what string, op func(ctx context.Context) error) *slOp {
	t.Helper()
	mu := held.serveMu
	if mu == nil {
		t.Fatal("Open left Repository.serveMu nil")
	}
	before := snapshotFiles(t, f.fs, f.path)
	o := slGo(t, op)
	deadline := time.After(slTimeout)
	// The serve holds a read lock, so TryRLock fails only once a writer queues.
	for mu.TryRLock() {
		mu.RUnlock()
		select {
		case <-o.done:
			t.Fatalf("%s returned %v during an upload-pack serve; want it to wait for the serve", what, o.err)
		case <-deadline:
			t.Fatalf("%s neither returned nor queued on the serve lock within %v", what, slTimeout)
		case <-time.After(time.Millisecond):
		}
	}
	if changed := rpChanged(before, snapshotFiles(t, f.fs, f.path)); len(changed) > 0 {
		t.Fatalf("%s changed %v during an upload-pack serve", what, changed)
	}
	return o
}

// slMaintain requires maintain to return want, nil within slTimeout.
func slMaintain(t *testing.T, repo *Repository, want bool, when string) {
	t.Helper()
	var busy bool
	slGo(t, func(ctx context.Context) (err error) {
		busy, err = repo.maintain(ctx)
		return err
	}).wait(t, "maintain "+when)
	if busy != want {
		t.Fatalf("maintain %s = %t, nil; want %t, nil", when, busy, want)
	}
}

// Repack waits for every kind of go-git upload-pack serve in flight, then succeeds.
func TestServeLockRepackWaitsForUploadPack(t *testing.T) {
	setGitBinary(t, "")
	clone := func(t *testing.T, want string) []byte { return uploadPackRequest(t, want, "", nil, true) }
	for _, tc := range []struct {
		name, protocol string
		mark           string // "" blocks the response's first write
		stateful       bool   // Serve rather than Stateless
		body           func(t *testing.T, want string) []byte
	}{
		{name: "StatelessV0Clone", mark: "PACK", body: clone},
		{name: "StatelessV0Round", body: func(t *testing.T, want string) []byte {
			return uploadPackRequest(t, want, "multi_ack_detailed", []string{want}, false)
		}},
		{name: "StatelessV2Fetch", protocol: "version=2", mark: "PACK", body: func(t *testing.T, want string) []byte {
			// 0001 is the delim-pkt ending the command's capabilities.
			return slices.Concat(pktLines(t, "command=fetch\n"), []byte("0001"), pktLines(t, "want "+want+"\n", "done\n", ""))
		}},
		{name: "ServeV0Clone", mark: "PACK", stateful: true, body: clone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := rpMixedRepo(t, memfs.New(), "/repo.git", false)
			body := tc.body(t, f.main.String())
			s := slStartServe(t, tc.mark, func(ctx context.Context, w io.Writer) error {
				if tc.stateful {
					return f.repo.Serve(ctx, struct {
						io.Reader
						io.Writer
					}{bytes.NewReader(body), w}, GitUploadPack, tc.protocol, ReceivePackHooks{})
				}
				return f.repo.Stateless(ctx, w, bytes.NewReader(body), GitUploadPack, tc.protocol, ReceivePackHooks{})
			})
			s.inFlight(t, "upload-pack serve")
			repack := slQueue(t, f, f.repo, "Repack", func(ctx context.Context) error { return f.repo.Repack(ctx, time.Time{}) })
			s.finish(t)
			repack.wait(t, "the queued Repack")
			rpNeedsRepack(t, f.repo, false, "after the queued Repack")
		})
	}
}

// Repack runs while a Stateless upload-pack waits for the rest of its request, which the serve then answers as it would have at once.
func TestServeLockRepackRunsWhileRequestPending(t *testing.T) {
	setGitBinary(t, "")
	for _, tc := range []struct {
		name, protocol string
		body           func(t *testing.T, want string) []byte
	}{
		{name: "V0Clone", body: func(t *testing.T, want string) []byte { return uploadPackRequest(t, want, "", nil, true) }},
		{name: "V2Fetch", protocol: "version=2", body: func(t *testing.T, want string) []byte {
			return slices.Concat(pktLines(t, "command=fetch\n"), []byte("0001"), pktLines(t, "want "+want+"\n", "done\n", ""))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := rpMixedRepo(t, memfs.New(), "/repo.git", false)
			body := tc.body(t, f.main.String())
			stall := &slStall{ctx: t.Context(), reached: make(chan struct{}), release: make(chan struct{})}
			// The request's last byte waits for the stall.
			in := io.MultiReader(bytes.NewReader(body[:len(body)-1]), stall, bytes.NewReader(body[len(body)-1:]))
			var out bytes.Buffer
			serve := slGo(t, func(ctx context.Context) error {
				return f.repo.Stateless(ctx, &out, in, GitUploadPack, tc.protocol, ReceivePackHooks{})
			})
			select {
			case <-stall.reached:
			case <-serve.done:
				t.Fatalf("upload-pack serve returned %v before waiting for the rest of its request", serve.err)
			case <-time.After(slTimeout):
				t.Fatalf("upload-pack serve did not wait for the rest of its request within %v", slTimeout)
			}
			slGo(t, func(ctx context.Context) error { return f.repo.Repack(ctx, time.Time{}) }).wait(t, "Repack while an upload-pack request is pending")
			rpNeedsRepack(t, f.repo, false, "after the Repack")
			close(stall.release)
			serve.wait(t, "the upload-pack serve once its request arrived")
			var want bytes.Buffer
			if err := f.repo.Stateless(t.Context(), &want, bytes.NewReader(body), GitUploadPack, tc.protocol, ReceivePackHooks{}); err != nil {
				t.Fatalf("the same request served at once: %v", err)
			}
			if _, pack := ruSplit(t, want.Bytes()); !bytes.HasPrefix(pack, []byte("PACK")) {
				t.Fatalf("the same request served at once answered %.40q, want a pack", want.Bytes())
			}
			if !bytes.Equal(out.Bytes(), want.Bytes()) {
				t.Errorf("the pending upload-pack answered %d bytes %.40q, want the %d bytes %.40q the same request gets at once",
					out.Len(), out.Bytes(), want.Len(), want.Bytes())
			}
		})
	}
}

// Upload-pack serves share the lock: two run at once, each held mid-pack until both are.
func TestServeLockUploadPacksRunTogether(t *testing.T) {
	setGitBinary(t, "")
	f := rpMixedRepo(t, memfs.New(), "/repo.git", false)
	first, second := slUploadPack(t, f.repo, f.main), slUploadPack(t, f.repo, f.main)
	first.inFlight(t, "the first of two concurrent upload-pack serves")
	second.inFlight(t, "the second of two concurrent upload-pack serves")
	first.finish(t)
	second.finish(t)
}

// maintain reports busy at once, writing nothing, while an upload-pack serve is in flight.
func TestServeLockMaintainBusyDuringUploadPack(t *testing.T) {
	setGitBinary(t, "")
	f := rpMixedRepo(t, memfs.New(), "/repo.git", false)
	rpNeedsRepack(t, f.repo, true, "before the serve")
	s := slUploadPack(t, f.repo, f.main)
	s.inFlight(t, "upload-pack serve")
	before := snapshotFiles(t, f.fs, f.path)
	slMaintain(t, f.repo, true, "during an upload-pack serve")
	if changed := rpChanged(before, snapshotFiles(t, f.fs, f.path)); len(changed) > 0 {
		t.Fatalf("busy maintain changed %v", changed)
	}
	s.finish(t)
}

// An idle maintain repacks a repository that needs it.
func TestServeLockMaintainRepacksWhenIdle(t *testing.T) {
	setGitBinary(t, "")
	f := rpMixedRepo(t, memfs.New(), "/repo.git", false)
	rpNeedsRepack(t, f.repo, true, "before maintain")
	slMaintain(t, f.repo, false, "on an idle repository needing a repack")
	rpLayout(t, f)
	rpNeedsRepack(t, f.repo, false, "after maintain")
}

// An idle maintain with nothing to repack writes nothing, though Repack would write an unborn repository's manifest.
func TestServeLockMaintainIdleWithoutWork(t *testing.T) {
	setGitBinary(t, "")
	repo := initTestRepo(t)
	rpNeedsRepack(t, repo, false, "on an unborn repository")
	before := snapshotFiles(t, repo.fs, repo.repoPath)
	slMaintain(t, repo, false, "with nothing to repack")
	if changed := rpChanged(before, snapshotFiles(t, repo.fs, repo.repoPath)); len(changed) > 0 {
		t.Fatalf("maintain with nothing to repack changed %v", changed)
	}
}

// Handles of one repository share its lock, even one opened anew after lruCache evicted the first.
func TestServeLockSharedAcrossHandles(t *testing.T) {
	setGitBinary(t, "")
	f := rpMixedRepo(t, memfs.New(), "/repo.git", false)
	lruCache.Remove(cacheKey{f.fs, f.path})
	other, err := Open(f.fs, f.path)
	if err != nil {
		t.Fatalf("reopen after eviction: %v", err)
	}
	if other == f.repo {
		t.Fatal("Open after lruCache eviction returned the evicted handle")
	}
	s := slUploadPack(t, f.repo, f.main)
	s.inFlight(t, "upload-pack serve on the first handle")
	repack := slQueue(t, f, f.repo, "Repack on a second handle", func(ctx context.Context) error { return other.Repack(ctx, time.Time{}) })
	s.finish(t)
	repack.wait(t, "the queued Repack on the second handle")
	rpNeedsRepack(t, other, false, "after the queued Repack")
}

// A live GC waits for an upload-pack serve in flight.
func TestServeLockLiveGCWaitsForUploadPack(t *testing.T) {
	setGitBinary(t, "")
	f := rpMixedRepo(t, memfs.New(), "/repo.git", false)
	s := slUploadPack(t, f.repo, f.main)
	s.inFlight(t, "upload-pack serve")
	live := slQueue(t, f, f.repo, "live GC", func(ctx context.Context) error {
		_, err := f.repo.GC(ctx, time.Time{}, false)
		return err
	})
	s.finish(t)
	live.wait(t, "the queued live GC")
}

// A dry-run GC completes while an upload-pack serve is held mid-pack.
func TestServeLockDryRunGCRunsDuringUploadPack(t *testing.T) {
	setGitBinary(t, "")
	f := rpMixedRepo(t, memfs.New(), "/repo.git", false)
	s := slUploadPack(t, f.repo, f.main)
	s.inFlight(t, "upload-pack serve")
	slGo(t, func(ctx context.Context) error {
		_, err := f.repo.GC(ctx, time.Time{}, true)
		return err
	}).wait(t, "dry-run GC during an upload-pack serve")
	s.finish(t)
}

// Receive-pack takes no serve lock: a push lands while an upload-pack serve is held mid-pack.
func TestServeLockReceivePackRunsDuringUploadPack(t *testing.T) {
	setGitBinary(t, "")
	f := rpMixedRepo(t, memfs.New(), "/repo.git", false)
	src, err := Init(t.Context(), memfs.New(), "/src.git", "main")
	if err != nil {
		t.Fatalf("init the push source: %v", err)
	}
	commit := rpCommit(t, src, "main", "pushed.txt", []byte("pushed during a clone\n"))
	var pack bytes.Buffer
	if _, err := packfile.NewEncoder(&pack, src.repo.Storer, false).Encode(rpCommitObjects(t, src, commit, "pushed.txt"), 10); err != nil {
		t.Fatalf("encode the pushed pack: %v", err)
	}
	body := append(pktLines(t, plumbing.ZeroHash.String()+" "+commit.String()+" refs/heads/pushed\x00report-status\n", ""), pack.Bytes()...)
	s := slUploadPack(t, f.repo, f.main)
	s.inFlight(t, "upload-pack serve")
	var out bytes.Buffer
	slGo(t, func(ctx context.Context) error {
		return f.repo.Stateless(ctx, &out, bytes.NewReader(body), GitReceivePack, "", ReceivePackHooks{})
	}).wait(t, "receive-pack during an upload-pack serve")
	if got, want := reportLines(t, &out, false), []string{"unpack ok", "ok refs/heads/pushed"}; !slices.Equal(got, want) {
		t.Fatalf("receive-pack report = %q, want %q", got, want)
	}
	s.finish(t)
}

// AdvertiseRefs takes no serve lock: it answers while a Repack is queued behind an upload-pack serve.
func TestServeLockAdvertiseRefsSkipsLock(t *testing.T) {
	setGitBinary(t, "")
	f := rpMixedRepo(t, memfs.New(), "/repo.git", false)
	s := slUploadPack(t, f.repo, f.main)
	s.inFlight(t, "upload-pack serve")
	repack := slQueue(t, f, f.repo, "Repack", func(ctx context.Context) error { return f.repo.Repack(ctx, time.Time{}) })
	slGo(t, func(ctx context.Context) error {
		return f.repo.AdvertiseRefs(ctx, io.Discard, GitUploadPack, "")
	}).wait(t, "AdvertiseRefs while a Repack queued behind an upload-pack serve")
	s.finish(t)
	repack.wait(t, "the queued Repack")
}
