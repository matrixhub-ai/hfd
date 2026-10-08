package mirror_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	xetclient "github.com/wzshiming/xet/client"
	xetshard "github.com/wzshiming/xet/shard"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// gatedStorage holds shard writes at gate (or fails them with err), keeping an accepted upload pending for as long as a test needs.
type gatedStorage struct {
	xetstorage.Storage
	gate      chan struct{}
	hit       chan struct{}
	hitOnce   sync.Once
	open      func()
	err       error
	putShards atomic.Int32
}

func newGatedStorage(inner xetstorage.Storage) *gatedStorage {
	g := &gatedStorage{Storage: inner, gate: make(chan struct{}), hit: make(chan struct{})}
	g.open = sync.OnceFunc(func() { close(g.gate) })
	return g
}

func failingStorage(inner xetstorage.Storage, err error) *gatedStorage {
	g := newGatedStorage(inner)
	g.err = err
	return g
}

func (g *gatedStorage) PutShard(ctx context.Context, s *xetshard.Shard) (bool, error) {
	g.putShards.Add(1)
	g.hitOnce.Do(func() { close(g.hit) })
	if g.err != nil {
		return false, g.err
	}
	select {
	case <-g.gate:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	return g.Storage.PutShard(ctx, s)
}

// waitHit blocks until an ingest is parked at the gate, so the object cannot be stored before the gate opens.
func (g *gatedStorage) waitHit(t *testing.T) {
	t.Helper()
	select {
	case <-g.hit:
	case <-time.After(30 * time.Second):
		t.Fatal("no ingest reached the storage gate")
	}
}

func newLocalStorage(t *testing.T) *storage.Storage {
	t.Helper()
	st, err := storage.NewStorage(storage.WithRootDir(newXETDataDir(t)))
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	return st
}

// newMirrorOver builds an engine-less Mirror over an explicit xet storage and data dir, so tests can wrap the storage and reopen the dirs.
func newMirrorOver(t *testing.T, dataDir string, xs xetstorage.Storage) *mirror.Mirror {
	t.Helper()
	m, err := mirror.NewMirror(
		mirror.WithXETStorage(xs),
		mirror.WithXETCache(xetclient.NewCache(filepath.Join(dataDir, "chunks"), 0, 0)),
		mirror.WithDataDir(dataDir),
	)
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}
	t.Cleanup(m.Wait)
	return m
}

func oidOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func spoolEntries(t *testing.T, dataDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dataDir, "spool"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read spool dir: %v", err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func assertOpenObject(t *testing.T, m *mirror.Mirror, oid string, want []byte) {
	t.Helper()
	rs, size, err := m.OpenObject(t.Context(), oid)
	if err != nil {
		t.Fatalf("open object: %v", err)
	}
	defer func() { _ = rs.Close() }()
	got, err := io.ReadAll(rs)
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	if size != int64(len(want)) || !bytes.Equal(got, want) {
		t.Fatalf("OpenObject = %d bytes (size %d), want %d bytes of the payload", len(got), size, len(want))
	}
}

func serveOID(t *testing.T, m *mirror.Mirror, oid, byteRange string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/objects/"+oid, nil)
	if byteRange != "" {
		req.Header.Set("Range", byteRange)
	}
	if !m.ServeOID(rec, req, oid) {
		t.Fatal("ServeOID = false")
	}
	return rec
}

func TestAcceptObjectServesWhilePending(t *testing.T) {
	ctx := t.Context()
	st := newLocalStorage(t)
	gs := newGatedStorage(st.XETStorage())
	m := newMirrorOver(t, st.XETDir(), gs)
	// Registered after m.Wait so it runs first (LIFO): a failing test must release the gate before waiting.
	t.Cleanup(gs.open)

	data := bytes.Repeat([]byte("accepted upload bytes. "), 4096)
	oid := oidOf(data)
	if err := m.AcceptObject(ctx, oid, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("accept object: %v", err)
	}
	gs.waitHit(t)

	if !m.HasObject(ctx, oid) || !m.KnowsObject(ctx, oid) {
		t.Fatal("pending object must be held and known")
	}
	if fh := m.FileHash(ctx, oid); fh != "" {
		t.Fatalf("FileHash = %q while pending, want empty", fh)
	}
	assertOpenObject(t, m, oid, data)

	rec := serveOID(t, m, oid, "")
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("ServeOID status = %d, %d bytes; want 200 with the payload", rec.Code, rec.Body.Len())
	}
	hd := rec.Result().Header
	if hd.Get("Content-Length") != fmt.Sprint(len(data)) || hd.Get("Content-Type") != "application/octet-stream" ||
		hd.Get("X-Linked-Etag") != `"`+oid+`"` || hd.Get("X-Linked-Size") != fmt.Sprint(len(data)) || hd.Get("ETag") != `"`+oid+`"` {
		t.Fatalf("ServeOID headers = %v", hd)
	}
	rec = serveOID(t, m, oid, "bytes=10-19")
	if rec.Code != http.StatusPartialContent || !bytes.Equal(rec.Body.Bytes(), data[10:20]) {
		t.Fatalf("ranged ServeOID status = %d, body %q; want 206 with bytes 10-19", rec.Code, rec.Body.String())
	}
	if entries := spoolEntries(t, st.XETDir()); !slices.Equal(entries, []string{oid}) {
		t.Fatalf("spool entries = %v, want only %s", entries, oid)
	}

	gs.open()
	m.Wait()
	if m.FileHash(ctx, oid) == "" {
		t.Fatal("FileHash empty after the ingest finished")
	}
	if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
		t.Fatalf("spool entries after ingest = %v, want none", entries)
	}
	assertOpenObject(t, m, oid, data)
}

func TestSpoolRejectsMismatch(t *testing.T) {
	ctx := t.Context()
	st := newLocalStorage(t)
	m := newMirrorOver(t, st.XETDir(), st.XETStorage())
	data := []byte("verified payload bytes")
	oid := oidOf(data)
	for name, put := range map[string]func(context.Context, string, io.Reader, int64) error{
		"AcceptObject": m.AcceptObject,
		"PutObject":    m.PutObject,
	} {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name string
				body []byte
				size int64
			}{
				{"HashMismatch", []byte("tampered payload bytes"), int64(len(data))},
				{"SizeMismatch", data, int64(len(data)) + 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if err := put(ctx, oid, bytes.NewReader(tc.body), tc.size); err == nil {
						t.Fatal("mismatching upload was accepted")
					}
					if m.HasObject(ctx, oid) {
						t.Fatal("HasObject = true after a rejected upload")
					}
					if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
						t.Fatalf("spool entries after a rejected upload = %v, want none", entries)
					}
				})
			}
		})
	}
}

func TestNewMirrorRecoversSpool(t *testing.T) {
	ctx := t.Context()
	st := newLocalStorage(t)
	data := bytes.Repeat([]byte("recovered upload bytes. "), 4096)
	oid := oidOf(data)
	spool := filepath.Join(st.XETDir(), "spool")
	if err := os.MkdirAll(spool, 0755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string][]byte{oid: data, "x.part": []byte("unfinished"), "put-123": []byte("legacy")} {
		if err := os.WriteFile(filepath.Join(spool, name), content, 0644); err != nil {
			t.Fatal(err)
		}
	}

	gs := newGatedStorage(st.XETStorage())
	m := newMirrorOver(t, st.XETDir(), gs)
	t.Cleanup(gs.open)
	if entries := spoolEntries(t, st.XETDir()); !slices.Equal(entries, []string{oid}) {
		t.Fatalf("spool entries after recovery = %v, want only %s", entries, oid)
	}
	if !m.HasObject(ctx, oid) {
		t.Fatal("recovered entry is not held")
	}
	assertOpenObject(t, m, oid, data)

	gs.open()
	m.Wait()
	if m.FileHash(ctx, oid) == "" {
		t.Fatal("recovered entry was not ingested")
	}
	if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
		t.Fatalf("spool entries after ingest = %v, want none", entries)
	}
}

func TestPendingIngestFailureKeepsSpool(t *testing.T) {
	ctx := t.Context()
	st := newLocalStorage(t)
	m := newMirrorOver(t, st.XETDir(), failingStorage(st.XETStorage(), errors.New("shard store offline")))
	data := bytes.Repeat([]byte("ingest failure bytes. "), 4096)
	oid := oidOf(data)
	if err := m.AcceptObject(ctx, oid, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("accept object: %v", err)
	}
	m.Wait()
	if !m.HasObject(ctx, oid) {
		t.Fatal("object no longer held after its ingest failed")
	}
	if fh := m.FileHash(ctx, oid); fh != "" {
		t.Fatalf("FileHash = %q after a failed ingest, want empty", fh)
	}
	assertOpenObject(t, m, oid, data)
	if entries := spoolEntries(t, st.XETDir()); !slices.Equal(entries, []string{oid}) {
		t.Fatalf("spool entries after a failed ingest = %v, want only %s", entries, oid)
	}
	// A synchronous writer replaces the failed entry; its retry fails against the same storage.
	if err := m.PutObject(ctx, oid, bytes.NewReader(data), int64(len(data))); err == nil || !strings.Contains(err.Error(), "shard store offline") {
		t.Fatalf("PutObject after a failed ingest = %v, want the ingest error", err)
	}
	synced := bytes.Repeat([]byte("synchronous ingest failure bytes. "), 4096)
	syncedOID := oidOf(synced)
	if err := m.PutObject(ctx, syncedOID, bytes.NewReader(synced), int64(len(synced))); err == nil || !strings.Contains(err.Error(), "shard store offline") {
		t.Fatalf("PutObject = %v, want the ingest error", err)
	}
	if !m.HasObject(ctx, syncedOID) {
		t.Fatal("PutObject's entry is not held after its ingest failed")
	}
	assertOpenObject(t, m, syncedOID, synced)

	// The next start over the same dirs retries with the working storage.
	next := newMirrorOver(t, st.XETDir(), st.XETStorage())
	next.Wait()
	for _, oid := range []string{oid, syncedOID} {
		if next.FileHash(ctx, oid) == "" {
			t.Fatalf("retry at the next start did not ingest %s", oid)
		}
	}
	if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
		t.Fatalf("spool entries after the retry = %v, want none", entries)
	}
	assertOpenObject(t, next, oid, data)
	assertOpenObject(t, next, syncedOID, synced)
}

func TestPutObjectStoresBeforeReturning(t *testing.T) {
	ctx := t.Context()
	st := newLocalStorage(t)
	m := newMirrorOver(t, st.XETDir(), st.XETStorage())
	data := bytes.Repeat([]byte("synchronous put bytes. "), 4096)
	oid := oidOf(data)
	if err := m.PutObject(ctx, oid, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("put object: %v", err)
	}
	if m.FileHash(ctx, oid) == "" {
		t.Fatal("PutObject returned before the object was stored")
	}
	if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
		t.Fatalf("spool entries after PutObject = %v, want none", entries)
	}
	assertOpenObject(t, m, oid, data)
}

func TestAcceptObjectDedupesConcurrentUploads(t *testing.T) {
	ctx := t.Context()
	st := newLocalStorage(t)
	gs := newGatedStorage(st.XETStorage())
	m := newMirrorOver(t, st.XETDir(), gs)
	t.Cleanup(gs.open)
	data := bytes.Repeat([]byte("concurrent upload bytes. "), 4096)
	oid := oidOf(data)

	const uploads = 4
	errs := make(chan error, uploads)
	for range uploads {
		go func() { errs <- m.AcceptObject(ctx, oid, bytes.NewReader(data), int64(len(data))) }()
	}
	for range uploads {
		if err := <-errs; err != nil {
			t.Fatalf("accept object: %v", err)
		}
	}
	gs.waitHit(t)
	if entries := spoolEntries(t, st.XETDir()); !slices.Equal(entries, []string{oid}) {
		t.Fatalf("spool entries = %v, want exactly one %s", entries, oid)
	}
	assertOpenObject(t, m, oid, data)

	gs.open()
	m.Wait()
	// Both ingest slots were free, so a duplicate entry would have written its own shard alongside.
	if n := gs.putShards.Load(); n != 1 {
		t.Fatalf("shard writes = %d, want 1 for one pending entry", n)
	}
	if m.FileHash(ctx, oid) == "" {
		t.Fatal("object not stored after the ingest")
	}
	if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
		t.Fatalf("spool entries after ingest = %v, want none", entries)
	}
}

var errSeam = errors.New("seam failure")

// stubSpoolSeams records the publication steps in order before delegating to the real ones; the step named fail returns errSeam instead.
func stubSpoolSeams(t *testing.T, fail string) *[]string {
	t.Helper()
	syncFile, rename, syncDir := *mirror.SyncSpoolFile, *mirror.RenameSpool, *mirror.SyncSpoolDir
	t.Cleanup(func() { *mirror.SyncSpoolFile, *mirror.RenameSpool, *mirror.SyncSpoolDir = syncFile, rename, syncDir })
	steps := new([]string)
	record := func(step string) error {
		*steps = append(*steps, step)
		if step == fail {
			return errSeam
		}
		return nil
	}
	*mirror.SyncSpoolFile = func(f *os.File) error {
		if err := record("sync file"); err != nil {
			return err
		}
		return syncFile(f)
	}
	*mirror.RenameSpool = func(from, to string) error {
		if err := record("rename"); err != nil {
			return err
		}
		return rename(from, to)
	}
	*mirror.SyncSpoolDir = func(dir string) error {
		if err := record("sync dir"); err != nil {
			return err
		}
		return syncDir(dir)
	}
	return steps
}

func TestAcceptObjectPublishesDurably(t *testing.T) {
	data := []byte("durably published bytes")
	oid := oidOf(data)
	t.Run("Order", func(t *testing.T) {
		st := newLocalStorage(t)
		m := newMirrorOver(t, st.XETDir(), st.XETStorage())
		steps := stubSpoolSeams(t, "")
		if err := m.AcceptObject(t.Context(), oid, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatalf("accept object: %v", err)
		}
		if want := []string{"sync file", "rename", "sync dir"}; !slices.Equal(*steps, want) {
			t.Fatalf("publication steps = %v, want %v before AcceptObject returns", *steps, want)
		}
	})
	for _, fail := range []string{"sync file", "rename", "sync dir"} {
		t.Run("Fail/"+fail, func(t *testing.T) {
			ctx := t.Context()
			st := newLocalStorage(t)
			m := newMirrorOver(t, st.XETDir(), st.XETStorage())
			stubSpoolSeams(t, fail)
			err := m.AcceptObject(ctx, oid, bytes.NewReader(data), int64(len(data)))
			if !errors.Is(err, errSeam) {
				t.Fatalf("AcceptObject = %v, want the %s failure", err, fail)
			}
			if m.HasObject(ctx, oid) {
				t.Fatal("HasObject = true after a failed publication")
			}
			if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
				t.Fatalf("spool entries after a failed publication = %v, want none", entries)
			}
		})
	}
}

func TestServeOIDServesStoredWithoutTargets(t *testing.T) {
	ctx := t.Context()
	st := newLocalStorage(t)
	m := newMirrorOver(t, st.XETDir(), st.XETStorage())
	data := bytes.Repeat([]byte("stored object bytes. "), 4096)
	oid := oidOf(data)
	if err := m.AcceptObject(ctx, oid, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("accept object: %v", err)
	}
	m.Wait()
	if m.FileHash(ctx, oid) == "" {
		t.Fatal("object not stored after the ingest")
	}
	if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
		t.Fatalf("spool entries after ingest = %v, want none", entries)
	}

	rec := serveOID(t, m, oid, "")
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("ServeOID status = %d, %d bytes; want 200 with the payload from storage", rec.Code, rec.Body.Len())
	}
	if got := rec.Result().Header.Get("X-Linked-Size"); got != fmt.Sprint(len(data)) {
		t.Fatalf("X-Linked-Size = %q, want %d", got, len(data))
	}
	rec = serveOID(t, m, oid, "bytes=10-19")
	if rec.Code != http.StatusPartialContent || !bytes.Equal(rec.Body.Bytes(), data[10:20]) {
		t.Fatalf("ranged ServeOID status = %d, body %q; want 206 with bytes 10-19", rec.Code, rec.Body.String())
	}
}

type probeKey struct{}

// probeStorage answers lookups carrying probeKey with not-found after running trap, which lets the pending entry retire first.
type probeStorage struct {
	*gatedStorage
	trap func()
}

func (s *probeStorage) GetFileHashBySHA256(ctx context.Context, namespace string, digest [32]byte) (xet.FileHash, error) {
	if ctx.Value(probeKey{}) != nil {
		s.trap()
		return xet.FileHash{}, errors.New("probe: not found")
	}
	return s.gatedStorage.GetFileHashBySHA256(ctx, namespace, digest)
}

// TestHasObjectChecksPendingBeforeStorage pins the lookup order: a storage miss followed by a retired entry must not read as absent.
func TestHasObjectChecksPendingBeforeStorage(t *testing.T) {
	ctx := t.Context()
	st := newLocalStorage(t)
	gs := newGatedStorage(st.XETStorage())
	ps := &probeStorage{gatedStorage: gs}
	m := newMirrorOver(t, st.XETDir(), ps)
	t.Cleanup(gs.open)
	ps.trap = func() {
		gs.open()
		m.Wait()
	}
	data := bytes.Repeat([]byte("lookup order bytes. "), 4096)
	oid := oidOf(data)
	if err := m.AcceptObject(ctx, oid, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("accept object: %v", err)
	}
	gs.waitHit(t)
	if !m.HasObject(context.WithValue(ctx, probeKey{}, true), oid) {
		t.Fatal("HasObject = false across the retirement of the pending entry")
	}
}

// waitSpoolEntries polls until the spool holds exactly want.
func waitSpoolEntries(t *testing.T, dataDir string, want []string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := spoolEntries(t, dataDir)
		if slices.Equal(got, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("spool entries = %v, want %v", got, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPutObjectJoinsConcurrentUpload(t *testing.T) {
	ctx := t.Context()
	st := newLocalStorage(t)
	gs := newGatedStorage(st.XETStorage())
	m := newMirrorOver(t, st.XETDir(), gs)
	t.Cleanup(gs.open)
	spooled := make(chan struct{}, 2)
	syncFile := *mirror.SyncSpoolFile
	t.Cleanup(func() { *mirror.SyncSpoolFile = syncFile })
	*mirror.SyncSpoolFile = func(f *os.File) error {
		spooled <- struct{}{}
		return syncFile(f)
	}
	data := bytes.Repeat([]byte("two writer bytes. "), 4096)
	oid := oidOf(data)

	errs := make(chan error, 2)
	for range 2 {
		go func() { errs <- m.PutObject(ctx, oid, bytes.NewReader(data), int64(len(data))) }()
	}
	for range 2 {
		select {
		case <-spooled:
		case <-time.After(30 * time.Second):
			t.Fatal("both writers did not finish spooling")
		}
	}
	gs.waitHit(t)
	waitSpoolEntries(t, st.XETDir(), []string{oid})
	select {
	case err := <-errs:
		t.Fatalf("PutObject returned %v while the ingest was held at the gate", err)
	default:
	}

	gs.open()
	for range 2 {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatalf("put object: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("PutObject did not return after the ingest was released")
		}
	}
	if n := gs.putShards.Load(); n != 1 {
		t.Fatalf("shard writes = %d, want 1 for one shared entry", n)
	}
	if m.FileHash(ctx, oid) == "" {
		t.Fatal("object not stored after the ingest")
	}
	if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
		t.Fatalf("spool entries after ingest = %v, want none", entries)
	}
}

func TestPutObjectCancelledWaitLeavesIngestRunning(t *testing.T) {
	st := newLocalStorage(t)
	gs := newGatedStorage(st.XETStorage())
	m := newMirrorOver(t, st.XETDir(), gs)
	t.Cleanup(gs.open)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	data := bytes.Repeat([]byte("cancelled waiter bytes. "), 4096)
	oid := oidOf(data)

	errs := make(chan error, 1)
	go func() { errs <- m.PutObject(ctx, oid, bytes.NewReader(data), int64(len(data))) }()
	gs.waitHit(t)
	cancel()
	select {
	case err := <-errs:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("PutObject = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("PutObject did not return after its context was cancelled")
	}
	if !m.HasObject(context.Background(), oid) {
		t.Fatal("object not held after the waiter gave up")
	}

	gs.open()
	m.Wait()
	if m.FileHash(context.Background(), oid) == "" {
		t.Fatal("object not stored by the ingest the waiter abandoned")
	}
	if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
		t.Fatalf("spool entries after ingest = %v, want none", entries)
	}
}

// TestPendingLookupsIgnoreOIDCase pins that an upload spelled in either hex case is one pending object under its canonical name.
func TestPendingLookupsIgnoreOIDCase(t *testing.T) {
	ctx := t.Context()
	st := newLocalStorage(t)
	gs := newGatedStorage(st.XETStorage())
	m := newMirrorOver(t, st.XETDir(), gs)
	t.Cleanup(gs.open)
	lower := bytes.Repeat([]byte("lowercase accepted bytes. "), 4096)
	upper := bytes.Repeat([]byte("uppercase accepted bytes. "), 4096)
	lowerOID, upperOID := oidOf(lower), oidOf(upper)
	if err := m.AcceptObject(ctx, lowerOID, bytes.NewReader(lower), int64(len(lower))); err != nil {
		t.Fatalf("accept lowercase spelling: %v", err)
	}
	if err := m.AcceptObject(ctx, strings.ToUpper(upperOID), bytes.NewReader(upper), int64(len(upper))); err != nil {
		t.Fatalf("accept uppercase spelling: %v", err)
	}
	gs.waitHit(t)
	want := []string{lowerOID, upperOID}
	slices.Sort(want)
	if entries := spoolEntries(t, st.XETDir()); !slices.Equal(entries, want) {
		t.Fatalf("spool entries = %v, want the canonical names %v", entries, want)
	}

	for _, tc := range []struct {
		name string
		oid  string
		want []byte
	}{
		{"UpperLookupOfLowerUpload", strings.ToUpper(lowerOID), lower},
		{"LowerLookupOfUpperUpload", upperOID, upper},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !m.HasObject(ctx, tc.oid) || !m.KnowsObject(ctx, tc.oid) {
				t.Fatalf("pending object not held under the spelling %s", tc.oid)
			}
			if fh := m.FileHash(ctx, tc.oid); fh != "" {
				t.Fatalf("FileHash = %q while pending, want empty", fh)
			}
			assertOpenObject(t, m, tc.oid, tc.want)
			rec := serveOID(t, m, tc.oid, "")
			if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), tc.want) {
				t.Fatalf("ServeOID status = %d, %d bytes; want 200 with the payload", rec.Code, rec.Body.Len())
			}
			rec = serveOID(t, m, tc.oid, "bytes=10-19")
			if rec.Code != http.StatusPartialContent || !bytes.Equal(rec.Body.Bytes(), tc.want[10:20]) {
				t.Fatalf("ranged ServeOID status = %d, body %q; want 206 with bytes 10-19", rec.Code, rec.Body.String())
			}
		})
	}

	gs.open()
	m.Wait()
	for _, oid := range []string{lowerOID, strings.ToUpper(upperOID)} {
		if m.FileHash(ctx, oid) == "" {
			t.Fatalf("object %s not stored after the ingest", oid)
		}
	}
	if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
		t.Fatalf("spool entries after ingest = %v, want none", entries)
	}
}

// A second spelling of a pending OID must never publish onto, or roll back over, the first entry's file.
func TestAcceptObjectCaseVariantLeavesPendingEntryIntact(t *testing.T) {
	data := bytes.Repeat([]byte("case variant bytes. "), 4096)
	oid := oidOf(data)
	upper := strings.ToUpper(oid)
	for _, tc := range []struct {
		fail   string
		joined bool
	}{
		// The variant joins the pending entry before any rename, so the failing directory sync is never reached.
		{fail: "sync dir", joined: true},
		// A variant whose own spooling fails reports it and leaves the entry alone.
		{fail: "sync file", joined: false},
	} {
		t.Run(strings.ReplaceAll(tc.fail, " ", "-"), func(t *testing.T) {
			ctx := t.Context()
			st := newLocalStorage(t)
			gs := newGatedStorage(st.XETStorage())
			m := newMirrorOver(t, st.XETDir(), gs)
			t.Cleanup(gs.open)
			if err := m.AcceptObject(ctx, oid, bytes.NewReader(data), int64(len(data))); err != nil {
				t.Fatalf("accept object: %v", err)
			}
			gs.waitHit(t)

			steps := stubSpoolSeams(t, tc.fail)
			err := m.AcceptObject(ctx, upper, bytes.NewReader(data), int64(len(data)))
			switch {
			case tc.joined && err != nil:
				t.Fatalf("AcceptObject(%s) = %v, want it to join the pending entry", upper, err)
			case !tc.joined && !errors.Is(err, errSeam):
				t.Fatalf("AcceptObject(%s) = %v, want the %s failure", upper, err, tc.fail)
			}
			if slices.Contains(*steps, "rename") {
				t.Fatalf("publication steps of the case variant = %v, want no second publication", *steps)
			}
			for _, spelling := range []string{oid, upper} {
				if !m.HasObject(ctx, spelling) {
					t.Fatalf("HasObject(%s) = false after the case variant upload", spelling)
				}
				assertOpenObject(t, m, spelling, data)
			}
			if entries := spoolEntries(t, st.XETDir()); !slices.Equal(entries, []string{oid}) {
				t.Fatalf("spool entries = %v, want only %s", entries, oid)
			}

			gs.open()
			m.Wait()
			if m.FileHash(ctx, upper) == "" {
				t.Fatal("object not stored after the ingest")
			}
			if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
				t.Fatalf("spool entries after ingest = %v, want none", entries)
			}
		})
	}
}

func TestNewMirrorRecoversCaseVariantSpoolEntry(t *testing.T) {
	ctx := t.Context()
	st := newLocalStorage(t)
	data := bytes.Repeat([]byte("recovered case variant bytes. "), 4096)
	oid := oidOf(data)
	upper := strings.ToUpper(oid)
	spool := filepath.Join(st.XETDir(), "spool")
	if err := os.MkdirAll(spool, 0755); err != nil {
		t.Fatal(err)
	}
	// On a case-sensitive filesystem both spellings exist; elsewhere the second write lands in the first file.
	for _, name := range []string{upper, oid} {
		if err := os.WriteFile(filepath.Join(spool, name), data, 0644); err != nil {
			t.Fatal(err)
		}
	}

	gs := newGatedStorage(st.XETStorage())
	m := newMirrorOver(t, st.XETDir(), gs)
	t.Cleanup(gs.open)
	// The uppercase name sorts first and keeps owning the OID at its on-disk name; a lowercase duplicate is removed.
	if entries := spoolEntries(t, st.XETDir()); !slices.Equal(entries, []string{upper}) {
		t.Fatalf("spool entries after recovery = %v, want only %s", entries, upper)
	}
	for _, spelling := range []string{oid, upper} {
		if !m.HasObject(ctx, spelling) {
			t.Fatalf("HasObject(%s) = false for the recovered entry", spelling)
		}
		assertOpenObject(t, m, spelling, data)
	}

	gs.open()
	m.Wait()
	if m.FileHash(ctx, oid) == "" {
		t.Fatal("recovered entry was not ingested")
	}
	if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
		t.Fatalf("spool entries after ingest = %v, want none", entries)
	}
	assertOpenObject(t, m, upper, data)
}

// captureLogs routes the default slog output into a buffer until the test ends; call it before newMirrorOver so
// Wait runs first.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := new(bytes.Buffer)
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// panickingStorage panics on every shard write, standing in for a bug in the xet pipeline or storage.
type panickingStorage struct{ xetstorage.Storage }

func (panickingStorage) PutShard(context.Context, *xetshard.Shard) (bool, error) {
	panic("shard store bug")
}

func TestIngestPanicKeepsObjectHeld(t *testing.T) {
	ctx := t.Context()
	logs := captureLogs(t)
	st := newLocalStorage(t)
	m := newMirrorOver(t, st.XETDir(), panickingStorage{st.XETStorage()})
	var opened []*os.File
	var openedMu sync.Mutex
	previous := *mirror.OpenSpoolFile
	*mirror.OpenSpoolFile = func(name string) (*os.File, error) {
		f, err := previous(name)
		if err == nil {
			openedMu.Lock()
			opened = append(opened, f)
			openedMu.Unlock()
		}
		return f, err
	}
	t.Cleanup(func() { *mirror.OpenSpoolFile = previous })
	data := bytes.Repeat([]byte("panicking ingest bytes. "), 4096)
	oid := oidOf(data)
	if err := m.AcceptObject(ctx, oid, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("accept object: %v", err)
	}
	// Returning at all proves the goroutine recovered: an unrecovered panic would have ended the test binary.
	m.Wait()
	openedMu.Lock()
	ingestFiles := slices.Clone(opened)
	openedMu.Unlock()
	if len(ingestFiles) != 1 {
		t.Fatalf("spool files opened by the ingest = %d, want 1", len(ingestFiles))
	}
	if _, err := ingestFiles[0].Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("spool file still open after the panicking ingest: stat err = %v, want closed", err)
	}
	if !m.HasObject(ctx, oid) {
		t.Fatal("object no longer held after its ingest panicked")
	}
	if fh := m.FileHash(ctx, oid); fh != "" {
		t.Fatalf("FileHash = %q after a panicking ingest, want empty", fh)
	}
	assertOpenObject(t, m, oid, data)
	if entries := spoolEntries(t, st.XETDir()); !slices.Equal(entries, []string{oid}) {
		t.Fatalf("spool entries after a panicking ingest = %v, want only %s", entries, oid)
	}
	if n := strings.Count(logs.String(), "level=WARN"); n != 1 || !strings.Contains(logs.String(), "ingest panicked: shard store bug") {
		t.Fatalf("warnings = %d, want exactly one naming the panic; log:\n%s", n, logs.String())
	}
	if err := m.PutObject(ctx, oid, bytes.NewReader(data), int64(len(data))); err == nil || !strings.Contains(err.Error(), "ingest panicked") {
		t.Fatalf("PutObject = %v, want the recovered panic as its error", err)
	}
}

func TestEmptyObjectIsHeldWithoutSpool(t *testing.T) {
	ctx := t.Context()
	st := newLocalStorage(t)
	m := newMirrorOver(t, st.XETDir(), st.XETStorage())
	empty := oidOf(nil)
	if err := m.AcceptObject(ctx, empty, bytes.NewReader(nil), 0); err != nil {
		t.Fatalf("accept the empty object: %v", err)
	}
	if err := m.PutObject(ctx, strings.ToUpper(empty), bytes.NewReader(nil), 0); err != nil {
		t.Fatalf("put the empty object: %v", err)
	}
	if err := m.AcceptObject(ctx, empty, strings.NewReader("x"), 1); err == nil {
		t.Fatal("a non-empty body was accepted under the empty object's OID")
	}
	m.Wait()
	for _, spelling := range []string{empty, strings.ToUpper(empty)} {
		if !m.HasObject(ctx, spelling) || !m.KnowsObject(ctx, spelling) {
			t.Fatalf("the empty object is not held under %s", spelling)
		}
		if fh := m.FileHash(ctx, spelling); fh != "" {
			t.Fatalf("FileHash(%s) = %q, want empty: the empty object is storage-only", spelling, fh)
		}
		assertOpenObject(t, m, spelling, nil)
	}
	rec := serveOID(t, m, empty, "")
	hd := rec.Result().Header
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 || hd.Get("Content-Length") != "0" ||
		hd.Get("X-Linked-Size") != "0" || hd.Get("X-Linked-Etag") != `"`+empty+`"` || hd.Get("ETag") != `"`+empty+`"` {
		t.Fatalf("ServeOID status = %d, %d bytes, headers %v; want 200 with an empty body and the LFS headers", rec.Code, rec.Body.Len(), hd)
	}
	if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
		t.Fatalf("spool entries = %v, want none for the empty object", entries)
	}

	// An earlier binary may have spooled it; the next start drops the entry instead of ingesting it forever.
	if err := os.WriteFile(filepath.Join(st.XETDir(), "spool", empty), nil, 0644); err != nil {
		t.Fatal(err)
	}
	next := newMirrorOver(t, st.XETDir(), st.XETStorage())
	next.Wait()
	if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
		t.Fatalf("spool entries after recovery = %v, want none", entries)
	}
	if !next.HasObject(ctx, empty) {
		t.Fatal("the empty object is not held after recovery")
	}
	assertOpenObject(t, next, empty, nil)
}

func TestRecoveryDropsCorruptSpoolEntry(t *testing.T) {
	ctx := t.Context()
	st := newLocalStorage(t)
	data := bytes.Repeat([]byte("corrupt recovery bytes. "), 4096)
	oid := oidOf(data)
	spool := filepath.Join(st.XETDir(), "spool")
	if err := os.MkdirAll(spool, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(spool, oid), data[:len(data)-100], 0644); err != nil {
		t.Fatal(err)
	}
	m := newMirrorOver(t, st.XETDir(), st.XETStorage())
	m.Wait()
	if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
		t.Fatalf("spool entries after recovery = %v, want the truncated entry gone", entries)
	}
	if m.HasObject(ctx, oid) {
		t.Fatal("HasObject = true for a truncated spool entry")
	}
	if err := m.AcceptObject(ctx, oid, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("re-upload: %v", err)
	}
	m.Wait()
	if m.FileHash(ctx, oid) == "" {
		t.Fatal("re-upload was not stored")
	}
	if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
		t.Fatalf("spool entries after the re-upload = %v, want none", entries)
	}
	assertOpenObject(t, m, oid, data)
}

// Only regular files are trusted at recovery: links and directories at OID names are removed, never followed.
func TestRecoveryRemovesNonRegularSpoolEntries(t *testing.T) {
	ctx := t.Context()
	st := newLocalStorage(t)
	spool := filepath.Join(st.XETDir(), "spool")
	if err := os.MkdirAll(spool, 0755); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	linked := bytes.Repeat([]byte("linked spool bytes. "), 4096)
	linkedOID := oidOf(linked)
	if err := os.WriteFile(filepath.Join(outside, "blob"), linked, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "blob"), filepath.Join(spool, linkedOID)); err != nil {
		t.Fatal(err)
	}
	dirLinkOID := oidOf([]byte("directory link"))
	if err := os.WriteFile(filepath.Join(outside, "canary"), []byte("keep"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(spool, dirLinkOID)); err != nil {
		t.Fatal(err)
	}
	dirOID := oidOf([]byte("directory"))
	if err := os.MkdirAll(filepath.Join(spool, dirOID, "nested"), 0755); err != nil {
		t.Fatal(err)
	}

	m := newMirrorOver(t, st.XETDir(), st.XETStorage())
	m.Wait()
	if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
		t.Fatalf("spool entries after recovery = %v, want none", entries)
	}
	for _, oid := range []string{linkedOID, dirLinkOID, dirOID} {
		if m.HasObject(ctx, oid) {
			t.Fatalf("HasObject(%s) = true for a non-regular spool entry", oid)
		}
	}
	for _, name := range []string{"blob", "canary"} {
		if _, err := os.Stat(filepath.Join(outside, name)); err != nil {
			t.Fatalf("link target %s was followed: %v", name, err)
		}
	}
}

func TestNewMirrorSurvivesUnreadableSpool(t *testing.T) {
	ctx := t.Context()
	st := newLocalStorage(t)
	if err := os.WriteFile(filepath.Join(st.XETDir(), "spool"), []byte("not a directory"), 0644); err != nil {
		t.Fatal(err)
	}
	m := newMirrorOver(t, st.XETDir(), st.XETStorage())
	data := []byte("unspoolable bytes")
	oid := oidOf(data)
	if err := m.AcceptObject(ctx, oid, bytes.NewReader(data), int64(len(data))); err == nil {
		t.Fatal("AcceptObject succeeded without a spool directory")
	}
	if m.HasObject(ctx, oid) {
		t.Fatal("HasObject = true after a failed upload")
	}
}

func TestReuploadReplacesFailedEntry(t *testing.T) {
	data := bytes.Repeat([]byte("retried upload bytes. "), 4096)
	oid := oidOf(data)
	for name, put := range map[string]func(*mirror.Mirror, context.Context) error{
		"AcceptObject": func(m *mirror.Mirror, ctx context.Context) error {
			if err := m.AcceptObject(ctx, oid, bytes.NewReader(data), int64(len(data))); err != nil {
				return err
			}
			m.Wait()
			return nil
		},
		"PutObject": func(m *mirror.Mirror, ctx context.Context) error {
			return m.PutObject(ctx, oid, bytes.NewReader(data), int64(len(data)))
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			st := newLocalStorage(t)
			gs := failingStorage(st.XETStorage(), errors.New("shard store offline"))
			m := newMirrorOver(t, st.XETDir(), gs)
			t.Cleanup(gs.open)
			if err := m.AcceptObject(ctx, oid, bytes.NewReader(data), int64(len(data))); err != nil {
				t.Fatalf("accept object: %v", err)
			}
			m.Wait()
			if !m.HasObject(ctx, oid) || m.FileHash(ctx, oid) != "" {
				t.Fatal("the failed entry is not held, or was stored after all")
			}

			// Wait ordered the ingest before this write; the storage is healthy for the retry.
			gs.err = nil
			gs.open()
			if err := put(m, ctx); err != nil {
				t.Fatalf("%s after the storage healed: %v", name, err)
			}
			if m.FileHash(ctx, oid) == "" {
				t.Fatalf("%s did not retry the failed ingest", name)
			}
			if n := gs.putShards.Load(); n != 2 {
				t.Fatalf("shard writes = %d, want the failed one and the retry", n)
			}
			if entries := spoolEntries(t, st.XETDir()); len(entries) != 0 {
				t.Fatalf("spool entries after the retry = %v, want none", entries)
			}
			assertOpenObject(t, m, oid, data)
		})
	}
}
