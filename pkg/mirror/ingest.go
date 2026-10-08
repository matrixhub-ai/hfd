package mirror

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"

	"github.com/wzshiming/xet"
	xetshard "github.com/wzshiming/xet/shard"
	xetstorage "github.com/wzshiming/xet/storage"
	xetupload "github.com/wzshiming/xet/upload"
)

// pendingIngestSlots bounds concurrent background ingests; each one already fans out m.concurrency xorb writes.
const pendingIngestSlots = 2

// Seams for the spool publication steps, swapped by tests to observe their order and fail them.
var (
	openSpoolFile = os.Open
	syncSpoolFile = (*os.File).Sync
	renameSpool   = os.Rename
	syncSpoolDir  = func(dir string) error {
		// Directory handles cannot be flushed on Windows; NTFS journals the rename itself.
		if runtime.GOOS == "windows" {
			return nil
		}
		d, err := os.Open(dir)
		if err != nil {
			return err
		}
		return errors.Join(d.Sync(), d.Close())
	}
)

// pendingObject owns the spool entry of one OID and its single background ingest.
type pendingObject struct {
	path string
	done chan struct{} // closed once the ingest has run; err is set before
	err  error
}

// failed reports whether the ingest has run and failed; a fresh upload then replaces the entry instead of joining it.
func (p *pendingObject) failed() bool {
	select {
	case <-p.done:
		return p.err != nil
	default:
		return false
	}
}

// AcceptObject verifies and spools the stream, then returns before the background xet ingest; the object is servable meanwhile.
func (m *Mirror) AcceptObject(ctx context.Context, oid string, r io.Reader, size int64) error {
	_, err := m.accept(ctx, oid, r, size)
	return err
}

// PutObject verifies and spools the stream and returns once the xet storage holds it; the ingest runs in the bounded
// background slots. The empty object is held without storage or spool.
func (m *Mirror) PutObject(ctx context.Context, oid string, r io.Reader, size int64) error {
	p, err := m.accept(ctx, oid, r, size)
	if err != nil || p == nil {
		return err
	}
	select {
	case <-p.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if p.err != nil {
		return fmt.Errorf("ingest object %s: %w", oid, p.err)
	}
	return nil
}

// accept spools and verifies the stream and registers it as pending; a nil entry means the storage already holds it.
func (m *Mirror) accept(ctx context.Context, oid string, r io.Reader, size int64) (*pendingObject, error) {
	key, digest, ok := canonicalOID(oid)
	if !ok {
		return nil, fmt.Errorf("invalid OID %q", oid)
	}
	part, err := m.spool(key, digest, r, size)
	if err != nil {
		return nil, err
	}
	if m.stored(ctx, key) {
		_ = os.Remove(part)
		return nil, nil
	}
	return m.register(key, part)
}

// register publishes a verified part as the pending entry of its canonical OID, or joins the entry another upload already owns.
// An entry whose ingest has failed is replaced by the fresh upload; its waiters already hold their error.
func (m *Mirror) register(key, part string) (*pendingObject, error) {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()
	if p, ok := m.pending[key]; ok {
		if !p.failed() {
			_ = os.Remove(part)
			return p, nil
		}
		delete(m.pending, key)
	}
	path := m.spoolPath(key)
	if err := publishSpool(part, path); err != nil {
		return nil, err
	}
	p := &pendingObject{path: path, done: make(chan struct{})}
	m.pending[key] = p
	m.scheduleIngest(key, p)
	return p, nil
}

// publishSpool renames a verified part to its OID path and makes the rename durable; on failure nothing is left behind.
func publishSpool(part, path string) error {
	if err := renameSpool(part, path); err != nil {
		_ = os.Remove(part)
		return fmt.Errorf("commit spool file: %w", err)
	}
	if err := syncSpoolDir(filepath.Dir(path)); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("sync spool dir: %w", err)
	}
	return nil
}

func (m *Mirror) spoolDir() string {
	return filepath.Join(m.dataDir, "spool")
}

// spoolPath is the complete, verified entry of a canonical OID; in-progress entries are <oid>.*.part.
func (m *Mirror) spoolPath(key string) string {
	return filepath.Join(m.spoolDir(), key)
}

// spool writes r to a new part file, verified against size and OID and synced; on failure the part is removed.
func (m *Mirror) spool(key string, digest [32]byte, r io.Reader, size int64) (string, error) {
	if err := os.MkdirAll(m.spoolDir(), 0755); err != nil {
		return "", fmt.Errorf("create spool dir: %w", err)
	}
	f, err := os.CreateTemp(m.spoolDir(), key+".*.part")
	if err != nil {
		return "", fmt.Errorf("create spool file: %w", err)
	}
	err = writeVerified(f, key, digest, r, size)
	if cerr := f.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("close spool file: %w", cerr)
	}
	if err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

func writeVerified(f *os.File, oid string, digest [32]byte, r io.Reader, size int64) error {
	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(hash, f), r)
	if err != nil {
		return fmt.Errorf("spool object: %w", err)
	}
	if size >= 0 && written != size {
		return fmt.Errorf("content size does not match: expected %d bytes, got %d", size, written)
	}
	if !bytes.Equal(hash.Sum(nil), digest[:]) {
		return fmt.Errorf("content hash does not match OID %s", oid)
	}
	if err := syncSpoolFile(f); err != nil {
		return fmt.Errorf("sync spool file: %w", err)
	}
	return nil
}

// stored reports whether the object needs no spool entry: the xet storage resolves it, or it is the empty object,
// which xet never indexes by SHA-256 and the mirror holds by definition.
func (m *Mirror) stored(ctx context.Context, oid string) bool {
	return isEmptyOID(oid) || m.FileHash(ctx, oid) != ""
}

// pendingEntry returns the spool entry of an object awaiting its ingest, whatever spelling of the OID the caller used.
func (m *Mirror) pendingEntry(oid string) (*pendingObject, bool) {
	key, _, ok := canonicalOID(oid)
	if !ok {
		return nil, false
	}
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()
	p, ok := m.pending[key]
	return p, ok
}

// recoverSpool re-registers the complete entries an earlier process left behind and removes everything else.
// It is best-effort: a scan or cleanup failure is logged and never fails the start.
func (m *Mirror) recoverSpool() {
	// Without a data dir the spool would be relative to the working directory; leave it alone.
	if m.dataDir == "" {
		return
	}
	entries, err := os.ReadDir(m.spoolDir())
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			slog.Warn("Mirror LFS spool scan failed; uploads an earlier process left pending are not recovered",
				"dir", m.spoolDir(), "error", err)
		}
		return
	}
	ctx := context.Background()
	for _, entry := range entries {
		name := entry.Name()
		path := filepath.Join(m.spoolDir(), name)
		key, _, ok := canonicalOID(name)
		// Only a regular file came from spool; a symlink or directory at an OID name is removed, never followed.
		if !ok || !entry.Type().IsRegular() {
			removeSpoolEntry(path, entry.IsDir())
			continue
		}
		if m.stored(ctx, key) {
			removeSpoolEntry(path, false)
			continue
		}
		// The entry keeps its on-disk name, canonical or not; a second spelling of the same OID (case-sensitive filesystems) is a duplicate.
		p := &pendingObject{path: path, done: make(chan struct{})}
		m.pendingMu.Lock()
		_, dup := m.pending[key]
		if !dup {
			m.pending[key] = p
		}
		m.pendingMu.Unlock()
		if dup {
			removeSpoolEntry(path, false)
			continue
		}
		m.scheduleIngest(key, p)
	}
}

// removeSpoolEntry deletes a spool entry (a stray directory with its contents); a failure is logged and left to
// the next start.
func removeSpoolEntry(path string, dir bool) {
	remove := os.Remove
	if dir {
		remove = os.RemoveAll
	}
	if err := remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		slog.Warn("Mirror LFS spool cleanup failed", "path", path, "error", err)
	}
}

// scheduleIngest runs the entry's single background ingest, detached from the request that spooled it.
func (m *Mirror) scheduleIngest(key string, p *pendingObject) {
	m.background.Go(func() {
		defer close(p.done)
		m.ingestSem <- struct{}{}
		defer func() { <-m.ingestSem }()
		// A panic in the xet pipeline or storage fails this entry only; unrecovered it would kill the process, and the
		// next start would re-schedule the same entry.
		defer func() {
			if r := recover(); r != nil {
				m.failIngest(key, p, fmt.Errorf("ingest panicked: %v", r), "stack", string(debug.Stack()))
			}
		}()
		if err := m.ingestPending(context.Background(), key, p); err != nil {
			m.failIngest(key, p, err)
		}
	})
}

// failIngest records the entry's error before done closes and logs it with the entry's fate; attrs extend the log line.
func (m *Mirror) failIngest(key string, p *pendingObject, err error, attrs ...any) {
	p.err = err
	msg := "Mirror LFS ingest failed; the spool entry was dropped"
	if m.held(key, p) {
		msg = "Mirror LFS ingest failed; the object stays servable from its spool until a re-upload or the next start retries"
	}
	slog.Warn(msg, append([]any{"oid", key, "error", err}, attrs...)...)
}

// ingestPending retires the entry once the storage resolves the OID, or once its spool file turns out gone or corrupt;
// any other failure keeps it pending and servable.
func (m *Mirror) ingestPending(ctx context.Context, key string, p *pendingObject) error {
	if !m.stored(ctx, key) {
		f, err := openSpoolFile(p.path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				m.retire(key, p)
			}
			return fmt.Errorf("open spool entry: %w", err)
		}
		if err := m.uploadSpoolClosing(ctx, f); err != nil {
			return err
		}
		if !m.stored(ctx, key) {
			// The upload indexed whatever the file holds; only a recovered entry can differ from its name.
			if err := verifySpool(p.path, key); err != nil {
				m.retire(key, p)
				return err
			}
			return errors.New("storage does not resolve the object after ingest")
		}
	}
	m.retire(key, p)
	return nil
}

// verifySpool re-hashes a spool entry against the OID that names it.
func verifySpool(path, key string) error {
	digest, _ := parseOID(key)
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("verify spool entry: %w", err)
	}
	defer func() { _ = f.Close() }()
	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return fmt.Errorf("verify spool entry: %w", err)
	}
	if sum := hash.Sum(nil); !bytes.Equal(sum, digest[:]) {
		return fmt.Errorf("spool entry does not match OID %s: sha256 %x", key, sum)
	}
	return nil
}

// held reports whether p is still the registered entry of its OID.
func (m *Mirror) held(key string, p *pendingObject) bool {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()
	return m.pending[key] == p
}

// retire drops the entry and its spool file under one lock so no concurrent upload lands between the two.
// On Windows the removal fails while a reader still has the file open; the next start deletes it, the storage holds the object.
func (m *Mirror) retire(key string, p *pendingObject) {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()
	if m.pending[key] == p {
		delete(m.pending, key)
	}
	removeSpoolEntry(p.path, false)
}

// uploadSpoolClosing closes f before the entry can be retired, even when the pipeline panics.
func (m *Mirror) uploadSpoolClosing(ctx context.Context, f *os.File) error {
	defer func() { _ = f.Close() }()
	return m.uploadSpool(ctx, f)
}

// uploadSpool runs the xet upload pipeline over a spooled, verified file.
func (m *Mirror) uploadSpool(ctx context.Context, f *os.File) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind spool file: %w", err)
	}
	opts := []xetupload.Option{xetupload.WithEnableSHA256(true), xetupload.WithCacheManager(m.xetCache.Upload)}
	if m.concurrency > 0 {
		opts = append(opts, xetupload.WithConcurrency(m.concurrency))
	}
	adapter := &localCAS{storage: m.xetStorage, namespace: xetNamespace}
	_, err := xetupload.UploadFile(ctx, adapter, f, opts...)
	return err
}

// localCAS adapts the xet storage to the standard upload pipeline so spool ingests write xorbs and shards without an HTTP hop.
type localCAS struct {
	storage   xetstorage.Storage
	namespace string
}

var _ xetupload.ClientAdapter = (*localCAS)(nil)

func (l *localCAS) HasXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error) {
	return l.storage.HasXorb(ctx, l.namespace, xorbHash)
}

func (l *localCAS) UploadXorb(ctx context.Context, xorbHash xet.XorbHash, reader io.ReadSeeker) (*xetupload.XorbUploadResponse, error) {
	wasInserted, err := l.storage.PutXorb(ctx, l.namespace, xorbHash, reader)
	if err != nil {
		return nil, err
	}
	return &xetupload.XorbUploadResponse{WasInserted: wasInserted}, nil
}

func (l *localCAS) UploadShard(ctx context.Context, shardObj *xetshard.Shard) (*xetupload.ShardUploadResponse, error) {
	wasInserted, err := l.storage.PutShard(ctx, shardObj)
	if err != nil {
		return nil, err
	}
	result := 0
	if wasInserted {
		result = 1
	}
	return &xetupload.ShardUploadResponse{Result: result}, nil
}

// Local shards are stored with raw chunk hashes, so keyed-shard candidates
// are unnecessary here.
func (l *localCAS) QueryDedupShards(ctx context.Context, chunkHashes []xet.ChunkHash, _ ...xet.ChunkHash) (map[xet.ChunkHash]xetshard.ChunkLocation, error) {
	results := make(map[xet.ChunkHash]xetshard.ChunkLocation, len(chunkHashes))
	for _, chunkHash := range chunkHashes {
		if _, ok := results[chunkHash]; ok {
			continue
		}
		shardObj, err := l.storage.GetShardByChunkHash(ctx, l.namespace, chunkHash)
		if err != nil || shardObj == nil {
			continue
		}
		// Register every chunk of the found shard, matching the remote
		// global-dedup behavior where one probe yields the whole shard.
		for h, loc := range shardObj.ChunkLocations() {
			if _, ok := results[h]; !ok {
				results[h] = loc
			}
		}
	}
	return results, nil
}
