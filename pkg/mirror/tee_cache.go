package mirror

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/matrixhub-ai/hfd/internal/utils"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/wzshiming/dl"
	"github.com/wzshiming/ioswmr"
	"github.com/wzshiming/xet"
	xetclient "github.com/wzshiming/xet/client"
	xethf "github.com/wzshiming/xet/hf"
	"golang.org/x/sync/singleflight"
)

// Blob tracks the state of an in-flight LFS object fetch, allowing concurrent readers to access
// the data as it is being downloaded and written to the local store.
type Blob struct {
	swmr    ioswmr.SWMR
	total   int64
	modTime time.Time
}

// NewReadSeeker returns a new ReadSeeker for serving in-flight content.
func (b *Blob) NewReadSeeker() io.ReadSeekCloser {
	return b.swmr.NewReadSeeker(0, int(b.total))
}

// Total returns the total size of the object being fetched.
func (b *Blob) Total() int64 {
	return b.total
}

// ModTime returns the Last-Modified time of the object being fetched, if available.
func (b *Blob) ModTime() time.Time {
	return b.modTime
}

// Progress returns the number of bytes currently available for reading.
func (b *Blob) Progress() int64 {
	return int64(b.swmr.Length())
}

// pendingObject holds metadata for a queued (not-yet-started) LFS object fetch.
type pendingObject struct {
	size      int64
	sourceURL string
}

// teeCache fetches LFS objects from an upstream source, tees the download
// stream into a local store, and allows concurrent readers to access
// in-flight data before the download completes.
//
// A single background worker goroutine drains the pending queue, but only
// starts the next download when all active (foreground + background) downloads
// have finished.
type teeCache struct {
	httpClient   *http.Client
	cache        sync.Map // oid -> *Blob (active downloads)
	pending      sync.Map // oid -> *pendingObject (queued, not yet started)
	storage      lfs.Storage
	promoteGroup singleflight.Group // deduplicates concurrent promotions per oid
	enableXET    bool
	concurrency  int
	cacheDir     string
	progressFunc func(name string, downloaded, total int64)

	queueMu     sync.Mutex // guards pendingQueue and queueCond
	pendingList *list.List // FIFO list of oid strings for the background worker
	queueCond   *sync.Cond // signaled when a new oid is enqueued
	activeMu    sync.Mutex // guards activeCount
	activeCount int        // number of in-flight downloads (foreground + background)
	idleCond    *sync.Cond // broadcast when activeCount drops to zero
}

// newTeeCache creates a new teeCache.
// storage is used to persist fetched objects and check if objects already exist locally.
func newTeeCache(storage lfs.Storage, concurrency int, enableXET bool, cacheDir string, progressFunc func(name string, downloaded, total int64)) *teeCache {
	p := &teeCache{
		httpClient:   utils.HTTPClient,
		storage:      storage,
		enableXET:    enableXET,
		concurrency:  concurrency,
		cacheDir:     cacheDir,
		progressFunc: progressFunc,
		pendingList:  list.New(),
	}

	if cacheDir == "" {
		cacheDir = path.Join(os.TempDir(), "hfd")
	}

	p.queueCond = sync.NewCond(&p.queueMu)
	p.idleCond = sync.NewCond(&p.activeMu)
	go p.backgroundWorker()
	return p
}

// Queue records LFS objects as pending background fetches keyed by sourceURL.
// No HTTP calls or downloads are started; actual fetching is deferred until Get
// promotes an object to a foreground download, or the background worker picks it up.
func (m *teeCache) Queue(sourceURL string, objects []lfs.LFSObject) {
	for _, obj := range objects {
		if m.storage.Exists(obj.Oid) {
			continue
		}
		if _, ok := m.cache.Load(obj.Oid); ok {
			continue
		}
		po := &pendingObject{
			size:      obj.Size,
			sourceURL: sourceURL,
		}
		if _, loaded := m.pending.LoadOrStore(obj.Oid, po); loaded {
			continue // already queued
		}
		m.queueMu.Lock()
		m.pendingList.PushBack(obj.Oid)
		m.queueCond.Signal()
		m.queueMu.Unlock()
	}
}

// Get returns the in-flight Blob for oid, promoting it from the background pending
// queue to an active foreground download if necessary.
// Returns nil if the object is neither pending nor already downloading.
func (m *teeCache) Get(oid string) *Blob {
	if f, ok := m.cache.Load(oid); ok {
		if blob, ok := f.(*Blob); ok {
			return blob
		}
	}

	p, ok := m.pending.Load(oid)
	if !ok {
		return nil
	}
	pending, ok := p.(*pendingObject)
	if !ok {
		return nil
	}

	return m.promote(oid, pending)
}

// promote transitions a pending object to an active foreground download.
// It creates the Blob and registers it in the cache before returning, so the
// caller can begin reading immediately while the download proceeds in the background.
// singleflight deduplicates concurrent promotions of the same oid without a global
// mutex, so promotions of different oids proceed concurrently.
func (m *teeCache) promote(oid string, pending *pendingObject) *Blob {
	result, _, _ := m.promoteGroup.Do(oid, func() (any, error) {
		// Double-check: another goroutine may have already promoted this object.
		if f, ok := m.cache.Load(oid); ok {
			if blob, ok := f.(*Blob); ok {
				return blob, nil
			}
		}

		p, ok := m.pending.Load(oid)
		if !ok {
			return nil, nil
		}
		pending, ok = p.(*pendingObject)
		if !ok {
			return nil, nil
		}

		if err := os.MkdirAll(m.cacheDir, 0700); err != nil {
			slog.Error("LFS tee cache: failed to create cache directory for foreground fetch", "oid", oid, "error", err)
			return nil, nil
		}

		tmpFile, err := os.OpenFile(path.Join(m.cacheDir, oid), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			slog.Error("LFS tee cache: failed to create temp file for foreground fetch", "oid", oid, "error", err)
			return nil, nil
		}

		m.pending.Delete(oid)

		ws := m.storeAndPersist(context.Background(), oid, pending.size, tmpFile)

		f, _ := m.cache.Load(oid)
		blob, _ := f.(*Blob)

		sourceURL := pending.sourceURL
		size := pending.size

		m.activeMu.Lock()
		m.activeCount++
		m.activeMu.Unlock()

		go m.startDownload(context.Background(), sourceURL, oid, size, ws)

		return blob, nil
	})
	if result == nil {
		return nil
	}
	return result.(*Blob)
}

// downloadDone decrements the active download counter and notifies the background worker
// if all downloads have completed.
func (m *teeCache) downloadDone() {
	m.activeMu.Lock()
	m.activeCount--
	if m.activeCount == 0 {
		m.idleCond.Broadcast()
	}
	m.activeMu.Unlock()
}

// backgroundWorker runs for the lifetime of the teeCache. It drains the pending list
// one item at a time, waiting for all active downloads to finish before starting the next.
func (m *teeCache) backgroundWorker() {
	for {
		// Wait for a queued oid.
		m.queueMu.Lock()
		for m.pendingList.Len() == 0 {
			m.queueCond.Wait()
		}
		elem := m.pendingList.Front()
		m.pendingList.Remove(elem)
		m.queueMu.Unlock()

		oid := elem.Value.(string)

		// Skip if Get already promoted this object to a foreground download.
		if _, ok := m.pending.Load(oid); !ok {
			continue
		}

		// Wait until there are no active downloads.
		m.activeMu.Lock()
		for m.activeCount > 0 {
			m.idleCond.Wait()
		}
		m.activeMu.Unlock()

		// Re-check: may have been promoted while we were waiting.
		p, ok := m.pending.Load(oid)
		if !ok {
			continue
		}
		pending, ok := p.(*pendingObject)
		if !ok {
			continue
		}

		m.promote(oid, pending)
	}
}

// startDownload fetches the download URL via the LFS batch API and then downloads the object
// into ws. It is intended to run in a goroutine after the Blob has been registered in the cache.
func (m *teeCache) startDownload(ctx context.Context, sourceURL, oid string, size int64, ws ioswmr.Writer) {
	defer m.downloadDone()
	client := lfs.NewClient(m.httpClient)
	batchResp, err := client.GetBatch(ctx, sourceURL, []lfs.LFSObject{{Oid: oid, Size: size}})
	if err != nil {
		slog.ErrorContext(ctx, "LFS tee cache: failed to get batch download URL", "oid", oid, "error", err)
		ws.CloseWithError(err)
		return
	}

	if len(batchResp.Objects) == 0 {
		err := fmt.Errorf("batch API returned no objects for oid %s", oid)
		slog.ErrorContext(ctx, "LFS tee cache: batch API returned no objects", "oid", oid)
		ws.CloseWithError(err)
		return
	}

	obj := batchResp.Objects[0]
	if obj.Error != nil {
		err := fmt.Errorf("batch API error: %v", obj.Error)
		slog.ErrorContext(ctx, "LFS tee cache: batch API error for object", "oid", oid, "error", obj.Error)
		ws.CloseWithError(err)
		return
	}

	downloadAction, ok := obj.Actions["download"]
	if !ok {
		err := fmt.Errorf("no download action in batch response for oid %s", oid)
		slog.ErrorContext(ctx, "LFS tee cache: no download action in batch response", "oid", oid)
		ws.CloseWithError(err)
		return
	}

	m.doDownload(ctx, sourceURL, oid, size, downloadAction, ws)
}

// doDownload dispatches to the XET or basic download backend, writing into the provided ws.
func (m *teeCache) doDownload(ctx context.Context, sourceURL, oid string, size int64, downloadAction lfs.Action, ws ioswmr.Writer) {
	if !m.enableXET {
		slog.InfoContext(ctx, "Fetching object from upstream", "oid", oid)
		if err := m.doDownloadBasic(ctx, oid, size, downloadAction, ws); err != nil {
			slog.ErrorContext(ctx, "LFS tee cache: basic download failed", "oid", oid, "error", err)
		}
		return
	}

	target, err := getXetTarget(sourceURL)
	if err != nil {
		slog.ErrorContext(ctx, "LFS tee cache: failed to parse XET target from source URL, falling back to basic download", "url", sourceURL, "error", err)
		if err := m.doDownloadBasic(ctx, oid, size, downloadAction, ws); err != nil {
			slog.ErrorContext(ctx, "LFS tee cache: basic download failed", "oid", oid, "error", err)
		}
		return
	}
	if target == nil {
		if err := m.doDownloadBasic(ctx, oid, size, downloadAction, ws); err != nil {
			slog.ErrorContext(ctx, "LFS tee cache: basic download failed", "oid", oid, "error", err)
		}
		return
	}

	slog.InfoContext(ctx, "Fetching object from upstream with XET", "oid", oid)
	err = m.doDownloadXET(ctx, target, oid, size, downloadAction, ws)
	if err != nil {
		slog.ErrorContext(ctx, "LFS tee cache: failed to fetch object with XET", "oid", oid, "error", err)
	}
}

// doDownloadBasic downloads the LFS object via plain HTTP into ws.
func (m *teeCache) doDownloadBasic(ctx context.Context, oid string, size int64, downloadAction lfs.Action, ws ioswmr.Writer) error {
	req, err := downloadAction.Request(ctx)
	if err != nil {
		ws.CloseWithError(err)
		return err
	}

	d := dl.NewDownloader(
		dl.WithHTTPClient(m.httpClient),
		dl.WithConcurrency(m.concurrency),
		dl.WithChunkSize(64*1024*1024), // 64MB
		dl.WithResume(true),
		dl.WithForceTryRange(true),
		dl.WithProgressFunc(m.progressFunc),
		dl.WithCacheDir(path.Join(m.cacheDir, "dl")),
	)

	if err := d.Download(ctx, oid, ws, req.URL.String()); err != nil {
		ws.CloseWithError(err)
		return err
	}

	for i := 0; i < 8; i++ {
		n, err := ws.Seek(0, io.SeekEnd)
		if err != nil {
			ws.CloseWithError(err)
			return err
		}
		if n == size {
			break
		}

		if n > size {
			ws.CloseWithError(fmt.Errorf("downloaded more bytes than expected: got %d, expected %d", n, size))
			return fmt.Errorf("downloaded more bytes than expected: got %d, expected %d", n, size)
		}

		slog.WarnContext(ctx, "Download incomplete, retrying", "oid", oid, "downloaded", n, "expected", size, "attempt", i+1)
		time.Sleep(time.Second << i)

		err = d.Download(ctx, oid, ws, req.URL.String())
		if err != nil {
			ws.CloseWithError(err)
			return err
		}
	}

	ws.Close()
	return nil
}

// doDownloadXET downloads the LFS object via the XET protocol into ws.
func (m *teeCache) doDownloadXET(ctx context.Context, target *xethf.Target, oid string, size int64, downloadAction lfs.Action, ws ioswmr.Writer) error {
	xetHash, err := parseXetHashFromDownloadHref(downloadAction.Href)
	if err != nil {
		return err
	}

	auth := xethf.NewReadTokenProvider(m.httpClient, *target, "")
	xc, err := xetclient.NewClient(
		xetclient.WithAuthProvider(auth),
		xetclient.WithConcurrency(m.concurrency),
		xetclient.WithProgressFunc(m.progressFunc),
		xetclient.WithCacheDir(path.Join(m.cacheDir, "xet")),
	)
	if err != nil {
		return err
	}

	if err := xc.DownloadFile(ctx, xetHash, ws); err != nil {
		ws.CloseWithError(err)
		return err
	}

	for i := 0; i < 8; i++ {
		n, err := ws.Seek(0, io.SeekEnd)
		if err != nil {
			ws.CloseWithError(err)
			return err
		}
		if n == size {
			break
		}

		if n > size {
			ws.CloseWithError(fmt.Errorf("downloaded more bytes than expected: got %d, expected %d", n, size))
			return fmt.Errorf("downloaded more bytes than expected: got %d, expected %d", n, size)
		}

		slog.WarnContext(ctx, "XET download incomplete, retrying", "oid", oid, "downloaded", n, "expected", size, "attempt", i+1)
		time.Sleep(time.Second << i)

		err = xc.DownloadFile(ctx, xetHash, ws)
		if err != nil {
			ws.CloseWithError(err)
			return err
		}
	}

	ws.Close()
	return nil
}

func parseXetHashFromDownloadHref(href string) (xet.Hash, error) {
	u, err := url.Parse(href)
	if err != nil {
		return xet.Hash{}, fmt.Errorf("parse download href: %w", err)
	}

	last := path.Base(strings.Trim(u.Path, "/"))
	if last == "" || last == "." || last == "/" {
		return xet.Hash{}, fmt.Errorf("empty xet hash in download href")
	}

	hash, err := xet.ParseHash(last)
	if err != nil {
		return xet.Hash{}, fmt.Errorf("invalid xet hash %q: %w", last, err)
	}
	return hash, nil
}

func (m *teeCache) storeAndPersist(ctx context.Context, oid string, size int64, buffer *os.File) ioswmr.Writer {
	f := &Blob{
		swmr: ioswmr.NewSWMR(
			buffer,
			ioswmr.WithAutoClose(),
			ioswmr.WithBeforeCloseFunc(func() {
				n, err := buffer.Seek(0, io.SeekEnd)
				if err != nil {
					slog.ErrorContext(ctx, "LFS tee cache: failed to seek to end of buffer before persisting", "oid", oid, "error", err)
					return
				}
				if n != size {
					slog.ErrorContext(ctx, "LFS tee cache: downloaded size does not match expected size", "oid", oid, "expected", size, "actual", n)
					return
				}

				if putter, ok := m.storage.(lfs.MovePutter); ok {
					err := putter.MovePut(oid, buffer.Name())
					if err != nil {
						slog.ErrorContext(ctx, "LFS tee cache: failed to move file into storage", "oid", oid, "error", err)
						return
					}
					m.cache.Delete(oid)
					return
				}

				osFile, err := os.Open(buffer.Name())
				if err != nil {
					slog.ErrorContext(ctx, "LFS tee cache: failed to open temporary file", "oid", oid, "error", err)
					return
				}
				defer osFile.Close()

				err = m.storage.Put(oid, osFile, size)
				if err != nil {
					slog.ErrorContext(ctx, "LFS tee cache: failed to put file into storage", "oid", oid, "error", err)
					return
				}

				m.cache.Delete(oid)

				os.Remove(buffer.Name())
			}),
		),
		total: size,
	}
	f.modTime = time.Now()

	m.cache.Store(oid, f)

	return f.swmr.Writer()
}

func getXetTarget(sourceURL string) (*xethf.Target, error) {
	u, err := url.Parse(sourceURL)
	if err != nil {
		return nil, err
	}

	switch u.Hostname() {
	default:
		return nil, nil
	case "huggingface.co", "hf.m.daocloud.io", "hf-mirror.com":
	}

	repoType := "model"

	parts := strings.SplitN(strings.Trim(u.Path, "/"), "/", 3)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid source URL path: %s", u.Path)
	}

	repoID := ""

	switch len(parts) {
	case 2:
		repoID = parts[0] + "/" + parts[1]
	case 3:
		repoType = parts[0]
		repoID = parts[1] + "/" + parts[2]
	}

	target := xethf.Target{
		Endpoint: fmt.Sprintf("%s://%s", u.Scheme, u.Host),
		RepoType: repoType,
		RepoID:   repoID,
		Revision: "main",
	}

	return &target, nil
}
