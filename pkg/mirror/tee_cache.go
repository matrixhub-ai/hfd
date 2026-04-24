package mirror

import (
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

// teeCache fetches LFS objects from an upstream source, tees the download
// stream into a local store, and allows concurrent readers to access
// in-flight data before the download completes.
type teeCache struct {
	httpClient   *http.Client
	cache        sync.Map
	storage      lfs.Storage
	mut          sync.Mutex
	enableXET    bool
	concurrency  int
	cacheDir     string
	group        singleflight.Group
	progressFunc func(name string, downloaded, total int64)
}

// newTeeCache creates a new teeCache.
// storage is used to persist fetched objects and check if objects already exist locally.
func newTeeCache(storage lfs.Storage, concurrency int, enableXET bool, progressFunc func(name string, downloaded, total int64)) *teeCache {
	p := &teeCache{
		httpClient:   utils.HTTPClient,
		storage:      storage,
		enableXET:    enableXET,
		concurrency:  concurrency,
		cacheDir:     path.Join(os.TempDir(), "hfd"),
		progressFunc: progressFunc,
	}

	return p
}

// Get returns a Blob for the given OID if it is currently being fetched, or nil if not.
func (m *teeCache) Get(oid string) *Blob {
	f, ok := m.cache.Load(oid)
	if !ok {
		return nil
	}
	blob, ok := f.(*Blob)
	if !ok {
		return nil
	}
	return blob
}

// StartFetch initiates fetching the specified LFS objects from the given source URL.
func (m *teeCache) StartFetch(ctx context.Context, sourceURL string, objects []lfs.LFSObject) error {
	client := lfs.NewClient(m.httpClient)

	missingObjects := m.collectMissingObjects(objects)
	if len(missingObjects) == 0 {
		return nil
	}

	batchResp, err := client.GetBatch(ctx, sourceURL, missingObjects)
	if err != nil {
		return err
	}

	m.mut.Lock()
	defer m.mut.Unlock()

	for _, obj := range batchResp.Objects {
		if obj.Error != nil {
			slog.ErrorContext(ctx, "LFS tee cache: batch API returned error for object, skipping", "oid", obj.Oid, "error", obj.Error)
			continue
		}

		downloadAction, ok := obj.Actions["download"]
		if !ok {
			continue
		}

		if _, ok := m.cache.Load(obj.Oid); ok {
			continue
		}

		m.group.DoChan(obj.Oid, func() (interface{}, error) {
			m.fetchSingleObject(context.Background(), sourceURL, obj.Oid, obj.Size, downloadAction)
			return nil, nil
		})
	}
	return nil
}

func (m *teeCache) collectMissingObjects(objects []lfs.LFSObject) []lfs.LFSObject {
	missingObjects := make([]lfs.LFSObject, 0, len(objects))
	for _, obj := range objects {
		if m.storage.Exists(obj.Oid) {
			continue
		}
		if _, ok := m.cache.Load(obj.Oid); ok {
			continue
		}
		missingObjects = append(missingObjects, obj)
	}
	return missingObjects
}

// fetchSingleObject fetches a single LFS object from upstream, tees the response
// body into the local storage while making it available for concurrent readers.
func (m *teeCache) fetchSingleObject(ctx context.Context, sourceURL, oid string, size int64, downloadAction lfs.Action) {
	if !m.enableXET {
		slog.InfoContext(ctx, "Fetching object from upstream", "oid", oid)
		m.fetchSingleObjectWithBasic(ctx, oid, size, downloadAction)
		return
	}
	target, err := getXetTarget(sourceURL)
	if err != nil {
		slog.ErrorContext(ctx, "LFS tee cache: failed to parse XET target from source URL, falling back to basic download", "url", sourceURL, "error", err)
		m.fetchSingleObjectWithBasic(ctx, oid, size, downloadAction)
		return
	}

	slog.InfoContext(ctx, "Fetching object from upstream with XET", "oid", oid)
	err = m.fetchSingleObjectWithXET(ctx, target, oid, size, downloadAction)
	if err != nil {
		slog.ErrorContext(ctx, "LFS tee cache: failed to fetch object with XET, falling back to basic download", "oid", oid, "error", err)
		return
	}
}

func (m *teeCache) fetchSingleObjectWithBasic(ctx context.Context, oid string, size int64, downloadAction lfs.Action) error {
	req, err := downloadAction.Request(ctx)
	if err != nil {
		return err
	}

	err = os.MkdirAll(m.cacheDir, 0700)
	if err != nil {
		return fmt.Errorf("create cache directory: %w", err)
	}

	dl := dl.NewDownloader(
		dl.WithHTTPClient(m.httpClient),
		dl.WithConcurrency(m.concurrency),
		dl.WithChunkSize(64*1024*1024), // 64MB
		dl.WithResume(true),
		dl.WithForceTryRange(true),
		dl.WithProgressFunc(m.progressFunc),
		dl.WithCacheDir(path.Join(m.cacheDir, "dl")),
	)

	tmpFile, err := os.OpenFile(path.Join(m.cacheDir, oid), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	ws := m.storeAndPersist(ctx, oid, size, tmpFile)

	err = dl.Download(ctx, oid, ws, req.URL.String())
	if err != nil {
		ws.CloseWithError(err)
		return err
	}
	ws.Close()

	return nil
}

func (m *teeCache) fetchSingleObjectWithXET(ctx context.Context, target *xethf.Target, oid string, size int64, downloadAction lfs.Action) error {
	xetHash, err := parseXetHashFromDownloadHref(downloadAction.Href)
	if err != nil {
		return err
	}

	err = os.MkdirAll(m.cacheDir, 0700)
	if err != nil {
		return fmt.Errorf("create cache directory: %w", err)
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

	tmpFile, err := os.OpenFile(path.Join(m.cacheDir, oid), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	ws := m.storeAndPersist(ctx, oid, size, tmpFile)

	err = xc.DownloadFile(ctx, xetHash, ws)
	if err != nil {
		ws.CloseWithError(err)
		return err
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
				if putter, ok := m.storage.(lfs.MovePutter); ok {
					err := putter.MovePut(oid, buffer.Name())
					if err != nil {
						slog.ErrorContext(ctx, "LFS tee cache: failed to move file into storage", "oid", oid, "error", err)
					}
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
