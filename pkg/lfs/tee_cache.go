package lfs

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/matrixhub-ai/hfd/internal/utils"
	"github.com/wzshiming/ioswmr"
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

// TeeCache fetches LFS objects from an upstream source, tees the download
// stream into a local store, and allows concurrent readers to access
// in-flight data before the download completes.
type TeeCache struct {
	httpClient *http.Client
	cache      sync.Map
	storage    Storage
	xetStorage Storage // optional xet CAS storage for caching xorbs
	mut        sync.Mutex
}

// TeeCacheOption configures a TeeCache.
type TeeCacheOption func(*TeeCache)

// WithXetStorage sets an additional storage backend for caching XET CAS objects (xorbs).
// When set, objects fetched from upstream will be stored in both the primary LFS storage
// and the xet storage.
func WithXetStorage(storage Storage) TeeCacheOption {
	return func(t *TeeCache) {
		t.xetStorage = storage
	}
}

// NewTeeCache creates a new TeeCache.
// storage is used to persist fetched objects and check if objects already exist locally.
func NewTeeCache(storage Storage, opts ...TeeCacheOption) *TeeCache {
	p := &TeeCache{
		httpClient: utils.HTTPClient,
		storage:    storage,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Get returns a Blob for the given OID if it is currently being fetched, or nil if not.
func (m *TeeCache) Get(oid string) *Blob {
	f, ok := m.cache.Load(oid)
	if !ok {
		return nil
	}
	tf, ok := f.(*Blob)
	if !ok {
		return nil
	}
	return tf
}

// StartFetch initiates fetching the specified LFS objects from the given source URL.
func (m *TeeCache) StartFetch(ctx context.Context, sourceURL string, objects []LFSObject) error {
	client := newClient(m.httpClient)

	missingObjects := make([]LFSObject, 0, len(objects))
	for _, obj := range objects {
		if m.storage.Exists(obj.Oid) {
			continue
		}
		if _, ok := m.cache.Load(obj.Oid); ok {
			continue
		}
		missingObjects = append(missingObjects, obj)
	}

	if len(missingObjects) == 0 {
		return nil
	}

	batchResp, err := client.GetBatch(ctx, sourceURL, missingObjects)
	if err != nil {
		return err
	}

	for _, obj := range batchResp.Objects {
		if obj.Error != nil {
			continue
		}

		downloadAction, ok := obj.Actions["download"]
		if !ok {
			continue
		}

		slog.InfoContext(ctx, "LFS tee cache: fetching object from upstream", "oid", obj.Oid)
		m.fetchSingleObject(context.Background(), obj.Oid, obj.Size, downloadAction)
	}
	return nil
}

// fetchSingleObject fetches a single LFS object from upstream, tees the response
// body into the local storage while making it available for concurrent readers.
func (m *TeeCache) fetchSingleObject(ctx context.Context, oid string, size int64, downloadAction action) {
	m.mut.Lock()
	defer m.mut.Unlock()
	_, ok := m.cache.Load(oid)
	if ok {
		return
	}

	req, err := downloadAction.Request(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "LFS tee cache: failed to create download request", "oid", oid, "error", err)
		return
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		slog.ErrorContext(ctx, "LFS tee cache: failed to download object", "oid", oid, "error", err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		resp.Body.Close()
		slog.ErrorContext(ctx, "LFS tee cache: unexpected status code when downloading object", "status", resp.StatusCode, "oid", oid, "url", req.URL, "body", string(body))
		return
	}

	f := &Blob{
		swmr: ioswmr.NewSWMR(
			ioswmr.NewMemoryOrTemporaryFileBuffer(nil, nil),
			ioswmr.WithAutoClose(),
			ioswmr.WithBeforeCloseFunc(func() {
				m.cache.Delete(oid)
			}),
		),
		total: size,
	}
	lastModified := resp.Header.Get("Last-Modified")
	if modTime, err := time.Parse(http.TimeFormat, lastModified); err != nil {
		f.modTime = time.Now()
	} else {
		f.modTime = modTime
	}

	m.cache.Store(oid, f)
	reader := f.swmr.NewReader(0)

	go func() {
		sw := f.swmr.Writer()
		defer sw.Close()
		defer resp.Body.Close()
		_, err := io.Copy(sw, resp.Body)
		sw.CloseWithError(err)
	}()

	go func() {
		defer reader.Close()
		if err := m.storage.Put(oid, reader, size); err != nil {
			slog.ErrorContext(ctx, "LFS tee cache: failed to storage object", "oid", oid, "error", err)
			return
		}
		// Also store in xet CAS storage when available, so the object
		// is accessible through both LFS and xet reconstruction paths.
		if m.xetStorage != nil {
			if getter, ok := m.storage.(Getter); ok {
				if xetReader, _, err := getter.Get(oid); err == nil {
					defer xetReader.Close()
					if putErr := m.xetStorage.Put(oid, xetReader, size); putErr != nil {
						slog.ErrorContext(ctx, "LFS tee cache: failed to store object in xet storage", "oid", oid, "error", putErr)
					}
				}
			}
		}
	}()
}
