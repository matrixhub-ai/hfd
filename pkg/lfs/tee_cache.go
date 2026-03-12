package lfs

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync"

	"github.com/wzshiming/ioswmr"
)

// Blob tracks the state of an in-flight LFS object fetch, allowing concurrent readers to access
// the data as it is being downloaded and written to the local store.
type Blob struct {
	swmr  ioswmr.SWMR
	total int64
}

// NewReadSeeker returns a new ReadSeeker for serving in-flight content.
func (f *Blob) NewReadSeeker() io.ReadSeekCloser {
	return f.swmr.NewReadSeeker(0, int(f.total))
}

// Total returns the total size of the object being fetched.
func (f *Blob) Total() int64 {
	return f.total
}

// Progress returns the number of bytes currently available for reading.
func (f *Blob) Progress() int64 {
	return int64(f.swmr.Length())
}

// TeeCache fetches LFS objects from an upstream source, tees the download
// stream into a local store, and allows concurrent readers to access
// in-flight data before the download completes.
type TeeCache struct {
	httpClient *http.Client
	cache      sync.Map
	store      Storage
}

// TeeCacheOption configures a TeeCache.
type TeeCacheOption func(*TeeCache)

// NewTeeCache creates a new TeeCache.
// store is used to persist fetched objects and check if objects already exist locally.
func NewTeeCache(httpClient *http.Client, store Storage, opts ...TeeCacheOption) *TeeCache {
	p := &TeeCache{
		httpClient: httpClient,
		store:      store,
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
	batchResp, err := client.GetBatch(ctx, sourceURL, objects)
	if err != nil {
		return err
	}

	for _, obj := range batchResp.Objects {
		_, ok := m.cache.Load(obj.Oid)
		if ok {
			continue
		}
		if m.store.Exists(obj.Oid) {
			continue
		}

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
// body into the local store while making it available for concurrent readers.
func (m *TeeCache) fetchSingleObject(ctx context.Context, oid string, size int64, downloadAction action) {
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

	m.cache.Store(oid, f)

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
		if err := m.store.Put(oid, reader, size); err != nil {
			slog.ErrorContext(ctx, "LFS tee cache: failed to store object", "oid", oid, "error", err)
			return
		}
	}()
}
