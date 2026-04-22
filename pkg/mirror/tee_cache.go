package mirror

import (
	"context"
	"errors"
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
	"github.com/wzshiming/ioswmr"
	"github.com/wzshiming/xet"
	xetclient "github.com/wzshiming/xet/client"
	xethf "github.com/wzshiming/xet/hf"
)

var errSkipXET = errors.New("skip xet")

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
	httpClient     *http.Client
	cache          sync.Map
	storage        lfs.Storage
	mut            sync.Mutex
	enableXet      bool
	xetConcurrency int
}

// newTeeCache creates a new teeCache.
// storage is used to persist fetched objects and check if objects already exist locally.
func newTeeCache(storage lfs.Storage, xetConcurrency int) *teeCache {
	p := &teeCache{
		httpClient:     utils.HTTPClient,
		storage:        storage,
		enableXet:      xetConcurrency > 0,
		xetConcurrency: xetConcurrency,
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

		m.fetchSingleObject(context.Background(), sourceURL, obj.Oid, obj.Size, downloadAction)
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
	if !m.enableXet {
		slog.InfoContext(ctx, "Fetching object from upstream", "oid", oid)
		m.fetchSingleObjectWithBasic(ctx, oid, size, downloadAction)
		return
	}
	if target, _ := getXetTarget(sourceURL); target != nil {
		slog.InfoContext(ctx, "Fetching object from upstream with XET", "oid", oid)
		err := m.fetchSingleObjectWithXET(ctx, target, oid, size, downloadAction)
		if err == nil {
			return
		}

		if errors.Is(err, errSkipXET) {
			slog.InfoContext(ctx, "LFS tee cache: skipping XET download due to missing credentials or unsupported endpoint", "oid", oid, "reason", err)
		} else {
			slog.ErrorContext(ctx, "LFS tee cache: failed to fetch object with XET, falling back to basic download", "oid", oid, "error", err)
		}
	} else {
		slog.InfoContext(ctx, "Fetching object from upstream", "oid", oid)
	}
	m.fetchSingleObjectWithBasic(ctx, oid, size, downloadAction)
}

func (m *teeCache) fetchSingleObjectWithBasic(ctx context.Context, oid string, size int64, downloadAction lfs.Action) {
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

	m.storeAndPersist(ctx, oid, size, resp.Header.Get("Last-Modified"), resp.Body)
}

func (m *teeCache) fetchSingleObjectWithXET(ctx context.Context, target *xethf.Target, oid string, size int64, downloadAction lfs.Action) error {
	xetHash, err := parseXetHashFromDownloadHref(downloadAction.Href)
	if err != nil {
		return err
	}

	auth := xethf.NewReadTokenProvider(m.httpClient, *target, "")
	xc := xetclient.NewClient(
		xetclient.WithAuthProvider(auth),
		xetclient.WithConcurrency(m.xetConcurrency),
	)

	reader, expectedSize, err := xc.DownloadFile(ctx, xetHash, nil)
	if err != nil {
		return err
	}

	if expectedSize > 0 {
		size = expectedSize
	}
	m.storeAndPersist(ctx, oid, size, "", io.NopCloser(reader))
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

func (m *teeCache) storeAndPersist(ctx context.Context, oid string, size int64, lastModified string, body io.ReadCloser) {
	tmpFile, err := os.CreateTemp("", "lfs-tee-cache-*")
	if err != nil {
		slog.ErrorContext(ctx, "LFS tee cache: failed to create temporary file", "oid", oid, "error", err)
		return
	}

	f := &Blob{
		swmr: ioswmr.NewSWMR(
			tmpFile,
			ioswmr.WithAutoClose(),
			ioswmr.WithBeforeCloseFunc(func() {
				if putter, ok := m.storage.(lfs.MovePutter); ok {
					err := putter.MovePut(oid, tmpFile.Name())
					if err != nil {
						slog.ErrorContext(ctx, "LFS tee cache: failed to move file into storage", "oid", oid, "error", err)
					}
					return
				}

				osFile, err := os.Open(tmpFile.Name())
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
			ioswmr.WithAfterCloseFunc(func(err error) error {
				if err != nil {
					return err
				}
				os.Remove(tmpFile.Name())
				return nil
			}),
		),
		total: size,
	}
	f.modTime = parseLastModified(lastModified)

	m.cache.Store(oid, f)

	go func() {
		sw := f.swmr.Writer()
		defer body.Close()
		_, err := io.Copy(sw, body)
		sw.CloseWithError(err)
	}()
}

func parseLastModified(lastModified string) time.Time {
	if modTime, err := time.Parse(http.TimeFormat, lastModified); err == nil {
		return modTime
	}
	return time.Now()
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
