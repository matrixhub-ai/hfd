package mirror

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/wzshiming/httpseek"
	xetmirror "github.com/wzshiming/xet/mirror"

	"github.com/matrixhub-ai/hfd/internal/stallguard"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
)

// xetNamespace is the single CAS namespace the data plane operates in,
// matching the namespace the xet mirror ingests into.
const xetNamespace = "default"

// resolveTarget locates a file in the upstream hub by its commit-pinned
// resolve key, along with the object size from the pointer that named it.
type resolveTarget struct {
	repoName string
	commit   string
	path     string
	size     int64
}

// RegisterObject registers the upstream resolve target for an OID so
// ServeOID (and LFS batch KnowsObject) can serve it, e.g. for revisions
// outside pull-scan tips.
func (m *Mirror) RegisterObject(oid, repoName, commit, path string, size int64) {
	m.oidIndex.Store(oid, resolveTarget{repoName: repoName, commit: commit, path: path, size: size})
}

// ServeOID serves the object by OID straight from the ingest engine: ready
// entries are served from storage, in-flight ingests are streamed from the
// growing spool (ingest-on-miss). It reports false when the OID is
// unregistered, there is no engine, or the upstream has no file, letting the
// caller answer its own 404.
func (m *Mirror) ServeOID(w http.ResponseWriter, r *http.Request, oid string) bool {
	if m.xetMirror == nil {
		return false
	}
	v, ok := m.oidIndex.Load(oid)
	if !ok {
		return false
	}
	t := v.(resolveTarget)
	res, err := m.resolve(r.Context(), t)
	if err != nil {
		return false
	}
	if res.Entry != nil {
		return m.serveIngested(w, r, oid)
	}
	if _, _, err := res.Stream.WaitMeta(r.Context()); err != nil {
		return false
	}
	size, ok := res.Stream.WaitSize(r.Context())
	if !ok {
		return false
	}
	if size >= 0 {
		rs := res.Stream.NewSeekReader(r.Context(), size)
		if rs == nil {
			return m.serveDrained(w, r, oid, t)
		}
		defer func() { _ = rs.Close() }()
		m.SetXETLinkHeaders(w, r, oid, size)
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(w, r, oid, time.Time{}, rs)
		return true
	}
	rc := res.Stream.NewReader(r.Context(), 0)
	if rc == nil {
		return m.serveDrained(w, r, oid, t)
	}
	defer func() { _ = rc.Close() }()
	// Size unknown until the ingest completes: stream the body; a copy error
	// after the first write cannot be reported anymore.
	_, _ = io.Copy(w, rc)
	return true
}

// resolve runs one engine resolution for the target, with the components in
// the escaped URL path form Resolve shares tasks and entries under.
func (m *Mirror) resolve(ctx context.Context, t resolveTarget) (*xetmirror.Resolution, error) {
	return m.xetMirror.Resolve(ctx,
		escapePath(strings.TrimPrefix(t.repoName, "/")),
		t.commit,
		escapePath(t.path),
	)
}

// serveIngested serves a fully ingested object from the xet storage.
func (m *Mirror) serveIngested(w http.ResponseWriter, r *http.Request, oid string) bool {
	rs, size, err := m.OpenObject(r.Context(), oid)
	if err != nil {
		return false
	}
	defer func() { _ = rs.Close() }()
	m.SetXETLinkHeaders(w, r, oid, size)
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, oid, time.Time{}, rs)
	return true
}

// serveDrained covers the rare race where the ingest finished and its spool
// was drained before the stream was attached: one re-resolve picks up the
// published terminal entry.
func (m *Mirror) serveDrained(w http.ResponseWriter, r *http.Request, oid string, t resolveTarget) bool {
	res, err := m.resolve(r.Context(), t)
	if err != nil || res.Entry == nil {
		return false
	}
	return m.serveIngested(w, r, oid)
}

// MintXETToken mints a short-lived CAS access token for xet transfers,
// returning the externally visible CAS base URL, the token, and its expiry.
func (m *Mirror) MintXETToken(r *http.Request) (casURL, token string, expiresAt time.Time) {
	if m.mint == nil {
		return "", "", time.Time{}
	}
	tok, exp := m.mint(time.Now())
	return m.ExternalBase(r), tok, time.Unix(exp, 0)
}

// CanMintToken reports whether the mirror can mint CAS access tokens.
func (m *Mirror) CanMintToken() bool {
	return m.mint != nil
}

// SetXETLinkHeaders writes the hub metadata headers for an LFS object, plus
// the xet Link headers and file hash when the object's reconstruction is
// known, steering capable clients to the CAS.
func (m *Mirror) SetXETLinkHeaders(w http.ResponseWriter, r *http.Request, oid string, size int64) {
	w.Header().Set("ETag", fmt.Sprintf("%q", oid))
	w.Header().Set("X-Linked-Etag", fmt.Sprintf("%q", oid))
	w.Header().Set("X-Linked-Size", strconv.FormatInt(size, 10))
	if m.xetStorage == nil {
		return
	}
	digest, ok := parseOID(oid)
	if !ok {
		return
	}
	fh, err := m.xetStorage.GetFileHashBySHA256(r.Context(), xetNamespace, digest)
	if err != nil {
		return
	}
	base := m.ExternalBase(r)
	w.Header().Add("Link", fmt.Sprintf("<%s/xet-token>; rel=\"xet-auth\", <%s/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\"", base, base, fh.String()))
	w.Header().Set("X-Xet-Hash", fh.String())
}

// ExternalBase returns the externally visible base URL, derived from the
// request when no external URL is configured.
func (m *Mirror) ExternalBase(r *http.Request) string {
	if m.externalURL != "" {
		return strings.TrimRight(m.externalURL, "/")
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	}
	return scheme + "://" + r.Host
}

// HasObject reports whether the xet storage holds a fully ingested file with
// the given SHA-256 OID.
func (m *Mirror) HasObject(ctx context.Context, oid string) bool {
	if m.xetStorage == nil {
		return false
	}
	digest, ok := parseOID(oid)
	if !ok {
		return false
	}
	_, err := m.xetStorage.GetFileHashBySHA256(ctx, xetNamespace, digest)
	return err == nil
}

// KnowsObject reports whether the object is either fully ingested or known
// from a pull scan (so it can be served, possibly by triggering an ingest).
func (m *Mirror) KnowsObject(ctx context.Context, oid string) bool {
	if m.HasObject(ctx, oid) {
		return true
	}
	_, ok := m.oidIndex.Load(oid)
	return ok
}

// OpenObject returns a reader over the reconstructed file with the given
// SHA-256 OID from the xet storage, along with its size.
func (m *Mirror) OpenObject(ctx context.Context, oid string) (io.ReadSeekCloser, int64, error) {
	if m.xetStorage == nil {
		return nil, 0, os.ErrNotExist
	}
	digest, ok := parseOID(oid)
	if !ok {
		return nil, 0, os.ErrNotExist
	}
	if _, err := m.xetStorage.GetFileHashBySHA256(ctx, xetNamespace, digest); err != nil {
		return nil, 0, os.ErrNotExist
	}
	rs, err := m.xetStorage.GetReconstructedFile(ctx, xetNamespace, digest)
	if err != nil {
		return nil, 0, fmt.Errorf("reconstruct object %s: %w", oid, err)
	}
	size, err := rs.Seek(0, io.SeekEnd)
	if err != nil {
		_ = rs.Close()
		return nil, 0, fmt.Errorf("size object %s: %w", oid, err)
	}
	if _, err := rs.Seek(0, io.SeekStart); err != nil {
		_ = rs.Close()
		return nil, 0, fmt.Errorf("rewind object %s: %w", oid, err)
	}
	return rs, size, nil
}

func parseOID(oid string) ([32]byte, bool) {
	var digest [32]byte
	raw, err := hex.DecodeString(oid)
	if err != nil || len(raw) != sha256.Size {
		return digest, false
	}
	copy(digest[:], raw)
	return digest, true
}

// prefetchLFS registers the scanned objects in the OID index and ingests the
// missing ones sequentially in the background, falling back to the source's
// git-lfs batch API when an ingest fails.
func (m *Mirror) prefetchLFS(sourceURL string, oids []string, targets map[string]resolveTarget) {
	if m.xetMirror == nil || len(oids) == 0 {
		return
	}
	for _, oid := range oids {
		t := targets[oid]
		m.RegisterObject(oid, t.repoName, t.commit, t.path, t.size)
	}
	m.background.Add(1)
	go func() {
		defer m.background.Done()
		ctx := context.Background()
		for _, oid := range oids {
			target := targets[oid]
			if m.HasObject(ctx, oid) {
				continue
			}
			if _, inflight := m.prefetching.LoadOrStore(oid, struct{}{}); inflight {
				continue
			}
			err := m.ingest(ctx, target)
			if err != nil {
				if fbErr := m.fallbackDownload(ctx, sourceURL, oid, target.size); fbErr != nil {
					slog.Warn("Mirror LFS prefetch failed", "repo", target.repoName, "path", target.path, "oid", oid, "error", err, "fallbackError", fbErr)
				} else {
					slog.Info("Mirror LFS prefetch fell back to the git-lfs batch API", "repo", target.repoName, "path", target.path, "oid", oid, "ingestError", err)
				}
			}
			m.prefetching.Delete(oid)
		}
	}()
}

// stallIdleWindow aborts a transfer phase that moves no bytes for this long;
// the stall guard sits below httpseek so aborted downloads resume with
// ranged retries instead of restarting.
const stallIdleWindow = 15 * time.Second

// newDownloadClient returns the client for object content downloads; it
// resumes interrupted response streams with ranged retries.
func newDownloadClient() *http.Client {
	return &http.Client{
		Transport: httpseek.NewMustReaderTransport(stallguard.NewTransport(http.DefaultTransport, stallIdleWindow), func(r *http.Request, retry int, err error) error {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if retry >= 8 {
				return fmt.Errorf("max retries reached for %s: %w", r.URL.String(), err)
			}
			backoff := 100 * time.Millisecond << retry
			slog.WarnContext(r.Context(), "Retrying interrupted download", "url", r.URL.String(), "retry", retry+1, "backoff", backoff, "error", err)
			time.Sleep(backoff)
			return nil
		}),
	}
}

// fallbackDownload fetches an object through the source's git-lfs batch API
// and ingests it into the xet storage, covering upstreams that do not expose
// the hub resolve API the xet mirror ingests through.
func (m *Mirror) fallbackDownload(ctx context.Context, sourceURL, oid string, size int64) error {
	if sourceURL == "" {
		return fmt.Errorf("no source URL for fallback")
	}
	batchResp, err := lfs.NewClient(m.httpClient).DownloadBatch(ctx, sourceURL, lfs.TransferCapabilities, []lfs.LFSObject{{Oid: oid, Size: size}})
	if err != nil {
		return fmt.Errorf("download batch: %w", err)
	}
	if len(batchResp.Objects) == 0 {
		return fmt.Errorf("download batch returned no objects")
	}
	obj := batchResp.Objects[0]
	if obj.Error != nil {
		return fmt.Errorf("download batch object error: %v", obj.Error)
	}
	action, ok := obj.Actions["download"]
	if !ok {
		return fmt.Errorf("no download action in batch response")
	}
	req, err := action.Request(ctx)
	if err != nil {
		return fmt.Errorf("build download request: %w", err)
	}
	resp, err := m.downloadClient.Do(req)
	if err != nil {
		return fmt.Errorf("download object: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("download returned unexpected status %d", resp.StatusCode)
	}
	if err := m.PutObject(ctx, oid, resp.Body, size); err != nil {
		return fmt.Errorf("store object: %w", err)
	}
	return nil
}

// ingest runs one ingest through the xet mirror and waits for the entry to
// land; abandoning the wait on ctx cancel never cancels the ingest itself.
func (m *Mirror) ingest(ctx context.Context, target resolveTarget) error {
	in, err := m.xetMirror.Ingest(
		escapePath(strings.TrimPrefix(target.repoName, "/")),
		target.commit,
		escapePath(target.path),
	)
	if err != nil {
		return err
	}
	select {
	case <-in.Done():
		_, err := in.Entry()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// escapePath escapes a path the way ServeHTTP sees it, keeping slashes, so
// Ingest shares tasks and entries with the HTTP resolve path.
func escapePath(p string) string {
	return (&url.URL{Path: p}).EscapedPath()
}
