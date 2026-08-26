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
	xettoken "github.com/wzshiming/xet/token"

	"github.com/matrixhub-ai/hfd/internal/stallguard"
	"github.com/matrixhub-ai/hfd/pkg/authenticate"
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

func (t resolveTarget) key() string {
	return "/" + strings.TrimPrefix(t.repoName, "/") + "/resolve/" + t.commit + "/" + t.path
}

// ServeResolve delegates a hub-style resolve request to the xet mirror data
// plane, which serves cached bytes, streams during ingest, or starts an
// ingest from the upstream. It reports false when no pull upstream is
// configured.
func (m *Mirror) ServeResolve(w http.ResponseWriter, r *http.Request, repoName, rev, filePath string) bool {
	if m.mirrorHandler == nil {
		return false
	}
	target := resolveTarget{repoName: repoName, commit: rev, path: filePath}
	m.serveKey(w, r, target.key())
	return true
}

// ServeObject serves an LFS object by OID from the data plane: directly from
// xet storage when ingested, or by delegating to the mirror's resolve path
// when the object is known from a pull scan (streaming while it ingests).
// It reports false when the object cannot be served.
func (m *Mirror) ServeObject(w http.ResponseWriter, r *http.Request, oid string) bool {
	if m.xetStorage == nil {
		return false
	}
	if m.ServeIngested(w, r, oid) {
		return true
	}
	if t, ok := m.oidIndex.Load(oid); ok {
		m.serveKey(w, r, t.(resolveTarget).key())
		return true
	}
	return false
}

// ServeIngested serves a fully ingested object straight from the xet storage,
// with the hub metadata headers and the xet Link headers so capable clients
// can fetch chunks through the CAS. It reports false when the object is not
// fully ingested.
func (m *Mirror) ServeIngested(w http.ResponseWriter, r *http.Request, oid string) bool {
	rs, size, err := m.OpenObject(r.Context(), oid)
	if err != nil {
		return false
	}
	defer func() {
		_ = rs.Close()
	}()
	m.setXETLinkHeaders(w, r, oid)
	setLinkedMetadataHeaders(w, oid, size)
	http.ServeContent(w, r, oid, time.Time{}, rs)
	return true
}

// ServeIngestedRedirect answers a resolve request for a fully ingested object
// the way the hub does: metadata plus xet Link headers, and a redirect to the
// sha256 bridge for the bytes. It reports false when the object is not fully
// ingested, so callers stream in-flight ingests instead.
func (m *Mirror) ServeIngestedRedirect(w http.ResponseWriter, r *http.Request, oid string) bool {
	rs, size, err := m.OpenObject(r.Context(), oid)
	if err != nil {
		return false
	}
	_ = rs.Close()
	m.setXETLinkHeaders(w, r, oid)
	setLinkedMetadataHeaders(w, oid, size)
	if size == 0 {
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
		return true
	}
	// The Location must be absolute: hub clients follow relative redirects
	// before reading metadata off the response they end up looking at.
	http.Redirect(w, r, m.externalBase(r)+"/xet-bridge/"+oid, http.StatusFound)
	return true
}

// setXETLinkHeaders attaches the xet Link headers and file hash when the
// object's reconstruction is known, steering capable clients to the CAS.
func (m *Mirror) setXETLinkHeaders(w http.ResponseWriter, r *http.Request, oid string) {
	digest, ok := parseOID(oid)
	if !ok {
		return
	}
	fh, err := m.xetStorage.GetFileHashBySHA256(r.Context(), xetNamespace, digest)
	if err != nil {
		return
	}
	base := m.externalBase(r)
	w.Header().Add("Link", fmt.Sprintf("<%s/xet-token>; rel=\"xet-auth\", <%s/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\"", base, base, fh.String()))
	w.Header().Set("X-Xet-Hash", fh.String())
}

// setLinkedMetadataHeaders writes the hub metadata headers for an LFS object.
func setLinkedMetadataHeaders(w http.ResponseWriter, oid string, size int64) {
	w.Header().Set("ETag", fmt.Sprintf("%q", oid))
	w.Header().Set("X-Linked-Etag", fmt.Sprintf("%q", oid))
	w.Header().Set("X-Linked-Size", strconv.FormatInt(size, 10))
}

// serveKey rewrites the request to the given resolve key and hands it to the
// xet mirror handler, preserving the original headers and host so Range
// requests and externally visible URLs keep working.
func (m *Mirror) serveKey(w http.ResponseWriter, r *http.Request, key string) {
	if m.mirrorHandler == nil {
		http.NotFound(w, r)
		return
	}
	r2 := r.Clone(r.Context())
	r2.URL = &url.URL{Path: key}
	r2.RequestURI = ""
	r2.Body = http.NoBody
	r2.ContentLength = 0
	m.mirrorHandler.ServeHTTP(w, r2)
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

// XETUploadEnabled reports whether the data plane can accept xet uploads.
func (m *Mirror) XETUploadEnabled() bool {
	return m.xetStorage != nil
}

// MintXETToken mints a short-lived CAS access token for xet transfers,
// returning the externally visible CAS base URL, the token, and its expiry.
func (m *Mirror) MintXETToken(r *http.Request) (casURL, token string, expiresAt time.Time) {
	now := time.Now()
	tok, exp := m.mintToken(now)
	return m.externalBase(r), tok, time.Unix(exp, 0)
}

// externalBase returns the externally visible base URL, derived from the
// request when no external URL is configured.
func (m *Mirror) externalBase(r *http.Request) string {
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
	if m.mirrorHandler == nil || len(oids) == 0 {
		return
	}
	for _, oid := range oids {
		m.oidIndex.Store(oid, targets[oid])
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
	in, err := m.mirrorHandler.Ingest(
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

// xetTokenTTL bounds the life of minted CAS tokens.
const xetTokenTTL = 60 * time.Minute

// xetTokenMethod and xetTokenPath are the fixed signing scope of CAS
// tokens: one grant covers the whole CAS surface. The slash makes the
// method an invalid HTTP token, so no wire request can ever replay a CAS
// token through the method+URL-bound user auth chain, and vice versa.
const (
	xetTokenMethod = "xet/cas"
	xetTokenPath   = "/"
)

// NewXETTokenScheme returns the CAS token mint/validate pair: hfd's signed
// token mechanism when a validator is injected, otherwise a process-local
// random-key issuer.
func NewXETTokenScheme(v authenticate.TokenSignValidator) (mint func(time.Time) (token string, exp int64), authFn func(string) bool, err error) {
	if v != nil {
		mint = func(now time.Time) (string, int64) {
			tok, err := v.Sign(context.Background(), xetTokenMethod, xetTokenPath, "xet-cas", xetTokenTTL)
			if err != nil || tok == "" {
				// Fail closed: an empty token cannot pass the CAS AuthFunc.
				slog.Error("mint CAS token", "error", err)
				return "", 0
			}
			return tok, now.Add(xetTokenTTL).Unix()
		}
		authFn = func(tok string) bool {
			_, _, ok, err := v.Validate(context.Background(), xetTokenMethod, xetTokenPath, tok)
			return err == nil && ok
		}
		return mint, authFn, nil
	}
	issuer, err := xettoken.NewIssuer(nil, xetTokenTTL)
	if err != nil {
		return nil, nil, fmt.Errorf("create token issuer: %w", err)
	}
	return issuer.Mint, func(tok string) bool { return issuer.Validate(tok, time.Now()) }, nil
}
