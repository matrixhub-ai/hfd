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
	"strings"
	"time"

	"github.com/wzshiming/xet/auth"
	xetmirror "github.com/wzshiming/xet/mirror"

	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/repository"
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

// This name is handed to the mirror source callback, whose URL the engine fetches through.
func canonicalRepoName(name string) string {
	return strings.TrimSuffix(strings.TrimPrefix(repository.ResolvePath(name), "/"), ".git")
}

// RegisterObject registers the upstream resolve target for an OID so
// ServeOID (and LFS batch KnowsObject) can serve it, e.g. for revisions
// outside pull-scan tips.
func (m *Mirror) RegisterObject(oid, repoName, commit, path string, size int64) {
	m.registerTarget(oid, resolveTarget{repoName: canonicalRepoName(repoName), commit: commit, path: path, size: size})
}

// registerTarget stores an already canonical target; stripping twice would rename a repository ending in .git.
func (m *Mirror) registerTarget(oid string, t resolveTarget) {
	m.oidMu.Lock()
	defer m.oidMu.Unlock()
	old := m.oidIndex[oid]
	for _, have := range old {
		if have == t {
			return
		}
	}
	// A fresh slice keeps readers iterating the old one unaffected.
	m.oidIndex[oid] = append([]resolveTarget{t}, old...)
}

func (m *Mirror) targets(oid string) []resolveTarget {
	m.oidMu.Lock()
	defer m.oidMu.Unlock()
	return m.oidIndex[oid]
}

// ServeOID serves the object by OID straight from the ingest engine: ready
// entries are served from storage, in-flight ingests are streamed from the
// growing spool (ingest-on-miss). Registered targets are tried in order. It
// reports false when the OID is unregistered, there is no engine, or no
// upstream has the file, letting the caller answer its own 404.
func (m *Mirror) ServeOID(w http.ResponseWriter, r *http.Request, oid string) bool {
	if m.xetMirror == nil {
		return false
	}
	for _, t := range m.targets(oid) {
		if m.serveTarget(w, r, oid, t) {
			return true
		}
	}
	return false
}

// serveTarget serves the object through one resolve target; false leaves the response unwritten.
func (m *Mirror) serveTarget(w http.ResponseWriter, r *http.Request, oid string, t resolveTarget) bool {
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
		lfs.SetObjectHeaders(w, oid, size)
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

// resolve runs one engine resolution for the target through its hub download URL.
func (m *Mirror) resolve(ctx context.Context, t resolveTarget) (*xetmirror.Resolution, error) {
	rawURL, token, err := m.upstreamURL(ctx, t)
	if err != nil {
		return nil, err
	}
	return m.xetMirror.Resolve(ctx, rawURL, token)
}

// upstreamURL derives the hub download URL and bearer of a target from the
// mirror source and sync user info callbacks, with resolvePullSource's precedence.
func (m *Mirror) upstreamURL(ctx context.Context, t resolveTarget) (rawURL, token string, err error) {
	if m.mirrorSourceFunc == nil {
		return "", "", fmt.Errorf("no mirror source configured for repository %q", t.repoName)
	}
	sourceURL, isMirror, err := m.mirrorSourceFunc(ctx, t.repoName)
	if err != nil {
		return "", "", err
	}
	if !isMirror {
		return "", "", fmt.Errorf("repository %q is not configured as a mirror", t.repoName)
	}
	u, err := url.Parse(sourceURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", "", fmt.Errorf("mirror source of repository %q is not an http(s) URL", t.repoName)
	}
	token, _ = u.User.Password()
	if m.syncUserInfoFunc != nil {
		ui, err := m.syncUserInfoFunc(ctx, t.repoName)
		if err != nil {
			return "", "", err
		}
		if ui != nil {
			token, _ = ui.Password()
		}
	}
	u.User = nil
	return strings.TrimSuffix(u.String(), "/") + "/resolve/" + t.commit + "/" + escapePath(t.path), token, nil
}

// serveIngested serves a fully ingested object from the xet storage.
func (m *Mirror) serveIngested(w http.ResponseWriter, r *http.Request, oid string) bool {
	rs, size, err := m.OpenObject(r.Context(), oid)
	if err != nil {
		return false
	}
	defer func() { _ = rs.Close() }()
	lfs.SetObjectHeaders(w, oid, size)
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

// MintXETToken mints a CAS grant and returns the external CAS URL, token, and expiry.
func (m *Mirror) MintXETToken(r *http.Request, g auth.Grant) (casURL, token string, expiresAt time.Time, err error) {
	if m.mint == nil {
		return "", "", time.Time{}, errors.New("no CAS token mint configured")
	}
	tok, exp, err := m.mint(g)
	if err != nil {
		return "", "", time.Time{}, err
	}
	return m.ExternalBase(r), tok, time.Unix(exp, 0), nil
}

// CanMintToken reports whether the mirror can mint CAS access tokens.
func (m *Mirror) CanMintToken() bool {
	return m.mint != nil
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

// FileHash returns the xet file hash of a fully ingested object, or "" when
// the xet storage does not hold it.
func (m *Mirror) FileHash(ctx context.Context, oid string) string {
	if m.xetStorage == nil {
		return ""
	}
	digest, ok := parseOID(oid)
	if !ok {
		return ""
	}
	fh, err := m.xetStorage.GetFileHashBySHA256(ctx, xetNamespace, digest)
	if err != nil {
		return ""
	}
	return fh.String()
}

// HasObject reports whether the xet storage holds a fully ingested file with
// the given SHA-256 OID.
func (m *Mirror) HasObject(ctx context.Context, oid string) bool {
	return m.FileHash(ctx, oid) != ""
}

// KnowsObject reports whether the object is either fully ingested or known
// from a pull scan (so it can be served, possibly by triggering an ingest).
func (m *Mirror) KnowsObject(ctx context.Context, oid string) bool {
	if m.HasObject(ctx, oid) {
		return true
	}
	return len(m.targets(oid)) > 0
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
		m.registerTarget(oid, targets[oid])
	}
	m.background.Go(func() {
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
	})
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
	resp, err := m.httpClient.Do(req)
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
	rawURL, token, err := m.upstreamURL(ctx, target)
	if err != nil {
		return err
	}
	in, err := m.xetMirror.Ingest(rawURL, token)
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
