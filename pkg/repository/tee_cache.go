package repository

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
)

// GitTeeCache proxies git HTTP requests to an upstream source while
// building a local mirror cache in the background, analogous to the
// LFS TeeCache for LFS objects.
type GitTeeCache struct {
	httpClient *http.Client
	inFlight   sync.Map // repoPath → *mirrorState
	logger     *slog.Logger
}

type mirrorState struct {
	sourceURL string
	done      chan struct{}
}

// GitTeeCacheOption configures a GitTeeCache.
type GitTeeCacheOption func(*GitTeeCache)

// WithGitTeeCacheLogger sets the logger for the GitTeeCache.
func WithGitTeeCacheLogger(logger *slog.Logger) GitTeeCacheOption {
	return func(c *GitTeeCache) {
		c.logger = logger
	}
}

// NewGitTeeCache creates a new GitTeeCache.
// httpClient is used for proxying git HTTP requests to the upstream source.
func NewGitTeeCache(httpClient *http.Client, opts ...GitTeeCacheOption) *GitTeeCache {
	c := &GitTeeCache{
		httpClient: httpClient,
		logger:     slog.Default(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// IsInFlight returns true if a mirror initialization is in progress for the given repo path.
func (c *GitTeeCache) IsInFlight(repoPath string) bool {
	v, ok := c.inFlight.Load(repoPath)
	if !ok {
		return false
	}
	state := v.(*mirrorState)
	select {
	case <-state.done:
		return false
	default:
		return true
	}
}

// StartInitMirror starts initializing a mirror repository in the background.
// If a mirror init is already in progress for the given path, this is a no-op.
func (c *GitTeeCache) StartInitMirror(repoPath, sourceURL string) {
	state := &mirrorState{
		sourceURL: sourceURL,
		done:      make(chan struct{}),
	}

	_, loaded := c.inFlight.LoadOrStore(repoPath, state)
	if loaded {
		return
	}

	c.logger.Info("Git tee cache: starting background mirror init", "repoPath", repoPath, "source", sourceURL)

	go func() {
		defer func() {
			close(state.done)
			c.inFlight.Delete(repoPath)
		}()

		_, err := InitMirror(context.Background(), repoPath, sourceURL)
		if err != nil {
			c.logger.Error("Git tee cache: failed to init mirror", "repoPath", repoPath, "error", err)
			_ = os.RemoveAll(repoPath)
			return
		}
		c.logger.Info("Git tee cache: mirror init complete", "repoPath", repoPath)
	}()
}

// ProxyInfoRefs proxies a git info/refs request to the upstream source.
func (c *GitTeeCache) ProxyInfoRefs(w http.ResponseWriter, r *http.Request, sourceURL string) error {
	gitURL := normalizeGitSourceURL(sourceURL)
	upstream := gitURL + "/info/refs?" + r.URL.RawQuery

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, upstream, nil)
	if err != nil {
		return err
	}
	if proto := r.Header.Get("Git-Protocol"); proto != "" {
		req.Header.Set("Git-Protocol", proto)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	copyResponseHeaders(w, resp)
	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, resp.Body)
	return err
}

// ProxyService proxies a git service request (e.g. git-upload-pack) to the upstream source.
func (c *GitTeeCache) ProxyService(w http.ResponseWriter, r *http.Request, sourceURL, service string) error {
	gitURL := normalizeGitSourceURL(sourceURL)
	upstream := gitURL + "/" + service

	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, upstream, r.Body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", r.Header.Get("Content-Type"))
	if proto := r.Header.Get("Git-Protocol"); proto != "" {
		req.Header.Set("Git-Protocol", proto)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	copyResponseHeaders(w, resp)
	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, resp.Body)
	return err
}

// normalizeGitSourceURL normalizes a source URL to have a .git suffix,
// matching the normalization done by InitMirror.
func normalizeGitSourceURL(sourceURL string) string {
	sourceURL = strings.TrimSuffix(sourceURL, "/")
	sourceURL = strings.TrimSuffix(sourceURL, ".git") + ".git"
	return sourceURL
}

// copyResponseHeaders copies response headers from the upstream response to the client writer.
func copyResponseHeaders(w http.ResponseWriter, resp *http.Response) {
	for k, v := range resp.Header {
		for _, vv := range v {
			w.Header().Add(k, vv)
		}
	}
}
