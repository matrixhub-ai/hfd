package repository

import (
	"context"
	"errors"
	"log"
	"os"
	"strings"
	"sync"
)

// ProxyManager handles opening repositories with optional proxy/mirror creation
// for repositories that don't exist locally.
type ProxyManager struct {
	proxyURL string
	mutexes  sync.Map
}

// NewProxyManager creates a new ProxyManager.
// If proxyURL is empty, proxy functionality is disabled.
func NewProxyManager(proxyURL string) *ProxyManager {
	return &ProxyManager{
		proxyURL: proxyURL,
	}
}

// OpenOrProxy opens a repository. If it doesn't exist locally and proxy mode is
// enabled and the service is a read operation (git-upload-pack), it creates a
// mirror from the proxy source.
func (p *ProxyManager) OpenOrProxy(ctx context.Context, repoPath, repoName, service string) (*Repository, error) {
	repo, err := Open(repoPath)
	if err == nil {
		return repo, nil
	}
	if !errors.Is(err, ErrRepositoryNotExists) {
		return nil, err
	}

	// Only proxy for read operations
	if service != "git-upload-pack" {
		return nil, ErrRepositoryNotExists
	}

	if p == nil || p.proxyURL == "" {
		return nil, ErrRepositoryNotExists
	}

	return p.initProxyRepo(ctx, repoPath, repoName)
}

func (p *ProxyManager) initProxyRepo(ctx context.Context, repoPath, repoName string) (*Repository, error) {
	// Use per-repo mutex to prevent concurrent initialization
	v, _ := p.mutexes.LoadOrStore(repoName, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	// Double-check after acquiring lock
	repo, err := Open(repoPath)
	if err == nil {
		if err := repo.SyncMirror(ctx); err != nil {
			log.Printf("Proxy: failed to sync mirror for %q: %v", repoName, err)
			return nil, ErrRepositoryNotExists
		}
		return repo, nil
	}

	// Create mirror from proxy source
	sourceURL := strings.TrimSuffix(p.proxyURL, "/") + "/" + repoName

	log.Printf("Proxy: initializing mirror for %q from %s", repoName, sourceURL)

	repo, err = InitMrror(ctx, repoPath, sourceURL)
	if err != nil {
		log.Printf("Proxy: failed to initialize mirror for %q: %v", repoName, err)
		_ = os.RemoveAll(repoPath)
		return nil, ErrRepositoryNotExists
	}

	if err := repo.SyncMirror(ctx); err != nil {
		log.Printf("Proxy: failed to sync mirror for %q: %v", repoName, err)
		return nil, ErrRepositoryNotExists
	}

	log.Printf("Proxy: successfully mirrored %q", repoName)
	return repo, nil
}
