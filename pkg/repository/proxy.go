package repository

import (
	"context"
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

func (p *ProxyManager) OpenOrProxy(ctx context.Context, repoPath, repoName string) (*Repository, error) {
	// Create mirror from proxy source
	sourceURL := strings.TrimSuffix(p.proxyURL, "/") + "/" + repoName

	log.Printf("Proxy: initializing mirror for %q from %s", repoName, sourceURL)

	repo, err := InitMirror(ctx, repoPath, sourceURL)
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
