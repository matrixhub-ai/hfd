package repository

import (
	"context"
	"os"
	"strings"
)

// ProxyManager handles opening repositories with optional proxy/mirror creation
// for repositories that don't exist locally.
type ProxyManager struct {
	proxyURL string
}

// NewProxyManager creates a new ProxyManager.
// If proxyURL is empty, proxy functionality is disabled.
func NewProxyManager(proxyURL string) *ProxyManager {
	p := &ProxyManager{
		proxyURL: proxyURL,
	}
	return p
}

func (p *ProxyManager) OpenOrProxy(ctx context.Context, repoPath, repoName string) (*Repository, error) {
	// Create mirror from proxy source
	sourceURL := strings.TrimSuffix(p.proxyURL, "/") + "/" + repoName

	repo, err := InitMirror(ctx, repoPath, sourceURL)
	if err != nil {
		_ = os.RemoveAll(repoPath)
		return nil, ErrRepositoryNotExists
	}

	if err := repo.SyncMirror(ctx); err != nil {
		_ = os.RemoveAll(repoPath)
		return nil, ErrRepositoryNotExists
	}

	return repo, nil
}
