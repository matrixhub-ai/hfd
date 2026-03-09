package repository

import (
	"context"
	"strings"
)

// ProxyFunc is a callback that returns the source URL for mirroring a repository
// that does not exist locally. Returning an empty string or an error disables
// proxy creation for that repository.
type ProxyFunc func(ctx context.Context, repoPath, repoName string) (string, error)

// NewProxyFunc creates a ProxyFunc that derives the source URL by appending
// repoName to baseURL.
func NewProxyFunc(baseURL string) ProxyFunc {
	return func(ctx context.Context, repoPath, repoName string) (string, error) {
		return strings.TrimSuffix(baseURL, "/") + "/" + repoName, nil
	}
}
