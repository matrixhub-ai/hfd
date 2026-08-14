package repository

import (
	"path/filepath"
	"slices"
	"strings"
)

// ResolvePath resolves the given URL path to a bare repository path.
func ResolvePath(urlPath string) string {
	urlPath = strings.TrimPrefix(urlPath, "/")
	if urlPath == "" {
		return ""
	}

	if !strings.HasSuffix(urlPath, ".git") {
		urlPath += ".git"
	}

	// Prevent path traversal outside the repositories filesystem.
	if slices.Contains(strings.Split(urlPath, "/"), "..") {
		return ""
	}

	return filepath.Join("/", urlPath)
}
