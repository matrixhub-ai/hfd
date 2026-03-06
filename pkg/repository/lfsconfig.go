package repository

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// EnsureLFSConfig ensures the repository has a .lfsconfig file with the given
// lfs.url value. If .lfsconfig already exists with the correct URL, this is a
// no-op. If the repository has no commits, this is a no-op.
// This is needed for git:// protocol clones where git-lfs cannot discover the
// LFS server URL automatically from the remote URL.
func (r *Repository) EnsureLFSConfig(ctx context.Context, lfsHref string) error {
	// Check if the repository has any commits
	_, err := r.repo.Head()
	if err != nil {
		return nil // No commits yet, nothing to do
	}

	// Check if .lfsconfig already exists with the correct URL
	blob, err := r.Blob("", ".lfsconfig")
	if err == nil {
		reader, err := blob.NewReader()
		if err == nil {
			content, _ := io.ReadAll(reader)
			_ = reader.Close()
			if strings.Contains(string(content), lfsHref) {
				return nil // Already configured correctly
			}
		}
	}

	// Create .lfsconfig content
	content := fmt.Sprintf("[lfs]\n\turl = %s\n", lfsHref)

	// Create commit with .lfsconfig
	_, err = r.CreateCommit(ctx, "", "Configure LFS URL", "hfd", "hfd@local",
		[]CommitOperation{{Type: CommitOperationAdd, Path: ".lfsconfig", Content: []byte(content)}}, "")
	return err
}
