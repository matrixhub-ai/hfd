package mirror

import (
	"context"

	"github.com/matrixhub-ai/hfd/pkg/lfs"
)

// Get attempts to retrieve the LFS object with the given OID from the mirror's tee cache.
func (m *Mirror) Get(oid string) *lfs.Blob {
	return m.lfsTeeCache.Get(oid)
}

// StartLFSFetch attempts to fetch the given LFS objects from the mirror's upstream source.
func (m *Mirror) StartLFSFetch(ctx context.Context, repoName string, objects []lfs.LFSObject) (string, bool, error) {
	sourceURL, isMirror, err := m.mirrorSourceFunc(ctx, repoName)
	if err != nil {
		return "", false, err
	}
	if !isMirror || sourceURL == "" {
		return "", false, nil
	}

	if err := m.lfsTeeCache.StartFetch(ctx, sourceURL, objects); err != nil {
		return sourceURL, false, err
	}

	return sourceURL, true, nil
}
