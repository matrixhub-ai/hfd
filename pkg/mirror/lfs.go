package mirror

import (
	"github.com/matrixhub-ai/hfd/pkg/lfs"
)

// Get attempts to retrieve the LFS object with the given OID from the mirror's tee cache.
func (m *Mirror) Get(oid string) *lfs.Blob {
	return m.lfsTeeCache.Get(oid)
}
