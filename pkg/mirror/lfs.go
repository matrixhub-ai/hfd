package mirror

// Get attempts to retrieve the LFS object with the given OID from the mirror's tee cache.
func (m *Mirror) Get(oid string) *Blob {
	return m.lfsTeeCache.Get(oid)
}
