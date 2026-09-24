package lfs

import (
	"bytes"
	"fmt"
	"io"

	"github.com/git-lfs/git-lfs/v3/lfs"
)

// MaxLFSPointerSize is git-lfs's blob size cutoff: only blobs below it are pointer candidates.
const MaxLFSPointerSize = 1024

// DecodePointer parses an LFS pointer from a reader; empty or oversized content is nil, nil, as in git-lfs.
func DecodePointer(r io.Reader) (*Pointer, error) {
	buf, err := io.ReadAll(io.LimitReader(r, MaxLFSPointerSize))
	if err != nil {
		return nil, err
	}
	if len(buf) == 0 || len(buf) >= MaxLFSPointerSize {
		return nil, nil
	}
	ptr, err := lfs.DecodePointer(bytes.NewReader(buf))
	if err != nil || ptr == nil {
		return nil, err
	}
	return &Pointer{
		pointer: ptr,
	}, nil
}

// Pointer represents a Git LFS pointer found in the repository.
type Pointer struct {
	pointer *lfs.Pointer
}

// OID returns the Git object ID (SHA-256 hash) of the LFS object that this pointer references.
func (p *Pointer) OID() string {
	return p.pointer.Oid
}

// PointerSize returns the size of the pointer itself in bytes, which is typically much smaller than the actual LFS object.
func (p *Pointer) Size() int64 {
	return p.pointer.Size
}

// OIDType returns the type of the OID, which is usually "sha256" for Git LFS pointers.
func (p *Pointer) OIDType() string {
	return p.pointer.OidType
}

// String returns a string representation of the LFS pointer.
func (p *Pointer) String() string {
	return fmt.Sprintf("<LFS Pointer oid=%s size=%d>", p.OID(), p.Size())
}
