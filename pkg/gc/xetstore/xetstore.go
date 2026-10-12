// Package xetstore adapts a xet store to gc.LFSStore: OIDs are the sha256 index keys, stored times the owning shards' mtimes.
package xetstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	xetstorage "github.com/wzshiming/xet/storage"
)

// zeroSHA256 is xet's shared empty-file marker in the sha256 index, never an OID.
var zeroSHA256 = strings.Repeat("0", 64)

// Store is a gc.LFSStore over a xet store.
type Store struct {
	st xetstorage.Storage
}

// New adapts st to gc.LFSStore.
func New(st xetstorage.Storage) *Store {
	return &Store{st: st}
}

// Walk reports every OID in the sha256 index with its shard's mtime, the zero time when the shard is missing.
func (s *Store) Walk(ctx context.Context, fn func(oid string, storedAt time.Time) error) error {
	shards := map[string]time.Time{}
	err := s.st.WalkShards(ctx, func(shardHash string, _ int64, modTime time.Time) error {
		shards[shardHash] = modTime
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk shards: %w", err)
	}
	var fnErr error
	err = s.st.WalkSHA256Index(ctx, func(sha256Hex, shardHash string) error {
		if sha256Hex == zeroSHA256 {
			return nil
		}
		fnErr = fn(sha256Hex, shards[shardHash])
		return fnErr
	})
	if fnErr != nil {
		return fnErr
	}
	if err != nil {
		return fmt.Errorf("walk sha256 index: %w", err)
	}
	return nil
}

// Unlink deletes oid's sha256 index entry; the shard and its data stay until xet's sweep finds them unreferenced.
func (s *Store) Unlink(ctx context.Context, oid string) (bool, error) {
	if oid == zeroSHA256 {
		return false, errors.New("the all-zero oid is xet's shared empty-file marker")
	}
	return s.st.DeleteSHA256IndexEntry(ctx, oid)
}
