package mirror

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/wzshiming/xet"
	xetshard "github.com/wzshiming/xet/shard"
	xetstorage "github.com/wzshiming/xet/storage"
	xetupload "github.com/wzshiming/xet/upload"
)

// PutObject verifies the stream against its OID and ingests it into the xet
// storage as chunk-deduplicated xorbs and shards, after which it is servable
// by OID. The stream is spooled to disk first: the upload pipeline needs a
// seekable source, and a hash mismatch must be rejected before anything is
// stored.
func (m *Mirror) PutObject(ctx context.Context, oid string, r io.Reader, size int64) error {
	digest, ok := parseOID(oid)
	if !ok {
		return fmt.Errorf("invalid OID %q", oid)
	}

	spoolDir := filepath.Join(m.dataDir, "spool")
	if err := os.MkdirAll(spoolDir, 0755); err != nil {
		return fmt.Errorf("create spool dir: %w", err)
	}
	f, err := os.CreateTemp(spoolDir, "put-*")
	if err != nil {
		return fmt.Errorf("create spool file: %w", err)
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}()

	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(hash, f), r)
	if err != nil {
		return fmt.Errorf("spool object: %w", err)
	}
	if size >= 0 && written != size {
		return fmt.Errorf("content size does not match: expected %d bytes, got %d", size, written)
	}
	if !bytes.Equal(hash.Sum(nil), digest[:]) {
		return fmt.Errorf("content hash does not match OID %s", oid)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind spool file: %w", err)
	}

	opts := []xetupload.Option{xetupload.WithEnableSHA256(true)}
	if m.concurrency > 0 {
		opts = append(opts, xetupload.WithConcurrency(m.concurrency))
	}
	adapter := &localCAS{storage: m.xetStorage, namespace: xetNamespace}
	if _, err := xetupload.UploadFile(ctx, adapter, f, opts...); err != nil {
		return fmt.Errorf("ingest object %s: %w", oid, err)
	}
	return nil
}

// localCAS adapts the xet storage to the standard upload pipeline so
// PutObject writes xorbs and shards without an HTTP hop, following xet's own
// local CAS adapter.
type localCAS struct {
	storage   xetstorage.Storage
	namespace string
}

var _ xetupload.ClientAdapter = (*localCAS)(nil)

func (l *localCAS) HasXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error) {
	return l.storage.HasXorb(ctx, l.namespace, xorbHash)
}

func (l *localCAS) UploadXorb(ctx context.Context, xorbHash xet.XorbHash, reader io.ReadSeeker) (*xetupload.XorbUploadResponse, error) {
	wasInserted, err := l.storage.PutXorb(ctx, l.namespace, xorbHash, reader)
	if err != nil {
		return nil, err
	}
	return &xetupload.XorbUploadResponse{WasInserted: wasInserted}, nil
}

func (l *localCAS) UploadShard(ctx context.Context, shardObj *xetshard.Shard) (*xetupload.ShardUploadResponse, error) {
	wasInserted, err := l.storage.PutShard(ctx, shardObj)
	if err != nil {
		return nil, err
	}
	result := 0
	if wasInserted {
		result = 1
	}
	return &xetupload.ShardUploadResponse{Result: result}, nil
}

// Local shards are stored with raw chunk hashes, so keyed-shard candidates
// are unnecessary here.
func (l *localCAS) QueryDedupShards(ctx context.Context, chunkHashes []xet.ChunkHash, _ ...xet.ChunkHash) (map[xet.ChunkHash]*xetupload.DeduplicationResult, error) {
	results := make(map[xet.ChunkHash]*xetupload.DeduplicationResult, len(chunkHashes))
	for _, chunkHash := range chunkHashes {
		if _, ok := results[chunkHash]; ok {
			continue
		}
		shardObj, err := l.storage.GetShardByChunkHash(ctx, l.namespace, chunkHash)
		if err != nil || shardObj == nil {
			results[chunkHash] = &xetupload.DeduplicationResult{ChunkHash: chunkHash, IsNew: true}
			continue
		}
		// Register every chunk of the found shard, matching the remote
		// global-dedup behavior where one probe yields the whole shard.
		for _, casBlock := range shardObj.CASInfos {
			for i, casChunk := range casBlock.Chunks {
				if _, ok := results[casChunk.ChunkHash]; ok {
					continue
				}
				results[casChunk.ChunkHash] = &xetupload.DeduplicationResult{
					ChunkHash:  casChunk.ChunkHash,
					IsNew:      false,
					XorbHash:   casBlock.CASHash,
					ChunkIndex: uint32(i),
				}
			}
		}
		if _, ok := results[chunkHash]; !ok {
			results[chunkHash] = &xetupload.DeduplicationResult{ChunkHash: chunkHash, IsNew: true}
		}
	}
	return results, nil
}
