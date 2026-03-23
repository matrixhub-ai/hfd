package xet

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"

	"github.com/gorilla/mux"

	pkgxet "github.com/matrixhub-ai/hfd/pkg/xet"
)

// uploadXorbResponse matches the CAS OpenAPI UploadXorbResponse schema.
type uploadXorbResponse struct {
	WasInserted bool `json:"was_inserted"`
}

// uploadShardResponse matches the CAS OpenAPI UploadShardResponse schema.
type uploadShardResponse struct {
	Result int `json:"result"`
}

// handleUploadXorb handles POST /v1/xorbs/{prefix}/{hash}
// Uploads a serialized Xorb to CAS storage.
func (h *Handler) handleUploadXorb(w http.ResponseWriter, r *http.Request) {
	if h.storage == nil {
		responseJSON(w, "xet storage not configured", http.StatusServiceUnavailable)
		return
	}

	vars := mux.Vars(r)
	prefix := vars["prefix"]
	hash := vars["hash"]

	// Validate prefix: only "default" is acceptable per CAS spec
	if prefix != "default" {
		responseJSON(w, fmt.Sprintf("invalid prefix %q, expected \"default\"", prefix), http.StatusBadRequest)
		return
	}

	inserted, err := h.storage.PutXorb(prefix, hash, r.Body, r.ContentLength)
	if err != nil {
		slog.WarnContext(r.Context(), "failed to store xorb", "prefix", prefix, "hash", hash, "error", err)
		responseJSON(w, fmt.Sprintf("failed to store xorb: %v", err), http.StatusInternalServerError)
		return
	}

	responseJSON(w, uploadXorbResponse{WasInserted: inserted}, http.StatusOK)
}

// handleUploadShard handles POST /v1/shards
// Uploads a Shard to CAS storage. The shard contains file reconstruction metadata.
func (h *Handler) handleUploadShard(w http.ResponseWriter, r *http.Request) {
	if h.storage == nil {
		responseJSON(w, "xet storage not configured", http.StatusServiceUnavailable)
		return
	}

	// Read the entire shard body to compute its key
	data, err := io.ReadAll(r.Body)
	if err != nil {
		responseJSON(w, fmt.Sprintf("failed to read shard body: %v", err), http.StatusBadRequest)
		return
	}

	// Parse shard to validate format and extract file entries for indexing
	entries, err := pkgxet.ParseShard(bytes.NewReader(data))
	if err != nil {
		slog.WarnContext(r.Context(), "invalid shard format", "error", err)
		responseJSON(w, fmt.Sprintf("invalid shard: %v", err), http.StatusBadRequest)
		return
	}

	// Use SHA-256 of the shard content as the storage key
	key := pkgxet.HashShardContent(data)

	result, err := h.storage.PutShard(key, bytes.NewReader(data))
	if err != nil {
		slog.WarnContext(r.Context(), "failed to store shard", "key", key, "error", err)
		responseJSON(w, fmt.Sprintf("failed to store shard: %v", err), http.StatusInternalServerError)
		return
	}

	if result == 1 {
		slog.InfoContext(r.Context(), "shard stored", "key", key, "files", len(entries))
	}

	responseJSON(w, uploadShardResponse{Result: result}, http.StatusOK)
}

// handleGetChunk handles GET /v1/chunks/{prefix}/{hash}
// Checks if a chunk exists in the CAS for deduplication purposes.
func (h *Handler) handleGetChunk(w http.ResponseWriter, r *http.Request) {
	if h.storage == nil {
		responseJSON(w, "xet storage not configured", http.StatusServiceUnavailable)
		return
	}

	vars := mux.Vars(r)
	prefix := vars["prefix"]
	hash := vars["hash"]

	// Validate prefix: only "default-merkledb" is acceptable for deduplication
	if prefix != "default-merkledb" {
		responseJSON(w, fmt.Sprintf("invalid prefix %q, expected \"default-merkledb\"", prefix), http.StatusBadRequest)
		return
	}

	// Check if the chunk is tracked. For now, always return 404 since we don't
	// implement global deduplication. The client will proceed to upload.
	_ = hash
	w.WriteHeader(http.StatusNotFound)
}

// indexRange matches the CAS OpenAPI IndexRange schema.
type indexRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// byteRange matches the CAS OpenAPI ByteRange schema.
type byteRange struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

// casReconstructionTerm matches the CAS OpenAPI CASReconstructionTerm schema.
type casReconstructionTerm struct {
	Hash           string     `json:"hash"`
	UnpackedLength uint64     `json:"unpacked_length"`
	Range          indexRange `json:"range"`
}

// casReconstructionFetchInfo matches the CAS OpenAPI CASReconstructionFetchInfo schema.
type casReconstructionFetchInfo struct {
	URL      string     `json:"url"`
	URLRange byteRange  `json:"url_range"`
	Range    indexRange `json:"range"`
}

// queryReconstructionResponse matches the CAS OpenAPI QueryReconstructionResponse (V1).
type queryReconstructionResponse struct {
	OffsetIntoFirstRange int                                       `json:"offset_into_first_range"`
	Terms                []casReconstructionTerm                   `json:"terms"`
	FetchInfo            map[string][]casReconstructionFetchInfo   `json:"fetch_info"`
}

// handleGetReconstructionV1 handles GET /v1/reconstructions/{file_id}
// Returns reconstruction information describing how to download and reassemble a file.
func (h *Handler) handleGetReconstructionV1(w http.ResponseWriter, r *http.Request) {
	if h.storage == nil {
		responseJSON(w, "xet storage not configured", http.StatusServiceUnavailable)
		return
	}

	vars := mux.Vars(r)
	fileID := vars["file_id"]

	terms, fetchInfo, err := h.buildReconstruction(r, fileID)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		slog.WarnContext(r.Context(), "reconstruction failed", "file_id", fileID, "error", err)
		responseJSON(w, fmt.Sprintf("reconstruction error: %v", err), http.StatusInternalServerError)
		return
	}

	if len(terms) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	resp := queryReconstructionResponse{
		OffsetIntoFirstRange: 0,
		Terms:                terms,
		FetchInfo:            fetchInfo,
	}

	responseJSON(w, resp, http.StatusOK)
}

// xorbRangeDescriptor matches the CAS OpenAPI XorbRangeDescriptor schema.
type xorbRangeDescriptor struct {
	Chunks indexRange `json:"chunks"`
	Bytes  byteRange `json:"bytes"`
}

// xorbMultiRangeFetch matches the CAS OpenAPI XorbMultiRangeFetch schema.
type xorbMultiRangeFetch struct {
	URL    string                `json:"url"`
	Ranges []xorbRangeDescriptor `json:"ranges"`
}

// queryReconstructionResponseV2 matches the CAS OpenAPI QueryReconstructionResponseV2.
type queryReconstructionResponseV2 struct {
	OffsetIntoFirstRange int                                `json:"offset_into_first_range"`
	Terms                []casReconstructionTerm            `json:"terms"`
	Xorbs                map[string][]xorbMultiRangeFetch   `json:"xorbs"`
}

// handleGetReconstructionV2 handles GET /v2/reconstructions/{file_id}
// V2 reconstruction endpoint optimized for multi-range fetching.
func (h *Handler) handleGetReconstructionV2(w http.ResponseWriter, r *http.Request) {
	if h.storage == nil {
		responseJSON(w, "xet storage not configured", http.StatusServiceUnavailable)
		return
	}

	vars := mux.Vars(r)
	fileID := vars["file_id"]

	terms, fetchInfo, err := h.buildReconstruction(r, fileID)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		slog.WarnContext(r.Context(), "reconstruction V2 failed", "file_id", fileID, "error", err)
		responseJSON(w, fmt.Sprintf("reconstruction error: %v", err), http.StatusInternalServerError)
		return
	}

	if len(terms) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Convert V1 fetch info to V2 xorbs format
	xorbs := make(map[string][]xorbMultiRangeFetch)
	for hash, infos := range fetchInfo {
		var fetches []xorbMultiRangeFetch
		for _, info := range infos {
			fetches = append(fetches, xorbMultiRangeFetch{
				URL: info.URL,
				Ranges: []xorbRangeDescriptor{
					{
						Chunks: info.Range,
						Bytes:  info.URLRange,
					},
				},
			})
		}
		xorbs[hash] = fetches
	}

	resp := queryReconstructionResponseV2{
		OffsetIntoFirstRange: 0,
		Terms:                terms,
		Xorbs:                xorbs,
	}

	responseJSON(w, resp, http.StatusOK)
}

// buildReconstruction scans all stored shards for the given file_id and builds
// reconstruction terms and fetch info. It returns the terms, fetch info map,
// and any error encountered.
func (h *Handler) buildReconstruction(r *http.Request, fileID string) ([]casReconstructionTerm, map[string][]casReconstructionFetchInfo, error) {
	// Search all stored shards for entries matching this file_id.
	// This is a scan-based approach suitable for a local CAS server.
	entry, err := h.findFileInShards(fileID)
	if err != nil {
		return nil, nil, err
	}
	if entry == nil {
		return nil, nil, os.ErrNotExist
	}

	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if fwdProto := r.Header.Get("X-Forwarded-Proto"); fwdProto != "" {
		scheme = fwdProto
	}
	origin := fmt.Sprintf("%s://%s", scheme, r.Host)

	var terms []casReconstructionTerm
	fetchInfo := make(map[string][]casReconstructionFetchInfo)

	for _, chunk := range entry.Chunks {
		// Build the term for this chunk range
		term := casReconstructionTerm{
			Hash: chunk.XorbHash,
			Range: indexRange{
				Start: int(chunk.StartChunk),
				End:   int(chunk.EndChunk),
			},
		}

		// Try to determine the unpacked length by reading the xorb footer
		xorbReader, xorbStat, err := h.storage.GetXorb("default", chunk.XorbHash)
		if err != nil {
			// If xorb not found, set unpacked_length to 0; client can still try
			term.UnpackedLength = 0
		} else {
			boundaries, parseErr := pkgxet.ParseXorbFooter(xorbReader)
			_ = xorbReader.Close()

			if parseErr == nil && len(boundaries) > 0 {
				// Compute byte range from chunk boundaries
				startByte, endByte, rangeErr := pkgxet.XorbContentRange(boundaries, chunk.StartChunk, chunk.EndChunk)
				if rangeErr == nil {
					term.UnpackedLength = endByte - startByte

					url := fmt.Sprintf("%s/v1/xorbs/default/%s", origin, chunk.XorbHash)
					info := casReconstructionFetchInfo{
						URL: url,
						Range: indexRange{
							Start: int(chunk.StartChunk),
							End:   int(chunk.EndChunk),
						},
						URLRange: byteRange{
							Start: 0,
							End:   uint64(xorbStat.Size()) - 1,
						},
					}
					fetchInfo[chunk.XorbHash] = append(fetchInfo[chunk.XorbHash], info)
				}
			} else {
				// Cannot parse footer; provide URL for the whole xorb
				url := fmt.Sprintf("%s/v1/xorbs/default/%s", origin, chunk.XorbHash)
				info := casReconstructionFetchInfo{
					URL: url,
					Range: indexRange{
						Start: int(chunk.StartChunk),
						End:   int(chunk.EndChunk),
					},
					URLRange: byteRange{
						Start: 0,
						End:   uint64(xorbStat.Size()) - 1,
					},
				}
				fetchInfo[chunk.XorbHash] = append(fetchInfo[chunk.XorbHash], info)
			}
		}

		terms = append(terms, term)
	}

	return terms, fetchInfo, nil
}

// findFileInShards searches all stored shards for a file entry matching the given fileID.
func (h *Handler) findFileInShards(fileID string) (*pkgxet.ShardFileEntry, error) {
	// Walk the shard directory to find all stored shards
	shardDir := h.storage.ShardDir()

	var found *pkgxet.ShardFileEntry
	err := walkFiles(shardDir, func(path string) error {
		if found != nil {
			return nil // already found
		}

		f, err := os.Open(path)
		if err != nil {
			return nil // skip unreadable files
		}
		defer f.Close()

		entries, err := pkgxet.ParseShard(f)
		if err != nil {
			return nil // skip unparseable shards
		}

		for i := range entries {
			if entries[i].FileHash == fileID {
				found = &entries[i]
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return found, nil
}

// walkFiles walks a directory tree and calls fn for each regular file.
func walkFiles(dir string, fn func(path string) error) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		path := dir + "/" + entry.Name()
		if entry.IsDir() {
			if err := walkFiles(path, fn); err != nil {
				return err
			}
		} else {
			if err := fn(path); err != nil {
				return err
			}
		}
	}
	return nil
}

// requestOrigin returns the base URL scheme+host from the request.
func requestOrigin(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if fwdProto := r.Header.Get("X-Forwarded-Proto"); fwdProto != "" {
		scheme = fwdProto
	}
	return fmt.Sprintf("%s://%s", scheme, r.Host)
}

// handleGetXorb handles GET /v1/xorbs/{prefix}/{hash}
// This is an extension endpoint that allows direct xorb download for reconstruction.
func (h *Handler) handleGetXorb(w http.ResponseWriter, r *http.Request) {
	if h.storage == nil {
		responseJSON(w, "xet storage not configured", http.StatusServiceUnavailable)
		return
	}

	vars := mux.Vars(r)
	prefix := vars["prefix"]
	hash := vars["hash"]

	content, stat, err := h.storage.GetXorb(prefix, hash)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Sprintf("failed to get xorb: %v", err), http.StatusInternalServerError)
		return
	}
	defer content.Close()

	w.Header().Set("ETag", fmt.Sprintf("\"%s\"", hash))
	http.ServeContent(w, r, hash, stat.ModTime(), content)
}

// hashToFileID converts a raw hash string to a file_id for reconstruction lookups.
func hashToFileID(hash string) string {
	h := sha256.Sum256([]byte(hash))
	return hex.EncodeToString(h[:])
}
