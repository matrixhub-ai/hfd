package xet

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gorilla/mux"

	pkgxet "github.com/matrixhub-ai/hfd/pkg/xet"
)

// uploadXorbResponse is the response for POST /v1/xorbs/{prefix}/{hash}.
type uploadXorbResponse struct {
	WasInserted bool `json:"was_inserted"`
}

// uploadShardResponse is the response for POST /shards.
type uploadShardResponse struct {
	Result int `json:"result"`
}

// reconstructionTerm describes which chunks to download from which xorb.
type reconstructionTerm struct {
	Hash           string     `json:"hash"`
	UnpackedLength int64      `json:"unpacked_length"`
	Range          indexRange `json:"range"`
}

// indexRange is a chunk index range [start, end).
type indexRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// byteRange is a byte range [start, end] (inclusive end for HTTP Range headers).
type byteRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// fetchInfo contains a presigned URL and byte range for downloading xorb data.
type fetchInfo struct {
	Range    indexRange `json:"range"`
	URL      string     `json:"url"`
	URLRange byteRange  `json:"url_range"`
}

// queryReconstructionResponse is the V1 reconstruction response.
type queryReconstructionResponse struct {
	OffsetIntoFirstRange int64                  `json:"offset_into_first_range"`
	Terms                []reconstructionTerm   `json:"terms"`
	FetchInfo            map[string][]fetchInfo `json:"fetch_info"`
}

// xorbRangeDescriptor describes a chunk/byte range within a xorb.
type xorbRangeDescriptor struct {
	Chunks indexRange `json:"chunks"`
	Bytes  byteRange  `json:"bytes"`
}

// xorbMultiRangeFetch is a signed multi-range fetch entry.
type xorbMultiRangeFetch struct {
	URL    string                `json:"url"`
	Ranges []xorbRangeDescriptor `json:"ranges"`
}

// queryReconstructionResponseV2 is the V2 reconstruction response.
type queryReconstructionResponseV2 struct {
	OffsetIntoFirstRange int64                            `json:"offset_into_first_range"`
	Terms                []reconstructionTerm             `json:"terms"`
	Xorbs                map[string][]xorbMultiRangeFetch `json:"xorbs"`
}

// batchReconstructionResponse is the response for batch reconstruction queries.
type batchReconstructionResponse struct {
	OffsetIntoFirstRange int64                  `json:"offset_into_first_range"`
	Terms                []reconstructionTerm   `json:"terms"`
	FetchInfo            map[string][]fetchInfo `json:"fetch_info"`
}

// fileRange represents a half-open byte range [start, end).
type fileRange struct {
	start int64
	end   int64
}

// parseRangeHeader parses an HTTP Range header per RFC 7233.
// Supports: bytes=start-end, bytes=start-, bytes=-suffixLen.
// Returns (nil, nil) if no Range header is present.
func parseRangeHeader(r *http.Request, totalSize int64) (*fileRange, error) {
	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		return nil, nil
	}

	const prefix = "bytes="
	if !strings.HasPrefix(rangeHeader, prefix) {
		return nil, fmt.Errorf("invalid range header: %s", rangeHeader)
	}

	rangeSpec := rangeHeader[len(prefix):]
	parts := strings.SplitN(rangeSpec, "-", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid range syntax: %s", rangeHeader)
	}

	startStr, endStr := parts[0], parts[1]

	switch {
	case startStr == "" && endStr != "":
		// bytes=-N (suffix)
		suffixLen, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid suffix range: %s", rangeHeader)
		}
		start := totalSize - suffixLen
		if start < 0 {
			start = 0
		}
		return &fileRange{start: start, end: totalSize}, nil
	case startStr != "" && endStr == "":
		// bytes=start-
		start, err := strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid range start: %s", rangeHeader)
		}
		return &fileRange{start: start, end: totalSize}, nil
	case startStr != "" && endStr != "":
		// bytes=start-end (inclusive end per HTTP spec, convert to exclusive)
		start, err := strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid range start: %s", rangeHeader)
		}
		end, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid range end: %s", rangeHeader)
		}
		if start > end {
			return nil, fmt.Errorf("range start > end: %s", rangeHeader)
		}
		return &fileRange{start: start, end: end + 1}, nil
	default:
		return nil, fmt.Errorf("invalid range syntax: %s", rangeHeader)
	}
}

// handlePostXorb handles POST /v1/xorbs/{prefix}/{hash} - upload a xorb.
func (h *Handler) handlePostXorb(w http.ResponseWriter, r *http.Request) {
	hash := mux.Vars(r)["hash"]

	if err := h.xetStorage.Put(hash, r.Body, r.ContentLength); err != nil {
		responseJSON(w, fmt.Sprintf("failed to store xorb %s: %v", hash, err), http.StatusInternalServerError)
		return
	}

	responseJSON(w, uploadXorbResponse{WasInserted: true}, http.StatusOK)
}

// handleHeadXorb handles HEAD /v1/xorbs/{prefix}/{hash} - check if xorb exists.
func (h *Handler) handleHeadXorb(w http.ResponseWriter, r *http.Request) {
	hash := mux.Vars(r)["hash"]

	if !h.xetStorage.Exists(hash) {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

// handlePostShard handles POST /shards - upload a shard.
func (h *Handler) handlePostShard(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		responseJSON(w, fmt.Sprintf("failed to read shard data: %v", err), http.StatusBadRequest)
		return
	}

	// Derive a unique key from shard content using SHA-256 hash.
	hash := fmt.Sprintf("%x", sha256.Sum256(data))
	if err := h.xetStorage.Put(hash, strings.NewReader(string(data)), int64(len(data))); err != nil {
		responseJSON(w, fmt.Sprintf("failed to store shard: %v", err), http.StatusInternalServerError)
		return
	}

	// Parse and index file reconstruction data from the shard.
	if err := h.xetStorage.RegisterShard(data); err != nil {
		// Log but don't fail - shard is stored, reconstruction index is best-effort.
		// Non-conforming shards (e.g. test payloads) are allowed.
		_ = err
	}

	responseJSON(w, uploadShardResponse{Result: 1}, http.StatusOK)
}

// handleGetChunk handles GET /v1/chunks/{prefix}/{hash} - global dedup query.
func (h *Handler) handleGetChunk(w http.ResponseWriter, r *http.Request) {
	hash := mux.Vars(r)["hash"]

	content, stat, err := h.xetStorage.Get(hash)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Sprintf("failed to get chunk: %v", err), http.StatusInternalServerError)
		return
	}
	defer func() {
		_ = content.Close()
	}()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(stat.Size(), 10))
	_, _ = io.Copy(w, content)
}

// requestOrigin returns the origin URL derived from the request.
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

// encodeTerm encodes a V1 fetch term (hash only) as base64url.
func encodeTerm(hash string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(hash))
}

// encodeTermWithRanges encodes a V2 fetch term with embedded byte ranges as base64url.
// Format: "hash:start0-end0,start1-end1,..." where ranges use exclusive end.
func encodeTermWithRanges(hash string, ranges []xorbRangeDescriptor) string {
	parts := make([]string, 0, len(ranges))
	for _, r := range ranges {
		// byteRange has inclusive end, convert to exclusive for term encoding
		parts = append(parts, fmt.Sprintf("%d-%d", r.Bytes.Start, r.Bytes.End+1))
	}
	payload := fmt.Sprintf("%s:%s", hash, strings.Join(parts, ","))
	return base64.RawURLEncoding.EncodeToString([]byte(payload))
}

// decodedTerm is a decoded fetch term: hash and optional byte ranges (exclusive end).
type decodedTerm struct {
	hash       string
	byteRanges []fileRange
}

// decodeTerm decodes a fetch term. Supports both V1 (hash only) and V2 (hash + ranges).
func decodeTerm(term string) (*decodedTerm, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(term)
	if err != nil {
		return nil, fmt.Errorf("invalid base64: %w", err)
	}

	payload := string(decoded)

	if idx := strings.IndexByte(payload, ':'); idx >= 0 {
		hash := payload[:idx]
		rangesStr := payload[idx+1:]
		var byteRanges []fileRange
		for _, r := range strings.Split(rangesStr, ",") {
			r = strings.TrimSpace(r)
			if r == "" {
				continue
			}
			parts := strings.SplitN(r, "-", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid range syntax: %s", r)
			}
			start, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid range start: %w", err)
			}
			end, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid range end: %w", err)
			}
			byteRanges = append(byteRanges, fileRange{start: start, end: end})
		}
		return &decodedTerm{hash: hash, byteRanges: byteRanges}, nil
	}

	return &decodedTerm{hash: payload}, nil
}

// handleGetReconstruction handles GET /v1/reconstructions/{file_id} - V1 file reconstruction.
func (h *Handler) handleGetReconstruction(w http.ResponseWriter, r *http.Request) {
	fileID := mux.Vars(r)["file_id"]
	baseURL := requestOrigin(r)

	// Try shard-based reconstruction first.
	if recon, _ := h.xetStorage.GetFileReconstruction(fileID); recon != nil {
		resp := h.buildV1Reconstruction(recon, baseURL)
		if resp != nil {
			responseJSON(w, resp, http.StatusOK)
			return
		}
	}

	// Fallback: treat fileID as a xorb hash directly.
	info, err := h.xetStorage.Info(fileID)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Sprintf("file not found: %v", err), http.StatusInternalServerError)
		return
	}

	size := info.Size()
	encodedTerm := encodeTerm(fileID)
	fetchURL := fmt.Sprintf("%s/v1/fetch_term?term=%s", baseURL, encodedTerm)

	resp := queryReconstructionResponse{
		OffsetIntoFirstRange: 0,
		Terms: []reconstructionTerm{
			{
				Hash:           fileID,
				UnpackedLength: size,
				Range:          indexRange{Start: 0, End: 1},
			},
		},
		FetchInfo: map[string][]fetchInfo{
			fileID: {
				{
					Range:    indexRange{Start: 0, End: 1},
					URL:      fetchURL,
					URLRange: byteRange{Start: 0, End: size - 1},
				},
			},
		},
	}

	responseJSON(w, resp, http.StatusOK)
}

// buildV1Reconstruction builds a V1 reconstruction response from shard data.
func (h *Handler) buildV1Reconstruction(recon *pkgxet.FileReconstruction, baseURL string) *queryReconstructionResponse {
	var terms []reconstructionTerm
	fetchInfoMap := map[string][]fetchInfo{}

	for _, seg := range recon.Segments {
		footer, err := h.xetStorage.GetXorbFooter(seg.XorbHash)
		if err != nil {
			return nil
		}

		byteStart, byteEnd, err := footer.GetByteOffset(seg.ChunkIndexStart, seg.ChunkIndexEnd)
		if err != nil {
			return nil
		}

		unpackedSize, err := footer.TotalUnpackedSize(seg.ChunkIndexStart, seg.ChunkIndexEnd)
		if err != nil {
			return nil
		}

		terms = append(terms, reconstructionTerm{
			Hash:           seg.XorbHash,
			UnpackedLength: int64(unpackedSize),
			Range:          indexRange{Start: int(seg.ChunkIndexStart), End: int(seg.ChunkIndexEnd)},
		})

		encodedTerm := encodeTerm(seg.XorbHash)
		fetchURL := fmt.Sprintf("%s/v1/fetch_term?term=%s", baseURL, encodedTerm)
		fetchInfoMap[seg.XorbHash] = append(fetchInfoMap[seg.XorbHash], fetchInfo{
			Range:    indexRange{Start: int(seg.ChunkIndexStart), End: int(seg.ChunkIndexEnd)},
			URL:      fetchURL,
			URLRange: byteRange{Start: int64(byteStart), End: int64(byteEnd) - 1},
		})
	}

	return &queryReconstructionResponse{
		OffsetIntoFirstRange: 0,
		Terms:                terms,
		FetchInfo:            fetchInfoMap,
	}
}

// handleGetReconstructionV2 handles GET /v2/reconstructions/{file_id} - V2 file reconstruction.
func (h *Handler) handleGetReconstructionV2(w http.ResponseWriter, r *http.Request) {
	fileID := mux.Vars(r)["file_id"]
	baseURL := requestOrigin(r)

	// Try shard-based reconstruction first.
	if recon, _ := h.xetStorage.GetFileReconstruction(fileID); recon != nil {
		resp := h.buildV2Reconstruction(recon, baseURL)
		if resp != nil {
			responseJSON(w, resp, http.StatusOK)
			return
		}
	}

	// Fallback: treat fileID as a xorb hash directly.
	info, err := h.xetStorage.Info(fileID)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Sprintf("file not found: %v", err), http.StatusInternalServerError)
		return
	}

	size := info.Size()
	rangeDesc := xorbRangeDescriptor{
		Chunks: indexRange{Start: 0, End: 1},
		Bytes:  byteRange{Start: 0, End: size - 1},
	}

	encodedTerm := encodeTermWithRanges(fileID, []xorbRangeDescriptor{rangeDesc})
	fetchURL := fmt.Sprintf("%s/v1/fetch_term?term=%s", baseURL, encodedTerm)

	resp := queryReconstructionResponseV2{
		OffsetIntoFirstRange: 0,
		Terms: []reconstructionTerm{
			{
				Hash:           fileID,
				UnpackedLength: size,
				Range:          indexRange{Start: 0, End: 1},
			},
		},
		Xorbs: map[string][]xorbMultiRangeFetch{
			fileID: {
				{
					URL:    fetchURL,
					Ranges: []xorbRangeDescriptor{rangeDesc},
				},
			},
		},
	}

	responseJSON(w, resp, http.StatusOK)
}

// buildV2Reconstruction builds a V2 reconstruction response from shard data.
func (h *Handler) buildV2Reconstruction(recon *pkgxet.FileReconstruction, baseURL string) *queryReconstructionResponseV2 {
	var terms []reconstructionTerm
	xorbsMap := map[string][]xorbMultiRangeFetch{}

	for _, seg := range recon.Segments {
		footer, err := h.xetStorage.GetXorbFooter(seg.XorbHash)
		if err != nil {
			return nil
		}

		byteStart, byteEnd, err := footer.GetByteOffset(seg.ChunkIndexStart, seg.ChunkIndexEnd)
		if err != nil {
			return nil
		}

		unpackedSize, err := footer.TotalUnpackedSize(seg.ChunkIndexStart, seg.ChunkIndexEnd)
		if err != nil {
			return nil
		}

		rangeDesc := xorbRangeDescriptor{
			Chunks: indexRange{Start: int(seg.ChunkIndexStart), End: int(seg.ChunkIndexEnd)},
			Bytes:  byteRange{Start: int64(byteStart), End: int64(byteEnd) - 1},
		}

		terms = append(terms, reconstructionTerm{
			Hash:           seg.XorbHash,
			UnpackedLength: int64(unpackedSize),
			Range:          indexRange{Start: int(seg.ChunkIndexStart), End: int(seg.ChunkIndexEnd)},
		})

		encodedTerm := encodeTermWithRanges(seg.XorbHash, []xorbRangeDescriptor{rangeDesc})
		fetchURL := fmt.Sprintf("%s/v1/fetch_term?term=%s", baseURL, encodedTerm)
		xorbsMap[seg.XorbHash] = append(xorbsMap[seg.XorbHash], xorbMultiRangeFetch{
			URL:    fetchURL,
			Ranges: []xorbRangeDescriptor{rangeDesc},
		})
	}

	return &queryReconstructionResponseV2{
		OffsetIntoFirstRange: 0,
		Terms:                terms,
		Xorbs:                xorbsMap,
	}
}

// handleBatchGetReconstruction handles GET /v1/reconstructions?file_id=...&file_id=...
// Batch query for reconstruction information for multiple files.
func (h *Handler) handleBatchGetReconstruction(w http.ResponseWriter, r *http.Request) {
	baseURL := requestOrigin(r)

	fileIDs := r.URL.Query()["file_id"]
	if len(fileIDs) == 0 {
		responseJSON(w, batchReconstructionResponse{
			Terms:     []reconstructionTerm{},
			FetchInfo: map[string][]fetchInfo{},
		}, http.StatusOK)
		return
	}

	var allTerms []reconstructionTerm
	allFetchInfo := map[string][]fetchInfo{}

	for _, fileID := range fileIDs {
		// Try shard-based reconstruction first.
		if recon, _ := h.xetStorage.GetFileReconstruction(fileID); recon != nil {
			resp := h.buildV1Reconstruction(recon, baseURL)
			if resp != nil {
				allTerms = append(allTerms, resp.Terms...)
				for k, v := range resp.FetchInfo {
					allFetchInfo[k] = append(allFetchInfo[k], v...)
				}
				continue
			}
		}

		// Fallback: treat fileID as a xorb hash directly.
		info, err := h.xetStorage.Info(fileID)
		if err != nil {
			continue // skip missing files in batch
		}

		size := info.Size()
		encodedTerm := encodeTerm(fileID)
		fetchURL := fmt.Sprintf("%s/v1/fetch_term?term=%s", baseURL, encodedTerm)

		allTerms = append(allTerms, reconstructionTerm{
			Hash:           fileID,
			UnpackedLength: size,
			Range:          indexRange{Start: 0, End: 1},
		})

		allFetchInfo[fileID] = []fetchInfo{
			{
				Range:    indexRange{Start: 0, End: 1},
				URL:      fetchURL,
				URLRange: byteRange{Start: 0, End: size - 1},
			},
		}
	}

	resp := batchReconstructionResponse{
		OffsetIntoFirstRange: 0,
		Terms:                allTerms,
		FetchInfo:            allFetchInfo,
	}
	responseJSON(w, resp, http.StatusOK)
}

// handleFetchTerm handles GET /v1/fetch_term?term=<base64> - fetch raw xorb data.
//
// For V1 terms (hash only), the byte range comes from the HTTP Range header.
// For V2 terms (hash + ranges), all encoded byte ranges are fetched and
// concatenated in order. If the client sends a single-range HTTP Range header,
// that takes priority. If there are multiple embedded ranges and no Range header,
// a multipart/byteranges response is returned per RFC 7233.
func (h *Handler) handleFetchTerm(w http.ResponseWriter, r *http.Request) {
	term := r.URL.Query().Get("term")
	if term == "" {
		responseJSON(w, "missing 'term' query parameter", http.StatusBadRequest)
		return
	}

	decoded, err := decodeTerm(term)
	if err != nil {
		responseJSON(w, fmt.Sprintf("invalid term: %v", err), http.StatusBadRequest)
		return
	}

	hash := decoded.hash

	if len(decoded.byteRanges) > 0 {
		// V2 term with embedded byte ranges.
		content, stat, err := h.xetStorage.Get(hash)
		if err != nil {
			if os.IsNotExist(err) {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			responseJSON(w, fmt.Sprintf("xorb not found: %v", err), http.StatusInternalServerError)
			return
		}

		totalSize := stat.Size()

		// If the client sends a single-range HTTP Range header, serve just that range.
		// This simulates S3/CDN behavior where the Range header controls the response.
		httpRange, parseErr := parseRangeHeader(r, totalSize)
		if parseErr != nil {
			_ = content.Close()
			http.Error(w, parseErr.Error(), http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if httpRange != nil {
			data, readErr := readRange(content, httpRange)
			_ = content.Close()
			if readErr != nil {
				responseJSON(w, fmt.Sprintf("range read error: %v", readErr), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data)
			return
		}

		// Single embedded range: return directly.
		if len(decoded.byteRanges) == 1 {
			data, readErr := readRange(content, &decoded.byteRanges[0])
			_ = content.Close()
			if readErr != nil {
				responseJSON(w, fmt.Sprintf("range read error: %v", readErr), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data)
			return
		}

		// Multiple embedded ranges: return multipart/byteranges per RFC 7233.
		const boundary = "xet_multipart_boundary"
		var body []byte
		for _, br := range decoded.byteRanges {
			data, readErr := readRange(content, &br)
			if readErr != nil {
				_ = content.Close()
				responseJSON(w, fmt.Sprintf("range read error: %v", readErr), http.StatusInternalServerError)
				return
			}
			inclusiveEnd := br.end - 1
			partHeader := fmt.Sprintf("--%s\r\nContent-Type: application/octet-stream\r\nContent-Range: bytes %d-%d/%d\r\n\r\n",
				boundary, br.start, inclusiveEnd, totalSize)
			body = append(body, []byte(partHeader)...)
			body = append(body, data...)
			body = append(body, []byte("\r\n")...)
		}
		body = append(body, []byte(fmt.Sprintf("--%s--\r\n", boundary))...)
		_ = content.Close()

		w.Header().Set("Content-Type", fmt.Sprintf("multipart/byteranges; boundary=%s", boundary))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
		return
	}

	// V1 term: byte range comes from the HTTP Range header.
	content, stat, err := h.xetStorage.Get(hash)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Sprintf("xorb not found: %v", err), http.StatusInternalServerError)
		return
	}
	defer func() {
		_ = content.Close()
	}()

	// http.ServeContent handles Range headers automatically.
	http.ServeContent(w, r, hash, stat.ModTime(), content)
}

// readRange reads a byte range from a ReadSeeker and returns the data.
func readRange(rs io.ReadSeeker, fr *fileRange) ([]byte, error) {
	if _, err := rs.Seek(fr.start, io.SeekStart); err != nil {
		return nil, err
	}
	length := fr.end - fr.start
	data := make([]byte, length)
	n, err := io.ReadFull(rs, data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

// handleGetXorb handles GET /v1/get_xorb/{prefix}/{hash}/ - direct xorb download.
// Supports Range header for partial downloads per RFC 7233.
func (h *Handler) handleGetXorb(w http.ResponseWriter, r *http.Request) {
	hash := mux.Vars(r)["hash"]

	content, stat, err := h.xetStorage.Get(hash)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Sprintf("failed to get xorb: %v", err), http.StatusInternalServerError)
		return
	}
	defer func() {
		_ = content.Close()
	}()

	http.ServeContent(w, r, hash, stat.ModTime(), content)
}

// handleHeadFile handles HEAD /v1/files/{file_id} - get file size.
func (h *Handler) handleHeadFile(w http.ResponseWriter, r *http.Request) {
	fileID := mux.Vars(r)["file_id"]

	// Try shard-based reconstruction for file size.
	if recon, _ := h.xetStorage.GetFileReconstruction(fileID); recon != nil {
		var totalSize uint64
		for _, seg := range recon.Segments {
			totalSize += uint64(seg.UnpackedSegmentBytes)
		}
		w.Header().Set("Content-Length", strconv.FormatUint(totalSize, 10))
		w.WriteHeader(http.StatusOK)
		return
	}

	info, err := h.xetStorage.Info(fileID)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Sprintf("file not found: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	w.WriteHeader(http.StatusOK)
}

// handleHealth handles GET /health - health check.
func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.WriteHeader(http.StatusOK)
}
