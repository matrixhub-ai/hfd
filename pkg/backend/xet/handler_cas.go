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

// handleGetReconstruction handles GET /v1/reconstructions/{file_id} - V1 file reconstruction.
func (h *Handler) handleGetReconstruction(w http.ResponseWriter, r *http.Request) {
	fileID := mux.Vars(r)["file_id"]
	baseURL := requestOrigin(r)

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
	encodedTerm := base64.RawURLEncoding.EncodeToString([]byte(fileID))
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

// handleGetReconstructionV2 handles GET /v2/reconstructions/{file_id} - V2 file reconstruction.
func (h *Handler) handleGetReconstructionV2(w http.ResponseWriter, r *http.Request) {
	fileID := mux.Vars(r)["file_id"]
	baseURL := requestOrigin(r)

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

	payload := fmt.Sprintf("%s:0-%d", fileID, size)
	encodedTerm := base64.RawURLEncoding.EncodeToString([]byte(payload))
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

// handleFetchTerm handles GET /v1/fetch_term?term=<base64> - fetch raw xorb data.
func (h *Handler) handleFetchTerm(w http.ResponseWriter, r *http.Request) {
	term := r.URL.Query().Get("term")
	if term == "" {
		responseJSON(w, "missing 'term' query parameter", http.StatusBadRequest)
		return
	}

	decoded, err := base64.RawURLEncoding.DecodeString(term)
	if err != nil {
		responseJSON(w, fmt.Sprintf("invalid term encoding: %v", err), http.StatusBadRequest)
		return
	}

	payload := string(decoded)

	// Parse hash (and optional byte ranges): "hash" or "hash:start-end,start-end"
	hash := payload
	if idx := strings.IndexByte(payload, ':'); idx >= 0 {
		hash = payload[:idx]
	}

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

	// Serve the full content; http.ServeContent handles Range headers.
	http.ServeContent(w, r, hash, stat.ModTime(), content)
}

// handleHeadFile handles HEAD /v1/files/{file_id} - get file size.
func (h *Handler) handleHeadFile(w http.ResponseWriter, r *http.Request) {
	fileID := mux.Vars(r)["file_id"]

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
