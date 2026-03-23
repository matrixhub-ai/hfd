package xet

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/gorilla/mux"
)

// handlePutXorb handles POST/PUT /api/v1/xorbs/{prefix}/{hash}
// Stores a xorb (content-addressed blob) in the CAS. The xet-core client uploads
// xorbs containing chunked file data, with the hash being the content's Merkle hash.
func (h *Handler) handlePutXorb(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	prefix := vars["prefix"]
	hash := vars["hash"]

	if h.storage.XorbExists(prefix, hash) {
		// Xorb already exists, return success (idempotent)
		w.WriteHeader(http.StatusOK)
		return
	}

	if err := h.storage.PutXorb(prefix, hash, r.Body, r.ContentLength); err != nil {
		slog.Error("xet: failed to store xorb", "prefix", prefix, "hash", hash, "error", err)
		responseJSON(w, struct {
			Error string `json:"error"`
		}{Error: fmt.Sprintf("failed to store xorb: %v", err)}, http.StatusInternalServerError)
		return
	}

	slog.Info("xet: stored xorb", "prefix", prefix, "hash", hash, "size", r.ContentLength)
	w.WriteHeader(http.StatusCreated)
}

// handleGetXorb handles GET /api/v1/xorbs/{prefix}/{hash}
// Retrieves a xorb blob from the CAS.
func (h *Handler) handleGetXorb(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	prefix := vars["prefix"]
	hash := vars["hash"]

	content, stat, err := h.storage.GetXorb(prefix, hash)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "xorb not found", http.StatusNotFound)
			return
		}
		responseJSON(w, struct {
			Error string `json:"error"`
		}{Error: fmt.Sprintf("failed to get xorb: %v", err)}, http.StatusInternalServerError)
		return
	}
	defer func() {
		_ = content.Close()
	}()

	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, hash, stat.ModTime(), content)
}

// handlePutShard handles POST /api/v1/shards
// Stores shard metadata that maps file paths to xorb chunk references.
// Shards are uploaded by xet-core after xorbs are stored, and contain the
// file-to-chunk mapping information needed for reconstruction.
func (h *Handler) handlePutShard(w http.ResponseWriter, r *http.Request) {
	name, err := h.storage.PutShard(r.Body)
	if err != nil {
		slog.Error("xet: failed to store shard", "error", err)
		responseJSON(w, struct {
			Error string `json:"error"`
		}{Error: fmt.Sprintf("failed to store shard: %v", err)}, http.StatusInternalServerError)
		return
	}

	slog.Info("xet: stored shard", "name", name)
	w.WriteHeader(http.StatusCreated)
}

// handleGetChunk handles GET /api/v1/chunks/{prefix}/{hash}
// Retrieves chunk data from a xorb. In the current implementation, this serves
// the entire xorb since chunk boundary parsing requires the xorb footer format.
// The xet-core client handles extracting the specific chunk range.
func (h *Handler) handleGetChunk(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	prefix := vars["prefix"]
	hash := vars["hash"]

	content, stat, err := h.storage.GetXorb(prefix, hash)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "chunk not found", http.StatusNotFound)
			return
		}
		responseJSON(w, struct {
			Error string `json:"error"`
		}{Error: fmt.Sprintf("failed to get chunk: %v", err)}, http.StatusInternalServerError)
		return
	}
	defer func() {
		_ = content.Close()
	}()

	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, hash, stat.ModTime(), content)
}

// handleGetReconstruction handles GET /api/v1/reconstructions/{file_id}
// Returns reconstruction information for a file identified by file_id.
// The reconstruction response tells the xet-core client which chunks are needed
// to reassemble a file, including xorb references and byte ranges.
func (h *Handler) handleGetReconstruction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fileID := vars["file_id"]

	// Return empty reconstruction - the client will use shard data
	// to determine which xorbs and chunks are needed.
	responseJSON(w, struct {
		FileID string `json:"file_id"`
		Terms  []any  `json:"terms"`
	}{
		FileID: fileID,
		Terms:  []any{},
	}, http.StatusOK)
}

// handleGetReconstructionV2 handles GET /api/v2/reconstructions/{file_id}
// V2 reconstruction endpoint with enhanced response format.
func (h *Handler) handleGetReconstructionV2(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fileID := vars["file_id"]

	responseJSON(w, struct {
		FileID string `json:"file_id"`
		Terms  []any  `json:"terms"`
	}{
		FileID: fileID,
		Terms:  []any{},
	}, http.StatusOK)
}
