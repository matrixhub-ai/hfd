package xet

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/gorilla/mux"
)

// handleGetObject handles GET /xet/cas/objects/{hash} - retrieves a CAS object by hash.
func (h *Handler) handleGetObject(w http.ResponseWriter, r *http.Request) {
	hash := mux.Vars(r)["hash"]

	content, stat, err := h.xetStorage.Get(hash)
	if err != nil {
		if os.IsNotExist(err) {
			responseJSON(w, fmt.Sprintf("object %s not found", hash), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Sprintf("failed to get object %s: %v", hash, err), http.StatusInternalServerError)
		return
	}
	defer func() {
		_ = content.Close()
	}()

	w.Header().Set("ETag", fmt.Sprintf("\"%s\"", hash))
	http.ServeContent(w, r, hash, stat.ModTime(), content)
}

// handlePutObject handles PUT /xet/cas/objects/{hash} - stores a CAS object.
func (h *Handler) handlePutObject(w http.ResponseWriter, r *http.Request) {
	hash := mux.Vars(r)["hash"]

	if err := h.xetStorage.Put(hash, r.Body, r.ContentLength); err != nil {
		responseJSON(w, fmt.Sprintf("failed to put object %s: %v", hash, err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// hasRequest represents the request body for the batch existence check.
type hasRequest struct {
	Hashes []string `json:"hashes"`
}

// hasResponse represents the response body for the batch existence check.
type hasResponse struct {
	Exists map[string]bool `json:"exists"`
}

// handleHas handles POST /xet/cas/objects/has - batch existence check.
func (h *Handler) handleHas(w http.ResponseWriter, r *http.Request) {
	var req hasRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responseJSON(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	resp := hasResponse{
		Exists: make(map[string]bool, len(req.Hashes)),
	}
	for _, hash := range req.Hashes {
		resp.Exists[hash] = h.xetStorage.Exists(hash)
	}

	responseJSON(w, resp, http.StatusOK)
}
