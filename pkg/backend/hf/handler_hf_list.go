package hf

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/matrixhub-ai/hfd/pkg/permission"
)

// handleList is the unified handler for listing repositories of different types (models, datasets, spaces).
func (h *Handler) handleList(w http.ResponseWriter, r *http.Request) {
	repoType := mux.Vars(r)["repoType"]
	q := parseListQuery(r)
	if !h.checkPermission(w, r, permission.OperationListRepos, repoType, permission.Context{Author: q.Author}) {
		return
	}

	items, more, err := h.listReposFunc(r.Context(), repoType, q)
	if err != nil {
		respondCatalogError(w, err)
		return
	}

	if more {
		nextURL := buildNextURL(r, encodeCursorOffset(q.Offset+len(items)))
		w.Header().Set("Link", fmt.Sprintf("<%s>; rel=\"next\"", nextURL))
	}

	if items == nil {
		items = []RepoListItem{}
	}

	responseJSON(w, items, http.StatusOK)
}

// parseListQuery extracts and parses the list query parameters from the request.
func parseListQuery(r *http.Request) ListQuery {
	query := r.URL.Query()
	q := ListQuery{
		Search:     query.Get("search"),
		Author:     query.Get("author"),
		FilterTags: query["filter"],
		SortField:  query.Get("sort"),
	}
	if v, err := strconv.Atoi(query.Get("limit")); err == nil && v > 0 {
		q.Limit = v
	}
	if cursor := query.Get("cursor"); cursor != "" {
		q.Offset = decodeCursorOffset(cursor)
	}
	return q
}

// cursorPayload is the JSON structure encoded inside the base64 cursor.
type cursorPayload struct {
	Offset int `json:"offset"`
}

// encodeCursorOffset encodes an offset into a base64 cursor string.
func encodeCursorOffset(offset int) string {
	data, _ := json.Marshal(cursorPayload{Offset: offset})
	return base64.URLEncoding.EncodeToString(data)
}

// decodeCursorOffset decodes a base64 cursor string into an offset.
// Returns 0 if the cursor is invalid.
func decodeCursorOffset(cursor string) int {
	data, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		// Try standard encoding as fallback
		data, err = base64.StdEncoding.DecodeString(cursor)
		if err != nil {
			return 0
		}
	}
	var payload cursorPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return 0
	}
	if payload.Offset < 0 {
		return 0
	}
	return payload.Offset
}

// buildNextURL constructs the full URL for the next page by replacing the
// cursor parameter while preserving all other query parameters.
func buildNextURL(r *http.Request, cursor string) string {
	origin := requestOrigin(r)
	q := make(url.Values)
	for k, vs := range r.URL.Query() {
		if k == "cursor" {
			continue
		}
		q[k] = vs
	}
	q.Set("cursor", cursor)
	return origin + r.URL.Path + "?" + q.Encode()
}
