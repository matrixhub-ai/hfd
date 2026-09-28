package hf

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
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

// handleListUserRepos serves GET /api/settings/repositories and its /api/organizations/{namespace}
// form by aggregating the list callback over every repository type.
//
// huggingface.co restricts the user form to the token owner's own repositories (anonymous callers
// get 401) and the organization form to organizations the caller belongs to, reporting each
// repository's share of that namespace's storage quota. hfd has no ownership model — identities are
// just validator names and repositories are plain directories — so the user form lists every
// repository the callback returns and storagePercent is the share of the listed total.
func (h *Handler) handleListUserRepos(w http.ResponseWriter, r *http.Request) {
	namespace := mux.Vars(r)["namespace"]
	repoTypes := []string{"models", "datasets", "spaces", "kernels"}
	for _, repoType := range repoTypes {
		if !h.checkPermission(w, r, permission.OperationListRepos, repoType, permission.Context{Author: namespace}) {
			return
		}
	}

	var result []RepoStorageInfo
	var total int64
	for _, repoType := range repoTypes {
		items, _, err := h.listReposFunc(r.Context(), repoType, ListQuery{Author: namespace, Expand: []string{"lastModified", "usedStorage"}})
		if err != nil {
			respondCatalogError(w, err)
			return
		}
		for _, item := range items {
			info := RepoStorageInfo{
				RepoID:     item.RepoID,
				Type:       strings.TrimSuffix(repoType, "s"),
				UpdatedAt:  item.LastModified,
				Visibility: "public",
				Storage:    item.UsedStorage,
			}
			if item.Private {
				info.Visibility = "private"
			}
			if info.UpdatedAt == "" {
				// The SDK rejects a missing updatedAt; callbacks that skip lastModified get the zero time.
				info.UpdatedAt = time.Time{}.UTC().Format(repository.TimeFormat)
			}
			total += info.Storage
			result = append(result, info)
		}
	}

	if total > 0 {
		for i := range result {
			result[i].StoragePercent = float64(result[i].Storage) * 100 / float64(total)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].RepoID != result[j].RepoID {
			return result[i].RepoID < result[j].RepoID
		}
		return result[i].Type < result[j].Type
	})

	if result == nil {
		result = []RepoStorageInfo{}
	}

	responseJSON(w, result, http.StatusOK)
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
	// huggingface_hub repeats expand=; the hub also takes expand[]= and comma lists.
	for _, values := range [][]string{query["expand"], query["expand[]"]} {
		for _, value := range values {
			for _, name := range strings.Split(value, ",") {
				if name != "" {
					q.Expand = append(q.Expand, name)
				}
			}
		}
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
