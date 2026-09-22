package hf

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	iofs "io/fs"
	"net/http"
	"net/url"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// handleList is the unified handler for listing repositories of different types (models, datasets, spaces).
func (h *Handler) handleList(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repoType := vars["repoType"]
	filter, err := parseRepoListFilter(r, repoType)
	if err != nil {
		responseJSON(w, err, http.StatusBadRequest)
		return
	}
	if !h.checkPermission(w, r, permission.OperationListRepos, repoType, permission.Context{Author: filter.author}) {
		return
	}
	h.handleListRepos(w, r, repoType, filter)
}

// Per type: the Hub's default projection, what full=true adds and every expand[] option it accepts.
var listFields = map[string]struct{ base, full, expand []string }{
	"models": {
		base:   []string{"modelId", "private", "downloads", "likes", "tags", "pipeline_tag", "library_name", "createdAt"},
		full:   []string{"author", "sha", "lastModified", "siblings", "gated"},
		expand: []string{"author", "baseModels", "cardData", "childrenModelCount", "config", "createdAt", "disabled", "downloads", "downloadsAllTime", "evalResults", "gated", "gguf", "inference", "inferenceProviderMapping", "lastModified", "library_name", "likes", "mask_token", "model-index", "pipeline_tag", "private", "resourceGroup", "safetensors", "sha", "siblings", "spaces", "tags", "transformersInfo", "trendingScore", "usedStorage", "widgetData", "xetEnabled"},
	},
	"datasets": {
		base:   []string{"author", "private", "downloads", "likes", "tags", "createdAt", "lastModified", "sha", "gated", "disabled", "description", "paperswithcode_id"},
		full:   []string{"cardData"},
		expand: []string{"author", "cardData", "citation", "createdAt", "description", "disabled", "downloads", "downloadsAllTime", "gated", "lastModified", "likes", "mainSize", "paperswithcode_id", "private", "resourceGroup", "sha", "siblings", "tags", "trendingScore", "usedStorage", "xetEnabled"},
	},
	"spaces": {
		base:   []string{"private", "likes", "tags", "createdAt", "sdk"},
		full:   []string{"author", "sha", "lastModified", "siblings", "cardData"},
		expand: []string{"author", "cardData", "createdAt", "datasets", "disabled", "lastModified", "likes", "models", "private", "region", "resourceGroup", "runtime", "sdk", "sha", "siblings", "subdomain", "tags", "trendingScore", "usedStorage", "xetEnabled"},
	},
}

// Matches the Hub's page cap.
const maxListLimit = 1000

// repoListFilter holds the parsed query parameters for listing repositories.
type repoListFilter struct {
	search     string
	author     string
	filterTags []string
	pipelines  []string
	fields     map[string]bool
	sortField  string
	asc        bool
	limit      int
	offset     int
}

// parseRepoListFilter extracts and validates the list query parameters of the request.
func parseRepoListFilter(r *http.Request, repoType string) (repoListFilter, error) {
	query := r.URL.Query()
	f := repoListFilter{
		search:     query.Get("search"),
		author:     query.Get("author"),
		filterTags: query["filter"],
		pipelines:  query["pipeline_tag"],
		fields:     map[string]bool{},
		sortField:  query.Get("sort"),
		limit:      maxListLimit,
	}
	spec := listFields[repoType]
	expand := slices.Concat(query["expand[]"], query["expand"])
	full, _ := strconv.ParseBool(query.Get("full"))
	switch {
	case len(expand) > 0:
		for _, name := range expand {
			if !slices.Contains(spec.expand, name) {
				return f, fmt.Errorf("Invalid option: expected one of %s", `"`+strings.Join(spec.expand, `"|"`)+`"`)
			}
			f.fields[name] = true
		}
	case full:
		for _, name := range spec.full {
			f.fields[name] = true
		}
		fallthrough
	default:
		for _, name := range spec.base {
			f.fields[name] = true
		}
	}
	switch f.sortField {
	case "", "likes", "downloads", "trendingScore", "trending_score", "createdAt", "lastModified":
	default:
		return f, fmt.Errorf("Invalid sort parameter: %s", f.sortField)
	}
	switch d := query.Get("direction"); d {
	case "", "-1":
	case "1":
		f.asc = true
	default:
		return f, fmt.Errorf("Invalid direction parameter: %s", d)
	}
	if v := query.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return f, fmt.Errorf("Invalid limit parameter: %s", v)
		}
		f.limit = min(n, maxListLimit)
	}
	if cursor := query.Get("cursor"); cursor != "" {
		var err error
		if f.offset, err = decodeCursorOffset(cursor); err != nil {
			return f, err
		}
	}
	return f, nil
}

// wants reports whether any of the named fields is projected.
func (f repoListFilter) wants(names ...string) bool {
	return slices.ContainsFunc(names, func(name string) bool { return f.fields[name] })
}

// handleListRepos is the unified handler for listing models, datasets, or spaces.
func (h *Handler) handleListRepos(w http.ResponseWriter, r *http.Request, repoType string, f repoListFilter) {
	items, err := h.buildRepoListItems(r.Context(), repoType, f)
	if err != nil {
		responseJSON(w, err, http.StatusInternalServerError)
		return
	}

	sortRepoItems(items, f.sortField, f.asc)

	// Apply cursor offset
	if f.offset >= len(items) {
		items = nil
	} else {
		items = items[f.offset:]
	}

	// Apply limit and set Link header if there are more results
	if len(items) > f.limit {
		items = items[:f.limit]

		nextCursor := encodeCursorOffset(f.offset + f.limit)
		nextURL := buildNextURL(r, nextCursor)
		w.Header().Set("Link", fmt.Sprintf("<%s>; rel=\"next\"", nextURL))
	}

	// The projection only needs the page's metadata and dates.
	out := make([]map[string]any, 0, len(items))
	for _, it := range items {
		h.fillListItem(it, repoType, f.wants(metaFields...), f.wants("sha", "lastModified", "createdAt"), f.wants("createdAt"))
		out = append(out, it.project(f.fields))
	}
	if err := r.Context().Err(); err != nil {
		responseJSON(w, err, http.StatusInternalServerError)
		return
	}

	responseJSON(w, out, http.StatusOK)
}

// metaFields are the projected fields that need the repository's card metadata.
var metaFields = []string{"tags", "pipeline_tag", "library_name", "sdk", "description", "paperswithcode_id", "cardData", "datasets", "models"}

// listItem is one repository being listed, with its metadata and dates read on demand.
type listItem struct {
	id, path                    string
	repo                        *repository.Repository
	rev                         string
	meta                        repoMetadata
	dates                       repoDates
	hasMeta, hasTip, hasCreated bool
}

// fillListItem reads what the caller needs and has not been read yet.
func (h *Handler) fillListItem(it *listItem, repoType string, meta, tip, created bool) {
	if meta && !it.hasMeta {
		it.meta, it.hasMeta = collectRepoMetadata(it.repo, it.rev, repoType), true
	}
	if (tip && !it.hasTip) || (created && !it.hasCreated) {
		it.dates = h.tips.dates(it.repo, it.path, it.rev, created)
		it.hasTip, it.hasCreated = true, it.hasCreated || created
	}
}

// project renders the item with the requested fields; id and trendingScore are always present.
func (it *listItem) project(fields map[string]bool) map[string]any {
	out := map[string]any{"id": it.id, "trendingScore": 0}
	set := func(name string, v any) {
		if fields[name] {
			out[name] = v
		}
	}
	setText := func(name, v string) {
		if v != "" {
			set(name, v)
		}
	}
	author, _, _ := strings.Cut(it.id, "/")
	set("modelId", it.id)
	set("author", author)
	for _, name := range []string{"private", "gated", "disabled"} {
		set(name, false)
	}
	for _, name := range []string{"downloads", "likes"} {
		set(name, 0)
	}
	if fields["tags"] {
		out["tags"] = append([]string{}, it.meta.tags...)
	}
	setText("pipeline_tag", it.meta.pipelineTag)
	setText("library_name", it.meta.libraryName)
	setText("sdk", it.meta.sdk)
	setText("description", it.meta.description)
	if c := it.meta.card; c != nil {
		setText("paperswithcode_id", c.PapersWithCodeID)
		if len(c.Datasets) > 0 {
			set("datasets", c.Datasets)
		}
		if len(c.Models) > 0 {
			set("models", c.Models)
		}
	}
	if it.dates.sha != "" {
		set("sha", it.dates.sha)
		set("lastModified", hubTime(it.dates.lastModified))
		if !it.dates.createdAt.IsZero() {
			set("createdAt", hubTime(it.dates.createdAt))
		}
	}
	// A card the Hub cannot serialize is dropped from this item alone.
	if fields["cardData"] {
		if raw := it.meta.cardJSON(); raw != nil {
			out["cardData"] = raw
		}
	}
	if fields["siblings"] {
		siblings, _ := repoSiblings(it.repo, it.rev)
		if siblings == nil {
			siblings = []sibling{}
		}
		out["siblings"] = siblings
	}
	return out
}

// buildRepoListItems walks the namespaces of repoType (or only f.author's) and returns the
// repositories passing the search, tag and pipeline filters. A repository removed since it was
// enumerated is skipped; any other storage failure is returned.
func (h *Handler) buildRepoListItems(ctx context.Context, repoType string, f repoListFilter) ([]*listItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fs := h.storage.RepositoriesFS()
	isModel := repoType == "models"

	baseDir := "/"
	if !isModel {
		baseDir = filepath.Join("/", repoType)
	}

	var roots []string
	if f.author != "" {
		// An author is one path element; a slash or dot path would walk out of its namespace.
		if strings.Contains(f.author, "/") || f.author == "." || f.author == ".." || (isModel && (f.author == "datasets" || f.author == "spaces")) {
			return nil, nil
		}
		roots = []string{filepath.Join(baseDir, f.author)}
	} else {
		namespaces, err := fs.ReadDir(baseDir)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			if errors.Is(err, iofs.ErrNotExist) {
				return nil, nil
			}
			return nil, err
		}
		for _, nsEntry := range namespaces {
			if !nsEntry.IsDir() {
				continue
			}
			nsName := nsEntry.Name()

			// For models, skip the datasets/ and spaces/ directories
			if isModel && (nsName == "datasets" || nsName == "spaces") {
				continue
			}
			roots = append(roots, filepath.Join(baseDir, nsName))
		}
	}

	// Filters and date sorts need every repository read; the projection later reads only the page.
	filtering := len(f.filterTags)+len(f.pipelines) > 0
	search := strings.ToLower(f.search)
	var items []*listItem
	for _, root := range roots {
		err := repository.Walk(ctx, fs, root, func(path string) error {
			rel, _ := filepath.Rel(baseDir, path)
			fullName := strings.TrimSuffix(rel, ".git")
			if search != "" && !strings.Contains(strings.ToLower(fullName), search) {
				return nil
			}

			repo, err := repository.Open(fs, path)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				if errors.Is(err, repository.ErrRepositoryNotExists) {
					return nil
				}
				return fmt.Errorf("failed to open repository %q: %w", fullName, err)
			}
			it := &listItem{id: fullName, path: path, repo: repo, rev: repo.DefaultBranch()}
			h.fillListItem(it, repoType, filtering, f.sortField == "createdAt" || f.sortField == "lastModified", f.sortField == "createdAt")
			if filtering && (!matchesAllTags(it.meta.tags, f.filterTags) || (len(f.pipelines) > 0 && !slices.Contains(f.pipelines, it.meta.pipelineTag))) {
				return nil
			}

			items = append(items, it)
			return nil
		})
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			// Only a namespace missing altogether (an unknown author) is not a storage failure.
			if errors.Is(err, iofs.ErrNotExist) {
				continue
			}
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// matchesAllTags checks if the repo tags contain all the filter tags.
func matchesAllTags(repoTags, filterTags []string) bool {
	tagSet := make(map[string]struct{}, len(repoTags))
	for _, t := range repoTags {
		tagSet[t] = struct{}{}
	}
	for _, f := range filterTags {
		if _, ok := tagSet[f]; !ok {
			return false
		}
	}
	return true
}

// sortRepoItems sorts the list by the given field and direction, ties broken by id.
// likes, downloads and trendingScore are all zero here, so those keys only sort by id.
func sortRepoItems(items []*listItem, sortField string, asc bool) {
	slices.SortStableFunc(items, func(a, b *listItem) int {
		var c int
		switch sortField {
		case "createdAt":
			c = a.dates.createdAt.Compare(b.dates.createdAt)
		case "lastModified":
			c = a.dates.lastModified.Compare(b.dates.lastModified)
		}
		if !asc {
			c = -c
		}
		if c == 0 {
			c = strings.Compare(a.id, b.id)
		}
		return c
	})
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

// decodeCursorOffset decodes a padded or unpadded base64url cursor into an offset.
func decodeCursorOffset(cursor string) (int, error) {
	data, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(cursor, "="))
	var payload cursorPayload
	if err == nil {
		err = json.Unmarshal(data, &payload)
	}
	if err != nil || payload.Offset < 0 {
		return 0, errors.New("Invalid cursor")
	}
	return payload.Offset, nil
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
