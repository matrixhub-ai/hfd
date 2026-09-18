package hf

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/go-git/go-billy/v6"
	"github.com/gorilla/mux"
	"github.com/matrixhub-ai/hfd/pkg/hfmeta"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// handleList is the unified handler for listing repositories of different types (models, datasets, spaces).
func (h *Handler) handleList(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repoType := vars["repoType"]
	filter := parseRepoListFilter(r)
	if !h.checkPermission(w, r, permission.OperationListRepos, repoType, permission.Context{Author: filter.author}) {
		return
	}
	h.handleListRepos(w, r, repoType, filter)
}

// repoListFilter holds the parsed query parameters for listing repositories.
type repoListFilter struct {
	search     string
	author     string
	filterTags []string
	sortField  string
	limit      int
	offset     int
}

// parseRepoListFilter extracts and parses the list query parameters from the request.
func parseRepoListFilter(r *http.Request) repoListFilter {
	query := r.URL.Query()
	f := repoListFilter{
		search:     query.Get("search"),
		author:     query.Get("author"),
		filterTags: query["filter"],
		sortField:  query.Get("sort"),
	}
	if v, err := strconv.Atoi(query.Get("limit")); err == nil && v > 0 {
		f.limit = v
	}
	if cursor := query.Get("cursor"); cursor != "" {
		f.offset = decodeCursorOffset(cursor)
	}
	return f
}

// handleListRepos is the unified handler for listing models, datasets, or spaces.
func (h *Handler) handleListRepos(w http.ResponseWriter, r *http.Request, repoType string, f repoListFilter) {
	isModel := repoType == "models"

	baseDir := "/"
	if !isModel {
		baseDir = filepath.Join("/", repoType)
	}

	items := buildRepoListItems(r.Context(), h.storage.RepositoriesFS(), baseDir, isModel, f)

	// Sort results
	sortRepoItems(items, f.sortField)

	// Apply cursor offset
	if f.offset > 0 && f.offset < len(items) {
		items = items[f.offset:]
	} else if f.offset >= len(items) {
		items = nil
	}

	// Apply limit and set Link header if there are more results
	if f.limit > 0 && len(items) > f.limit {
		items = items[:f.limit]

		nextCursor := encodeCursorOffset(f.offset + f.limit)
		nextURL := buildNextURL(r, nextCursor)
		w.Header().Set("Link", fmt.Sprintf("<%s>; rel=\"next\"", nextURL))
	}

	if items == nil {
		items = []repoListItem{}
	}

	responseJSON(w, items, http.StatusOK)
}

// repoRoots returns the namespace directories to walk under baseDir, or only author's.
func repoRoots(fs billy.Filesystem, baseDir string, isModel bool, author string) []string {
	if author != "" {
		// An author is one path element; a slash or dot path would walk out of its namespace.
		if strings.Contains(author, "/") || author == "." || author == ".." || (isModel && (author == "datasets" || author == "spaces")) {
			return nil
		}
		return []string{filepath.Join(baseDir, author)}
	}
	namespaces, err := fs.ReadDir(baseDir)
	if err != nil {
		return nil
	}
	var roots []string
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
	return roots
}

// buildRepoListItems walks the namespaces under baseDir (or only f.author's) and returns the
// list items passing the search and tag filters, with metadata read from each repository.
func buildRepoListItems(ctx context.Context, fs billy.Filesystem, baseDir string, isModel bool, f repoListFilter) []repoListItem {
	var items []repoListItem
	for _, root := range repoRoots(fs, baseDir, isModel, f.author) {
		// An unreadable directory ends this namespace's walk; the others still get listed.
		_ = repository.Walk(ctx, fs, root, func(path string) error {
			rel, _ := filepath.Rel(baseDir, path)
			fullName := strings.TrimSuffix(rel, ".git")
			if f.search != "" && !strings.Contains(strings.ToLower(fullName), strings.ToLower(f.search)) {
				return nil
			}

			item := repoListItem{
				RepoID: fullName,
			}
			if isModel {
				item.ModelID = fullName
			}

			if repo, err := repository.Open(fs, path); err == nil {
				meta := collectRepoMetadata(repo, repo.DefaultBranch())
				item.Tags = meta.tags
				item.PipelineTag = meta.pipelineTag
				item.LibraryName = meta.libraryName
			}

			if len(f.filterTags) > 0 && !matchesAllTags(item.Tags, f.filterTags) {
				return nil
			}

			items = append(items, item)
			return nil
		})
	}
	return items
}

// repoMetadata holds metadata extracted from a repository's README.md
// front matter and config.json. It is the single source of truth for
// tag collection, pipeline_tag, library_name, cardData, and createdAt,
// used by both the list endpoints and the individual repo info endpoint.
type repoMetadata struct {
	tags        []string
	pipelineTag string
	libraryName string
	cardData    any
	card        hfmeta.Card
}

// collectRepoMetadata reads metadata from an already-opened repository at the
// given revision. It extracts tags, pipeline_tag, library_name, and cardData
// from README.md YAML front matter and config.json, and derives createdAt
// from the latest commit date.
func collectRepoMetadata(repo *repository.Repository, rev string) repoMetadata {
	var meta repoMetadata

	seen := make(map[string]struct{})
	addTag := func(tag string) {
		if tag == "" {
			return
		}
		if _, ok := seen[tag]; !ok {
			seen[tag] = struct{}{}
			meta.tags = append(meta.tags, tag)
		}
	}

	// README.md YAML front matter
	if blob, err := repo.Blob(rev, "README.md"); err == nil {
		if rc, err := blob.NewReader(); err == nil {
			if rm, err := hfmeta.ParseReadme(rc); err == nil {
				for _, tag := range rm.Tags() {
					addTag(tag)
				}
				meta.pipelineTag = rm.Card.PipelineTag
				meta.libraryName = rm.Card.LibraryName
				meta.cardData = rm.CardData
				meta.card = *rm.Card
			}
			rc.Close()
		}
	}

	// config.json
	if blob, err := repo.Blob(rev, "config.json"); err == nil {
		if rc, err := blob.NewReader(); err == nil {
			if cfg, err := hfmeta.ParseConfigData(rc); err == nil {
				for _, tag := range cfg.Tags() {
					addTag(tag)
				}
			}
			rc.Close()
		}
	}

	return meta
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

// sortRepoItems sorts the list by the given field.
func sortRepoItems(items []repoListItem, sortField string) {
	switch sortField {
	case "downloads":
		sort.Slice(items, func(i, j int) bool {
			return items[i].Downloads > items[j].Downloads
		})
	case "likes":
		sort.Slice(items, func(i, j int) bool {
			return items[i].Likes > items[j].Likes
		})
	case "trending_score", "trendingScore":
		sort.Slice(items, func(i, j int) bool {
			return items[i].TrendingScore > items[j].TrendingScore
		})
	default:
		// Default: sort alphabetically by ID
		sort.Slice(items, func(i, j int) bool {
			return items[i].RepoID < items[j].RepoID
		})
	}
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
