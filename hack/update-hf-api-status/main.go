// update-api-status fetches the HuggingFace OpenAPI v3 spec, compares it
// against the routes registered by the huggingface backend handler, and
// rewrites the "API Status" section of README.md with an up-to-date table.
//
// Usage (run from the repository root):
//
//	go run ./hack/update-api-status
//	make update-api-status
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/mux"

	backendhf "github.com/wzshiming/hfd/pkg/backend/hf"
)

const (
	openAPIURL     = "https://huggingface.co/.well-known/openapi.json"
	openAPIViewURL = "https://huggingface.co/spaces/huggingface/openapi"
	apiStatisPath  = "hf-api-status.md"
)

// OpenAPISpec is a minimal representation of an OpenAPI v3 spec.
type OpenAPISpec struct {
	Info  OpenAPIInfo            `json:"info"`
	Paths map[string]OpenAPIPath `json:"paths"`
}

// OpenAPIInfo holds top-level spec metadata.
type OpenAPIInfo struct {
	Title   string `json:"title"`
	Version string `json:"version"`
}

// OpenAPIPath maps HTTP methods (lowercase) to their operations.
type OpenAPIPath map[string]OpenAPIOperation

// OpenAPIOperation is the per-method operation descriptor.
type OpenAPIOperation struct {
	Summary    string   `json:"summary"`
	Tags       []string `json:"tags"`
	Deprecated bool     `json:"deprecated"`
}

func main() {
	spec, err := fetchSpec(openAPIURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error fetching OpenAPI spec: %v\n", err)
		os.Exit(1)
	}

	router := buildRouter()
	table := buildTable(spec, router)

	err = os.WriteFile(apiStatisPath, []byte(table), 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error writing README.md: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("API status updated successfully.")
}

// fetchSpec downloads and parses the OpenAPI spec at url.
func fetchSpec(url string) (*OpenAPISpec, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request for %s: %w", url, err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned %s", url, resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	var spec OpenAPISpec
	if err := json.Unmarshal(body, &spec); err != nil {
		return nil, fmt.Errorf("parsing JSON: %w", err)
	}
	return &spec, nil
}

// buildRouter creates the huggingface handler (no storage needed for route
// inspection) and returns its underlying mux.Router.
func buildRouter() *mux.Router {
	return backendhf.NewHandler().Router()
}

// apiEntry is one row in the completion table.
type apiEntry struct {
	Method      string
	Path        string
	Summary     string
	Tags        []string
	Implemented bool
}

// httpMethods lists the HTTP verbs we care about in display order.
var httpMethods = []string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"}

// buildTable iterates over every path/method combination in spec and checks
// whether the router has a matching registered route.
func buildTable(spec *OpenAPISpec, router *mux.Router) string {
	var entries []apiEntry

	for path, pathItem := range spec.Paths {
		for _, method := range httpMethods {
			op, ok := pathItem[strings.ToLower(method)]
			if !ok {
				continue
			}
			entries = append(entries, apiEntry{
				Method:      method,
				Path:        path,
				Summary:     op.Summary,
				Tags:        op.Tags,
				Implemented: isImplemented(router, method, path),
			})
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		if tagsOrder(entries[i].Tags) != tagsOrder(entries[j].Tags) {
			return tagsOrder(entries[i].Tags) < tagsOrder(entries[j].Tags)
		}
		if entries[i].Path != entries[j].Path {
			return entries[i].Path < entries[j].Path
		}
		return methodOrder(entries[i].Method) < methodOrder(entries[j].Method)
	})

	var sb strings.Builder
	sb.WriteString("# API Status\n\n")
	sb.WriteString("Compared against the [HuggingFace OpenAPI v3](" + openAPIURL + ") spec")
	sb.WriteString(" ([interactive viewer](" + openAPIViewURL + ")).\n\n")
	sb.WriteString("| Status | Method | Path | Tags | Description |\n")
	sb.WriteString("|:------:|--------|------|------|-------------|\n")
	for _, e := range entries {
		status := "❌"
		if e.Implemented {
			status = "✅"
		}
		tags := formatTags(e.Tags, e.Method, e.Path)
		sb.WriteString(fmt.Sprintf("| %s | `%s` | `%s` | %s | %s |\n",
			status, e.Method, e.Path, tags, e.Summary))
	}
	return sb.String()
}

// methodOrder returns a sort key for common HTTP methods.
func methodOrder(m string) int {
	order := map[string]int{
		"GET": 0, "HEAD": 1, "POST": 2, "PUT": 3, "PATCH": 4, "DELETE": 5,
	}
	if v, ok := order[m]; ok {
		return v
	}
	return 99
}

// tagOrder returns a sort key for common OpenAPI tags.
func tagOrder(t string) int {
	order := map[string]int{
		"models": 0, "datasets": 1, "spaces": 2, "repos": 3,
	}
	if v, ok := order[t]; ok {
		return v
	}
	return 99
}

// tagsOrder returns a comma-separated list of tags sorted by tagOrder.
func tagsOrder(tags []string) int {
	if len(tags) == 0 {
		return 99
	}
	minOrder := 99
	for _, t := range tags {
		if o := tagOrder(t); o < minOrder {
			minOrder = o
		}
	}
	return minOrder
}

// formatTags converts a slice of tag names into a comma-separated list of
// Markdown links pointing to the corresponding section in the OpenAPI viewer.
// e.g. ["Models"] → "[Models](https://huggingface.co/spaces/huggingface/openapi#tag/Models)"
func formatTags(tags []string, method, path string) string {
	if len(tags) == 0 {
		return ""
	}
	parts := make([]string, 0, len(tags))
	for _, tag := range tags {
		parts = append(parts, fmt.Sprintf("[%s](%s#tag/%s/%s%s)", tag, openAPIViewURL, tag, method, path))
	}
	return strings.Join(parts, ", ")
}

// isImplemented returns true if the router has a registered route that matches
// method + a synthesised test URL derived from the OpenAPI path.
func isImplemented(router *mux.Router, method, openapiPath string) bool {
	testURL := openAPIPathToTestURL(openapiPath)
	req := httptest.NewRequest(method, testURL, nil)
	var match mux.RouteMatch
	return router.Match(req, &match) && match.Handler != nil
}

// paramRe matches OpenAPI path parameters like {model_id} or {revision}.
var paramRe = regexp.MustCompile(`\{([^}]+)\}`)

// openAPIPathToTestURL replaces OpenAPI path parameters with concrete test
// values so that the URL can be matched against the mux router.
func openAPIPathToTestURL(path string) string {
	return paramRe.ReplaceAllStringFunc(path, func(m string) string {
		param := m[1 : len(m)-1]
		switch param {
		case "model_id", "dataset_id", "space_id", "repo_id":
			// Simulate an org/name style repo identifier, which mux captures
			// with the greedy pattern {repo:.+} allowing slashes.
			return "testorg/testrepo"
		case "revision":
			return "main"
		case "filename", "path", "treeid":
			return "README.md"
		case "branch":
			return "dev"
		case "tag":
			return "v1.0"
		default:
			return "testvalue"
		}
	})
}
