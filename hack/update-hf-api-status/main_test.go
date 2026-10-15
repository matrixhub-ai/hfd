package main

import (
	"net/http"
	"testing"
)

// TestBuildRouterCatalogRoutes pins that the status table counts the catalog routes hfd wires as implemented.
func TestBuildRouterCatalogRoutes(t *testing.T) {
	router := buildRouter()
	for _, route := range []struct{ method, path string }{
		{http.MethodPost, "/api/repos/create"},
		{http.MethodDelete, "/api/repos/delete"},
		{http.MethodPost, "/api/repos/move"},
		{http.MethodPost, "/api/{repoType}/{namespace}/{repo}/duplicate"},
		{http.MethodGet, "/api/{repoType}/{namespace}/{repo}/duplicate/status"},
		{http.MethodPut, "/api/models/{repo_id}/settings"},
		{http.MethodGet, "/api/models"},
		{http.MethodGet, "/api/whoami-v2"},
		{http.MethodGet, "/api/models/{repo_id}"},
	} {
		if !isImplemented(router, route.method, route.path) {
			t.Errorf("%s %s is not in the route table", route.method, route.path)
		}
	}
}

// TestOpenAPIPathToTestURLRepoType pins that {repoType} becomes a repository type the hf routes accept.
func TestOpenAPIPathToTestURLRepoType(t *testing.T) {
	if got, want := openAPIPathToTestURL("/api/{repoType}/{namespace}/{repo}/duplicate"), "/api/models/testvalue/testvalue/duplicate"; got != want {
		t.Errorf("openAPIPathToTestURL = %q, want %q", got, want)
	}
}
