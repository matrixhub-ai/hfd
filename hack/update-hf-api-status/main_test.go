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
