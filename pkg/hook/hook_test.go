package hook

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestBuildHookContext(t *testing.T) {
	routerTable := map[string]string{
		"GET /api/whoami-v2":          "opWhoami",
		"GET /api/{namespace}/{repo}": "opGetRepo",
		"POST /api/repos/create":      "opCreateRepo",
		"GET /api/{repoType:models|datasets|spaces}/{namespace}/{repo}":                   "opGetResource",
		"GET /api/{namespace}/{repo}/resolve/{revpath:.*}":                                "opResolve",
		"GET /api/{repoType:models|datasets|spaces}/{namespace}/{repo}/tree/{revpath:.*}": "opTree",
	}

	tests := []struct {
		name           string
		method         string
		url            string
		headers        map[string]string
		wantOperation  string
		wantPathParams map[string]string
		wantErr        bool
	}{
		{
			name:          "whoami-v2",
			method:        http.MethodGet,
			url:           "/api/whoami-v2",
			headers:       map[string]string{},
			wantOperation: "opWhoami",
			wantPathParams: map[string]string{
				"namespace": "",
				"repo":      "",
			},
			wantErr: false,
		},
		{
			name:          "get repo",
			method:        http.MethodGet,
			url:           "/api/bigscience/bloom",
			headers:       map[string]string{},
			wantOperation: "opGetRepo",
			wantPathParams: map[string]string{
				"namespace": "bigscience",
				"repo":      "bloom",
			},
			wantErr: false,
		},
		{
			name:          "create repo",
			method:        http.MethodPost,
			url:           "/api/repos/create",
			headers:       map[string]string{},
			wantOperation: "opCreateRepo",
			wantPathParams: map[string]string{
				"namespace": "",
				"repo":      "",
			},
			wantErr: false,
		},
		{
			name:          "get resource - model",
			method:        http.MethodGet,
			url:           "/api/models/bigscience/bloom",
			headers:       map[string]string{},
			wantOperation: "opGetResource",
			wantPathParams: map[string]string{
				"repoType":  "models",
				"namespace": "bigscience",
				"repo":      "bloom",
			},
			wantErr: false,
		},
		{
			name:          "get resource - dataset",
			method:        http.MethodGet,
			url:           "/api/datasets/huggingface/dataset",
			headers:       map[string]string{},
			wantOperation: "opGetResource",
			wantPathParams: map[string]string{
				"repoType":  "datasets",
				"namespace": "huggingface",
				"repo":      "dataset",
			},
			wantErr: false,
		},
		{
			name:          "get resource - space",
			method:        http.MethodGet,
			url:           "/api/spaces/demo/app",
			headers:       map[string]string{},
			wantOperation: "opGetResource",
			wantPathParams: map[string]string{
				"repoType":  "spaces",
				"namespace": "demo",
				"repo":      "app",
			},
			wantErr: false,
		},
		{
			name:          "resolve - main branch",
			method:        http.MethodGet,
			url:           "/api/bigscience/bloom/resolve/main",
			headers:       map[string]string{},
			wantOperation: "opResolve",
			wantPathParams: map[string]string{
				"namespace": "bigscience",
				"repo":      "bloom",
				"revpath":   "main",
			},
			wantErr: false,
		},
		{
			name:          "resolve - with path",
			method:        http.MethodGet,
			url:           "/api/bigscience/bloom/resolve/main/config.json",
			headers:       map[string]string{},
			wantOperation: "opResolve",
			wantPathParams: map[string]string{
				"namespace": "bigscience",
				"repo":      "bloom",
				"revpath":   "main/config.json",
			},
			wantErr: false,
		},
		{
			name:          "tree - model",
			method:        http.MethodGet,
			url:           "/api/models/bigscience/bloom/tree/main",
			headers:       map[string]string{},
			wantOperation: "opTree",
			wantPathParams: map[string]string{
				"repoType":  "models",
				"namespace": "bigscience",
				"repo":      "bloom",
				"revpath":   "main",
			},
			wantErr: false,
		},
		{
			name:          "tree - with nested path",
			method:        http.MethodGet,
			url:           "/api/datasets/huggingface/dataset/tree/main/data/train",
			headers:       map[string]string{},
			wantOperation: "opTree",
			wantPathParams: map[string]string{
				"repoType":  "datasets",
				"namespace": "huggingface",
				"repo":      "dataset",
				"revpath":   "main/data/train",
			},
			wantErr: false,
		},
		{
			name:           "unmatched route",
			method:         http.MethodDelete,
			url:            "/api/unknown",
			headers:        map[string]string{},
			wantOperation:  "",
			wantPathParams: nil,
			wantErr:        false,
		},
		{
			name:           "method mismatch",
			method:         http.MethodPost,
			url:            "/api/whoami-v2",
			headers:        map[string]string{},
			wantOperation:  "",
			wantPathParams: nil,
			wantErr:        false,
		},
		{
			name:   "with headers",
			method: http.MethodGet,
			url:    "/api/bigscience/bloom",
			headers: map[string]string{
				"Content-Type":  "application/json",
				"Authorization": "Bearer token",
			},
			wantOperation: "opGetRepo",
			wantPathParams: map[string]string{
				"namespace": "bigscience",
				"repo":      "bloom",
			},
			wantErr: false,
		},
		{
			name:          "with query params",
			method:        http.MethodGet,
			url:           "/api/bigscience/bloom?files=config.json",
			headers:       map[string]string{},
			wantOperation: "opGetRepo",
			wantPathParams: map[string]string{
				"namespace": "bigscience",
				"repo":      "bloom",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			middleware := NewHookMiddleware(nil, nil, nil, routerTable)

			req := httptest.NewRequest(tt.method, tt.url, nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			hc, err := middleware.buildHookContext(req)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildHookContext() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if hc.Method != tt.method {
				t.Errorf("buildHookContext() Method = %v, want %v", hc.Method, tt.method)
			}

			// check URL
			reqURL, _ := url.Parse(tt.url)
			if hc.URL != reqURL.String() {
				t.Errorf("buildHookContext() URL = %v, want %v", hc.URL, reqURL.String())
			}

			// check query params
			expectedQuery := reqURL.Query()
			for k, v := range expectedQuery {
				if hc.QueryParams.Get(k) != v[0] {
					t.Errorf("buildHookContext() QueryParams[%s] = %v, want %v", k, hc.QueryParams.Get(k), v[0])
				}
			}

			// check operation
			if hc.Operation != tt.wantOperation {
				t.Errorf("buildHookContext() Operation = %v, want %v", hc.Operation, tt.wantOperation)
			}

			// check path params
			if tt.wantPathParams != nil {
				for k, v := range tt.wantPathParams {
					if v != "" && hc.PathParams[k] != v {
						t.Errorf("buildHookContext() PathParams[%s] = %v, want %v", k, hc.PathParams[k], v)
					}
				}
			}

			// check headers
			for k, v := range tt.headers {
				if hc.Headers[k] != v {
					t.Errorf("buildHookContext() Headers[%s] = %v, want %v", k, hc.Headers[k], v)
				}
			}
		})
	}
}

func TestBuildHookContext_PathParamsExtraction(t *testing.T) {
	routerTable := map[string]string{
		"GET /api/{org}/{repo}/commits": "GET_COMMITS",
	}

	middleware := NewHookMiddleware(nil, nil, nil, routerTable)

	req := httptest.NewRequest(http.MethodGet, "/api/myorg/myrepo/commits", nil)
	hc, err := middleware.buildHookContext(req)
	if err != nil {
		t.Fatalf("buildHookContext() unexpected error: %v", err)
	}

	if hc.Operation != "GET_COMMITS" {
		t.Errorf("buildHookContext() Operation = %v, want GET_COMMITS", hc.Operation)
	}

	if hc.PathParams["org"] != "myorg" {
		t.Errorf("buildHookContext() PathParams[org] = %v, want myorg", hc.PathParams["org"])
	}

	if hc.PathParams["repo"] != "myrepo" {
		t.Errorf("buildHookContext() PathParams[repo] = %v, want myrepo", hc.PathParams["repo"])
	}
}

func TestBuildHookContext_HeadersPreservation(t *testing.T) {
	middleware := NewHookMiddleware(nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	req.Header.Set("X-Custom-Header", "custom-value")
	req.Header.Set("Authorization", "Bearer secret-token")

	hc, err := middleware.buildHookContext(req)
	if err != nil {
		t.Fatalf("buildHookContext() unexpected error: %v", err)
	}

	if hc.Headers["X-Custom-Header"] != "custom-value" {
		t.Errorf("buildHookContext() Headers[X-Custom-Header] = %v, want custom-value", hc.Headers["X-Custom-Header"])
	}

	if hc.Headers["Authorization"] != "Bearer secret-token" {
		t.Errorf("buildHookContext() Headers[Authorization] = %v, want Bearer secret-token", hc.Headers["Authorization"])
	}
}

func TestBuildHookContext_QueryParamsParsing(t *testing.T) {
	middleware := NewHookMiddleware(nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/search?q=golang&lang=go&lang=rust", nil)

	hc, err := middleware.buildHookContext(req)
	if err != nil {
		t.Fatalf("buildHookContext() unexpected error: %v", err)
	}

	if hc.QueryParams.Get("q") != "golang" {
		t.Errorf("buildHookContext() QueryParams.Get(q) = %v, want golang", hc.QueryParams.Get("q"))
	}

	// check multi-value params
	langs := hc.QueryParams["lang"]
	if len(langs) != 2 || langs[0] != "go" || langs[1] != "rust" {
		t.Errorf("buildHookContext() QueryParams[lang] = %v, want [go rust]", langs)
	}
}
