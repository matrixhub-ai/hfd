package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"

	"github.com/matrixhub-ai/hfd/internal/server"
	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

func ExampleNewHTTPHandler() {
	root, err := os.MkdirTemp("", "hfd-server-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(root)
	validator := authenticate.TokenValidatorFunc(func(ctx context.Context, token string) (string, bool, bool, error) {
		if token == "s3cret" {
			return "alice", false, true, nil
		}
		return "", false, false, nil
	})
	preOpen := func(ctx context.Context, _ string, _ bool) error {
		if authenticate.IdentityFrom(ctx).Name() == "alice" {
			return nil
		}
		return fs.ErrPermission
	}
	st, err := storage.NewStorage(storage.WithRootDir(root))
	if err != nil {
		panic(err)
	}
	// Catalog routes are off until a callback is registered; Hooks carries the filesystem defaults.
	hooks := &server.Hooks{Storage: st}
	list := func(ctx context.Context, repoType string, query backendhf.ListQuery) ([]backendhf.RepoListItem, bool, error) {
		if authenticate.IdentityFrom(ctx).Name() != "alice" {
			return nil, false, fs.ErrPermission
		}
		return hooks.ListRepos(ctx, repoType, query)
	}
	handler := server.NewHTTPHandler(server.Options{
		Storage:        st,
		Authenticators: &authenticate.Authenticators{Token: validator},
		PreOpen:        preOpen,
		HFOptions:      []backendhf.Option{backendhf.WithWhoamiFunc(hooks.Whoami), backendhf.WithListReposFunc(list)},
	})
	get := func(path, bearer string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		if bearer != "" {
			request.Header.Set("Authorization", "Bearer "+bearer)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, request)
		return rec
	}
	whoami := get("/api/whoami-v2", "s3cret")
	var body struct{ Name string }
	if err := json.Unmarshal(whoami.Body.Bytes(), &body); err != nil {
		panic(err)
	}
	// list gates /api/models and PreOpen gates org/repo: alice passes (404: org/repo is missing), anonymous callers get 403.
	fmt.Println(whoami.Code, body.Name, get("/api/models", "s3cret").Code, get("/api/models", "").Code,
		get("/api/models/org/repo", "s3cret").Code, get("/api/models/org/repo", "").Code)
	// Output: 200 alice 200 403 404 403
}
