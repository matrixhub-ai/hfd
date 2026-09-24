package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"

	"github.com/matrixhub-ai/hfd/internal/server"
	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/permission"
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
	hook := permission.PermissionHookFunc(func(ctx context.Context, op permission.Operation, repoName string, opCtx permission.Context) (bool, error) {
		return authenticate.IdentityFrom(ctx).Name() == "alice", nil
	})
	st, err := storage.NewStorage(storage.WithRootDir(root))
	if err != nil {
		panic(err)
	}
	handler := server.NewHTTPHandler(server.Options{
		Storage:        st,
		Authenticators: &authenticate.Authenticators{Token: validator},
		Permission:     hook,
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
	// The hook sees the authenticated name on the gated listing route; anonymous callers are refused.
	fmt.Println(whoami.Code, body.Name, get("/api/models", "s3cret").Code, get("/api/models", "").Code)
	// Output: 200 alice 200 403
}
