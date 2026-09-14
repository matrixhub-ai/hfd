package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/server"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

type principal struct {
	ID   int
	name string
}

// Name returns the principal's display name.
func (p principal) Name() string { return p.name }

// Email returns the principal's email address.
func (p principal) Email() string { return "" }

func ExampleNewHTTPHandler() {
	root, err := os.MkdirTemp("", "hfd-server-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(root)
	validator := authenticate.TokenValidatorFunc(func(ctx context.Context, token string) (authenticate.Identity, error) {
		if token == "s3cret" {
			return principal{ID: 7, name: "alice"}, nil
		}
		return nil, authenticate.ErrUnauthenticated
	})
	hook := permission.PermissionHookFunc(func(ctx context.Context, op permission.Operation, repoName string, opCtx permission.Context) (bool, error) {
		identity, ok := authenticate.IdentityFrom(ctx).(principal)
		return ok && identity.ID == 7, nil
	})
	handler := server.NewHTTPHandler(server.Options{
		Storage:        storage.NewStorage(storage.WithRootDir(root)),
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
	// The hook sees the principal type on the gated listing route; anonymous callers are refused.
	fmt.Println(whoami.Code, body.Name, get("/api/models", "s3cret").Code, get("/api/models", "").Code)
	// Output: 200 alice 200 403
}
