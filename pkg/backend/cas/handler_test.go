package cas

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"
	"time"

	xetclient "github.com/wzshiming/xet/client"
	xetmirror "github.com/wzshiming/xet/mirror"
	xetserver "github.com/wzshiming/xet/server"
	xethf "github.com/wzshiming/xet/server/hf"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/permission"
)

// sentinelNext marks requests that fell past the handler's route table.
type sentinelNext struct{}

func (sentinelNext) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusTeapot)
}

// newXETStack assembles the minting mirror and the xet CAS composition the
// way cmd/hfd does; the hub front end answers the read-token routes when an
// upstream exists, as the ungated pass-through test observes.
func newXETStack(t *testing.T) (m *mirror.Mirror, composition http.Handler) {
	t.Helper()
	upstream := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(upstream.Close)
	dataDir := filepath.Join(t.TempDir(), "xet")
	client, err := xetclient.NewClient(xetclient.WithCacheDir(filepath.Join(dataDir, "chunks")))
	if err != nil {
		t.Fatalf("new xet client: %v", err)
	}
	xs, err := xetstorage.NewFileStorage(
		xetstorage.WithBasePath(filepath.Join(dataDir, "storage")),
	)
	if err != nil {
		t.Fatalf("new xet storage: %v", err)
	}
	mint, authFn, err := authenticate.NewXETTokenScheme(nil)
	if err != nil {
		t.Fatalf("new token scheme: %v", err)
	}
	engine, err := xetmirror.NewMirror(
		xetmirror.WithStorage(xs),
		xetmirror.WithUpstream(upstream.URL),
		xetmirror.WithCacheDir(filepath.Join(dataDir, "mirror")),
		xetmirror.WithClient(client),
	)
	if err != nil {
		t.Fatalf("new xet mirror engine: %v", err)
	}
	var hubHandler http.Handler = xethf.NewHandler(
		xethf.WithMirror(engine),
		xethf.WithMintToken(mint),
		xethf.WithNext(http.NotFoundHandler()),
	)
	composition = xetserver.NewHandler(
		xetserver.WithStorage(xs),
		xetserver.WithAuthFunc(authFn),
		xetserver.WithNext(hubHandler),
	)
	m, err = mirror.NewMirror(
		mirror.WithXETStorage(xs),
		mirror.WithXETClient(client),
		mirror.WithMintToken(mint),
	)
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}
	return m, composition
}

// newTokenHandler builds a handler over the minting mirror with the sentinel
// as next, so tests observe requests that delegate past the route table.
func newTokenHandler(t *testing.T, hook permission.PermissionHookFunc) *Handler {
	t.Helper()
	m, _ := newXETStack(t)
	return NewHandler(
		WithMirror(m),
		WithPermissionHookFunc(hook),
		WithNext(sentinelNext{}),
	)
}

func get(h *Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestWriteTokenGate(t *testing.T) {
	allow := true
	var gotRepos []string
	var gotOps []permission.Operation
	var gotCtxs []permission.Context
	hook := func(ctx context.Context, op permission.Operation, repoName string, permCtx permission.Context) (bool, error) {
		gotOps = append(gotOps, op)
		gotRepos = append(gotRepos, repoName)
		gotCtxs = append(gotCtxs, permCtx)
		return allow, nil
	}
	h := newTokenHandler(t, hook)

	for _, tt := range []struct {
		path     string
		wantRepo string
	}{
		{"/api/models/org/repo/xet-write-token/main", "org/repo"},
		{"/api/datasets/org/repo/xet-write-token/main", "datasets/org/repo"},
		{"/api/spaces/org/repo/xet-write-token/main", "spaces/org/repo"},
	} {
		gotOps, gotRepos, gotCtxs = nil, nil, nil
		get(h, tt.path)
		if !slices.Equal(gotOps, []permission.Operation{permission.OperationUpdateRepo}) ||
			!slices.Equal(gotRepos, []string{tt.wantRepo}) {
			t.Errorf("GET %s hook saw ops %v repos %v, want [%v] [%q]", tt.path, gotOps, gotRepos, permission.OperationUpdateRepo, tt.wantRepo)
		}
		if len(gotCtxs) != 1 || gotCtxs[0] != (permission.Context{}) {
			t.Errorf("GET %s hook saw contexts %+v, want one zero Context", tt.path, gotCtxs)
		}
	}

	allow = false
	if rec := get(h, "/api/models/org/repo/xet-write-token/main"); rec.Code != http.StatusForbidden {
		t.Errorf("denied write token status = %d, want 403", rec.Code)
	}
}

func TestWriteTokenHookError(t *testing.T) {
	hook := func(context.Context, permission.Operation, string, permission.Context) (bool, error) {
		return false, fmt.Errorf("hook exploded")
	}
	h := newTokenHandler(t, hook)

	if rec := get(h, "/api/models/org/repo/xet-write-token/main"); rec.Code != http.StatusInternalServerError {
		t.Errorf("hook error status = %d, want 500", rec.Code)
	}
}

func TestWriteTokenMints(t *testing.T) {
	hook := func(context.Context, permission.Operation, string, permission.Context) (bool, error) {
		return true, nil
	}
	h := newTokenHandler(t, hook)

	rec := get(h, "/api/models/org/repo/xet-write-token/main")
	if rec.Code != http.StatusOK {
		t.Fatalf("write token status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("write token Content-Type = %q, want application/json", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("write token Cache-Control = %q, want no-store", got)
	}
	var tok struct {
		CasURL      string `json:"casUrl"`
		AccessToken string `json:"accessToken"`
		Exp         int64  `json:"exp"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&tok); err != nil {
		t.Fatalf("decode write token: %v", err)
	}
	if tok.CasURL == "" || tok.AccessToken == "" || tok.Exp <= time.Now().Unix() {
		t.Fatalf("write token incomplete: %+v", tok)
	}
}

func TestWriteTokenDelegatesNonGET(t *testing.T) {
	var gotOps []permission.Operation
	hook := func(_ context.Context, op permission.Operation, _ string, _ permission.Context) (bool, error) {
		gotOps = append(gotOps, op)
		return true, nil
	}
	h := newTokenHandler(t, hook)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/models/org/repo/xet-write-token/main", nil))
	if !slices.Equal(gotOps, []permission.Operation{permission.OperationUpdateRepo}) {
		t.Errorf("hook saw ops %v, want [%v]", gotOps, permission.OperationUpdateRepo)
	}
	// Non-GET write-token semantics belong to the composition mounted as
	// next — the sentinel here.
	if rec.Code != http.StatusTeapot {
		t.Errorf("POST write token status = %d, want 418 delegated to next", rec.Code)
	}
}

func TestReadTokenDelegates(t *testing.T) {
	var hookCalls []string
	hook := func(_ context.Context, op permission.Operation, repoName string, _ permission.Context) (bool, error) {
		hookCalls = append(hookCalls, fmt.Sprintf("%v %s", op, repoName))
		return true, nil
	}
	m, composition := newXETStack(t)
	h := NewHandler(
		WithMirror(m),
		WithPermissionHookFunc(hook),
		WithNext(composition),
	)

	// Read-token and /xet-token carry no gate route; the xet hub front end
	// answers them past the chain.
	for _, path := range []string{"/api/models/org/repo/xet-read-token/main", "/xet-token"} {
		rec := get(h, path)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", path, rec.Code)
		}
		var tok struct {
			AccessToken string `json:"accessToken"`
		}
		if err := json.NewDecoder(rec.Body).Decode(&tok); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		if tok.AccessToken == "" {
			t.Fatalf("%s missing accessToken", path)
		}
	}
	if len(hookCalls) != 0 {
		t.Errorf("hook called for ungated token paths: %v", hookCalls)
	}
}

func TestRoutesFallThrough(t *testing.T) {
	var hookCalls []string
	hook := func(_ context.Context, op permission.Operation, repoName string, _ permission.Context) (bool, error) {
		hookCalls = append(hookCalls, fmt.Sprintf("%v %s", op, repoName))
		return true, nil
	}
	h := newTokenHandler(t, hook)

	for _, path := range []string{
		"/api/foobar/org/repo/xet-write-token/main",
		"/api/models/org/repo/extra/xet-write-token/main",
		"/api/models/org/repo/xet-write-token/",
		"/api/models/org/repo/xet-read-token/main",
		"/xet-token",
		"/xet-bridge/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		// CAS paths match no gate route; in production the composition
		// sits behind this handler as next — here next is the sentinel.
		"/v1/xorbs/whatever",
	} {
		if rec := get(h, path); rec.Code != http.StatusTeapot {
			t.Errorf("GET %s status = %d, want 418 fall-through", path, rec.Code)
		}
	}
	if len(hookCalls) != 0 {
		t.Errorf("hook called for fall-through paths: %v", hookCalls)
	}
}
