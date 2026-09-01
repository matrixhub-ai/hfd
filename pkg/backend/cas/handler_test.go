package cas

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strconv"
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
// way cmd/hfd does; the shadow test mounts the composition as next.
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

func TestTokenGate(t *testing.T) {
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
		wantOp   permission.Operation
		wantRepo string
	}{
		{"/api/models/org/repo/xet-write-token/main", permission.OperationUpdateRepo, "org/repo"},
		{"/api/datasets/org/repo/xet-write-token/main", permission.OperationUpdateRepo, "datasets/org/repo"},
		{"/api/spaces/org/repo/xet-write-token/main", permission.OperationUpdateRepo, "spaces/org/repo"},
		{"/api/models/org/repo/xet-read-token/main", permission.OperationReadRepo, "org/repo"},
		{"/api/datasets/org/repo/xet-read-token/main", permission.OperationReadRepo, "datasets/org/repo"},
		{"/api/spaces/org/repo/xet-read-token/main", permission.OperationReadRepo, "spaces/org/repo"},
		// Encoded spellings decode to one permission identity.
		{"/api/models/org/priv%61te/xet-read-token/main", permission.OperationReadRepo, "org/private"},
		{"/api/models/or%67/repo/xet-write-token/main", permission.OperationUpdateRepo, "org/repo"},
	} {
		gotOps, gotRepos, gotCtxs = nil, nil, nil
		get(h, tt.path)
		if !slices.Equal(gotOps, []permission.Operation{tt.wantOp}) ||
			!slices.Equal(gotRepos, []string{tt.wantRepo}) {
			t.Errorf("GET %s hook saw ops %v repos %v, want [%v] [%q]", tt.path, gotOps, gotRepos, tt.wantOp, tt.wantRepo)
		}
		if len(gotCtxs) != 1 || gotCtxs[0] != (permission.Context{}) {
			t.Errorf("GET %s hook saw contexts %+v, want one zero Context", tt.path, gotCtxs)
		}
	}

	allow = false
	for _, path := range []string{
		"/api/models/org/repo/xet-write-token/main",
		"/api/models/org/repo/xet-read-token/main",
	} {
		if rec := get(h, path); rec.Code != http.StatusForbidden {
			t.Errorf("denied GET %s status = %d, want 403", path, rec.Code)
		}
		// The gate precedes the method check: denied non-GETs never reach next.
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
		if rec.Code != http.StatusForbidden {
			t.Errorf("denied POST %s status = %d, want 403", path, rec.Code)
		}
	}
}

func TestTokenHookError(t *testing.T) {
	hook := func(context.Context, permission.Operation, string, permission.Context) (bool, error) {
		return false, fmt.Errorf("hook exploded")
	}
	h := newTokenHandler(t, hook)

	for _, path := range []string{
		"/api/models/org/repo/xet-write-token/main",
		"/api/models/org/repo/xet-read-token/main",
	} {
		if rec := get(h, path); rec.Code != http.StatusInternalServerError {
			t.Errorf("hook error GET %s status = %d, want 500", path, rec.Code)
		}
	}
}

// assertMintedToken checks the dual contract: JSON body plus matching X-Xet-* headers.
func assertMintedToken(t *testing.T, path string, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, want 200", path, rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("GET %s Content-Type = %q, want application/json", path, got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("GET %s Cache-Control = %q, want no-store", path, got)
	}
	var tok struct {
		CasURL      string `json:"casUrl"`
		AccessToken string `json:"accessToken"`
		Exp         int64  `json:"exp"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&tok); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	if tok.CasURL == "" || tok.AccessToken == "" || tok.Exp <= time.Now().Unix() {
		t.Fatalf("GET %s token incomplete: %+v", path, tok)
	}
	// httptest requests carry host example.com; the mirror derives the base.
	if tok.CasURL != "http://example.com" {
		t.Errorf("GET %s casUrl = %q, want http://example.com", path, tok.CasURL)
	}
	if got := rec.Header().Get("X-Xet-Cas-Url"); got != tok.CasURL {
		t.Errorf("GET %s X-Xet-Cas-Url = %q, want %q", path, got, tok.CasURL)
	}
	if got := rec.Header().Get("X-Xet-Access-Token"); got != tok.AccessToken {
		t.Errorf("GET %s X-Xet-Access-Token = %q, want %q", path, got, tok.AccessToken)
	}
	if got := rec.Header().Get("X-Xet-Token-Expiration"); got != strconv.FormatInt(tok.Exp, 10) {
		t.Errorf("GET %s X-Xet-Token-Expiration = %q, want %d", path, got, tok.Exp)
	}
}

func TestTokenMints(t *testing.T) {
	hook := func(context.Context, permission.Operation, string, permission.Context) (bool, error) {
		return true, nil
	}
	h := newTokenHandler(t, hook)

	for _, path := range []string{
		"/api/models/org/repo/xet-write-token/main",
		"/api/models/org/repo/xet-read-token/main",
		"/xet-token",
	} {
		assertMintedToken(t, path, get(h, path))
	}
}

func TestXetTokenSkipsHook(t *testing.T) {
	var hookCalls []string
	hook := func(_ context.Context, op permission.Operation, repoName string, _ permission.Context) (bool, error) {
		hookCalls = append(hookCalls, fmt.Sprintf("%v %s", op, repoName))
		return false, nil
	}
	h := newTokenHandler(t, hook)

	// A denying hook must not matter: /xet-token has no repo context to gate.
	assertMintedToken(t, "/xet-token", get(h, "/xet-token"))
	if len(hookCalls) != 0 {
		t.Errorf("hook called for /xet-token: %v", hookCalls)
	}
}

func TestReadTokenEscapedRevision(t *testing.T) {
	hook := func(context.Context, permission.Operation, string, permission.Context) (bool, error) {
		return true, nil
	}
	h := newTokenHandler(t, hook)

	// UseEncodedPath keeps the escaped revision a single segment.
	path := "/api/models/org/repo/xet-read-token/refs%2Fpr%2F1"
	assertMintedToken(t, path, get(h, path))
}

func TestTokenDelegatesNonGET(t *testing.T) {
	var events []string
	hook := func(_ context.Context, op permission.Operation, _ string, _ permission.Context) (bool, error) {
		events = append(events, fmt.Sprintf("gate %v", op))
		return true, nil
	}
	m, _ := newXETStack(t)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		events = append(events, "next")
		w.WriteHeader(http.StatusTeapot)
	})
	h := NewHandler(WithMirror(m), WithPermissionHookFunc(hook), WithNext(next))

	// Non-GET token semantics belong to the composition mounted as next.
	for _, tt := range []struct {
		path       string
		wantEvents []string
	}{
		{"/api/models/org/repo/xet-write-token/main", []string{fmt.Sprintf("gate %v", permission.OperationUpdateRepo), "next"}},
		{"/api/models/org/repo/xet-read-token/main", []string{fmt.Sprintf("gate %v", permission.OperationReadRepo), "next"}},
		{"/xet-token", []string{"next"}},
	} {
		events = nil
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, tt.path, nil))
		if !slices.Equal(events, tt.wantEvents) {
			t.Errorf("POST %s events = %v, want %v (gate before next)", tt.path, events, tt.wantEvents)
		}
		if rec.Code != http.StatusTeapot {
			t.Errorf("POST %s status = %d, want 418 delegated to next", tt.path, rec.Code)
		}
	}
}

func TestNilMirrorDelegates(t *testing.T) {
	h := NewHandler(WithNext(sentinelNext{}))

	for _, path := range []string{
		"/api/models/org/repo/xet-write-token/main",
		"/api/models/org/repo/xet-read-token/main",
		"/xet-token",
	} {
		if rec := get(h, path); rec.Code != http.StatusTeapot {
			t.Errorf("nil-mirror GET %s status = %d, want 418 delegated to next", path, rec.Code)
		}
	}
}

func TestNoMintMirrorDelegates(t *testing.T) {
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
	// No WithMintToken: CanMintToken() is false.
	m, err := mirror.NewMirror(
		mirror.WithXETStorage(xs),
		mirror.WithXETClient(client),
	)
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}
	h := NewHandler(WithMirror(m), WithNext(sentinelNext{}))

	for _, path := range []string{
		"/api/models/org/repo/xet-write-token/main",
		"/api/models/org/repo/xet-read-token/main",
		"/xet-token",
	} {
		if rec := get(h, path); rec.Code != http.StatusTeapot {
			t.Errorf("no-mint GET %s status = %d, want 418 delegated to next", path, rec.Code)
		}
	}
}

func TestTokenRoutesShadowHubFrontEnd(t *testing.T) {
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

	// The hub front end behind next also answers these routes but never sets
	// Cache-Control — assertMintedToken proves cas answered first.
	rec := get(h, "/api/models/org/repo/xet-read-token/main")
	assertMintedToken(t, "/api/models/org/repo/xet-read-token/main", rec)
	wantCalls := []string{fmt.Sprintf("%v org/repo", permission.OperationReadRepo)}
	if !slices.Equal(hookCalls, wantCalls) {
		t.Errorf("read-token hook calls = %v, want %v", hookCalls, wantCalls)
	}

	hookCalls = nil
	assertMintedToken(t, "/xet-token", get(h, "/xet-token"))
	if len(hookCalls) != 0 {
		t.Errorf("hook called for /xet-token: %v", hookCalls)
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
		"/api/foobar/org/repo/xet-read-token/main",
		"/api/models/org/repo/extra/xet-read-token/main",
		"/api/models/org/repo/xet-read-token/",
		// Vars that decode to extra separators name no canonical repo.
		"/api/models/org%2Fx/repo/xet-write-token/main",
		"/api/models/org/re%2Fpo/xet-read-token/main",
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
