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

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	xetclient "github.com/wzshiming/xet/client"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/permission"
)

// sentinelNext marks requests that fell past the handler's route table.
type sentinelNext struct{}

func (sentinelNext) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusTeapot)
}

// newMintingMirror assembles a mirror that can mint CAS credentials, the
// pieces built the way cmd/hfd does.
func newMintingMirror(t *testing.T, opts ...mirror.Option) (*mirror.Mirror, *auth.Issuer) {
	t.Helper()
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
	issuer, err := auth.NewIssuer(nil, time.Hour, nil)
	if err != nil {
		t.Fatalf("new issuer: %v", err)
	}
	m, err := mirror.NewMirror(append([]mirror.Option{
		mirror.WithXETStorage(xs),
		mirror.WithXETClient(client),
		mirror.WithMintToken(issuer.Sign),
	}, opts...)...)
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}
	return m, issuer
}

// newTokenHandler builds a handler over the minting mirror with the sentinel
// as next, so tests observe requests that delegate past the route table.
func newTokenHandler(t *testing.T, hook permission.PermissionHookFunc) *Handler {
	t.Helper()
	m, _ := newMintingMirror(t)
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

func TestFileTokenRoutes(t *testing.T) {
	h := newTokenHandler(t, nil)
	for _, tt := range []struct {
		path string
		code int
	}{
		{"/xet-token", http.StatusTeapot},
		{"/xet-token/not-a-hash", http.StatusTeapot},
		{"/xet-token/" + (xet.FileHash{1, 2, 3}).String(), http.StatusTeapot},
	} {
		t.Run(tt.path, func(t *testing.T) {
			rec := get(h, tt.path)
			if rec.Code != tt.code {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tt.code, rec.Body.String())
			}
		})
	}
}

func TestTokenMints(t *testing.T) {
	hook := func(context.Context, permission.Operation, string, permission.Context) (bool, error) {
		return true, nil
	}
	m, issuer := newMintingMirror(t)
	h := NewHandler(WithMirror(m), WithPermissionHookFunc(hook))
	for _, tt := range []struct {
		path  string
		grant auth.Grant
	}{
		{"/api/models/org/repo/xet-write-token/main", auth.Grant{Permission: auth.Write}},
		{"/api/models/org/repo/xet-read-token/main", auth.Grant{Permission: auth.Read}},
	} {
		rec := get(h, tt.path)
		assertMintedToken(t, tt.path, rec)
		if grant, ok := issuer.Validate(rec.Header().Get("X-Xet-Access-Token")); !ok || grant != tt.grant {
			t.Errorf("GET %s grant = %+v, valid = %v, want %+v", tt.path, grant, ok, tt.grant)
		}
	}
}

func TestTokenMintError(t *testing.T) {
	m, _ := newMintingMirror(t, mirror.WithMintToken(func(auth.Grant) (string, int64, error) {
		return "", 0, fmt.Errorf("mint failed")
	}))
	h := NewHandler(WithMirror(m))
	for _, path := range []string{
		"/api/models/org/repo/xet-write-token/main",
		"/api/models/org/repo/xet-read-token/main",
	} {
		rec := get(h, path)
		if rec.Code != http.StatusInternalServerError || !json.Valid(rec.Body.Bytes()) {
			t.Errorf("GET %s status = %d, body = %s", path, rec.Code, rec.Body.String())
		}
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
	m, _ := newMintingMirror(t)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		events = append(events, "next")
		w.WriteHeader(http.StatusTeapot)
	})
	h := NewHandler(WithMirror(m), WithPermissionHookFunc(hook), WithNext(next))

	// Non-GET token semantics belong to the chain behind this handler.
	for _, tt := range []struct {
		path       string
		wantEvents []string
	}{
		{"/api/models/org/repo/xet-write-token/main", []string{fmt.Sprintf("gate %v", permission.OperationUpdateRepo), "next"}},
		{"/api/models/org/repo/xet-read-token/main", []string{fmt.Sprintf("gate %v", permission.OperationReadRepo), "next"}},
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
	} {
		if rec := get(h, path); rec.Code != http.StatusTeapot {
			t.Errorf("no-mint GET %s status = %d, want 418 delegated to next", path, rec.Code)
		}
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
