package hf

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	xetclient "github.com/wzshiming/xet/client"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/permission"
)

type sentinelNext struct{}

func (sentinelNext) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusTeapot)
}

type hookCall struct {
	op   permission.Operation
	repo string
	ctx  permission.Context
}

func newXETMirror(t *testing.T, opts ...mirror.Option) *mirror.Mirror {
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
	m, err := mirror.NewMirror(append([]mirror.Option{
		mirror.WithXETStorage(xs),
		mirror.WithXETClient(client),
	}, opts...)...)
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}
	return m
}

func newMintingMirror(t *testing.T, opts ...mirror.Option) (*mirror.Mirror, *auth.Issuer) {
	t.Helper()
	issuer, err := auth.NewIssuer(nil, time.Hour, nil)
	if err != nil {
		t.Fatalf("new issuer: %v", err)
	}
	return newXETMirror(t, append([]mirror.Option{mirror.WithMintToken(issuer.Sign)}, opts...)...), issuer
}

func newTokenHandler(t *testing.T, hook permission.PermissionHookFunc) *Handler {
	t.Helper()
	m, _ := newMintingMirror(t)
	return NewHandler(WithMirror(m), WithPermissionHookFunc(hook), WithNext(sentinelNext{}))
}

func serveGet(h *Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func hasToken(rec *httptest.ResponseRecorder) bool {
	return rec.Header().Get("X-Xet-Access-Token") != "" || strings.Contains(rec.Body.String(), "accessToken")
}

func assertMintedToken(t *testing.T, path string, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, want 200; body: %s", path, rec.Code, rec.Body.String())
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

func TestXETTokenGate(t *testing.T) {
	allow, hookErr := true, error(nil)
	var calls []hookCall
	hook := func(_ context.Context, op permission.Operation, repoName string, permCtx permission.Context) (bool, error) {
		calls = append(calls, hookCall{op, repoName, permCtx})
		return allow, hookErr
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
		calls = nil
		rec := serveGet(h, tt.path)
		want := []hookCall{{tt.wantOp, tt.wantRepo, permission.Context{Ref: "main"}}}
		if rec.Code != http.StatusOK || !slices.Equal(calls, want) {
			t.Errorf("GET %s status = %d, hook calls = %+v; want 200 and %+v", tt.path, rec.Code, calls, want)
		}
	}

	for _, tt := range []struct {
		name  string
		allow bool
		err   error
		code  int
	}{
		{"denied", false, nil, http.StatusForbidden},
		{"hook error", false, fmt.Errorf("hook exploded"), http.StatusInternalServerError},
	} {
		allow, hookErr = tt.allow, tt.err
		for _, path := range []string{
			"/api/models/org/repo/xet-write-token/main",
			"/api/models/org/repo/xet-read-token/main",
		} {
			rec := serveGet(h, path)
			if rec.Code != tt.code || !json.Valid(rec.Body.Bytes()) || hasToken(rec) {
				t.Errorf("%s GET %s status = %d, body = %s; want %d JSON without a token", tt.name, path, rec.Code, rec.Body.String(), tt.code)
			}
		}
	}
}

func TestXETTokenMints(t *testing.T) {
	m, issuer := newMintingMirror(t)
	// Default handler: nil hook allows, nil next answers 404.
	h := NewHandler(WithMirror(m))
	for _, tt := range []struct {
		path  string
		grant auth.Grant
	}{
		{"/api/models/org/repo/xet-write-token/main", auth.Grant{Permission: auth.Write}},
		{"/api/models/org/repo/xet-read-token/main", auth.Grant{Permission: auth.Read}},
	} {
		rec := serveGet(h, tt.path)
		assertMintedToken(t, tt.path, rec)
		if grant, ok := issuer.Validate(rec.Header().Get("X-Xet-Access-Token")); !ok || grant != tt.grant {
			t.Errorf("GET %s grant = %+v, valid = %v, want %+v", tt.path, grant, ok, tt.grant)
		}
	}
}

func TestXETTokenMintError(t *testing.T) {
	m, _ := newMintingMirror(t, mirror.WithMintToken(func(auth.Grant) (string, int64, error) {
		return "", 0, fmt.Errorf("mint failed")
	}))
	h := NewHandler(WithMirror(m))
	for _, path := range []string{
		"/api/models/org/repo/xet-write-token/main",
		"/api/models/org/repo/xet-read-token/main",
	} {
		rec := serveGet(h, path)
		if rec.Code != http.StatusInternalServerError || !json.Valid(rec.Body.Bytes()) || hasToken(rec) {
			t.Errorf("GET %s status = %d, body = %s; want 500 JSON without a token", path, rec.Code, rec.Body.String())
		}
	}
}

func TestXETTokenEscapedRevision(t *testing.T) {
	var calls []hookCall
	hook := func(_ context.Context, op permission.Operation, repoName string, permCtx permission.Context) (bool, error) {
		calls = append(calls, hookCall{op, repoName, permCtx})
		return true, nil
	}
	m, issuer := newMintingMirror(t)
	h := NewHandler(WithMirror(m), WithPermissionHookFunc(hook), WithNext(sentinelNext{}))

	// The decoded router hands the hook the unescaped revision.
	for _, tt := range []struct {
		path  string
		want  hookCall
		grant auth.Grant
	}{
		{"/api/models/org/repo/xet-read-token/refs%2Fpr%2F1", hookCall{permission.OperationReadRepo, "org/repo", permission.Context{Ref: "refs/pr/1"}}, auth.Grant{Permission: auth.Read}},
		{"/api/datasets/org/repo/xet-write-token/refs%2Fpr%2F1", hookCall{permission.OperationUpdateRepo, "datasets/org/repo", permission.Context{Ref: "refs/pr/1"}}, auth.Grant{Permission: auth.Write}},
	} {
		calls = nil
		rec := serveGet(h, tt.path)
		assertMintedToken(t, tt.path, rec)
		if !slices.Equal(calls, []hookCall{tt.want}) {
			t.Errorf("GET %s hook calls = %+v, want [%+v]", tt.path, calls, tt.want)
		}
		if grant, ok := issuer.Validate(rec.Header().Get("X-Xet-Access-Token")); !ok || grant != tt.grant {
			t.Errorf("GET %s grant = %+v, valid = %v, want %+v", tt.path, grant, ok, tt.grant)
		}
	}
}

func TestXETTokenUnavailable(t *testing.T) {
	allow, hookErr, calls := true, error(nil), 0
	hook := func(context.Context, permission.Operation, string, permission.Context) (bool, error) {
		calls++
		return allow, hookErr
	}
	for _, tt := range []struct {
		name string
		h    *Handler
	}{
		// Default next: a nil mirror must answer, not dereference.
		{"nil mirror", NewHandler(WithPermissionHookFunc(hook))},
		{"no mint", NewHandler(WithMirror(newXETMirror(t)), WithPermissionHookFunc(hook), WithNext(sentinelNext{}))},
	} {
		for _, path := range []string{
			"/api/models/org/repo/xet-write-token/main",
			"/api/models/org/repo/xet-read-token/main",
		} {
			allow, hookErr, calls = true, nil, 0
			rec := serveGet(tt.h, path)
			if rec.Code != http.StatusNotFound || !json.Valid(rec.Body.Bytes()) || calls != 1 {
				t.Errorf("%s GET %s status = %d, body = %s, hook calls = %d; want 404 JSON after one gate call", tt.name, path, rec.Code, rec.Body.String(), calls)
			}
			// The gate still decides before availability is reported.
			allow = false
			if rec := serveGet(tt.h, path); rec.Code != http.StatusForbidden {
				t.Errorf("%s denied GET %s status = %d, want 403", tt.name, path, rec.Code)
			}
			hookErr = fmt.Errorf("hook exploded")
			if rec := serveGet(tt.h, path); rec.Code != http.StatusInternalServerError {
				t.Errorf("%s hook error GET %s status = %d, want 500", tt.name, path, rec.Code)
			}
		}
	}
}

func TestXETTokenMethodNotAllowed(t *testing.T) {
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

	for _, method := range []string{http.MethodPost, http.MethodHead} {
		for _, path := range []string{
			"/api/models/org/repo/xet-write-token/main",
			"/api/models/org/repo/xet-read-token/main",
		} {
			events = nil
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
			if rec.Code != http.StatusMethodNotAllowed || len(events) != 0 || hasToken(rec) {
				t.Errorf("%s %s status = %d, events = %v, body = %s; want 405 with no gate, next or token", method, path, rec.Code, events, rec.Body.String())
			}
		}
	}
}

func TestXETTokenRoutesFallThrough(t *testing.T) {
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
		// Encoded separators decode to extra segments that name no route.
		"/api/models/org%2Fx/repo/xet-write-token/main",
		"/api/models/org/re%2Fpo/xet-read-token/main",
		"/xet-token",
		"/xet-token/not-a-hash",
		"/xet-token/" + (xet.FileHash{1, 2, 3}).String(),
		"/xet-bridge/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		"/v1/xorbs/whatever",
	} {
		if rec := serveGet(h, path); rec.Code != http.StatusTeapot {
			t.Errorf("GET %s status = %d, want 418 fall-through", path, rec.Code)
		}
	}
	if len(hookCalls) != 0 {
		t.Errorf("hook called for fall-through paths: %v", hookCalls)
	}
}
