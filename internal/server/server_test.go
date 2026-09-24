package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendssh "github.com/matrixhub-ai/hfd/pkg/backend/ssh"
	"github.com/matrixhub-ai/hfd/pkg/gc"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
	"golang.org/x/crypto/ssh"
)

func newStorage(t *testing.T, opts ...storage.Option) *storage.Storage {
	t.Helper()
	st, err := storage.NewStorage(append([]storage.Option{storage.WithRootDir(t.TempDir())}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestChainOrder(t *testing.T) {
	var got []string
	tag := func(name string) middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = append(got, name)
				next.ServeHTTP(w, r)
			})
		}
	}
	tail := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, "tail")
		w.WriteHeader(http.StatusTeapot)
	})
	serve := func(h http.Handler) int {
		got = nil
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		return rec.Code
	}

	cases := []struct {
		name string
		h    http.Handler
		want []string
	}{
		{"first arg is outermost", chain(tail, tag("a"), tag("b"), tag("c")), []string{"a", "b", "c", "tail"}},
		{"no middlewares is tail", chain(tail), []string{"tail"}},
		{"passthrough is next", passthrough(tail), []string{"tail"}},
	}
	for _, tc := range cases {
		if code := serve(tc.h); code != http.StatusTeapot {
			t.Errorf("%s: status = %d, want %d", tc.name, code, http.StatusTeapot)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: order = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestNewHTTPHandler(t *testing.T) {
	var accessLog bytes.Buffer
	issuer, err := auth.NewIssuer(nil, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	fileHash := xet.FileHash{1, 2, 3}
	casToken, _, err := issuer.Sign(auth.Grant{Permission: auth.Read, File: &fileHash})
	if err != nil {
		t.Fatal(err)
	}
	xsStorage := newStorage(t)
	xs := xsStorage.XETStorage()
	casOptions := Options{
		Storage:        newStorage(t, storage.WithXETStorage(xs)),
		CASAuthorizer:  issuer,
		Authenticators: &authenticate.Authenticators{Token: authenticate.NewSimpleTokenValidator("bob", "t0k")},
	}
	bobToken := &authenticate.Authenticators{Token: authenticate.NewSimpleTokenValidator("bob", "t0k")}
	deny := func(context.Context, permission.Operation, string, permission.Context) (bool, error) {
		return false, nil
	}
	hooks := &Hooks{Storage: newStorage(t)}
	cases := []struct {
		name        string
		options     Options
		path        string
		method      string
		token       string
		status      int
		body        string
		identity    authenticate.Identity
		defaultNext bool
	}{
		{name: "anonymous tail", path: "/nothing", status: http.StatusTeapot, identity: authenticate.Anonymous},
		{name: "authentication override", options: Options{Authenticate: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				next.ServeHTTP(w, request.WithContext(authenticate.WithIdentity(request.Context(), authenticate.NewIdentity("alice", ""))))
			})
		}}, path: "/nothing", status: http.StatusTeapot, identity: authenticate.NewIdentity("alice", "")},
		{name: "token before hf", options: Options{Authenticators: bobToken, HFOptions: []backendhf.Option{backendhf.WithWhoamiFunc(hooks.Whoami)}},
			path: "/api/whoami-v2", token: "t0k", status: http.StatusOK, body: `"name":"bob"`},
		{name: "nil whoami falls to Next", options: Options{Authenticators: bobToken}, path: "/api/whoami-v2", token: "t0k", status: http.StatusTeapot, identity: authenticate.NewIdentity("bob", "")},
		{name: "nil list falls to Next", path: "/api/models", status: http.StatusTeapot, identity: authenticate.Anonymous},
		{name: "custom whoami sees identity", options: Options{Authenticators: bobToken, HFOptions: []backendhf.Option{
			backendhf.WithWhoamiFunc(func(ctx context.Context) (*backendhf.WhoamiResponse, error) {
				return &backendhf.WhoamiResponse{Name: "custom-" + authenticate.IdentityFrom(ctx).Name()}, nil
			}),
		}}, path: "/api/whoami-v2", token: "t0k", status: http.StatusOK, body: `"name":"custom-bob"`},
		{name: "CAS before user authentication", options: casOptions, path: "/v1/reconstructions/" + fileHash.String(), token: casToken, status: http.StatusNotFound},
		{name: "CAS token is not a user", options: casOptions, path: "/api/whoami-v2", token: casToken, status: http.StatusUnauthorized},
		{name: "nil CAS authorizer denies read", path: "/v1/reconstructions/" + fileHash.String(), status: http.StatusUnauthorized},
		{name: "nil CAS authorizer denies write", method: http.MethodPost, path: "/v1/xorbs/default/" + fileHash.String(), status: http.StatusUnauthorized},
		{name: "internal disabled", path: "/internal/objects", status: http.StatusTeapot, identity: authenticate.Anonymous},
		{name: "internal default tail", path: "/internal/objects", status: http.StatusNotFound, defaultNext: true},
		{name: "internal usage disabled", path: "/internal/usage", status: http.StatusTeapot, identity: authenticate.Anonymous},
		{name: "internal usage enabled", options: Options{InternalGC: gc.NewCollector(xsStorage.RepositoriesFS(), xs)}, path: "/internal/usage", status: http.StatusOK},
		{name: "access log", options: Options{AccessLog: &accessLog}, path: "/nothing", status: http.StatusTeapot, identity: authenticate.Anonymous},
		{name: "permission denied", options: Options{Permission: deny, HFOptions: []backendhf.Option{backendhf.WithListReposFunc(hooks.ListRepos)}}, path: "/api/models", status: http.StatusForbidden},
		{name: "permission denied skips callback", options: Options{Permission: deny, HFOptions: []backendhf.Option{
			backendhf.WithListReposFunc(func(context.Context, string, backendhf.ListQuery) ([]backendhf.RepoListItem, bool, error) {
				t.Error("list callback ran despite permission denial")
				return nil, false, nil
			}),
		}}, path: "/api/models", status: http.StatusForbidden},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			options := test.options
			if options.Storage == nil {
				options.Storage = newStorage(t)
			}
			var gotIdentity authenticate.Identity
			if !test.defaultNext {
				options.Next = http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
					gotIdentity = authenticate.IdentityFrom(request.Context())
					w.WriteHeader(http.StatusTeapot)
				})
			}
			method := test.method
			if method == "" {
				method = http.MethodGet
			}
			request := httptest.NewRequest(method, test.path, nil)
			if test.token != "" {
				request.Header.Set("Authorization", "Bearer "+test.token)
			}
			recorder := httptest.NewRecorder()
			NewHTTPHandler(options).ServeHTTP(recorder, request)
			if recorder.Code != test.status {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, test.status, recorder.Body.String())
			}
			if !strings.Contains(recorder.Body.String(), test.body) {
				t.Errorf("body = %s, want containing %s", recorder.Body.String(), test.body)
			}
			if !reflect.DeepEqual(gotIdentity, test.identity) {
				t.Errorf("tail identity = %#v, want %#v", gotIdentity, test.identity)
			}
			if options.AccessLog != nil && accessLog.Len() == 0 {
				t.Error("access log is empty")
			}
		})
	}
}

// TestNewHTTPHandlerCatalogLifecycle drives the six catalog routes through the chain with Hooks wired like cmd/hfd.
func TestNewHTTPHandlerCatalogLifecycle(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	st := newStorage(t)
	hooks := &Hooks{Storage: st}
	handler := NewHTTPHandler(Options{
		Storage:        st,
		Authenticators: &authenticate.Authenticators{Token: authenticate.NewSimpleTokenValidator("bob", "t0k")},
		PreReceive:     hooks.PreReceive,
		PostReceive:    hooks.PostReceive,
		HFOptions: []backendhf.Option{
			backendhf.WithCreateRepoFunc(hooks.CreateRepo),
			backendhf.WithDeleteRepoFunc(hooks.DeleteRepo),
			backendhf.WithMoveRepoFunc(hooks.MoveRepo),
			backendhf.WithUpdateRepoSettingsFunc(hooks.UpdateRepoSettings),
			backendhf.WithListReposFunc(hooks.ListRepos),
			backendhf.WithWhoamiFunc(hooks.Whoami),
		},
	})
	do := func(t *testing.T, method, path, body string, want int) string {
		t.Helper()
		request := httptest.NewRequest(method, path, strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer t0k")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != want {
			t.Fatalf("%s %s: status = %d, want %d: %s", method, path, recorder.Code, want, recorder.Body)
		}
		return recorder.Body.String()
	}
	repoPath := func(name string) string { return repository.ResolvePath(name) }

	do(t, http.MethodPost, "/api/repos/create", `{"type":"dataset","name":"repo","organization":"org"}`, http.StatusOK)
	if !repository.IsRepository(st.RepositoriesFS(), repoPath("datasets/org/repo")) {
		t.Fatal("create did not initialize datasets/org/repo on the storage")
	}
	if body := do(t, http.MethodGet, "/api/datasets?author=org", "", http.StatusOK); !strings.Contains(body, `"id":"org/repo"`) {
		t.Errorf("list = %s, want org/repo", body)
	}
	do(t, http.MethodPut, "/api/datasets/org/repo/settings", `{"private":true}`, http.StatusOK)
	do(t, http.MethodPost, "/api/repos/move", `{"fromRepo":"org/repo","toRepo":"org/moved","type":"dataset"}`, http.StatusOK)
	if !repository.IsRepository(st.RepositoriesFS(), repoPath("datasets/org/moved")) {
		t.Fatal("move did not rename the repository on the storage")
	}
	do(t, http.MethodDelete, "/api/repos/delete", `{"type":"dataset","name":"moved","organization":"org"}`, http.StatusOK)
	if repository.IsRepository(st.RepositoriesFS(), repoPath("datasets/org/moved")) {
		t.Fatal("delete left the repository on the storage")
	}
	if body := do(t, http.MethodGet, "/api/whoami-v2", "", http.StatusOK); !strings.Contains(body, `"name":"bob"`) {
		t.Errorf("whoami = %s, want bob", body)
	}
	for _, want := range []string{
		`msg="Create repository" user=bob repo=datasets/org/repo`,
		`msg="Post-receive hook" user=bob repo=datasets/org/repo`,
		`msg="List repositories" user=bob repoType=datasets author=org`,
		`msg="Update repository settings" user=bob repo=datasets/org/repo`,
		`msg="Move repository" user=bob from=datasets/org/repo to=datasets/org/moved`,
		`msg="Delete repository" user=bob repo=datasets/org/moved`,
		`msg=Whoami user=bob`,
	} {
		if strings.Count(logs.String(), want) != 1 {
			t.Errorf("logs = %s\nwant exactly one %q", logs.String(), want)
		}
	}
}

func TestNewSSHServer(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	hostKey, err := ssh.NewSignerFromKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if got := NewSSHServer(Options{
		Storage:    newStorage(t),
		SSHOptions: []backendssh.Option{backendssh.WithLFSURL("https://example.com")},
	}, hostKey); got == nil {
		t.Fatal("SSH server is nil")
	}
}

func TestStorageRequired(t *testing.T) {
	for name, build := range map[string]func(){
		"HTTP": func() { NewHTTPHandler(Options{}) },
		"SSH":  func() { NewSSHServer(Options{}, nil) },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if got := recover(); got != "server: Options.Storage is required" {
					t.Errorf("panic = %v, want required storage", got)
				}
			}()
			build()
		})
	}
}
