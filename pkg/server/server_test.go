package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	xetlocal "github.com/wzshiming/xet/storage/local"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	backendssh "github.com/matrixhub-ai/hfd/pkg/backend/ssh"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/storage"
	"golang.org/x/crypto/ssh"
)

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
	xs, err := xetlocal.NewStorage(xetlocal.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	casOptions := Options{
		XETStorage:     xs,
		CASAuthorizer:  issuer,
		Authenticators: &authenticate.Authenticators{Token: authenticate.NewSimpleTokenValidator("bob", "t0k")},
	}
	cases := []struct {
		name        string
		options     Options
		path        string
		method      string
		token       string
		status      int
		identity    authenticate.Identity
		defaultNext bool
	}{
		{name: "anonymous tail", path: "/nothing", status: http.StatusTeapot, identity: authenticate.Anonymous},
		{name: "authentication override", options: Options{Authenticate: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				next.ServeHTTP(w, request.WithContext(authenticate.WithIdentity(request.Context(), authenticate.NewIdentity("alice", ""))))
			})
		}}, path: "/nothing", status: http.StatusTeapot, identity: authenticate.NewIdentity("alice", "")},
		{name: "token before hf", options: Options{Authenticators: &authenticate.Authenticators{Token: authenticate.NewSimpleTokenValidator("bob", "t0k")}}, path: "/api/whoami-v2", token: "t0k", status: http.StatusOK},
		{name: "CAS before user authentication", options: casOptions, path: "/v1/reconstructions/" + fileHash.String(), token: casToken, status: http.StatusNotFound},
		{name: "CAS token is not a user", options: casOptions, path: "/api/whoami-v2", token: casToken, status: http.StatusUnauthorized},
		{name: "nil CAS authorizer denies read", options: Options{XETStorage: xs}, path: "/v1/reconstructions/" + fileHash.String(), status: http.StatusUnauthorized},
		{name: "nil CAS authorizer denies write", options: Options{XETStorage: xs}, method: http.MethodPost, path: "/v1/xorbs/default/" + fileHash.String(), status: http.StatusUnauthorized},
		{name: "nil XET storage delegates", path: "/v1/reconstructions/" + fileHash.String(), status: http.StatusTeapot, identity: authenticate.Anonymous},
		{name: "internal disabled", path: "/internal/objects", status: http.StatusTeapot, identity: authenticate.Anonymous},
		{name: "internal default tail", path: "/internal/objects", status: http.StatusNotFound, defaultNext: true},
		{name: "access log", options: Options{AccessLog: &accessLog}, path: "/nothing", status: http.StatusTeapot, identity: authenticate.Anonymous},
		{name: "permission denied", options: Options{Permission: func(context.Context, permission.Operation, string, permission.Context) (bool, error) {
			return false, nil
		}}, path: "/api/models", status: http.StatusForbidden},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			options := test.options
			options.Storage = storage.NewStorage(storage.WithRootDir(t.TempDir()))
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
			if !reflect.DeepEqual(gotIdentity, test.identity) {
				t.Errorf("tail identity = %#v, want %#v", gotIdentity, test.identity)
			}
			if options.AccessLog != nil && accessLog.Len() == 0 {
				t.Error("access log is empty")
			}
		})
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
		Storage:    storage.NewStorage(storage.WithRootDir(t.TempDir())),
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
