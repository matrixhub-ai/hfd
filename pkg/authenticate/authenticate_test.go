package authenticate

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type tokenSignValidatorFunc func(context.Context, string, string, string) (string, bool, bool, error)

func (validate tokenSignValidatorFunc) Validate(ctx context.Context, method, path, token string) (string, bool, bool, error) {
	return validate(ctx, method, path, token)
}

func (tokenSignValidatorFunc) Sign(context.Context, string, string, string, time.Duration) (string, error) {
	panic("unexpected Sign call")
}

func TestBasicAuthHandlerContract(t *testing.T) {
	testMiddlewareContract(t, true, func(validate func() (string, bool, bool, error), next http.Handler) http.Handler {
		return BasicAuthHandler(BasicAuthValidatorFunc(func(context.Context, string, string) (string, bool, bool, error) {
			return validate()
		}), next)
	})
}

func TestTokenValidatorHandlerContract(t *testing.T) {
	testMiddlewareContract(t, false, func(validate func() (string, bool, bool, error), next http.Handler) http.Handler {
		return TokenValidatorHandler(TokenValidatorFunc(func(context.Context, string) (string, bool, bool, error) {
			return validate()
		}), next)
	})
}

func TestTokenSignValidatorHandlerContract(t *testing.T) {
	testMiddlewareContract(t, false, func(validate func() (string, bool, bool, error), next http.Handler) http.Handler {
		return TokenSignValidatorHandler(tokenSignValidatorFunc(func(context.Context, string, string, string) (string, bool, bool, error) {
			return validate()
		}), next)
	})
}

func testMiddlewareContract(t *testing.T, basic bool, middleware func(func() (string, bool, bool, error), http.Handler) http.Handler) {
	t.Helper()
	alice := NewIdentity("alice", "alice@example.com")
	failed := errors.New("validator failed")
	for _, test := range []struct {
		name       string
		user       string
		next, ok   bool
		err        error
		credential bool
		existing   Identity
		wantCode   int
		want       Identity
	}{
		{"declined", "", true, false, nil, true, nil, http.StatusOK, Anonymous},
		{"rejected", "", false, false, nil, true, nil, http.StatusUnauthorized, nil},
		{"internal error", "", false, false, failed, true, nil, http.StatusInternalServerError, nil},
		{"error beats ok", "alice", false, true, failed, true, nil, http.StatusInternalServerError, nil},
		{"authenticated", "alice", false, true, nil, true, nil, http.StatusOK, NewIdentity("alice", "")},
		{"authenticated despite next", "alice", true, true, nil, true, nil, http.StatusOK, NewIdentity("alice", "")},
		{"user ignored when rejected", "alice", false, false, nil, true, nil, http.StatusUnauthorized, nil},
		{"user ignored when declined", "alice", true, false, nil, true, nil, http.StatusOK, Anonymous},
		{"unnamed user", "", false, true, nil, true, nil, http.StatusOK, Anonymous},
		{"explicit anonymous", "alice", false, true, nil, true, Anonymous, http.StatusOK, NewIdentity("alice", "")},
		{"no credential", "", false, false, nil, false, nil, http.StatusOK, Anonymous},
		{"already authenticated", "", false, false, nil, true, alice, http.StatusOK, alice},
	} {
		t.Run(test.name, func(t *testing.T) {
			called, reached := false, false
			wantCalled := test.credential && (test.existing == nil || test.existing == Anonymous)
			next := AnonymousAuthenticateHandler(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				reached = true
				if got := IdentityFrom(request.Context()); got != test.want {
					t.Errorf("identity = %v, want %v", got, test.want)
				}
				if got := IsAnonymous(IdentityFrom(request.Context())); got != (test.want == Anonymous) {
					t.Errorf("IsAnonymous = %v, want %v", got, test.want == Anonymous)
				}
			}))
			handler := middleware(func() (string, bool, bool, error) {
				called = true
				if !wantCalled {
					t.Fatal("validator must not be called")
				}
				return test.user, test.next, test.ok, test.err
			}, next)
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			if test.credential {
				if basic {
					request.SetBasicAuth("alice", "secret")
				} else {
					request.Header.Set("Authorization", "Bearer token")
				}
			}
			if test.existing != nil {
				request = request.WithContext(WithIdentity(request.Context(), test.existing))
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.wantCode || reached != (test.wantCode == http.StatusOK) || called != wantCalled {
				t.Errorf("code=%d reached=%v called=%v; want code=%d", recorder.Code, reached, called, test.wantCode)
			}
			wantChallenge := ""
			if basic && test.wantCode == http.StatusUnauthorized {
				wantChallenge = `Basic realm="hfd"`
			}
			if got := recorder.Header().Get("WWW-Authenticate"); got != wantChallenge {
				t.Errorf("WWW-Authenticate = %q, want %q", got, wantChallenge)
			}
		})
	}
}

func TestAuthenticateBasicAuth(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	basicAuth := NewSimpleBasicAuthValidator("admin", "secret")
	handler := BasicAuthHandler(basicAuth, AnonymousAuthenticateHandler(inner))

	t.Run("valid basic auth", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.SetBasicAuth("admin", "secret")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", rr.Code)
		}
	})

	t.Run("invalid basic auth", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.SetBasicAuth("admin", "wrong")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Errorf("Expected 401, got %d", rr.Code)
		}
	})

	t.Run("no auth falls through to anonymous", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", rr.Code)
		}
	})
}

func TestAuthenticateBearerToken(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	key := []byte("my-token")
	tokenSignValidator := NewTokenSignValidator(key)
	handler := BasicAuthHandler(NewSimpleBasicAuthValidator("admin", "my-token"),
		TokenSignValidatorHandler(tokenSignValidator, AnonymousAuthenticateHandler(inner)))

	// Generate a valid signed token
	validToken, _ := tokenSignValidator.Sign(context.Background(), http.MethodGet, "/", "admin", time.Hour)

	t.Run("valid bearer token", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", rr.Code)
		}
	})

	t.Run("invalid bearer token falls through to anonymous", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer wrong-token")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("Expected 200 (anonymous fallback), got %d", rr.Code)
		}
	})

	t.Run("invalid signed token is rejected", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+signedTokenPrefix+"wrong-token")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Errorf("Expected 401, got %d", rr.Code)
		}
	})
}

func TestAuthenticateStaticBearerToken(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	tokenAuth := NewSimpleTokenValidator("admin", "my-static-token")
	handler := TokenValidatorHandler(tokenAuth, AnonymousAuthenticateHandler(inner))

	t.Run("valid static bearer token", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer my-static-token")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", rr.Code)
		}
	})

	t.Run("invalid static bearer token falls through to anonymous", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+signedTokenPrefix+"wrong-token")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("Expected 200 (anonymous fallback), got %d", rr.Code)
		}
	})

	t.Run("invalid static bearer token without prefix falls through to anonymous", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer wrong-token")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code == http.StatusOK {
			t.Errorf("Expected non-200 for invalid token, got %d", rr.Code)
		}
	})
}

func TestSimpleAuthenticator(t *testing.T) {
	pubKey := []byte("fake-marshaled-key")
	ctx := context.Background()

	t.Run("ValidateBasicAuth valid", func(t *testing.T) {
		auth := NewSimpleBasicAuthValidator("admin", "secret")
		user, next, ok, err := auth.Validate(ctx, "admin", "secret")
		if user != "admin" || next || !ok || err != nil {
			t.Errorf("Validate = (%q, %v, %v, %v), want (admin, false, true, nil)", user, next, ok, err)
		}
	})

	t.Run("ValidateBasicAuth invalid password", func(t *testing.T) {
		auth := NewSimpleBasicAuthValidator("admin", "secret")
		user, next, ok, err := auth.Validate(ctx, "admin", "wrong")
		if user != "" || next || ok || err != nil {
			t.Errorf("Validate = (%q, %v, %v, %v), want (\"\", false, false, nil)", user, next, ok, err)
		}
	})

	t.Run("ValidateBasicAuth invalid username", func(t *testing.T) {
		auth := NewSimpleBasicAuthValidator("admin", "secret")
		user, next, ok, err := auth.Validate(ctx, "other", "secret")
		if user != "" || next || ok || err != nil {
			t.Errorf("Validate = (%q, %v, %v, %v), want (\"\", false, false, nil)", user, next, ok, err)
		}
	})

	t.Run("ValidateToken valid", func(t *testing.T) {
		tokenAuth := NewSimpleTokenValidator("admin", "my-token")
		user, next, ok, err := tokenAuth.Validate(ctx, "my-token")
		if user != "admin" || next || !ok || err != nil {
			t.Errorf("Validate = (%q, %v, %v, %v), want (admin, false, true, nil)", user, next, ok, err)
		}
	})

	t.Run("ValidateToken signed prefix declined", func(t *testing.T) {
		tokenAuth := NewSimpleTokenValidator("admin", "my-token")
		user, next, ok, err := tokenAuth.Validate(ctx, signedTokenPrefix+"my-token")
		if user != "" || !next || ok || err != nil {
			t.Errorf("Validate = (%q, %v, %v, %v), want (\"\", true, false, nil)", user, next, ok, err)
		}
	})

	t.Run("ValidateToken JWT round-trip", func(t *testing.T) {
		tokenSignValidator := NewTokenSignValidator([]byte("secret"))
		token, _ := tokenSignValidator.Sign(ctx, http.MethodGet, "http://example.com", "admin", time.Hour)
		user, next, ok, err := tokenSignValidator.Validate(ctx, http.MethodGet, "http://example.com", token)
		if user != "admin" || next || !ok || err != nil {
			t.Errorf("Validate = (%q, %v, %v, %v), want (admin, false, true, nil)", user, next, ok, err)
		}
	})

	t.Run("ValidateToken signed token invalid", func(t *testing.T) {
		tokenSignValidator := NewTokenSignValidator([]byte("secret"))
		token, _ := NewTokenSignValidator([]byte("other")).Sign(ctx, http.MethodGet, "http://example.com", "admin", time.Hour)
		user, next, ok, err := tokenSignValidator.Validate(ctx, http.MethodGet, "http://example.com", token)
		if user != "" || next || ok || err != nil {
			t.Errorf("Validate = (%q, %v, %v, %v), want (\"\", false, false, nil)", user, next, ok, err)
		}
	})

	t.Run("ValidateToken invalid", func(t *testing.T) {
		auth := NewSimpleTokenValidator("admin", "secret")
		user, next, ok, err := auth.Validate(ctx, "wrong")
		if user != "" || next || ok || err != nil {
			t.Errorf("Validate = (%q, %v, %v, %v), want (\"\", false, false, nil)", user, next, ok, err)
		}
	})

	t.Run("ValidatePublicKey valid", func(t *testing.T) {
		auth := NewSimplePublicKeyValidator(map[string]string{string(pubKey): ""})
		user, next, ok, err := auth.Validate(ctx, "git", "type", pubKey)
		if user != "git" || next || !ok || err != nil {
			t.Errorf("Validate = (%q, %v, %v, %v), want (git, false, true, nil)", user, next, ok, err)
		}
	})

	t.Run("ValidatePublicKey bound user", func(t *testing.T) {
		auth := NewSimplePublicKeyValidator(map[string]string{string(pubKey): "deploy"})
		user, next, ok, err := auth.Validate(ctx, "git", "type", pubKey)
		if user != "deploy" || next || !ok || err != nil {
			t.Errorf("Validate = (%q, %v, %v, %v), want (deploy, false, true, nil)", user, next, ok, err)
		}
	})

	t.Run("ValidatePublicKey invalid", func(t *testing.T) {
		auth := NewSimplePublicKeyValidator(map[string]string{string(pubKey): ""})
		user, next, ok, err := auth.Validate(ctx, "git", "type", []byte("unknown-key"))
		if user != "" || next || ok || err != nil {
			t.Errorf("Validate = (%q, %v, %v, %v), want (\"\", false, false, nil)", user, next, ok, err)
		}
	})

	t.Run("LFSAuthHeaders", func(t *testing.T) {
		auth := NewTokenSignValidator([]byte("secret"))
		token, _ := auth.Sign(ctx, http.MethodGet, "http://example.com", "admin", time.Hour)
		if token == "" {
			t.Fatal("Expected non-empty token")
		}
		user, next, ok, err := auth.Validate(ctx, http.MethodGet, "http://example.com", token)
		if user != "admin" || next || !ok || err != nil {
			t.Errorf("Validate = (%q, %v, %v, %v), want (admin, false, true, nil)", user, next, ok, err)
		}
	})
}

func TestSimpleAuthenticatorSingleMethodInterfaces(t *testing.T) {
	pubKey := []byte("fake-marshaled-key")
	ctx := context.Background()

	t.Run("BasicAuthValidator", func(t *testing.T) {
		var v BasicAuthValidator = NewSimpleBasicAuthValidator("admin", "secret")
		user, next, ok, err := v.Validate(ctx, "admin", "secret")
		if user != "admin" || next || !ok || err != nil {
			t.Errorf("BasicAuthValidator: got (%q, %v, %v, %v)", user, next, ok, err)
		}
	})

	t.Run("TokenValidator", func(t *testing.T) {
		var v TokenValidator = NewSimpleTokenValidator("admin", "my-token")
		user, next, ok, err := v.Validate(ctx, "my-token")
		if user != "admin" || next || !ok || err != nil {
			t.Errorf("TokenValidator: got (%q, %v, %v, %v)", user, next, ok, err)
		}
	})

	t.Run("PublicKeyValidator", func(t *testing.T) {
		var v PublicKeyValidator = NewSimplePublicKeyValidator(map[string]string{string(pubKey): ""})
		user, next, ok, err := v.Validate(ctx, "git", "type", pubKey)
		if user != "git" || next || !ok || err != nil {
			t.Errorf("PublicKeyValidator: got (%q, %v, %v, %v)", user, next, ok, err)
		}
	})

	t.Run("TokenSignValidator", func(t *testing.T) {
		var v TokenSignValidator = NewTokenSignValidator([]byte("secret"))
		token, _ := v.Sign(ctx, http.MethodGet, "http://example.com", "admin", time.Hour)
		if token == "" {
			t.Error("Expected non-empty token")
		}
		user, next, ok, err := v.Validate(ctx, http.MethodGet, "http://example.com", token)
		if user != "admin" || next || !ok || err != nil {
			t.Errorf("TokenSignValidator: got (%q, %v, %v, %v)", user, next, ok, err)
		}
	})
}

func TestSimpleAuthenticatorNoCredentials(t *testing.T) {
	ctx := context.Background()

	t.Run("empty username disables basic auth", func(t *testing.T) {
		auth := NewSimpleBasicAuthValidator("", "secret")
		user, next, ok, err := auth.Validate(ctx, "", "secret")
		if user != "" || next || ok || err != nil {
			t.Errorf("Validate = (%q, %v, %v, %v), want (\"\", false, false, nil)", user, next, ok, err)
		}
	})

	t.Run("empty password disables token auth", func(t *testing.T) {
		auth := NewSimpleTokenValidator("admin", "")
		user, next, ok, err := auth.Validate(ctx, "")
		if user != "" || !next || ok || err != nil {
			t.Errorf("Validate = (%q, %v, %v, %v), want (\"\", true, false, nil)", user, next, ok, err)
		}
	})

	t.Run("no authorized keys disables public key auth", func(t *testing.T) {
		auth := NewSimplePublicKeyValidator(nil)
		user, next, ok, err := auth.Validate(ctx, "git", "type", []byte("some-key"))
		if user != "" || next || ok || err != nil {
			t.Errorf("Validate = (%q, %v, %v, %v), want (\"\", false, false, nil)", user, next, ok, err)
		}
	})

	t.Run("LFSAuthHeaders nil when no credentials", func(t *testing.T) {
		auth := NewTokenSignValidator([]byte(""))
		token, err := auth.Sign(ctx, http.MethodGet, "http://example.com", "someone", time.Hour)
		if token != "" || err != nil {
			t.Errorf("Sign = (%q, %v), want (\"\", nil)", token, err)
		}
		user, next, ok, err := auth.Validate(ctx, http.MethodGet, "http://example.com", signedTokenPrefix+"anything")
		if user != "" || !next || ok || err != nil {
			t.Errorf("Validate = (%q, %v, %v, %v), want (\"\", true, false, nil)", user, next, ok, err)
		}
	})
}

func TestHTTPMiddleware(t *testing.T) {
	basicAuth := NewSimpleBasicAuthValidator("admin", "secret")
	tokenSignValidator := NewTokenSignValidator([]byte("secret"))

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := IdentityFrom(r.Context())
		if id.Name() != "admin" {
			t.Errorf("Expected user 'admin', got %q", id.Name())
		}
		w.WriteHeader(http.StatusOK)
	})

	handler := BasicAuthHandler(basicAuth,
		TokenSignValidatorHandler(tokenSignValidator, inner))

	t.Run("basic auth via HTTPMiddleware", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.SetBasicAuth("admin", "secret")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", rr.Code)
		}
	})

	t.Run("bearer token via HTTPMiddleware", func(t *testing.T) {
		validToken, _ := tokenSignValidator.Sign(context.Background(), http.MethodGet, "/", "admin", time.Hour)
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", rr.Code)
		}
	})
}

func TestNoAuthenticate(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := IdentityFrom(r.Context())
		if !IsAnonymous(id) {
			t.Errorf("Expected user 'anonymous', got %q", id.Name())
		}
		w.WriteHeader(http.StatusOK)
	})

	handler := AnonymousAuthenticateHandler(inner)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", rr.Code)
	}
}
