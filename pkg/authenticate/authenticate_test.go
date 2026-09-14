package authenticate

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type tokenSignValidatorFunc func(context.Context, string, string, string) (Identity, error)

func (validate tokenSignValidatorFunc) Validate(ctx context.Context, method, path, token string) (Identity, error) {
	return validate(ctx, method, path, token)
}

func (tokenSignValidatorFunc) Sign(context.Context, string, string, Identity, time.Duration) (string, error) {
	panic("unexpected Sign call")
}

func TestBasicAuthHandlerContract(t *testing.T) {
	testMiddlewareContract(t, true, func(validate func() (Identity, error), next http.Handler) http.Handler {
		return BasicAuthHandler(BasicAuthValidatorFunc(func(context.Context, string, string) (Identity, error) {
			return validate()
		}), next)
	})
}

func TestTokenValidatorHandlerContract(t *testing.T) {
	testMiddlewareContract(t, false, func(validate func() (Identity, error), next http.Handler) http.Handler {
		return TokenValidatorHandler(TokenValidatorFunc(func(context.Context, string) (Identity, error) {
			return validate()
		}), next)
	})
}

func TestTokenSignValidatorHandlerContract(t *testing.T) {
	testMiddlewareContract(t, false, func(validate func() (Identity, error), next http.Handler) http.Handler {
		return TokenSignValidatorHandler(tokenSignValidatorFunc(func(context.Context, string, string, string) (Identity, error) {
			return validate()
		}), next)
	})
}

func testMiddlewareContract(t *testing.T, basic bool, middleware func(func() (Identity, error), http.Handler) http.Handler) {
	t.Helper()
	alice := NewIdentity("alice", "alice@example.com")
	for _, test := range []struct {
		name       string
		id         Identity
		err        error
		credential bool
		existing   Identity
		wantCode   int
		want       Identity
	}{
		{"not handled", nil, nil, true, nil, http.StatusOK, Anonymous},
		{"rejected", nil, ErrUnauthenticated, true, nil, http.StatusUnauthorized, nil},
		{"wrapped rejection", nil, fmt.Errorf("rejected: %w", ErrUnauthenticated), true, nil, http.StatusUnauthorized, nil},
		{"internal error", nil, errors.New("validator failed"), true, nil, http.StatusInternalServerError, nil},
		{"authenticated", alice, nil, true, nil, http.StatusOK, alice},
		{"unnamed identity", NewIdentity("", ""), nil, true, nil, http.StatusOK, Anonymous},
		{"explicit anonymous", alice, nil, true, Anonymous, http.StatusOK, alice},
		{"no credential", nil, nil, false, nil, http.StatusOK, Anonymous},
		{"already authenticated", nil, nil, true, alice, http.StatusOK, alice},
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
			handler := middleware(func() (Identity, error) {
				called = true
				if !wantCalled {
					t.Fatal("validator must not be called")
				}
				return test.id, test.err
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
	validToken, _ := tokenSignValidator.Sign(context.Background(), http.MethodGet, "/", NewIdentity("admin", ""), time.Hour)

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
		user, err := auth.Validate(ctx, "admin", "secret")
		if err != nil || user == nil {
			t.Error("Expected valid basic auth to succeed")
		}
		if user.Name() != "admin" {
			t.Errorf("Expected user 'admin', got %q", user)
		}
	})

	t.Run("ValidateBasicAuth invalid password", func(t *testing.T) {
		auth := NewSimpleBasicAuthValidator("admin", "secret")
		id, err := auth.Validate(ctx, "admin", "wrong")
		if id != nil || !errors.Is(err, ErrUnauthenticated) {
			t.Error("Expected invalid password to fail")
		}
	})

	t.Run("ValidateBasicAuth invalid username", func(t *testing.T) {
		auth := NewSimpleBasicAuthValidator("admin", "secret")
		id, err := auth.Validate(ctx, "other", "secret")
		if id != nil || !errors.Is(err, ErrUnauthenticated) {
			t.Error("Expected invalid username to fail")
		}
	})

	t.Run("ValidateToken valid", func(t *testing.T) {
		tokenAuth := NewSimpleTokenValidator("admin", "my-token")
		user, err := tokenAuth.Validate(ctx, "my-token")
		if err != nil || user == nil {
			t.Error("Expected valid token to succeed")
		}
		if user.Name() != "admin" {
			t.Errorf("Expected user 'admin', got %q", user)
		}
	})

	t.Run("ValidateToken JWT round-trip", func(t *testing.T) {
		tokenSignValidator := NewTokenSignValidator([]byte("secret"))
		token, _ := tokenSignValidator.Sign(ctx, http.MethodGet, "http://example.com", NewIdentity("admin", ""), time.Hour)
		user, err := tokenSignValidator.Validate(ctx, http.MethodGet, "http://example.com", token)
		if err != nil || user == nil {
			t.Error("Expected signed token to be valid")
		}
		if user.Name() != "admin" {
			t.Errorf("Expected user 'admin', got %q", user)
		}
	})

	t.Run("ValidateToken invalid", func(t *testing.T) {
		auth := NewSimpleTokenValidator("admin", "secret")
		id, err := auth.Validate(ctx, "wrong")
		if id != nil || !errors.Is(err, ErrUnauthenticated) {
			t.Error("Expected invalid token to fail")
		}
	})

	t.Run("ValidatePublicKey valid", func(t *testing.T) {
		auth := NewSimplePublicKeyValidator(map[string]string{string(pubKey): ""})
		user, err := auth.Validate(ctx, "git", "type", pubKey)
		if err != nil || user == nil {
			t.Error("Expected valid public key to succeed")
		}
		if user.Name() != "git" {
			t.Errorf("Expected user 'git', got %q", user)
		}
	})

	t.Run("ValidatePublicKey invalid", func(t *testing.T) {
		auth := NewSimplePublicKeyValidator(map[string]string{string(pubKey): ""})
		id, err := auth.Validate(ctx, "git", "type", []byte("unknown-key"))
		if id != nil || !errors.Is(err, ErrUnauthenticated) {
			t.Error("Expected invalid public key to fail")
		}
	})

	t.Run("LFSAuthHeaders", func(t *testing.T) {
		auth := NewTokenSignValidator([]byte("secret"))
		token, _ := auth.Sign(ctx, http.MethodGet, "http://example.com", NewIdentity("admin", ""), time.Hour)
		if token == "" {
			t.Fatal("Expected non-empty token")
		}
		user, err := auth.Validate(ctx, http.MethodGet, "http://example.com", token)
		if err != nil || user == nil {
			t.Fatal("Failed to verify token")
		}
		if user.Name() != "admin" {
			t.Errorf("Expected subject 'admin', got %q", user)
		}
	})
}

func TestSimpleAuthenticatorSingleMethodInterfaces(t *testing.T) {
	pubKey := []byte("fake-marshaled-key")
	ctx := context.Background()

	t.Run("BasicAuthValidator", func(t *testing.T) {
		var v BasicAuthValidator = NewSimpleBasicAuthValidator("admin", "secret")
		user, err := v.Validate(ctx, "admin", "secret")
		if err != nil || user == nil || user.Name() != "admin" {
			t.Errorf("BasicAuthValidator: got user=%v, err=%v", user, err)
		}
	})

	t.Run("TokenValidator", func(t *testing.T) {
		var v TokenValidator = NewSimpleTokenValidator("admin", "my-token")
		user, err := v.Validate(ctx, "my-token")
		if err != nil || user == nil || user.Name() != "admin" {
			t.Errorf("TokenValidator: got user=%v, err=%v", user, err)
		}
	})

	t.Run("PublicKeyValidator", func(t *testing.T) {
		var v PublicKeyValidator = NewSimplePublicKeyValidator(map[string]string{string(pubKey): ""})
		user, err := v.Validate(ctx, "git", "type", pubKey)
		if err != nil || user == nil || user.Name() != "git" {
			t.Errorf("PublicKeyValidator: got user=%v, err=%v", user, err)
		}
	})

	t.Run("TokenSignValidator", func(t *testing.T) {
		var v TokenSignValidator = NewTokenSignValidator([]byte("secret"))
		token, _ := v.Sign(ctx, http.MethodGet, "http://example.com", NewIdentity("admin", ""), time.Hour)
		if token == "" {
			t.Error("Expected non-empty token")
		}
		user, err := v.Validate(ctx, http.MethodGet, "http://example.com", token)
		if err != nil || user == nil || user.Name() != "admin" {
			t.Errorf("TokenSignValidator: got user=%v, err=%v", user, err)
		}
	})
}

func TestSimpleAuthenticatorNoCredentials(t *testing.T) {
	ctx := context.Background()

	t.Run("empty username disables basic auth", func(t *testing.T) {
		auth := NewSimpleBasicAuthValidator("", "secret")
		id, err := auth.Validate(ctx, "", "secret")
		if id != nil || !errors.Is(err, ErrUnauthenticated) {
			t.Error("Expected empty username to disable basic auth")
		}
	})

	t.Run("empty password disables token auth", func(t *testing.T) {
		auth := NewSimpleTokenValidator("admin", "")
		id, err := auth.Validate(ctx, "")
		if id != nil || err != nil {
			t.Error("Expected empty password to disable token auth")
		}
	})

	t.Run("no authorized keys disables public key auth", func(t *testing.T) {
		auth := NewSimplePublicKeyValidator(nil)
		id, err := auth.Validate(ctx, "git", "type", []byte("some-key"))
		if id != nil || !errors.Is(err, ErrUnauthenticated) {
			t.Error("Expected no authorized keys to disable public key auth")
		}
	})

	t.Run("LFSAuthHeaders nil when no credentials", func(t *testing.T) {
		auth := NewTokenSignValidator([]byte(""))
		token, _ := auth.Sign(ctx, http.MethodGet, "http://example.com", NewIdentity("someone", ""), time.Hour)
		if token != "" {
			t.Errorf("Expected empty token, got %q", token)
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
		validToken, _ := tokenSignValidator.Sign(context.Background(), http.MethodGet, "/", NewIdentity("admin", ""), time.Hour)
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
