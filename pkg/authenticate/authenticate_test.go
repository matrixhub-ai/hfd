package authenticate

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

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
		user, _, ok, _ := auth.Validate(ctx, "admin", "secret")
		if !ok {
			t.Error("Expected valid basic auth to succeed")
		}
		if user != "admin" {
			t.Errorf("Expected user 'admin', got %q", user)
		}
	})

	t.Run("ValidateBasicAuth invalid password", func(t *testing.T) {
		auth := NewSimpleBasicAuthValidator("admin", "secret")
		_, _, ok, _ := auth.Validate(ctx, "admin", "wrong")
		if ok {
			t.Error("Expected invalid password to fail")
		}
	})

	t.Run("ValidateBasicAuth invalid username", func(t *testing.T) {
		auth := NewSimpleBasicAuthValidator("admin", "secret")
		_, _, ok, _ := auth.Validate(ctx, "other", "secret")
		if ok {
			t.Error("Expected invalid username to fail")
		}
	})

	t.Run("ValidateToken valid", func(t *testing.T) {
		tokenAuth := NewSimpleTokenValidator("admin", "my-token")
		user, _, ok, _ := tokenAuth.Validate(ctx, "my-token")
		if !ok {
			t.Error("Expected valid token to succeed")
		}
		if user != "admin" {
			t.Errorf("Expected user 'admin', got %q", user)
		}
	})

	t.Run("ValidateToken JWT round-trip", func(t *testing.T) {
		tokenSignValidator := NewTokenSignValidator([]byte("secret"))
		token, _ := tokenSignValidator.Sign(ctx, http.MethodGet, "http://example.com", "admin", time.Hour)
		user, _, ok, _ := tokenSignValidator.Validate(ctx, http.MethodGet, "http://example.com", token)
		if !ok {
			t.Error("Expected signed token to be valid")
		}
		if user != "admin" {
			t.Errorf("Expected user 'admin', got %q", user)
		}
	})

	t.Run("ValidateToken invalid", func(t *testing.T) {
		auth := NewSimpleTokenValidator("admin", "secret")
		_, _, ok, _ := auth.Validate(ctx, "wrong")
		if ok {
			t.Error("Expected invalid token to fail")
		}
	})

	t.Run("ValidatePublicKey valid", func(t *testing.T) {
		auth := NewSimplePublicKeyValidator([][]byte{pubKey})
		user, _, ok, _ := auth.Validate(ctx, "git", "type", pubKey)
		if !ok {
			t.Error("Expected valid public key to succeed")
		}
		if user != "git" {
			t.Errorf("Expected user 'git', got %q", user)
		}
	})

	t.Run("ValidatePublicKey invalid", func(t *testing.T) {
		auth := NewSimplePublicKeyValidator([][]byte{pubKey})
		_, _, ok, _ := auth.Validate(ctx, "git", "type", []byte("unknown-key"))
		if ok {
			t.Error("Expected invalid public key to fail")
		}
	})

	t.Run("LFSAuthHeaders", func(t *testing.T) {
		auth := NewTokenSignValidator([]byte("secret"))
		token, _ := auth.Sign(ctx, http.MethodGet, "http://example.com", "admin", time.Hour)
		if token == "" {
			t.Fatal("Expected non-empty token")
		}
		user, _, ok, _ := auth.Validate(ctx, http.MethodGet, "http://example.com", token)
		if !ok {
			t.Fatal("Failed to verify token")
		}
		if user != "admin" {
			t.Errorf("Expected subject 'admin', got %q", user)
		}
	})
}

func TestSimpleAuthenticatorSingleMethodInterfaces(t *testing.T) {
	pubKey := []byte("fake-marshaled-key")
	ctx := context.Background()

	t.Run("BasicAuthValidator", func(t *testing.T) {
		var v BasicAuthValidator = NewSimpleBasicAuthValidator("admin", "secret")
		user, _, ok, _ := v.Validate(ctx, "admin", "secret")
		if !ok || user != "admin" {
			t.Errorf("BasicAuthValidator: got user=%q, ok=%v", user, ok)
		}
	})

	t.Run("TokenValidator", func(t *testing.T) {
		var v TokenValidator = NewSimpleTokenValidator("admin", "my-token")
		user, _, ok, _ := v.Validate(ctx, "my-token")
		if !ok || user != "admin" {
			t.Errorf("TokenValidator: got user=%q, ok=%v", user, ok)
		}
	})

	t.Run("PublicKeyValidator", func(t *testing.T) {
		var v PublicKeyValidator = NewSimplePublicKeyValidator([][]byte{pubKey})
		user, _, ok, _ := v.Validate(ctx, "git", "type", pubKey)
		if !ok || user != "git" {
			t.Errorf("PublicKeyValidator: got user=%q, ok=%v", user, ok)
		}
	})

	t.Run("TokenSignValidator", func(t *testing.T) {
		var v TokenSignValidator = NewTokenSignValidator([]byte("secret"))
		token, _ := v.Sign(ctx, http.MethodGet, "http://example.com", "admin", time.Hour)
		if token == "" {
			t.Error("Expected non-empty token")
		}
		user, _, ok, _ := v.Validate(ctx, http.MethodGet, "http://example.com", token)
		if !ok || user != "admin" {
			t.Errorf("TokenSignValidator: got user=%q, ok=%v", user, ok)
		}
	})
}

func TestSimpleAuthenticatorNoCredentials(t *testing.T) {
	ctx := context.Background()

	t.Run("empty username disables basic auth", func(t *testing.T) {
		auth := NewSimpleBasicAuthValidator("", "secret")
		_, _, ok, _ := auth.Validate(ctx, "", "secret")
		if ok {
			t.Error("Expected empty username to disable basic auth")
		}
	})

	t.Run("empty password disables token auth", func(t *testing.T) {
		auth := NewSimpleTokenValidator("admin", "")
		_, _, ok, _ := auth.Validate(ctx, "")
		if ok {
			t.Error("Expected empty password to disable token auth")
		}
	})

	t.Run("no authorized keys disables public key auth", func(t *testing.T) {
		auth := NewSimplePublicKeyValidator(nil)
		_, _, ok, _ := auth.Validate(ctx, "git", "type", []byte("some-key"))
		if ok {
			t.Error("Expected no authorized keys to disable public key auth")
		}
	})

	t.Run("LFSAuthHeaders nil when no credentials", func(t *testing.T) {
		auth := NewTokenSignValidator([]byte(""))
		token, _ := auth.Sign(ctx, http.MethodGet, "http://example.com", "someone", time.Hour)
		if token != "" {
			t.Errorf("Expected empty token, got %q", token)
		}
	})
}

func TestHTTPMiddleware(t *testing.T) {
	basicAuth := NewSimpleBasicAuthValidator("admin", "secret")
	tokenSignValidator := NewTokenSignValidator([]byte("secret"))

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userInfo, ok := GetUserInfo(r.Context())
		if !ok {
			t.Error("Expected user in context")
		}
		if userInfo.User != "admin" {
			t.Errorf("Expected user 'admin', got %q", userInfo.User)
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
		userInfo, ok := GetUserInfo(r.Context())
		if !ok {
			t.Error("Expected user in context")
		}
		if userInfo.User != Anonymous {
			t.Errorf("Expected user 'anonymous', got %q", userInfo.User)
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

func TestNewTokenRecognizer(t *testing.T) {
	recognize := func(token string) bool { return token == "known" }
	tests := []struct {
		name      string
		validator TokenValidator
		token     string
		wantUser  string
		wantNext  bool
		wantOK    bool
	}{
		{"recognized token", NewTokenRecognizer("svc", recognize), "known", "svc", false, true},
		{"unrecognized token falls through", NewTokenRecognizer("svc", recognize), "other", "", true, false},
		{"nil recognize never matches", NewTokenRecognizer("svc", nil), "known", "", true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			user, next, ok, err := tt.validator.Validate(context.Background(), tt.token)
			if err != nil {
				t.Fatalf("Validate error = %v", err)
			}
			if user != tt.wantUser || next != tt.wantNext || ok != tt.wantOK {
				t.Errorf("Validate = (%q, %v, %v), want (%q, %v, %v)", user, next, ok, tt.wantUser, tt.wantNext, tt.wantOK)
			}
		})
	}
}
