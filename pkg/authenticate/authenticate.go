package authenticate

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

const Anonymous = "<anonymous>"

type contextKey struct{}

type contextValue struct {
	User string
}

// WithContext returns a new context with the user set.
func WithContext(ctx context.Context, user string) context.Context {
	return context.WithValue(ctx, contextKey{}, contextValue{User: user})
}

// GetUser retrieves the user from the context.
func GetUser(ctx context.Context) (string, bool) {
	val, ok := ctx.Value(contextKey{}).(contextValue)
	if !ok {
		return "", false
	}
	return val.User, true
}

// BasicAuthValidator validates username/password credentials.
type BasicAuthValidator interface {
	Validate(ctx context.Context, username, password string) (user string, next, ok bool)
}

// TokenValidator validates a bearer token.
type TokenValidator interface {
	Validate(ctx context.Context, token string) (user string, next, ok bool)
}

// PublicKeyValidator validates an SSH public key.
type PublicKeyValidator interface {
	Validate(ctx context.Context, username string, keyType string, marshaledKey []byte) (user string, next, ok bool)
}

// TokenSignValidator is an interface for signing and validating tokens.
type TokenSignValidator interface {
	Sign(ctx context.Context, method, url string, username string, expiration time.Duration) (token string)
	Validate(ctx context.Context, method, url string, token string) (user string, next, ok bool)
}

// simpleBasicAuthValidator implements BasicAuthValidator with in-memory credentials.
type simpleBasicAuthValidator struct {
	username string
	password string
}

// NewSimpleBasicAuthValidator creates a SimpleBasicAuthValidator with static credentials.
func NewSimpleBasicAuthValidator(username, password string) BasicAuthValidator {
	return &simpleBasicAuthValidator{
		username: username,
		password: password,
	}
}

func (a *simpleBasicAuthValidator) Validate(_ context.Context, username, password string) (string, bool, bool) {
	if a.username != "" &&
		username == a.username &&
		password == a.password {
		return username, false, true
	}
	return "", false, false
}

// simplePublicKeyValidator implements PublicKeyValidator with in-memory authorized keys.
type simplePublicKeyValidator struct {
	authorizedKeys map[string]bool
}

// NewSimplePublicKeyValidator creates a PublicKeyValidator.
func NewSimplePublicKeyValidator(authorizedKeys [][]byte) PublicKeyValidator {
	keys := make(map[string]bool, len(authorizedKeys))
	for _, k := range authorizedKeys {
		keys[string(k)] = true
	}
	return &simplePublicKeyValidator{
		authorizedKeys: keys,
	}
}

func (a *simplePublicKeyValidator) Validate(_ context.Context, username string, keyType string, marshaledKey []byte) (string, bool, bool) {
	if a.authorizedKeys[string(marshaledKey)] {
		return username, false, true
	}
	return "", false, false
}

// simpleTokenValidator implements TokenValidator with a static token.
type simpleTokenValidator struct {
	username string
	token    string
}

// NewSimpleTokenValidator creates a TokenValidator.
func NewSimpleTokenValidator(username string, token string) TokenValidator {
	return &simpleTokenValidator{
		username: username,
		token:    token,
	}
}

func (a *simpleTokenValidator) Validate(_ context.Context, token string) (string, bool, bool) {
	if a.token == "" {
		return "", true, false
	}

	if strings.HasPrefix(token, signedTokenPrefix) {
		return "", true, false
	}

	if token == a.token {
		return a.username, false, true
	}

	return "", false, false
}

type tokenSignValidator struct {
	key []byte
}

// NewTokenSignValidator creates a TokenSignValidator.
func NewTokenSignValidator(key []byte) TokenSignValidator {
	return &tokenSignValidator{
		key: key,
	}
}

const signedTokenPrefix = "sign:"

func (a *tokenSignValidator) Sign(_ context.Context, method, url string, username string, expiration time.Duration) string {
	if len(a.key) == 0 {
		return ""
	}

	token, err := signToken(a.key, username, time.Now().Add(expiration), method, url)
	if err != nil {
		return ""
	}
	return signedTokenPrefix + token
}

func (a *tokenSignValidator) Validate(_ context.Context, method, url string, token string) (string, bool, bool) {
	if len(a.key) == 0 {
		return "", true, false
	}

	if !strings.HasPrefix(token, signedTokenPrefix) {
		return "", true, false
	}

	token = strings.TrimPrefix(token, signedTokenPrefix)

	username, ok := verifyToken(a.key, token, method, url)
	if !ok {
		return "", false, false
	}
	return username, false, true
}

// BasicAuthHandler returns an HTTP middleware that authenticates via Basic auth.
// If Basic auth is present and valid, the user is set in context.
// If Basic auth is present but invalid, returns 401.
// If no Basic auth is present, the request passes through to the next handler.
func BasicAuthHandler(auth BasicAuthValidator, h http.Handler) http.Handler {
	if auth == nil {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := GetUser(r.Context())
		if ok && user != Anonymous {
			h.ServeHTTP(w, r)
			return
		}
		username, password, ok := r.BasicAuth()
		if ok {
			user, next, valid := auth.Validate(r.Context(), username, password)
			if valid {
				r = r.WithContext(WithContext(r.Context(), user))
				h.ServeHTTP(w, r)
				return
			}
			if !next {
				w.Header().Set("WWW-Authenticate", `Basic realm="hfd"`)
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}
		h.ServeHTTP(w, r)
	})
}

// TokenSignValidatorHandler returns an HTTP middleware that authenticates via signed Bearer tokens.
// If a signed Bearer token is present and valid, the user is set in context.
// If the token is not a valid signed token, the request passes through to the next handler.
func TokenSignValidatorHandler(auth TokenSignValidator, h http.Handler) http.Handler {
	if auth == nil {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := GetUser(r.Context())
		if ok && user != Anonymous {
			h.ServeHTTP(w, r)
			return
		}
		if token, ok := parseBearerToken(r); ok {
			user, next, valid := auth.Validate(r.Context(), r.Method, r.URL.RequestURI(), token)
			if valid {
				r = r.WithContext(WithContext(r.Context(), user))
				h.ServeHTTP(w, r)
				return
			}
			if !next {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}
		h.ServeHTTP(w, r)
	})
}

// TokenValidatorHandler returns an HTTP middleware that authenticates via static Bearer tokens.
// If a static Bearer token is present and valid, the user is set in context.
// If the token is not a valid static token, the request passes through to the next handler.
func TokenValidatorHandler(auth TokenValidator, h http.Handler) http.Handler {
	if auth == nil {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := GetUser(r.Context())
		if ok && user != Anonymous {
			h.ServeHTTP(w, r)
			return
		}
		if token, ok := parseBearerToken(r); ok {
			user, next, valid := auth.Validate(r.Context(), token)
			if valid {
				r = r.WithContext(WithContext(r.Context(), user))
				h.ServeHTTP(w, r)
				return
			}
			if !next {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}
		h.ServeHTTP(w, r)
	})
}

// AnonymousAuthenticateHandler is a middleware that allows users to be anonymous by default
// if they do not have authentication credentials. If the user is already set in
// context (by an outer auth handler), it passes through unchanged.
func AnonymousAuthenticateHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, ok := GetUser(r.Context())
		if !ok {
			r = r.WithContext(WithContext(r.Context(), Anonymous))
		}
		h.ServeHTTP(w, r)
	})
}

// parseBearerToken extracts the Bearer token from the Authorization header.
func parseBearerToken(r *http.Request) (string, bool) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return "", false
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(auth, prefix) {
		return "", false
	}
	return auth[len(prefix):], true
}

// tokenPayload is the JSON structure for the token claims.
type tokenPayload struct {
	Sub string `json:"sub"`
	Exp int64  `json:"exp"`
}

// signToken creates an HMAC-SHA256 signed token with the given claims and expiration.
func signToken(key []byte, subject string, exp time.Time, extras ...string) (string, error) {
	payload, err := json.Marshal(tokenPayload{Sub: subject, Exp: exp.Unix()})
	if err != nil {
		return "", err
	}
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	header, err := io.ReadAll(io.LimitReader(rand.Reader, 32))
	if err != nil {
		return "", err
	}
	signingInput := base64.RawURLEncoding.EncodeToString(header) + "." + encodedPayload
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(signingInput))
	for _, extra := range extras {
		mac.Write([]byte(extra))
	}
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + sig, nil
}

// verifyToken verifies an HMAC-SHA256 signed token and returns the claims.
func verifyToken(key []byte, token string, extras ...string) (string, bool) {
	parts := strings.SplitN(token, ".", 3)
	if len(parts) != 3 {
		return "", false
	}
	signingInput := parts[0] + "." + parts[1]
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", false
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(signingInput))
	for _, extra := range extras {
		mac.Write([]byte(extra))
	}
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return "", false
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", false
	}
	var claims tokenPayload
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return "", false
	}
	if claims.Exp > 0 && time.Now().Unix() >= claims.Exp {
		return "", false
	}
	if claims.Sub == "" {
		return "", false
	}
	return claims.Sub, true
}
