package authenticate

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Identity is the authenticated principal; embedders may carry their own type and type-assert it in hooks.
// Name() must be non-empty; unnamed identities are treated as anonymous (see IsAnonymous) and replaced by Anonymous in the HTTP chain.
type Identity interface {
	Name() string
	Email() string
}

// AnonymousName is the Name of unauthenticated principals.
const AnonymousName = "<anonymous>"

// Anonymous is the identity of unauthenticated requests.
var Anonymous Identity = NewIdentity(AnonymousName, "")

type identity struct {
	name, email string
}

func (id identity) Name() string  { return id.name }
func (id identity) Email() string { return id.email }

// NewIdentity returns the built-in Identity carrying just a name and an email.
func NewIdentity(name, email string) Identity {
	return identity{name: name, email: email}
}

// IsAnonymous reports whether id is nil, unnamed, or the anonymous principal.
func IsAnonymous(id Identity) bool {
	return id == nil || id.Name() == "" || id.Name() == AnonymousName
}

type contextKey struct{}

// WithIdentity returns a context carrying id.
func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, contextKey{}, id)
}

// IdentityFrom returns the context's identity, or Anonymous when none was set.
func IdentityFrom(ctx context.Context) Identity {
	id, _ := ctx.Value(contextKey{}).(Identity)
	if id == nil {
		return Anonymous
	}
	return id
}

// BasicAuthValidator validates username/password credentials: err is a 500, ok authenticates user, next falls through to the next scheme, otherwise 401.
type BasicAuthValidator interface {
	Validate(ctx context.Context, username, password string) (user string, next, ok bool, err error)
}

// BasicAuthValidatorFunc is a helper type to allow using functions as BasicAuthValidators.
type BasicAuthValidatorFunc func(ctx context.Context, username, password string) (user string, next, ok bool, err error)

func (f BasicAuthValidatorFunc) Validate(ctx context.Context, username, password string) (user string, next, ok bool, err error) {
	return f(ctx, username, password)
}

// TokenValidator validates a bearer token; results follow the BasicAuthValidator contract.
type TokenValidator interface {
	Validate(ctx context.Context, token string) (user string, next, ok bool, err error)
}

// TokenValidatorFunc is a helper type to allow using functions as TokenValidators.
type TokenValidatorFunc func(ctx context.Context, token string) (user string, next, ok bool, err error)

func (f TokenValidatorFunc) Validate(ctx context.Context, token string) (user string, next, ok bool, err error) {
	return f(ctx, token)
}

// PublicKeyValidator validates an SSH public key for the client-claimed username; results follow the BasicAuthValidator contract.
type PublicKeyValidator interface {
	Validate(ctx context.Context, username string, keyType string, marshaledKey []byte) (user string, next, ok bool, err error)
}

// PublicKeyValidatorFunc is a helper type to allow using functions as PublicKeyValidators.
type PublicKeyValidatorFunc func(ctx context.Context, username string, keyType string, marshaledKey []byte) (user string, next, ok bool, err error)

func (f PublicKeyValidatorFunc) Validate(ctx context.Context, username string, keyType string, marshaledKey []byte) (user string, next, ok bool, err error) {
	return f(ctx, username, keyType, marshaledKey)
}

// TokenSignValidator signs method+path-bound tokens for a username and validates them; Validate follows the BasicAuthValidator contract.
type TokenSignValidator interface {
	Sign(ctx context.Context, method, path string, username string, expiration time.Duration) (token string, err error)
	Validate(ctx context.Context, method, path string, token string) (user string, next, ok bool, err error)
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

func (a *simpleBasicAuthValidator) Validate(_ context.Context, username, password string) (string, bool, bool, error) {
	if a.username != "" &&
		username == a.username &&
		password == a.password {
		return username, false, true, nil
	}
	return "", false, false, nil
}

// simplePublicKeyValidator implements PublicKeyValidator with in-memory authorized keys.
type simplePublicKeyValidator struct {
	authorizedKeys map[string]string
}

// NewSimplePublicKeyValidator maps string(marshaledKey) to the user each key authenticates as.
// An empty value trusts the client-claimed SSH username for that key.
func NewSimplePublicKeyValidator(authorizedKeys map[string]string) PublicKeyValidator {
	return &simplePublicKeyValidator{
		authorizedKeys: authorizedKeys,
	}
}

func (a *simplePublicKeyValidator) Validate(_ context.Context, username string, keyType string, marshaledKey []byte) (string, bool, bool, error) {
	bound, ok := a.authorizedKeys[string(marshaledKey)]
	if !ok {
		return "", false, false, nil
	}
	if bound == "" {
		bound = username
	}
	return bound, false, true, nil
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

func (a *simpleTokenValidator) Validate(_ context.Context, token string) (string, bool, bool, error) {
	if a.token == "" {
		return "", true, false, nil
	}

	if strings.HasPrefix(token, signedTokenPrefix) {
		return "", true, false, nil
	}

	if token == a.token {
		return a.username, false, true, nil
	}

	return "", false, false, nil
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

func (a *tokenSignValidator) Sign(_ context.Context, method, path string, username string, expiration time.Duration) (string, error) {
	if len(a.key) == 0 {
		return "", nil
	}

	if !strings.HasPrefix(path, "/") {
		u, err := url.Parse(path)
		if err != nil {
			return "", fmt.Errorf("invalid path for signing: %w", err)
		}
		path = u.Path
	}

	token, err := signToken(a.key, username, time.Now().Add(expiration), method, path)
	if err != nil {
		return "", err
	}
	return signedTokenPrefix + token, nil
}

func (a *tokenSignValidator) Validate(_ context.Context, method, path string, token string) (string, bool, bool, error) {
	if len(a.key) == 0 {
		return "", true, false, nil
	}

	if !strings.HasPrefix(token, signedTokenPrefix) {
		return "", true, false, nil
	}

	token = strings.TrimPrefix(token, signedTokenPrefix)

	if !strings.HasPrefix(path, "/") {
		u, err := url.Parse(path)
		if err != nil {
			return "", false, false, nil
		}
		path = u.Path
	}

	username, ok := verifyToken(a.key, token, method, path)
	if !ok {
		return "", false, false, nil
	}
	return username, false, true, nil
}

// Authenticators bundles the optional validators for each authentication scheme.
type Authenticators struct {
	BasicAuth BasicAuthValidator
	Token     TokenValidator
	TokenSign TokenSignValidator
	PublicKey PublicKeyValidator
}

// Handler authenticates HTTP requests with the configured validators before
// delegating to the next handler.
type Handler struct {
	basicAuth BasicAuthValidator
	token     TokenValidator
	tokenSign TokenSignValidator
	next      http.Handler
	chain     http.Handler
}

// Option defines a functional option for configuring the Handler.
type Option func(*Handler)

// WithBasicAuthValidator sets the Basic auth validator.
func WithBasicAuthValidator(v BasicAuthValidator) Option {
	return func(h *Handler) {
		h.basicAuth = v
	}
}

// WithTokenValidator sets the static Bearer token validator.
func WithTokenValidator(v TokenValidator) Option {
	return func(h *Handler) {
		h.token = v
	}
}

// WithTokenSignValidator sets the signed Bearer token validator.
func WithTokenSignValidator(v TokenSignValidator) Option {
	return func(h *Handler) {
		h.tokenSign = v
	}
}

// WithNext sets the next http.Handler to call once the request is authenticated.
func WithNext(next http.Handler) Option {
	return func(h *Handler) {
		h.next = next
	}
}

// NewHandler creates a new Handler; requests pass Basic auth, signed token,
// and static token validation before falling back to anonymous.
func NewHandler(opts ...Option) *Handler {
	h := &Handler{}
	for _, opt := range opts {
		opt(h)
	}
	next := h.next
	if next == nil {
		next = http.NotFoundHandler()
	}
	chain := AnonymousAuthenticateHandler(next)
	chain = TokenValidatorHandler(h.token, chain)
	chain = TokenSignValidatorHandler(h.tokenSign, chain)
	chain = BasicAuthHandler(h.basicAuth, chain)
	h.chain = chain
	return h
}

// ServeHTTP implements the http.Handler interface.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.chain.ServeHTTP(w, r)
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
		if !IsAnonymous(IdentityFrom(r.Context())) {
			h.ServeHTTP(w, r)
			return
		}
		username, password, ok := r.BasicAuth()
		if ok {
			user, next, valid, err := auth.Validate(r.Context(), username, password)
			if err != nil {
				slog.WarnContext(r.Context(), "basic auth validation error", "error", err)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			if valid {
				r = r.WithContext(WithIdentity(r.Context(), NewIdentity(user, "")))
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
		if !IsAnonymous(IdentityFrom(r.Context())) {
			h.ServeHTTP(w, r)
			return
		}
		if token, ok := parseBearerToken(r); ok {
			user, next, valid, err := auth.Validate(r.Context(), r.Method, r.URL.RequestURI(), token)
			if err != nil {
				slog.WarnContext(r.Context(), "token sign validation error", "error", err)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			if valid {
				r = r.WithContext(WithIdentity(r.Context(), NewIdentity(user, "")))
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
		if !IsAnonymous(IdentityFrom(r.Context())) {
			h.ServeHTTP(w, r)
			return
		}
		if token, ok := parseBearerToken(r); ok {
			user, next, valid, err := auth.Validate(r.Context(), token)
			if err != nil {
				slog.WarnContext(r.Context(), "token validation error", "error", err)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			if valid {
				r = r.WithContext(WithIdentity(r.Context(), NewIdentity(user, "")))
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

// AnonymousAuthenticateHandler stores Anonymous when no outer auth handler
// established a named identity, so downstream always finds one.
func AnonymousAuthenticateHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if IsAnonymous(IdentityFrom(r.Context())) {
			r = r.WithContext(WithIdentity(r.Context(), Anonymous))
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
