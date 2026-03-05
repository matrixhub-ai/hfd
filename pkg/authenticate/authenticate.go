package authenticate

import (
	"context"
	"net/http"
	"strings"
)

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

// NoAuthenticate is a middleware that sets the user to "anonymous" in the context without requiring authentication.
func NoAuthenticate(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(WithContext(r.Context(), "anonymous"))
		h.ServeHTTP(w, r)
	})
}

// Authenticate is a middleware that checks for basic auth or bearer token and sets the user in the context.
func Authenticate(u, p string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if ok {
			if username == u && password == p {
				r = r.WithContext(WithContext(r.Context(), username))
				h.ServeHTTP(w, r)
				return
			}
			w.Header().Set("WWW-Authenticate", `Basic realm="hfd"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Try Bearer token authentication (used by huggingface-cli)
		if token, ok := parseBearerToken(r); ok {
			if token == p {
				r = r.WithContext(WithContext(r.Context(), u))
				h.ServeHTTP(w, r)
				return
			}
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		w.Header().Set("WWW-Authenticate", `Basic realm="hfd"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
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
