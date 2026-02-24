package authenticate

import (
	"net/http"

	"github.com/gorilla/context"
)

func Authenticate(u, p string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="gitd"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		if username != u || password != p {
			w.Header().Set("WWW-Authenticate", `Basic realm="gitd"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		context.Set(r, "USER", username)
		h.ServeHTTP(w, r)
	})
}
