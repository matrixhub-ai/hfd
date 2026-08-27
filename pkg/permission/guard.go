package permission

import (
	"errors"
	"log/slog"
	"net/http"
)

// Responder writes a failure message with the given status code.
type Responder func(w http.ResponseWriter, message string, statusCode int)

// Guard runs a permission hook and renders failures through Respond.
type Guard struct {
	Hook    PermissionHookFunc
	Respond Responder
}

// Allow reports whether the operation may proceed, writing the failure response otherwise.
func (g Guard) Allow(w http.ResponseWriter, r *http.Request, op Operation, repoName string, opCtx Context) bool {
	err := g.Hook.Check(r.Context(), op, repoName, opCtx)
	if err == nil {
		return true
	}
	if errors.Is(err, ErrDenied) {
		g.Respond(w, "permission denied", http.StatusForbidden)
		return false
	}
	slog.ErrorContext(r.Context(), "permission hook error", "op", op, "repo", repoName, "error", err)
	g.Respond(w, err.Error(), http.StatusInternalServerError)
	return false
}
