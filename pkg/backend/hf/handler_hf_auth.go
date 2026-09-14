package hf

import (
	"net/http"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
)

// handleWhoami handles GET /api/whoami-v2
func (h *Handler) handleWhoami(w http.ResponseWriter, r *http.Request) {
	id := authenticate.IdentityFrom(r.Context())
	if authenticate.IsAnonymous(id) {
		responseJSON(w, map[string]string{"error": "Unauthorized"}, http.StatusUnauthorized)
		return
	}

	resp := whoamiResponse{
		Type:          "user",
		ID:            id.Name(),
		Name:          id.Name(),
		Fullname:      id.Name(),
		Email:         id.Email(),
		EmailVerified: false,
		IsPro:         false,
		CanPay:        false,
		Orgs:          []any{},
		Auth: authInfo{
			AccessToken: accessToken{
				DisplayName: "token",
				Role:        "write",
			},
		},
	}

	responseJSON(w, resp, http.StatusOK)
}
