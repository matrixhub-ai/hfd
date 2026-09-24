package hf

import (
	"net/http"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
)

// handleWhoami handles GET /api/whoami-v2
func (h *Handler) handleWhoami(w http.ResponseWriter, r *http.Request) {
	if authenticate.IsAnonymous(authenticate.IdentityFrom(r.Context())) {
		responseJSON(w, map[string]string{"error": "Unauthorized"}, http.StatusUnauthorized)
		return
	}

	resp, err := h.whoamiFunc(r.Context())
	if err != nil {
		respondCatalogError(w, err)
		return
	}

	responseJSON(w, resp, http.StatusOK)
}
