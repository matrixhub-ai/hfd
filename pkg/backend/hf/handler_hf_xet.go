package hf

import (
	"fmt"
	"net/http"
	"time"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/permission"
)

// xetTokenResponse matches the CasJWTInfo format expected by xet-core clients:
//
//	{ "casUrl": "...", "exp": 1234567890, "accessToken": "..." }
//
// Reference: xet-core xet_client/src/hub_client/types.rs
type xetTokenResponse struct {
	CasURL      string `json:"casUrl"`
	Exp         int64  `json:"exp"`
	AccessToken string `json:"accessToken"`
}

const xetTokenExpiration = time.Hour

// handleXetReadToken handles GET /api/{repoType}/{namespace}/{repo}/xet-read-token/{rev}
// Returns a CAS access token with read scope for the xet CAS service.
func (h *Handler) handleXetReadToken(w http.ResponseWriter, r *http.Request) {
	h.handleXetToken(w, r, "read")
}

// handleXetWriteToken handles GET /api/{repoType}/{namespace}/{repo}/xet-write-token/{rev}
// Returns a CAS access token with write scope for the xet CAS service.
func (h *Handler) handleXetWriteToken(w http.ResponseWriter, r *http.Request) {
	h.handleXetToken(w, r, "write")
}

// handleXetToken is the shared implementation for xet-read-token and xet-write-token endpoints.
// It returns the CAS endpoint URL, an access token, and expiration.
//
// The response headers follow the xet-core convention:
//
//	X-Xet-Cas-Url: <CAS endpoint base URL>
//	X-Xet-Access-Token: <token>
//	X-Xet-Token-Expiration: <unix timestamp>
//
// Reference: xet-core git_xet/src/constants.rs
func (h *Handler) handleXetToken(w http.ResponseWriter, r *http.Request, scope string) {
	if !h.xetEnabled {
		responseJSON(w, "xet backend not enabled", http.StatusNotFound)
		return
	}

	info := getRepoInformation(r)

	// Check permissions
	if h.permissionHookFunc != nil {
		op := permission.OperationReadRepo
		if scope == "write" {
			op = permission.OperationUpdateRepo
		}
		if ok, err := h.permissionHookFunc(r.Context(), op, info.RepoName, permission.Context{}); err != nil {
			responseJSON(w, err.Error(), http.StatusInternalServerError)
			return
		} else if !ok {
			responseJSON(w, "permission denied", http.StatusForbidden)
			return
		}
	}

	// Derive the CAS endpoint from the request origin.
	// The server itself acts as the CAS endpoint.
	casURL := requestOrigin(r)

	expiration := time.Now().Add(xetTokenExpiration)

	// Generate a signed access token for CAS access.
	var accessToken string
	user, _ := authenticate.GetUserInfo(r.Context())
	if h.tokenSignValidator != nil {
		// Sign a token that grants access to the CAS endpoints
		token, err := h.tokenSignValidator.Sign(r.Context(), http.MethodGet, "/v1/", user.User, xetTokenExpiration)
		if err != nil {
			responseJSON(w, fmt.Sprintf("failed to sign xet token: %v", err), http.StatusInternalServerError)
			return
		}
		accessToken = token
	} else {
		// Forward the client's Authorization header as the access token
		accessToken = r.Header.Get("Authorization")
		if len(accessToken) > 7 && accessToken[:7] == "Bearer " {
			accessToken = accessToken[7:]
		}
	}

	// Set response headers per xet-core convention
	w.Header().Set("X-Xet-Cas-Url", casURL)
	w.Header().Set("X-Xet-Access-Token", accessToken)
	w.Header().Set("X-Xet-Token-Expiration", fmt.Sprintf("%d", expiration.Unix()))

	responseJSON(w, xetTokenResponse{
		CasURL:      casURL,
		Exp:         expiration.Unix(),
		AccessToken: accessToken,
	}, http.StatusOK)
}
