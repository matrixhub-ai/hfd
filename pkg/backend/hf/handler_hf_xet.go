package hf

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/permission"
)

const xetTokenExpiration = time.Hour

// handleXetReadToken handles POST /api/{repoType}/{namespace}/{repo}/xet-read-token/{rev}
// Returns a signed access token and the CAS endpoint URL for xet-core read operations.
// The xet-core client calls this endpoint to obtain credentials before downloading files
// via the CAS protocol.
func (h *Handler) handleXetReadToken(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := getRepoInformation(r)
	rev := vars["rev"]

	if h.permissionHookFunc != nil {
		if ok, err := h.permissionHookFunc(r.Context(), permission.OperationReadRepo, ri.RepoName, permission.Context{Ref: rev}); err != nil {
			responseJSON(w, err.Error(), http.StatusInternalServerError)
			return
		} else if !ok {
			responseJSON(w, "permission denied", http.StatusForbidden)
			return
		}
	}

	origin := requestOrigin(r)
	endpoint := origin + "/api/v1"

	expiration := time.Now().Add(xetTokenExpiration)
	token := ""

	if h.tokenSignValidator != nil {
		user, _ := authenticate.GetUserInfo(r.Context())
		signed, err := h.tokenSignValidator.Sign(r.Context(), http.MethodGet, endpoint, user.User, xetTokenExpiration)
		if err != nil {
			responseJSON(w, fmt.Sprintf("failed to sign xet token: %v", err), http.StatusInternalServerError)
			return
		}
		token = signed
	} else {
		// Forward the client's existing authorization
		token = r.Header.Get("Authorization")
	}

	w.Header().Set("X-Xet-Endpoint", endpoint)
	w.Header().Set("X-Xet-Access-Token", token)
	w.Header().Set("X-Xet-Expiration", expiration.UTC().Format(time.RFC3339))
	w.WriteHeader(http.StatusOK)
}

// handleXetWriteToken handles POST /api/{repoType}/{namespace}/{repo}/xet-write-token/{rev}
// Returns a signed access token and the CAS endpoint URL for xet-core write operations.
// The xet-core client calls this endpoint to obtain credentials before uploading files
// via the CAS protocol (xorbs and shards).
func (h *Handler) handleXetWriteToken(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ri := getRepoInformation(r)
	rev := vars["rev"]

	if h.permissionHookFunc != nil {
		if ok, err := h.permissionHookFunc(r.Context(), permission.OperationUpdateRepo, ri.RepoName, permission.Context{Ref: rev}); err != nil {
			responseJSON(w, err.Error(), http.StatusInternalServerError)
			return
		} else if !ok {
			responseJSON(w, "permission denied", http.StatusForbidden)
			return
		}
	}

	origin := requestOrigin(r)
	endpoint := origin + "/api/v1"

	expiration := time.Now().Add(xetTokenExpiration)
	token := ""

	if h.tokenSignValidator != nil {
		user, _ := authenticate.GetUserInfo(r.Context())
		signed, err := h.tokenSignValidator.Sign(r.Context(), http.MethodPut, endpoint, user.User, xetTokenExpiration)
		if err != nil {
			responseJSON(w, fmt.Sprintf("failed to sign xet token: %v", err), http.StatusInternalServerError)
			return
		}
		token = signed
	} else {
		token = r.Header.Get("Authorization")
	}

	w.Header().Set("X-Xet-Endpoint", endpoint)
	w.Header().Set("X-Xet-Access-Token", token)
	w.Header().Set("X-Xet-Expiration", expiration.UTC().Format(time.RFC3339))
	w.WriteHeader(http.StatusOK)
}
