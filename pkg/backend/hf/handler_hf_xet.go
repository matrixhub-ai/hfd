package hf

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/matrixhub-ai/hfd/pkg/permission"
)

const (
	// xetTokenExpiration is the default expiration time for xet tokens.
	xetTokenExpiration = time.Hour

	// headerXetEndpoint is the header name for the xet CAS endpoint.
	headerXetEndpoint = "X-Xet-Endpoint"
	// headerXetAccessToken is the header name for the xet access token.
	headerXetAccessToken = "X-Xet-Access-Token"
	// headerXetExpiration is the header name for the xet token expiration (Unix epoch).
	headerXetExpiration = "X-Xet-Expiration"
)

// handleXetReadToken handles GET /api/{repoType}/{namespace}/{repo}/xet-read-token/{rev}
func (h *Handler) handleXetReadToken(w http.ResponseWriter, r *http.Request) {
	h.handleXetToken(w, r, permission.OperationReadRepo)
}

// handleXetWriteToken handles GET /api/{repoType}/{namespace}/{repo}/xet-write-token/{rev}
func (h *Handler) handleXetWriteToken(w http.ResponseWriter, r *http.Request) {
	h.handleXetToken(w, r, permission.OperationUpdateRepo)
}

func (h *Handler) handleXetToken(w http.ResponseWriter, r *http.Request, op permission.Operation) {
	if !h.xetEnabled {
		responseJSON(w, "xet backend not configured", http.StatusNotImplemented)
		return
	}

	ri := getRepoInformation(r)

	if h.permissionHookFunc != nil {
		if ok, err := h.permissionHookFunc(r.Context(), op, ri.RepoName, permission.Context{}); err != nil {
			responseJSON(w, err.Error(), http.StatusInternalServerError)
			return
		} else if !ok {
			responseJSON(w, "permission denied", http.StatusForbidden)
			return
		}
	}

	repoPath := h.storage.ResolvePath(ri.RepoName)
	if repoPath == "" {
		responseJSON(w, fmt.Errorf("repository %q not found", ri.RepoName), http.StatusNotFound)
		return
	}

	expiration := time.Now().Add(xetTokenExpiration)
	token := r.Header.Get("Authorization")

	w.Header().Set(headerXetEndpoint, requestOrigin(r))
	w.Header().Set(headerXetAccessToken, token)
	w.Header().Set(headerXetExpiration, strconv.FormatInt(expiration.Unix(), 10))
	w.WriteHeader(http.StatusOK)
}
