package hf

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/wzshiming/xet/auth"

	"github.com/matrixhub-ai/hfd/pkg/permission"
)

// Read tokens are CAS-wide by hash, matching huggingface.co.
func (h *Handler) handleXETWriteToken(w http.ResponseWriter, r *http.Request) {
	h.handleXETToken(w, r, permission.OperationUpdateRepo, auth.Write)
}

func (h *Handler) handleXETReadToken(w http.ResponseWriter, r *http.Request) {
	h.handleXETToken(w, r, permission.OperationReadRepo, auth.Read)
}

func (h *Handler) handleXETToken(w http.ResponseWriter, r *http.Request, op permission.Operation, perm auth.Permission) {
	ri := getRepoInformation(r)
	if !h.checkPermission(w, r, op, ri.RepoName, permission.Context{Ref: mux.Vars(r)["rev"]}) {
		return
	}
	if h.mirror == nil || !h.mirror.CanMintToken() {
		responseJSON(w, "CAS token minting is not available", http.StatusNotFound)
		return
	}
	casURL, token, expiresAt, err := h.mirror.MintXETToken(r, auth.Grant{Permission: perm})
	if err != nil {
		slog.WarnContext(r.Context(), "mint CAS token", "error", err)
		responseJSON(w, "failed to mint CAS token", http.StatusInternalServerError)
		return
	}
	respondXETToken(w, casURL, token, expiresAt.Unix())
}

// New hub clients read headers; older clients read the JSON body.
func respondXETToken(w http.ResponseWriter, casURL, token string, exp int64) {
	w.Header().Set("X-Xet-Cas-Url", casURL)
	w.Header().Set("X-Xet-Access-Token", token)
	w.Header().Set("X-Xet-Token-Expiration", strconv.FormatInt(exp, 10))
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"casUrl":      casURL,
		"accessToken": token,
		"exp":         exp,
	})
}
