package huggingface

import (
	"net/http"

	"github.com/wzshiming/hfd/pkg/authenticate"
)

// HFWhoamiResponse represents the response for the /api/whoami-v2 endpoint.
type HFWhoamiResponse struct {
	Type          string     `json:"type"`
	ID            string     `json:"id"`
	Name          string     `json:"name"`
	Fullname      string     `json:"fullname"`
	Email         string     `json:"email,omitempty"`
	EmailVerified bool       `json:"emailVerified"`
	IsPro         bool       `json:"isPro"`
	CanPay        bool       `json:"canPay"`
	AvatarURL     string     `json:"avatarUrl,omitempty"`
	Orgs          []any      `json:"orgs"`
	Auth          HFAuthInfo `json:"auth"`
}

// HFAuthInfo represents the auth section of the whoami response.
type HFAuthInfo struct {
	AccessToken HFAccessToken `json:"accessToken"`
}

// HFAccessToken represents the access token info in the whoami response.
type HFAccessToken struct {
	DisplayName string `json:"displayName"`
	Role        string `json:"role"`
}

// handleWhoami handles GET /api/whoami-v2
func (h *Handler) handleWhoami(w http.ResponseWriter, r *http.Request) {
	userInfo, ok := authenticate.GetUserInfo(r.Context())
	if !ok || userInfo.User == authenticate.Anonymous {
		responseJSON(w, map[string]string{"error": "Unauthorized"}, http.StatusUnauthorized)
		return
	}

	resp := HFWhoamiResponse{
		Type:          "user",
		ID:            userInfo.User,
		Name:          userInfo.User,
		Fullname:      userInfo.User,
		Email:         userInfo.Email,
		EmailVerified: false,
		IsPro:         false,
		CanPay:        false,
		Orgs:          []any{},
		Auth: HFAuthInfo{
			AccessToken: HFAccessToken{
				DisplayName: "token",
				Role:        "write",
			},
		},
	}

	responseJSON(w, resp, http.StatusOK)
}
