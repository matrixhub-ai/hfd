package ssh

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/permission"
)

// lfsAuthResponse is the JSON response returned by git-lfs-authenticate.
type lfsAuthResponse struct {
	Href      string            `json:"href"`
	Header    map[string]string `json:"header,omitempty"`
	ExpiresIn int               `json:"expires_in,omitempty"`
}

func lfsHref(httpURL, repoName string) string {
	href := strings.TrimRight(httpURL, "/") + "/" + strings.TrimPrefix(repoName, "/")
	if !strings.HasSuffix(href, ".git") {
		href += ".git"
	}
	href += "/info/lfs"
	return href
}

// executeLFSAuthenticate handles the git-lfs-authenticate command by returning
// a JSON response with the LFS API endpoint URL.
func (s *Server) executeLFSAuthenticate(ctx context.Context, channel ssh.Channel, repoName string, operation string) {
	if s.lfsURL == "" {
		sendExitStatus(channel, 1, "server not configured for host url")
		return
	}

	if operation != "download" && operation != "upload" {
		slog.ErrorContext(ctx, "ssh protocol: git-lfs-authenticate: invalid operation", "operation", operation)
		sendExitStatus(channel, 1, "invalid LFS operation")
		return
	}

	repoPath := s.storage.ResolvePath(repoName)
	if repoPath == "" {
		sendExitStatus(channel, 1, "repository not found")
		return
	}

	if s.permissionHookFunc != nil {
		op := permission.OperationReadRepo
		if operation == "upload" {
			op = permission.OperationUpdateRepo
		}
		if ok, err := s.permissionHookFunc(ctx, op, repoName, permission.Context{}); err != nil {
			slog.WarnContext(ctx, "ssh protocol: permission hook error", "operation", operation, "repo", repoName, "error", err)
			sendExitStatus(channel, 1, "")
			return
		} else if !ok {
			sendExitStatus(channel, 1, "permission denied")
			return
		}
	}

	// Build the LFS API href
	href := lfsHref(s.lfsURL, repoName)

	resp := lfsAuthResponse{
		Href:      href,
		Header:    make(map[string]string),
		ExpiresIn: 3600,
	}

	// Include authentication headers when a token signer is configured,
	// so LFS clients can authenticate with the HTTP server.
	if s.tokenSignValidator != nil {
		userInfo, _ := authenticate.GetUserInfo(ctx)
		batchURL := href + "/objects/batch"
		if token, err := s.tokenSignValidator.Sign(ctx, http.MethodPost, batchURL, userInfo.User, time.Duration(resp.ExpiresIn)*time.Second); err != nil {
			slog.WarnContext(ctx, "ssh protocol: failed to sign LFS auth token", "error", err)
		} else if token != "" {
			resp.Header["Authorization"] = "Bearer " + token
		}
	}

	data, err := json.Marshal(resp)
	if err != nil {
		slog.ErrorContext(ctx, "ssh protocol: failed to marshal LFS auth response", "error", err)
		sendExitStatus(channel, 1, "")
		return
	}

	if _, err := channel.Write(data); err != nil {
		slog.ErrorContext(ctx, "ssh protocol: failed to write LFS auth response", "error", err)
		sendExitStatus(channel, 1, "")
		return
	}

	sendExitStatus(channel, 0, "")
}
