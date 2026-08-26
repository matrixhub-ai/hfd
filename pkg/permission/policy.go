package permission

import (
	"context"
	"log/slog"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
)

// AllowAll returns a hook that allows every operation.
func AllowAll() PermissionHookFunc {
	return func(ctx context.Context, op Operation, repoName string, opCtx Context) (bool, error) {
		return true, nil
	}
}

// RequireAuthenticated returns a hook that allows only named, non-anonymous users.
func RequireAuthenticated() PermissionHookFunc {
	return func(ctx context.Context, op Operation, repoName string, opCtx Context) (bool, error) {
		userInfo, ok := authenticate.GetUserInfo(ctx)
		if !ok || userInfo.User == "" || userInfo.User == authenticate.Anonymous {
			return false, nil
		}
		return true, nil
	}
}

// SplitReadWrite dispatches read operations to read and everything else to write; a nil leg allows.
func SplitReadWrite(read, write PermissionHookFunc) PermissionHookFunc {
	return func(ctx context.Context, op Operation, repoName string, opCtx Context) (bool, error) {
		leg := write
		if op.IsRead() {
			leg = read
		}
		if leg == nil {
			return true, nil
		}
		return leg(ctx, op, repoName, opCtx)
	}
}

// Logged returns a hook that logs each check before delegating to hook; a nil hook allows.
func Logged(hook PermissionHookFunc) PermissionHookFunc {
	return func(ctx context.Context, op Operation, repoName string, opCtx Context) (bool, error) {
		userInfo, _ := authenticate.GetUserInfo(ctx)
		slog.InfoContext(ctx, "Permission check", "user", userInfo.User, "op", op, "repo", repoName, "context", opCtx)
		if hook == nil {
			return true, nil
		}
		return hook(ctx, op, repoName, opCtx)
	}
}
