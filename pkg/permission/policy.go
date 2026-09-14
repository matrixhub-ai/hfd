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
		return !authenticate.IsAnonymous(authenticate.IdentityFrom(ctx)), nil
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

// All returns a hook that allows only when every non-nil hook allows.
func All(hooks ...PermissionHookFunc) PermissionHookFunc {
	return func(ctx context.Context, op Operation, repoName string, opCtx Context) (bool, error) {
		for _, hook := range hooks {
			if hook == nil {
				continue
			}
			ok, err := hook(ctx, op, repoName, opCtx)
			if !ok || err != nil {
				return ok, err
			}
		}
		return true, nil
	}
}

// MirrorRoles reports whether a repository is pulled from or pushed to a remote; *mirror.Mirror implements it.
type MirrorRoles interface {
	IsMirrorSource(ctx context.Context, repoName string) (bool, error)
	IsMirrorDestination(ctx context.Context, repoName string) (bool, error)
}

// PullMirrorReadOnly returns a hook that refuses updates to repositories mirrored from a remote and not pushed back to one, a move's destination included; a nil interface (not a typed nil) allows everything.
func PullMirrorReadOnly(m MirrorRoles) PermissionHookFunc {
	return func(ctx context.Context, op Operation, repoName string, opCtx Context) (bool, error) {
		if m == nil || !op.IsUpdate() {
			return true, nil
		}
		names := []string{repoName}
		if opCtx.DestRepo != "" {
			names = append(names, opCtx.DestRepo)
		}
		for _, name := range names {
			source, err := m.IsMirrorSource(ctx, name)
			if err != nil {
				return false, err
			}
			if !source {
				continue
			}
			destination, err := m.IsMirrorDestination(ctx, name)
			if err != nil || !destination {
				return false, err
			}
		}
		return true, nil
	}
}

// Logged returns a hook that logs each check before delegating to hook; a nil hook allows.
func Logged(hook PermissionHookFunc) PermissionHookFunc {
	return func(ctx context.Context, op Operation, repoName string, opCtx Context) (bool, error) {
		slog.InfoContext(ctx, "Permission check", "user", authenticate.IdentityFrom(ctx).Name(), "op", op, "repo", repoName, "context", opCtx)
		if hook == nil {
			return true, nil
		}
		return hook(ctx, op, repoName, opCtx)
	}
}
