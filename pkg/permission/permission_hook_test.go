package permission_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wzshiming/hfd/pkg/permission"
)

func TestOperationConstants(t *testing.T) {
	// Verify enum values are distinct and non-zero (except OperationUnknown)
	ops := []permission.Operation{
		permission.OperationCreateRepo,
		permission.OperationDeleteRepo,
		permission.OperationReadRepo,
		permission.OperationUpdateRepo,
	}
	seen := map[permission.Operation]bool{}
	for _, op := range ops {
		if op == 0 {
			t.Errorf("Operation %v should not be zero value", op)
		}
		if seen[op] {
			t.Errorf("Duplicate operation value: %v", op)
		}
		seen[op] = true
	}

	// OperationUnknown should be zero
	if permission.OperationUnknown != 0 {
		t.Errorf("OperationUnknown should be zero, got %d", permission.OperationUnknown)
	}
}

func TestOperationString(t *testing.T) {
	tests := []struct {
		op   permission.Operation
		want string
	}{
		{permission.OperationUnknown, "unknown"},
		{permission.OperationCreateRepo, "create_repo"},
		{permission.OperationDeleteRepo, "delete_repo"},
		{permission.OperationReadRepo, "read_repo"},
		{permission.OperationUpdateRepo, "update_repo"},
		{permission.Operation(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.op.String(); got != tt.want {
			t.Errorf("Operation(%d).String() = %q, want %q", int(tt.op), got, tt.want)
		}
	}
}

func TestAuthHookAllow(t *testing.T) {
	hook := func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) error {
		return nil
	}

	if err := hook(context.Background(), permission.OperationReadRepo, "test-repo", permission.Context{}); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestAuthHookDeny(t *testing.T) {
	errDenied := errors.New("access denied")
	hook := func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) error {
		return errDenied
	}

	err := hook(context.Background(), permission.OperationUpdateRepo, "test-repo", permission.Context{})
	if !errors.Is(err, errDenied) {
		t.Errorf("expected errDenied, got %v", err)
	}
}

func TestAuthHookFineGrainedOperations(t *testing.T) {
	// Hook that allows reads and fetches, denies writes/deletes/force-push, allows proxy reads
	hook := func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) error {
		switch op {
		case permission.OperationReadRepo:
			return nil
		case permission.OperationCreateRepo,
			permission.OperationDeleteRepo,
			permission.OperationUpdateRepo:
			return errors.New("access denied for " + op.String())
		default:
			return errors.New("unknown operation")
		}
	}

	tests := []struct {
		op      permission.Operation
		wantErr bool
	}{
		{permission.OperationReadRepo, false},
		{permission.OperationCreateRepo, true},
		{permission.OperationDeleteRepo, true},
		{permission.OperationUpdateRepo, true},
	}

	for _, tt := range tests {
		err := hook(context.Background(), tt.op, "test-repo", permission.Context{})
		if (err != nil) != tt.wantErr {
			t.Errorf("op=%s: got err=%v, wantErr=%v", tt.op, err, tt.wantErr)
		}
	}
}

func TestAuthHookRepoBasedDecision(t *testing.T) {
	// Hook that only allows access to specific repos
	hook := func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) error {
		if repo != "allowed-repo" {
			return errors.New("access denied for repo")
		}
		return nil
	}

	if err := hook(context.Background(), permission.OperationReadRepo, "allowed-repo", permission.Context{}); err != nil {
		t.Errorf("expected access to be allowed, got %v", err)
	}

	if err := hook(context.Background(), permission.OperationReadRepo, "denied-repo", permission.Context{}); err == nil {
		t.Error("expected access to be denied")
	}
}
