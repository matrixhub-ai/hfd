package permission_test

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/permission"
)

func TestAllowAll(t *testing.T) {
	hook := permission.AllowAll()
	ok, err := hook(context.Background(), permission.OperationDeleteRepo, "any/repo", permission.Context{})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !ok {
		t.Fatal("expected allow")
	}
}

func TestRequireAuthenticated(t *testing.T) {
	hook := permission.RequireAuthenticated()

	tests := []struct {
		name string
		ctx  context.Context
		want bool
	}{
		{"NoUserInfo", context.Background(), false},
		{"Anonymous", authenticate.WithContext(context.Background(), authenticate.UserInfo{User: authenticate.Anonymous}), false},
		{"EmptyUser", authenticate.WithContext(context.Background(), authenticate.UserInfo{}), false},
		{"NamedUser", authenticate.WithContext(context.Background(), authenticate.UserInfo{User: "alice"}), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, err := hook(tt.ctx, permission.OperationReadRepo, "test-repo", permission.Context{})
			if err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
			if ok != tt.want {
				t.Errorf("got ok=%v, want %v", ok, tt.want)
			}
		})
	}
}

func TestSplitReadWrite(t *testing.T) {
	tests := []struct {
		op        permission.Operation
		wantWrite bool
	}{
		{permission.OperationReadRepo, false},
		{permission.OperationUpdateRepo, true},
		{permission.OperationCreateRepo, true},
		{permission.OperationDeleteRepo, true},
		{permission.OperationUnknown, true},
	}
	for _, tt := range tests {
		t.Run(tt.op.String(), func(t *testing.T) {
			var readCalled, writeCalled bool
			read := func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) (bool, error) {
				readCalled = true
				return true, nil
			}
			write := func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) (bool, error) {
				writeCalled = true
				return true, nil
			}
			hook := permission.SplitReadWrite(read, write)
			if _, err := hook(context.Background(), tt.op, "test-repo", permission.Context{}); err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
			if readCalled == tt.wantWrite || writeCalled != tt.wantWrite {
				t.Errorf("op=%s: readCalled=%v writeCalled=%v, wantWrite=%v", tt.op, readCalled, writeCalled, tt.wantWrite)
			}
		})
	}
}

func TestSplitReadWriteNilLegs(t *testing.T) {
	deny := func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) (bool, error) {
		return false, nil
	}

	// Nil read leg allows reads even when the write leg denies.
	hook := permission.SplitReadWrite(nil, deny)
	if ok, err := hook(context.Background(), permission.OperationReadRepo, "test-repo", permission.Context{}); err != nil || !ok {
		t.Errorf("nil read leg: got (%v, %v), want allow", ok, err)
	}

	// Nil write leg allows writes even when the read leg denies.
	hook = permission.SplitReadWrite(deny, nil)
	if ok, err := hook(context.Background(), permission.OperationUpdateRepo, "test-repo", permission.Context{}); err != nil || !ok {
		t.Errorf("nil write leg: got (%v, %v), want allow", ok, err)
	}
}

func TestLoggedDelegates(t *testing.T) {
	errBoom := errors.New("boom")
	tests := []struct {
		name    string
		hook    permission.PermissionHookFunc
		wantOK  bool
		wantErr error
	}{
		{"Allow", func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) (bool, error) {
			return true, nil
		}, true, nil},
		{"Deny", func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) (bool, error) {
			return false, nil
		}, false, nil},
		{"Error", func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) (bool, error) {
			return false, errBoom
		}, false, errBoom},
		{"NilHook", nil, true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, err := permission.Logged(tt.hook)(context.Background(), permission.OperationReadRepo, "test-repo", permission.Context{})
			if ok != tt.wantOK {
				t.Errorf("got ok=%v, want %v", ok, tt.wantOK)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("got err=%v, want %v", err, tt.wantErr)
			}
		})
	}
}
