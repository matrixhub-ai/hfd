package permission_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/permission"
)

func TestCheckNilHook(t *testing.T) {
	var hook permission.PermissionHookFunc
	if err := hook.Check(context.Background(), permission.OperationUpdateRepo, "test-repo", permission.Context{}); err != nil {
		t.Fatalf("nil hook should allow, got %v", err)
	}
}

func TestCheckAllow(t *testing.T) {
	hook := permission.PermissionHookFunc(func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) (bool, error) {
		return true, nil
	})
	if err := hook.Check(context.Background(), permission.OperationReadRepo, "test-repo", permission.Context{}); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestCheckDeny(t *testing.T) {
	hook := permission.PermissionHookFunc(func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) (bool, error) {
		return false, nil
	})
	err := hook.Check(context.Background(), permission.OperationReadRepo, "test-repo", permission.Context{})
	if !errors.Is(err, permission.ErrDenied) {
		t.Fatalf("expected ErrDenied, got %v", err)
	}
}

func TestCheckErrorPassthrough(t *testing.T) {
	errBoom := errors.New("boom")
	hook := permission.PermissionHookFunc(func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) (bool, error) {
		return false, errBoom
	})
	err := hook.Check(context.Background(), permission.OperationReadRepo, "test-repo", permission.Context{})
	if !errors.Is(err, errBoom) {
		t.Fatalf("expected boom error, got %v", err)
	}
	if errors.Is(err, permission.ErrDenied) {
		t.Fatalf("hook error must not be ErrDenied, got %v", err)
	}
}

// recordingResponder captures the arguments Guard.Allow passes to Respond.
type recordingResponder struct {
	called     bool
	message    string
	statusCode int
}

func (rr *recordingResponder) respond(w http.ResponseWriter, message string, statusCode int) {
	rr.called = true
	rr.message = message
	rr.statusCode = statusCode
}

func TestGuardAllowNilHook(t *testing.T) {
	rr := &recordingResponder{}
	g := permission.Guard{Respond: rr.respond}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if !g.Allow(httptest.NewRecorder(), r, permission.OperationReadRepo, "test-repo", permission.Context{}) {
		t.Fatal("nil hook should allow")
	}
	if rr.called {
		t.Fatal("responder should not be called on allow")
	}
}

func TestGuardAllowDeny(t *testing.T) {
	rr := &recordingResponder{}
	g := permission.Guard{
		Hook: func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) (bool, error) {
			return false, nil
		},
		Respond: rr.respond,
	}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if g.Allow(httptest.NewRecorder(), r, permission.OperationUpdateRepo, "test-repo", permission.Context{}) {
		t.Fatal("deny should not allow")
	}
	if !rr.called {
		t.Fatal("responder should be called on deny")
	}
	if rr.message != "permission denied" || rr.statusCode != http.StatusForbidden {
		t.Errorf("got (%q, %d), want (%q, %d)", rr.message, rr.statusCode, "permission denied", http.StatusForbidden)
	}
}

func TestGuardAllowError(t *testing.T) {
	errBoom := errors.New("boom")
	rr := &recordingResponder{}
	g := permission.Guard{
		Hook: func(ctx context.Context, op permission.Operation, repo string, opCtx permission.Context) (bool, error) {
			return false, errBoom
		},
		Respond: rr.respond,
	}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if g.Allow(httptest.NewRecorder(), r, permission.OperationReadRepo, "test-repo", permission.Context{}) {
		t.Fatal("hook error should not allow")
	}
	if !rr.called {
		t.Fatal("responder should be called on hook error")
	}
	if rr.message != errBoom.Error() || rr.statusCode != http.StatusInternalServerError {
		t.Errorf("got (%q, %d), want (%q, %d)", rr.message, rr.statusCode, errBoom.Error(), http.StatusInternalServerError)
	}
}
