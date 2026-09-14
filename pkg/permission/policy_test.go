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
		{"NoIdentity", context.Background(), false},
		{"Anonymous", authenticate.WithIdentity(context.Background(), authenticate.Anonymous), false},
		{"EmptyUser", authenticate.WithIdentity(context.Background(), authenticate.NewIdentity("", "")), false},
		{"NamedUser", authenticate.WithIdentity(context.Background(), authenticate.NewIdentity("alice", "")), true},
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
		{permission.OperationListRepos, false},
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

func TestAll(t *testing.T) {
	errBoom := errors.New("boom")
	type result struct {
		allow bool
		err   error
	}
	allow, deny, failure := &result{allow: true}, &result{}, &result{err: errBoom}
	tests := []struct {
		name      string
		legs      []*result
		want      bool
		wantErr   error
		wantCalls int
	}{
		{"NoHooks", nil, true, nil, 0},
		{"AllNil", []*result{nil, nil}, true, nil, 0},
		{"SkipNil", []*result{nil, allow, nil, allow}, true, nil, 2},
		{"DenyStops", []*result{allow, deny, allow}, false, nil, 2},
		{"ErrorStops", []*result{allow, failure, allow}, false, errBoom, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var hooks []permission.PermissionHookFunc
			calls := 0
			ctx := context.Background()
			opCtx := permission.Context{Ref: "main", DestRepo: "other/repo"}
			for _, leg := range tt.legs {
				if leg == nil {
					hooks = append(hooks, nil)
					continue
				}
				hooks = append(hooks, func(gotCtx context.Context, op permission.Operation, repo string, gotOpCtx permission.Context) (bool, error) {
					calls++
					if gotCtx != ctx || op != permission.OperationUpdateRepo || repo != "test-repo" || gotOpCtx != opCtx {
						t.Error("hook arguments were not forwarded")
					}
					return leg.allow, leg.err
				})
			}
			ok, err := permission.All(hooks...)(ctx, permission.OperationUpdateRepo, "test-repo", opCtx)
			if ok != tt.want || !errors.Is(err, tt.wantErr) || calls != tt.wantCalls {
				t.Fatalf("got (%v, %v), calls=%d; want (%v, %v), calls=%d", ok, err, calls, tt.want, tt.wantErr, tt.wantCalls)
			}
		})
	}
}

type fakeMirrorRoles struct {
	src, dest           bool
	srcOnly             string // when set, only this repo name counts as a source
	err, destErr        error
	srcCalls, destCalls int
}

func (m *fakeMirrorRoles) IsMirrorSource(ctx context.Context, repoName string) (bool, error) {
	m.srcCalls++
	if m.srcOnly != "" {
		return repoName == m.srcOnly, m.err
	}
	return m.src, m.err
}

func (m *fakeMirrorRoles) IsMirrorDestination(ctx context.Context, repoName string) (bool, error) {
	m.destCalls++
	return m.dest, m.destErr
}

func TestPullMirrorReadOnly(t *testing.T) {
	errBoom := errors.New("boom")
	tests := []struct {
		name     string
		op       permission.Operation
		roles    *fakeMirrorRoles
		opCtx    permission.Context
		want     bool
		wantErr  error
		wantSrc  int
		wantDest int
	}{
		{"PullOnlyUpdate", permission.OperationUpdateRepo, &fakeMirrorRoles{src: true}, permission.Context{}, false, nil, 1, 1},
		{"PullPushUpdate", permission.OperationUpdateRepo, &fakeMirrorRoles{src: true, dest: true}, permission.Context{}, true, nil, 1, 1},
		{"LocalUpdate", permission.OperationUpdateRepo, &fakeMirrorRoles{}, permission.Context{}, true, nil, 1, 0},
		{"PushOnlyUpdate", permission.OperationUpdateRepo, &fakeMirrorRoles{dest: true}, permission.Context{}, true, nil, 1, 0},
		{"MoveIntoPullMirror", permission.OperationUpdateRepo, &fakeMirrorRoles{srcOnly: "mirrored"}, permission.Context{DestRepo: "mirrored"}, false, nil, 2, 1},
		{"MoveOutOfPullMirror", permission.OperationUpdateRepo, &fakeMirrorRoles{srcOnly: "test-repo"}, permission.Context{DestRepo: "local"}, false, nil, 1, 1},
		{"MoveBetweenLocal", permission.OperationUpdateRepo, &fakeMirrorRoles{}, permission.Context{DestRepo: "local"}, true, nil, 2, 0},
		{"Read", permission.OperationReadRepo, &fakeMirrorRoles{src: true}, permission.Context{}, true, nil, 0, 0},
		{"List", permission.OperationListRepos, &fakeMirrorRoles{src: true}, permission.Context{}, true, nil, 0, 0},
		{"Create", permission.OperationCreateRepo, &fakeMirrorRoles{src: true}, permission.Context{}, true, nil, 0, 0},
		{"Delete", permission.OperationDeleteRepo, &fakeMirrorRoles{src: true}, permission.Context{}, true, nil, 0, 0},
		{"SourceError", permission.OperationUpdateRepo, &fakeMirrorRoles{err: errBoom}, permission.Context{}, false, errBoom, 1, 0},
		{"DestinationError", permission.OperationUpdateRepo, &fakeMirrorRoles{src: true, destErr: errBoom}, permission.Context{}, false, errBoom, 1, 1},
		{"Nil", permission.OperationUpdateRepo, nil, permission.Context{}, true, nil, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var roles permission.MirrorRoles
			if tt.roles != nil {
				roles = tt.roles
			}
			ok, err := permission.PullMirrorReadOnly(roles)(context.Background(), tt.op, "test-repo", tt.opCtx)
			if ok != tt.want || !errors.Is(err, tt.wantErr) {
				t.Fatalf("got (%v, %v), want (%v, %v)", ok, err, tt.want, tt.wantErr)
			}
			if tt.roles != nil && (tt.roles.srcCalls != tt.wantSrc || tt.roles.destCalls != tt.wantDest) {
				t.Errorf("source calls=%d, destination calls=%d; want %d, %d", tt.roles.srcCalls, tt.roles.destCalls, tt.wantSrc, tt.wantDest)
			}
		})
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
