package lfs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/permission"
)

// TestLockRoutesRepoName checks bare repository names and rejects old lock URLs.
func TestLockRoutesRepoName(t *testing.T) {
	const batch = `{"operation":"download","objects":[{"oid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":1}]}`
	for _, row := range []struct {
		method string
		route  string
		body   string
		op     permission.Operation
		repo   string
		status int
	}{
		{http.MethodGet, "/org/name.git/info/lfs/locks", "", permission.OperationReadRepo, "org/name", http.StatusForbidden},
		{http.MethodGet, "/org/name/info/lfs/locks", "", permission.OperationReadRepo, "org/name", http.StatusForbidden},
		{http.MethodPost, "/org/name.git/info/lfs/locks", `{"path":"model.bin"}`, permission.OperationUpdateRepo, "org/name", http.StatusForbidden},
		{http.MethodPost, "/org/name/info/lfs/locks", `{"path":"model.bin"}`, permission.OperationUpdateRepo, "org/name", http.StatusForbidden},
		{http.MethodPost, "/org/name.git/info/lfs/locks/verify", `{}`, permission.OperationReadRepo, "org/name", http.StatusForbidden},
		{http.MethodPost, "/org/name/info/lfs/locks/verify", `{}`, permission.OperationReadRepo, "org/name", http.StatusForbidden},
		{http.MethodPost, "/org/name.git/info/lfs/locks/deadbeef/unlock", `{}`, permission.OperationUpdateRepo, "org/name", http.StatusForbidden},
		{http.MethodPost, "/org/name/info/lfs/locks/deadbeef/unlock", `{}`, permission.OperationUpdateRepo, "org/name", http.StatusForbidden},
		{http.MethodPost, "/org/name.git/info/lfs/objects/batch", batch, permission.OperationReadRepo, "org/name", http.StatusForbidden},
		{http.MethodPost, "/org/name/info/lfs/objects/batch", batch, permission.OperationReadRepo, "org/name", http.StatusForbidden},
		{http.MethodPost, "/org/name.git/locks", `{"path":"model.bin"}`, 0, "", http.StatusTeapot},
		{http.MethodPost, "/org/name/locks", `{"path":"model.bin"}`, 0, "", http.StatusTeapot},
	} {
		t.Run(row.method+row.route, func(t *testing.T) {
			var gotOp permission.Operation
			var gotRepo string
			handler := NewHandler(
				WithPermissionHookFunc(func(_ context.Context, op permission.Operation, repoName string, _ permission.Context) (bool, error) {
					gotOp, gotRepo = op, repoName
					return false, nil
				}),
				WithNext(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusTeapot)
				})),
			)
			req := httptest.NewRequest(row.method, row.route, strings.NewReader(row.body))
			req.Header.Set("Accept", "application/vnd.git-lfs+json")
			req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != row.status || gotRepo != row.repo || gotOp != row.op {
				t.Fatalf("status=%d repo=%q op=%s; want status=%d repo=%q op=%s; body=%s", rec.Code, gotRepo, gotOp, row.status, row.repo, row.op, rec.Body.String())
			}
		})
	}
}
