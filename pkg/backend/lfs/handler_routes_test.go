package lfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

const (
	testBatchDownload = `{"operation":"download","objects":[{"oid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":1}]}`
	testBatchUpload   = `{"operation":"upload","objects":[{"oid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":1}]}`
)

func lfsTestRequest(method, target, body string) *http.Request {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set("Accept", metaMediaType)
	req.Header.Set("Content-Type", metaMediaType)
	return req
}

// TestLockRoutesRepoName checks the PreOpen hook gets the bare repository name and write flag, and rejects old lock URLs.
func TestLockRoutesRepoName(t *testing.T) {
	type call struct {
		repo  string
		write bool
	}
	for _, row := range []struct {
		name   string
		method string
		path   string
		body   string
		calls  []call
		status int
	}{
		{"LocksList", http.MethodGet, "/info/lfs/locks", "", []call{{"org/name", false}}, http.StatusForbidden},
		{"LockCreate", http.MethodPost, "/info/lfs/locks", `{"path":"model.bin"}`, []call{{"org/name", true}}, http.StatusForbidden},
		{"LocksVerify", http.MethodPost, "/info/lfs/locks/verify", `{}`, []call{{"org/name", false}}, http.StatusForbidden},
		{"Unlock", http.MethodPost, "/info/lfs/locks/deadbeef/unlock", `{}`, []call{{"org/name", true}}, http.StatusForbidden},
		// Lock routes admit before decoding the body, so a malformed one is still refused by the hook.
		{"LockCreateMalformed", http.MethodPost, "/info/lfs/locks", `{`, []call{{"org/name", true}}, http.StatusForbidden},
		{"LocksVerifyMalformed", http.MethodPost, "/info/lfs/locks/verify", `{`, []call{{"org/name", false}}, http.StatusForbidden},
		{"UnlockMalformed", http.MethodPost, "/info/lfs/locks/deadbeef/unlock", `{`, []call{{"org/name", true}}, http.StatusForbidden},
		{"BatchDownload", http.MethodPost, "/info/lfs/objects/batch", testBatchDownload, []call{{"org/name", false}}, http.StatusForbidden},
		{"BatchUpload", http.MethodPost, "/info/lfs/objects/batch", testBatchUpload, []call{{"org/name", true}}, http.StatusForbidden},
		// No object carries the repository name, so it must come from the route.
		{"BatchNoObjects", http.MethodPost, "/info/lfs/objects/batch", `{"operation":"upload","objects":[]}`, []call{{"org/name", true}}, http.StatusForbidden},
		{"OldLockURL", http.MethodPost, "/locks", `{"path":"model.bin"}`, nil, http.StatusTeapot},
	} {
		for _, suffix := range []string{".git", ""} {
			t.Run(row.name+suffix, func(t *testing.T) {
				var calls []call
				handler := NewHandler(
					WithPreOpenHookFunc(func(_ context.Context, repoName string, write bool) error {
						calls = append(calls, call{repoName, write})
						return fmt.Errorf("x: %w", fs.ErrPermission)
					}),
					WithNext(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						w.WriteHeader(http.StatusTeapot)
					})),
				)
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, lfsTestRequest(row.method, "/org/name"+suffix+row.path, row.body))
				if rec.Code != row.status || !slices.Equal(calls, row.calls) {
					t.Fatalf("status=%d PreOpen calls=%v; want status=%d calls=%v; body=%s", rec.Code, calls, row.status, row.calls, rec.Body.String())
				}
			})
		}
	}
}

// TestPreOpenRefusalResponses pins each refusal's status, fixed text in the route's JSON shape, and 401 challenge.
func TestPreOpenRefusalResponses(t *testing.T) {
	routes := []struct {
		name   string
		method string
		target string
		body   string
		field  string
	}{
		{"LocksList", http.MethodGet, "/org/name.git/info/lfs/locks", "", "message"},
		{"LockCreate", http.MethodPost, "/org/name.git/info/lfs/locks", `{"path":"model.bin"}`, "message"},
		{"LocksVerify", http.MethodPost, "/org/name.git/info/lfs/locks/verify", `{}`, "message"},
		{"Unlock", http.MethodPost, "/org/name.git/info/lfs/locks/deadbeef/unlock", `{}`, "message"},
		{"BatchDownload", http.MethodPost, "/org/name.git/info/lfs/objects/batch", testBatchDownload, "error"},
		{"BatchUpload", http.MethodPost, "/org/name.git/info/lfs/objects/batch", testBatchUpload, "error"},
	}
	for _, refusal := range []struct {
		name      string
		err       error
		status    int
		challenge string
		text      string
	}{
		{"Permission", fmt.Errorf("x: %w", fs.ErrPermission), http.StatusForbidden, "", "permission denied"},
		{"Unauthenticated", fmt.Errorf("x: %w", authenticate.ErrUnauthenticated), http.StatusUnauthorized, `Basic realm="hfd"`, "Unauthorized"},
		{"NotExists", fmt.Errorf("x: %w", repository.ErrRepositoryNotExists), http.StatusNotFound, "", `repository "org/name" not found`},
		{"Other", errors.New("boom"), http.StatusInternalServerError, "", "boom"},
	} {
		for _, route := range routes {
			t.Run(refusal.name+"/"+route.name, func(t *testing.T) {
				handler := NewHandler(WithPreOpenHookFunc(func(context.Context, string, bool) error {
					return refusal.err
				}))
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, lfsTestRequest(route.method, route.target, route.body))
				challenge := rec.Result().Header.Get("WWW-Authenticate")
				var body map[string]string
				err := json.Unmarshal(rec.Body.Bytes(), &body)
				text, ok := body[route.field]
				ok = ok && err == nil && len(body) == 1 && text == refusal.text
				if rec.Code != refusal.status || challenge != refusal.challenge || !ok {
					t.Fatalf("PreOpen error %q: status=%d WWW-Authenticate=%q body=%s; want status=%d WWW-Authenticate=%q body {%q: %q}",
						refusal.err, rec.Code, challenge, rec.Body.String(), refusal.status, refusal.challenge, route.field, refusal.text)
				}
			})
		}
	}
}

// TestPreOpenRefusalCreatesNoLock pins that a refused lock creation stores nothing.
func TestPreOpenRefusalCreatesNoLock(t *testing.T) {
	refusal := fmt.Errorf("x: %w", fs.ErrPermission)
	handler := NewHandler(WithPreOpenHookFunc(func(context.Context, string, bool) error {
		return refusal
	}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, lfsTestRequest(http.MethodPost, "/org/name.git/info/lfs/locks", `{"path":"model.bin"}`))
	if rec.Code != http.StatusForbidden {
		t.Errorf("refused lock create: status=%d, want %d; body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	refusal = nil
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, lfsTestRequest(http.MethodGet, "/org/name.git/info/lfs/locks", ""))
	var list lfs.LockList
	if err := json.Unmarshal(rec.Body.Bytes(), &list); rec.Code != http.StatusOK || err != nil || len(list.Locks) != 0 {
		t.Fatalf("locks after a refused create: status=%d body=%s; want %d with none", rec.Code, rec.Body.String(), http.StatusOK)
	}
}

// TestPreOpenRefusalKeepsLock pins that a refused unlock leaves the lock in place.
func TestPreOpenRefusalKeepsLock(t *testing.T) {
	var refusal error
	handler := NewHandler(WithPreOpenHookFunc(func(context.Context, string, bool) error {
		return refusal
	}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, lfsTestRequest(http.MethodPost, "/org/name.git/info/lfs/locks", `{"path":"model.bin"}`))
	var created lfs.LockResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &created); rec.Code != http.StatusCreated || err != nil || created.Lock == nil {
		t.Fatalf("lock create: status=%d body=%s; want %d with a lock", rec.Code, rec.Body.String(), http.StatusCreated)
	}
	refusal = fmt.Errorf("x: %w", fs.ErrPermission)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, lfsTestRequest(http.MethodPost, "/org/name.git/info/lfs/locks/"+created.Lock.Id+"/unlock", `{}`))
	if rec.Code != http.StatusForbidden {
		t.Errorf("refused unlock: status=%d, want %d; body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	refusal = nil
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, lfsTestRequest(http.MethodGet, "/org/name.git/info/lfs/locks", ""))
	var list lfs.LockList
	if err := json.Unmarshal(rec.Body.Bytes(), &list); rec.Code != http.StatusOK || err != nil || len(list.Locks) != 1 || list.Locks[0].Id != created.Lock.Id {
		t.Fatalf("locks after a refused unlock: status=%d body=%s; want %d with lock %s", rec.Code, rec.Body.String(), http.StatusOK, created.Lock.Id)
	}
}

// TestNilPreOpenHookAllows pins that a nil PreOpen hook lets the lock and batch routes serve.
func TestNilPreOpenHookAllows(t *testing.T) {
	handler := NewHandler(WithPreOpenHookFunc(nil))
	serve := func(method, target, body string, want int) []byte {
		t.Helper()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, lfsTestRequest(method, target, body))
		if rec.Code != want {
			t.Fatalf("%s %s with a nil PreOpen hook: status=%d, want %d; body=%s", method, target, rec.Code, want, rec.Body.String())
		}
		return rec.Body.Bytes()
	}
	var created lfs.LockResponse
	if err := json.Unmarshal(serve(http.MethodPost, "/org/name.git/info/lfs/locks", `{"path":"model.bin"}`, http.StatusCreated), &created); err != nil || created.Lock == nil {
		t.Fatalf("lock create response = %+v, %v; want a lock", created, err)
	}
	serve(http.MethodGet, "/org/name.git/info/lfs/locks", "", http.StatusOK)
	serve(http.MethodPost, "/org/name.git/info/lfs/locks/verify", `{}`, http.StatusOK)
	serve(http.MethodPost, "/org/name.git/info/lfs/locks/"+created.Lock.Id+"/unlock", `{}`, http.StatusOK)
	serve(http.MethodPost, "/org/name.git/info/lfs/objects/batch", testBatchDownload, http.StatusOK)
	serve(http.MethodPost, "/org/name.git/info/lfs/objects/batch", testBatchUpload, http.StatusOK)
}
