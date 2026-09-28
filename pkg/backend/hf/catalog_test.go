package hf

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// catalogRequests lists the catalog endpoints with a valid body each.
var catalogRequests = []struct {
	method, target, body string
}{
	{http.MethodPost, "/api/repos/create", `{"type":"model","name":"repo","organization":"org"}`},
	{http.MethodDelete, "/api/repos/delete", `{"type":"model","name":"repo","organization":"org"}`},
	{http.MethodPost, "/api/repos/move", `{"fromRepo":"org/repo","toRepo":"org/moved"}`},
	{http.MethodPut, "/api/models/org/repo/settings", `{"private":true}`},
	{http.MethodGet, "/api/models?author=org", ""},
	{http.MethodGet, "/api/settings/repositories", ""},
	{http.MethodGet, "/api/organizations/org/settings/repositories", ""},
	{http.MethodGet, "/api/whoami-v2", ""},
}

func TestCatalogRoutesFallThrough(t *testing.T) {
	dataDir := t.TempDir()
	st := newStorage(t, dataDir)
	var calls []string
	h := NewHandler(
		WithStorage(st),
		WithPermissionHookFunc(func(_ context.Context, op permission.Operation, repo string, _ permission.Context) (bool, error) {
			calls = append(calls, fmt.Sprintf("permission %v %s", op, repo))
			return true, nil
		}),
		WithPreOpenHookFunc(func(_ context.Context, repo string, _ bool) error {
			calls = append(calls, "preopen "+repo)
			return nil
		}),
		WithPostReceiveHookFunc(func(_ context.Context, repo string, _ []receive.RefUpdate) error {
			calls = append(calls, "postreceive "+repo)
			return nil
		}),
		WithNext(sentinelNext{}),
	)
	ctx := authenticate.WithIdentity(context.Background(), authenticate.NewIdentity("alice", "alice@example.com"))
	for _, tc := range catalogRequests {
		for _, body := range []string{tc.body, "{not json"} {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.target, strings.NewReader(body)).WithContext(ctx))
			if rec.Code != http.StatusTeapot {
				t.Errorf("%s %s body %q: status = %d, want 418 fall-through", tc.method, tc.target, body, rec.Code)
			}
		}
	}
	if len(calls) != 0 {
		t.Errorf("hooks called for unregistered catalog routes: %v", calls)
	}
	if entries, err := os.ReadDir(filepath.Join(dataDir, "repositories")); !errors.Is(err, fs.ErrNotExist) && (err != nil || len(entries) != 0) {
		t.Errorf("repositories dir entries = %v, err = %v; want none", entries, err)
	}
}

// catalogRoutes maps each catalog route template to its method.
var catalogRoutes = map[string]string{
	"/api/repos/create": http.MethodPost,
	"/api/repos/delete": http.MethodDelete,
	"/api/repos/move":   http.MethodPost,
	"/api/{repoType:models|datasets|spaces|kernels}/{namespace}/{repo}/settings": http.MethodPut,
	"/api/{repoType:models|datasets|spaces|kernels}":                             http.MethodGet,
	"/api/settings/repositories":                                                 http.MethodGet,
	"/api/organizations/{namespace}/settings/repositories":                       http.MethodGet,
	"/api/whoami-v2": http.MethodGet,
}

func registeredCatalogRoutes(t *testing.T, h *Handler) map[string]string {
	t.Helper()
	got := map[string]string{}
	total := 0
	err := h.Router().Walk(func(route *mux.Route, _ *mux.Router, _ []*mux.Route) error {
		total++
		tmpl, err := route.GetPathTemplate()
		if err != nil {
			return err
		}
		methods, err := route.GetMethods()
		if err != nil {
			return err
		}
		if want, ok := catalogRoutes[tmpl]; ok && slices.Contains(methods, want) {
			got[tmpl] = want
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk routes: %v", err)
	}
	if total < 20 {
		t.Fatalf("only %d routes registered; the builtin routes went missing", total)
	}
	return got
}

func TestCatalogRouteRegistration(t *testing.T) {
	if got := registeredCatalogRoutes(t, NewHandler()); len(got) != 0 {
		t.Errorf("bare handler registers catalog routes %v, want none", got)
	}
	if got := registeredCatalogRoutes(t, NewHandler((&catalogFakes{}).options()...)); fmt.Sprint(got) != fmt.Sprint(catalogRoutes) {
		t.Errorf("all callbacks register %v, want all of %v", got, catalogRoutes)
	}
	one := NewHandler(WithWhoamiFunc(func(context.Context) (*WhoamiResponse, error) { return nil, nil }))
	if got := registeredCatalogRoutes(t, one); len(got) != 1 || got["/api/whoami-v2"] != http.MethodGet {
		t.Errorf("whoami-only handler registers %v, want only /api/whoami-v2", got)
	}
	list := NewHandler(WithListReposFunc(func(context.Context, string, ListQuery) ([]RepoListItem, bool, error) { return nil, false, nil }))
	wantList := map[string]string{
		"/api/{repoType:models|datasets|spaces|kernels}":       http.MethodGet,
		"/api/settings/repositories":                           http.MethodGet,
		"/api/organizations/{namespace}/settings/repositories": http.MethodGet,
	}
	if got := registeredCatalogRoutes(t, list); fmt.Sprint(got) != fmt.Sprint(wantList) {
		t.Errorf("list-only handler registers %v, want %v", got, wantList)
	}
}

// catalogFakes are storage-free callbacks that record their arguments.
type catalogFakes struct {
	calls   []string
	err     error
	updates []receive.RefUpdate
	items   []RepoListItem
	more    bool
}

func (f *catalogFakes) options() []Option {
	return []Option{
		WithCreateRepoFunc(func(_ context.Context, name string, req CreateRepoRequest) ([]receive.RefUpdate, error) {
			f.calls = append(f.calls, fmt.Sprintf("create %s %+v", name, req))
			return f.updates, f.err
		}),
		WithDeleteRepoFunc(func(_ context.Context, name string) error {
			f.calls = append(f.calls, "delete "+name)
			return f.err
		}),
		WithMoveRepoFunc(func(_ context.Context, from, to string) error {
			f.calls = append(f.calls, "move "+from+" "+to)
			return f.err
		}),
		WithUpdateRepoSettingsFunc(func(_ context.Context, name string, s RepoSettings) error {
			f.calls = append(f.calls, fmt.Sprintf("settings %s %v %v", name, s.Private != nil && *s.Private, s.Gated))
			return f.err
		}),
		WithListReposFunc(func(_ context.Context, repoType string, q ListQuery) ([]RepoListItem, bool, error) {
			f.calls = append(f.calls, fmt.Sprintf("list %s %+v", repoType, q))
			return f.items, f.more, f.err
		}),
		WithWhoamiFunc(func(ctx context.Context) (*WhoamiResponse, error) {
			f.calls = append(f.calls, "whoami "+authenticate.IdentityFrom(ctx).Name())
			return &WhoamiResponse{Type: "user", Name: "custom-" + authenticate.IdentityFrom(ctx).Name(), Orgs: []any{}}, f.err
		}),
	}
}

// newCatalogHandler builds a handler with nil storage, fake callbacks, and recording hooks.
func newCatalogHandler(t *testing.T, fakes *catalogFakes, hookErr error, hooks *[]string) *Handler {
	t.Helper()
	opts := append(fakes.options(),
		WithPermissionHookFunc(func(_ context.Context, op permission.Operation, repo string, pc permission.Context) (bool, error) {
			*hooks = append(*hooks, fmt.Sprintf("permission %v %s %+v", op, repo, pc))
			if hookErr != nil {
				return false, hookErr
			}
			return true, nil
		}),
		WithPostReceiveHookFunc(func(_ context.Context, repo string, updates []receive.RefUpdate) error {
			*hooks = append(*hooks, fmt.Sprintf("postreceive %s %d", repo, len(updates)))
			return nil
		}),
		WithNext(sentinelNext{}),
	)
	return NewHandler(opts...)
}

func serveAs(h *Handler, method, target, body string, id authenticate.Identity) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	h.ServeHTTP(rec, req.WithContext(authenticate.WithIdentity(req.Context(), id)))
	return rec
}

var alice = authenticate.NewIdentity("alice", "alice@example.com")

func TestCatalogCallbackArguments(t *testing.T) {
	cursor := encodeCursorOffset(3)
	tests := []struct {
		name, method, target, body   string
		wantCall, wantHook, wantBody string
	}{
		{"Create", http.MethodPost, "/api/repos/create", `{"type":"dataset","name":"repo","organization":"org","private":true}`,
			"create datasets/org/repo {Type:dataset Name:repo Organization:org Private:true}",
			"permission create_repo datasets/org/repo {Ref: DestRepo: Author:}",
			`{"url":"http://example.com/datasets/org/repo"}`},
		{"Delete", http.MethodDelete, "/api/repos/delete", `{"type":"space","name":"repo","organization":"org"}`,
			"delete spaces/org/repo", "permission delete_repo spaces/org/repo {Ref: DestRepo: Author:}", ""},
		{"Move", http.MethodPost, "/api/repos/move", `{"fromRepo":"org/repo","toRepo":"org/moved","type":"dataset"}`,
			"move datasets/org/repo datasets/org/moved", "permission update_repo datasets/org/repo {Ref: DestRepo:datasets/org/moved Author:}", ""},
		{"Settings", http.MethodPut, "/api/spaces/org/repo/settings", `{"private":true,"gated":"auto"}`,
			"settings spaces/org/repo true auto", "permission update_repo spaces/org/repo {Ref: DestRepo: Author:}", ""},
		{"List", http.MethodGet, "/api/datasets?search=llm&author=org&filter=a&filter=b&sort=likes&limit=2&cursor=" + cursor, "",
			"list datasets {Search:llm Author:org FilterTags:[a b] SortField:likes Expand:[] Limit:2 Offset:3}",
			"permission list_repos datasets {Ref: DestRepo: Author:org}", `[]`},
		{"Whoami", http.MethodGet, "/api/whoami-v2", "", "whoami alice", "", `"name":"custom-alice"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakes := &catalogFakes{}
			var hooks []string
			h := newCatalogHandler(t, fakes, nil, &hooks)
			rec := serveAs(h, tc.method, tc.target, tc.body, alice)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body)
			}
			if len(fakes.calls) != 1 || fakes.calls[0] != tc.wantCall {
				t.Errorf("callback calls = %q, want [%q]", fakes.calls, tc.wantCall)
			}
			wantHooks := []string{}
			if tc.wantHook != "" {
				wantHooks = []string{tc.wantHook}
			}
			if fmt.Sprint(hooks) != fmt.Sprint(wantHooks) {
				t.Errorf("hook calls = %q, want %q", hooks, wantHooks)
			}
			if body := strings.TrimSpace(rec.Body.String()); !strings.Contains(body, tc.wantBody) {
				t.Errorf("body = %s, want containing %s", body, tc.wantBody)
			}
		})
	}
}

// TestCatalogListUserReposArguments pins that the settings/repositories forms gate and then call the
// list callback once per repository type, in order, with the namespace as author.
func TestCatalogListUserReposArguments(t *testing.T) {
	tests := []struct {
		name, target, author string
		id                   authenticate.Identity
	}{
		{"User", "/api/settings/repositories", "", alice},
		{"Organization", "/api/organizations/org/settings/repositories", "org", alice},
		{"Anonymous", "/api/settings/repositories", "", authenticate.Anonymous},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakes := &catalogFakes{}
			var hooks []string
			h := newCatalogHandler(t, fakes, nil, &hooks)
			rec := serveAs(h, http.MethodGet, tc.target, "", tc.id)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body)
			}
			var wantCalls, wantHooks []string
			for _, repoType := range []string{"models", "datasets", "spaces", "kernels"} {
				wantCalls = append(wantCalls, fmt.Sprintf("list %s %+v", repoType, ListQuery{Author: tc.author, Expand: []string{"lastModified", "usedStorage"}}))
				wantHooks = append(wantHooks, "permission list_repos "+repoType+" {Ref: DestRepo: Author:"+tc.author+"}")
			}
			if fmt.Sprint(fakes.calls) != fmt.Sprint(wantCalls) {
				t.Errorf("callback calls = %q, want %q", fakes.calls, wantCalls)
			}
			if fmt.Sprint(hooks) != fmt.Sprint(wantHooks) {
				t.Errorf("hook calls = %q, want %q", hooks, wantHooks)
			}
			if body := strings.TrimSpace(rec.Body.String()); body != "[]" {
				t.Errorf("body = %s, want []", body)
			}
			if link := rec.Header().Get("Link"); link != "" {
				t.Errorf("Link = %q, want none", link)
			}
		})
	}
}

// TestCatalogListUserReposResponse pins the RepoStorageInfo shape huggingface_hub's RepoStorageInfo(**item) needs.
func TestCatalogListUserReposResponse(t *testing.T) {
	fakes := &catalogFakes{items: []RepoListItem{
		{RepoID: "org/a", LastModified: "2026-01-02T03:04:05.000Z", UsedStorage: 300},
		{RepoID: "org/b", Private: true, UsedStorage: 100},
	}}
	var hooks []string
	h := newCatalogHandler(t, fakes, nil, &hooks)
	rec := serveAs(h, http.MethodGet, "/api/settings/repositories", "", alice)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	var raw []map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil || len(raw) == 0 {
		t.Fatalf("body = %s, err = %v; want a non-empty array", rec.Body, err)
	}
	if keys, want := slices.Sorted(maps.Keys(raw[0])), []string{"id", "storage", "storagePercent", "type", "updatedAt", "visibility"}; !slices.Equal(keys, want) {
		t.Errorf("keys = %v, want %v", keys, want)
	}
	var infos []RepoStorageInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &infos); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// The fake returns both items for every type, so each id appears once per type (sorted by id, then type) and the total is 1600.
	var want []RepoStorageInfo
	for _, id := range []string{"org/a", "org/b"} {
		for _, typ := range []string{"dataset", "kernel", "model", "space"} {
			info := RepoStorageInfo{RepoID: id, Type: typ, UpdatedAt: "2026-01-02T03:04:05.000Z", Visibility: "public", Storage: 300, StoragePercent: 18.75}
			if id == "org/b" {
				info = RepoStorageInfo{RepoID: id, Type: typ, UpdatedAt: "0001-01-01T00:00:00.000Z", Visibility: "private", Storage: 100, StoragePercent: 6.25}
			}
			want = append(want, info)
		}
	}
	if len(infos) != len(want) {
		t.Fatalf("entries = %+v, want %+v", infos, want)
	}
	for i := range want {
		if math.Abs(infos[i].StoragePercent-want[i].StoragePercent) > 1e-9 {
			t.Errorf("entry %d storagePercent = %v, want %v", i, infos[i].StoragePercent, want[i].StoragePercent)
		}
		infos[i].StoragePercent = want[i].StoragePercent
		if infos[i] != want[i] {
			t.Errorf("entry %d = %+v, want %+v", i, infos[i], want[i])
		}
	}
}

func TestParseListQueryExpand(t *testing.T) {
	tests := []struct {
		query string
		want  []string
	}{
		{"", nil},
		{"?expand=lastModified&expand=usedStorage", []string{"lastModified", "usedStorage"}},
		{"?expand[]=a&expand[]=b", []string{"a", "b"}},
		{"?expand=a,b,,c", []string{"a", "b", "c"}},
		{"?expand=", nil},
	}
	for _, tc := range tests {
		if got := parseListQuery(httptest.NewRequest(http.MethodGet, "/api/models"+tc.query, nil)).Expand; !slices.Equal(got, tc.want) {
			t.Errorf("%q: Expand = %v, want %v", tc.query, got, tc.want)
		}
	}
}

// TestCatalogCallbackSkipped pins that gates run before the callback and never reach it.
func TestCatalogCallbackSkipped(t *testing.T) {
	tests := []struct {
		name, method, target, body string
		id                         authenticate.Identity
		hookErr                    error
		want                       int
	}{
		{"CreateDenied", http.MethodPost, "/api/repos/create", `{"name":"repo","organization":"org"}`, alice, permission.ErrDenied, http.StatusForbidden},
		{"DeleteDenied", http.MethodDelete, "/api/repos/delete", `{"name":"repo","organization":"org"}`, alice, permission.ErrDenied, http.StatusForbidden},
		{"MoveDenied", http.MethodPost, "/api/repos/move", `{"fromRepo":"org/repo","toRepo":"org/moved"}`, alice, permission.ErrDenied, http.StatusForbidden},
		{"SettingsDenied", http.MethodPut, "/api/models/org/repo/settings", `{}`, alice, permission.ErrDenied, http.StatusForbidden},
		{"ListDenied", http.MethodGet, "/api/models", "", alice, permission.ErrDenied, http.StatusForbidden},
		{"ListHookError", http.MethodGet, "/api/models", "", alice, errors.New("boom"), http.StatusInternalServerError},
		{"ListUserReposDenied", http.MethodGet, "/api/settings/repositories", "", alice, permission.ErrDenied, http.StatusForbidden},
		{"ListOrgReposDenied", http.MethodGet, "/api/organizations/org/settings/repositories", "", alice, permission.ErrDenied, http.StatusForbidden},
		{"ListUserReposHookError", http.MethodGet, "/api/settings/repositories", "", alice, errors.New("boom"), http.StatusInternalServerError},
		{"CreateMalformed", http.MethodPost, "/api/repos/create", `{not json`, alice, nil, http.StatusBadRequest},
		{"DeleteMalformed", http.MethodDelete, "/api/repos/delete", `{not json`, alice, nil, http.StatusBadRequest},
		{"MoveMalformed", http.MethodPost, "/api/repos/move", `{not json`, alice, nil, http.StatusBadRequest},
		{"SettingsMalformed", http.MethodPut, "/api/models/org/repo/settings", `{not json`, alice, nil, http.StatusBadRequest},
		{"CreateInvalidName", http.MethodPost, "/api/repos/create", `{"name":"repo","organization":"org/.."}`, alice, nil, http.StatusBadRequest},
		{"DeleteInvalidName", http.MethodDelete, "/api/repos/delete", `{"name":"repo","organization":"org/.."}`, alice, nil, http.StatusNotFound},
		{"MoveInvalidSource", http.MethodPost, "/api/repos/move", `{"fromRepo":"org/../repo","toRepo":"org/moved"}`, alice, nil, http.StatusNotFound},
		{"MoveInvalidDestination", http.MethodPost, "/api/repos/move", `{"fromRepo":"org/repo","toRepo":"org/../moved"}`, alice, nil, http.StatusBadRequest},
		{"WhoamiAnonymous", http.MethodGet, "/api/whoami-v2", "", authenticate.Anonymous, nil, http.StatusUnauthorized},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakes := &catalogFakes{}
			var hooks []string
			h := newCatalogHandler(t, fakes, tc.hookErr, &hooks)
			rec := serveAs(h, tc.method, tc.target, tc.body, tc.id)
			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d: %s", rec.Code, tc.want, rec.Body)
			}
			if len(fakes.calls) != 0 {
				t.Errorf("callback calls = %q, want none", fakes.calls)
			}
			for _, hook := range hooks {
				if strings.HasPrefix(hook, "postreceive") {
					t.Errorf("post-receive ran: %q", hook)
				}
			}
		})
	}
}

func TestCatalogCallbackErrors(t *testing.T) {
	statuses := []struct {
		err  error
		want int
	}{
		{fmt.Errorf("wrapped: %w", repository.ErrRepositoryNotExists), http.StatusNotFound},
		{fmt.Errorf("wrapped: %w", repository.ErrRepositoryAlreadyExists), http.StatusConflict},
		{errors.New("backend down"), http.StatusInternalServerError},
	}
	for _, tc := range catalogRequests {
		for _, sc := range statuses {
			t.Run(fmt.Sprintf("%s %s %d", tc.method, tc.target, sc.want), func(t *testing.T) {
				fakes := &catalogFakes{err: sc.err}
				var hooks []string
				h := newCatalogHandler(t, fakes, nil, &hooks)
				rec := serveAs(h, tc.method, tc.target, tc.body, alice)
				var body struct{ Error string }
				if rec.Code != sc.want || json.Unmarshal(rec.Body.Bytes(), &body) != nil || body.Error != sc.err.Error() {
					t.Errorf("status = %d, body = %s; want %d with error %q", rec.Code, rec.Body, sc.want, sc.err)
				}
				if len(fakes.calls) != 1 {
					t.Errorf("callback calls = %q, want exactly one", fakes.calls)
				}
				for _, hook := range hooks {
					if strings.HasPrefix(hook, "postreceive") {
						t.Errorf("post-receive ran after callback error: %q", hook)
					}
				}
			})
		}
	}
}

func TestCatalogCreatePostReceive(t *testing.T) {
	for _, tc := range []struct {
		name    string
		updates []receive.RefUpdate
		want    []string
	}{
		{"NilUpdates", nil, nil},
		{"InitialCommit", []receive.RefUpdate{receive.NewRefUpdate(receive.ZeroHash, strings.Repeat("a", 40), "refs/heads/main", nil)}, []string{"postreceive org/repo 1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakes := &catalogFakes{updates: tc.updates}
			var hooks []string
			h := newCatalogHandler(t, fakes, nil, &hooks)
			if rec := serveAs(h, http.MethodPost, "/api/repos/create", `{"name":"repo","organization":"org"}`, alice); rec.Code != http.StatusOK {
				t.Fatalf("status = %d: %s", rec.Code, rec.Body)
			}
			var got []string
			for _, hook := range hooks {
				if strings.HasPrefix(hook, "postreceive") {
					got = append(got, hook)
				}
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("post-receive calls = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCatalogListPage(t *testing.T) {
	fakes := &catalogFakes{items: []RepoListItem{{RepoID: "org/a"}, {RepoID: "org/b"}}, more: true}
	var hooks []string
	h := newCatalogHandler(t, fakes, nil, &hooks)
	rec := serveAs(h, http.MethodGet, "/api/models?limit=2&author=org&cursor="+encodeCursorOffset(4), "", alice)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	var items []RepoListItem
	if err := json.Unmarshal(rec.Body.Bytes(), &items); err != nil || len(items) != 2 || items[0].RepoID != "org/a" {
		t.Errorf("items = %s, err = %v; want the two returned items", rec.Body, err)
	}
	want := fmt.Sprintf(`<http://example.com/api/models?author=org&cursor=%s&limit=2>; rel="next"`, encodeCursorOffset(6))
	if link := rec.Header().Get("Link"); link != want {
		t.Errorf("Link = %q, want %q", link, want)
	}

	fakes.more = false
	rec = serveAs(h, http.MethodGet, "/api/models?limit=2", "", alice)
	if rec.Header().Get("Link") != "" {
		t.Errorf("Link = %q, want none when no more pages", rec.Header().Get("Link"))
	}
}
