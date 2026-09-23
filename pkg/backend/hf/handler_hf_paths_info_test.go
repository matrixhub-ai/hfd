package hf

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/storage/filesystem"

	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

var (
	pathsInfoDay1 = time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	pathsInfoDay2 = time.Date(2024, 2, 1, 12, 0, 0, 0, time.UTC)
	pathsInfoDay3 = time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
)

type permCall struct {
	op        permission.Operation
	repo, ref string
}

func addOps(files map[string]string) []repository.CommitOperation {
	var ops []repository.CommitOperation
	for _, p := range slices.Sorted(maps.Keys(files)) {
		ops = append(ops, repository.CommitOperation{Type: repository.CommitOperationAdd, Path: p, Content: []byte(files[p])})
	}
	return ops
}

// commitAt commits ops onto branch of repoName, creating the repository when needed, and dates the commit when.
func commitAt(t *testing.T, st *storage.Storage, repoName, branch, message string, ops []repository.CommitOperation, when time.Time) string {
	t.Helper()
	ctx := context.Background()
	fs, repoPath := st.RepositoriesFS(), repository.ResolvePath(repoName)
	repo, err := repository.Open(fs, repoPath)
	if err != nil {
		if repo, err = repository.Init(ctx, fs, repoPath, "main"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := repo.CreateCommit(ctx, branch, message, "Alice", "alice@example.com", ops, ""); err != nil {
		t.Fatal(err)
	}
	return redate(t, fs, repoPath, branch, when)
}

// redate rewrites the tip of branch with when as author and committer time, keeping tree, parents and message.
func redate(t *testing.T, fs billy.Filesystem, repoPath, branch string, when time.Time) string {
	t.Helper()
	r, err := git.Open(filesystem.NewStorage(chroot.New(fs, repoPath), cache.NewObjectLRUDefault()), nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := r.Reference(plumbing.NewBranchReferenceName(branch), true)
	if err != nil {
		t.Fatal(err)
	}
	c, err := r.CommitObject(ref.Hash())
	if err != nil {
		t.Fatal(err)
	}
	c.Author.When, c.Committer.When = when, when
	obj := r.Storer.NewEncodedObject()
	if err := c.Encode(obj); err != nil {
		t.Fatal(err)
	}
	hash, err := r.Storer.SetEncodedObject(obj)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Storer.SetReference(plumbing.NewHashReference(ref.Name(), hash)); err != nil {
		t.Fatal(err)
	}
	return hash.String()
}

func postPathsInfo(h http.Handler, target, contentType, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func pathsInfoOK(t *testing.T, h http.Handler, target, contentType, body string) []map[string]any {
	t.Helper()
	rec := postPathsInfo(h, target, contentType, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: status %d: %s", target, rec.Code, rec.Body)
	}
	var entries []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &entries); err != nil {
		t.Fatalf("decode %s: %v", rec.Body, err)
	}
	return entries
}

func pathsInfoByPath(entries []map[string]any) map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, e := range entries {
		out[e["path"].(string)] = e
	}
	return out
}

func lastCommitID(e map[string]any) string {
	lc, _ := e["lastCommit"].(map[string]any)
	id, _ := lc["id"].(string)
	return id
}

func TestHuggingFacePathsInfoEntriesAndForms(t *testing.T) {
	st := storage.NewStorage(storage.WithRootDir(t.TempDir()))
	var checks []permCall
	var opened []string
	h := NewHandler(WithStorage(st),
		WithPermissionHookFunc(func(_ context.Context, op permission.Operation, name string, c permission.Context) (bool, error) {
			checks = append(checks, permCall{op, name, c.Ref})
			return name != "alice/secret", nil
		}),
		WithPreOpenHookFunc(func(_ context.Context, name string, _ bool) error {
			opened = append(opened, name)
			return nil
		}))
	pointer := hfLFSPointerText("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 12345)
	sha1 := commitAt(t, st, "alice/m1", "main", "Add README.md", addOps(map[string]string{"README.md": "r"}), pathsInfoDay1)
	sha2 := commitAt(t, st, "alice/m1", "main", "Add sub/a.txt", addOps(map[string]string{"sub/a.txt": "aa"}), pathsInfoDay2)
	sha3 := commitAt(t, st, "alice/m1", "main", "Add model.bin", addOps(map[string]string{"model.bin": pointer}), pathsInfoDay3)
	commitAt(t, st, "alice/m1", "feature/x", "Add f.txt", addOps(map[string]string{"f.txt": "f"}), pathsInfoDay1)
	commitAt(t, st, "datasets/alice/d1", "main", "Add data.csv", addOps(map[string]string{"data.csv": "1,2"}), pathsInfoDay1)
	repo, err := repository.Open(st.RepositoriesFS(), repository.ResolvePath("alice/m1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateTag("v1", sha2); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Init(context.Background(), st.RepositoriesFS(), repository.ResolvePath("alice/unborn"), "main"); err != nil {
		t.Fatal(err)
	}

	const target = "/api/models/alice/m1/paths-info/main"
	body := `{"paths":["README.md","sub","sub/a.txt","model.bin","missing.txt","sub/none","README.md/x","sub/./a.txt","sub/"],"expand":true}`
	entries := pathsInfoOK(t, h, target, "application/json", body)
	var paths []string
	for _, e := range entries {
		paths = append(paths, e["path"].(string))
	}
	if want := []string{"README.md", "sub", "sub/a.txt", "model.bin"}; !slices.Equal(paths, want) {
		t.Fatalf("paths %v, want %v", paths, want)
	}
	got := pathsInfoByPath(entries)
	readme := got["README.md"]
	if readme["type"] != "file" || readme["size"] != float64(1) || len(readme["oid"].(string)) != 40 || readme["lfs"] != nil {
		t.Errorf("README %v", readme)
	}
	if lc := readme["lastCommit"].(map[string]any); lc["id"] != sha1 || lc["title"] != "Add README.md" || lc["date"] != "2024-01-01T12:00:00.000Z" {
		t.Errorf("README lastCommit %v", lc)
	}
	sub := got["sub"]
	if sub["type"] != "directory" || len(sub["oid"].(string)) != 40 || sub["oid"] == readme["oid"] || sub["lfs"] != nil || lastCommitID(sub) != sha2 {
		t.Errorf("sub %v", sub)
	}
	if a := got["sub/a.txt"]; a["type"] != "file" || a["size"] != float64(2) || lastCommitID(a) != sha2 {
		t.Errorf("sub/a.txt %v", a)
	}
	bin := got["model.bin"]
	lfs, _ := bin["lfs"].(map[string]any)
	if bin["size"] != float64(12345) || lfs == nil || lfs["oid"] != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" || lfs["size"] != float64(12345) || lfs["pointerSize"] != float64(len(pointer)) || lastCommitID(bin) != sha3 {
		t.Errorf("model.bin %v", bin)
	}
	if !slices.Contains(checks, permCall{permission.OperationReadRepo, "alice/m1", "main"}) || !slices.Contains(opened, "alice/m1") {
		t.Errorf("checks %v opened %v", checks, opened)
	}

	entries = pathsInfoOK(t, h, target, "application/json", `{"paths":["README.md"]}`)
	if len(entries) != 1 || entries[0]["lastCommit"] != nil {
		t.Errorf("without expand %v", entries)
	}
	entries = pathsInfoOK(t, h, target, "application/json", `{"paths":["README.md"]}`+"\n")
	if len(entries) != 1 {
		t.Errorf("trailing newline %v", entries)
	}
	entries = pathsInfoOK(t, h, target, "", `{"paths":["README.md"]}`)
	if len(entries) != 1 {
		t.Errorf("no content type %v", entries)
	}
	if rec := postPathsInfo(h, target, "application/json", `{"paths":[]}`); rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Errorf("no paths: %d %s", rec.Code, rec.Body)
	}

	form := url.Values{"paths": {"README.md", "sub/a.txt"}, "expand": {"True"}}.Encode()
	entries = pathsInfoOK(t, h, target, "application/x-www-form-urlencoded", form)
	if len(entries) != 2 || entries[1]["path"] != "sub/a.txt" || entries[0]["lastCommit"] == nil {
		t.Errorf("form %v", entries)
	}
	form = url.Values{"paths": {"README.md"}, "expand": {"False"}}.Encode()
	entries = pathsInfoOK(t, h, target, "application/x-www-form-urlencoded; charset=utf-8", form)
	if len(entries) != 1 || entries[0]["lastCommit"] != nil {
		t.Errorf("form without expand %v", entries)
	}
	multipart := "--b\r\nContent-Disposition: form-data; name=\"paths\"\r\n\r\nsub/a.txt\r\n--b\r\nContent-Disposition: form-data; name=\"expand\"\r\n\r\n1\r\n--b--\r\n"
	entries = pathsInfoOK(t, h, target, "multipart/form-data; boundary=b", multipart)
	if len(entries) != 1 || entries[0]["path"] != "sub/a.txt" || entries[0]["lastCommit"] == nil {
		t.Errorf("multipart %v", entries)
	}
	// Form bodies padded to exactly the cap are fine; one byte more, or a malformed field, is a 400 before the pre-open hook.
	capForm := "paths=README.md&pad="
	capForm += strings.Repeat("a", maxPathsInfoBody-len(capForm))
	capMultipart := multipart + strings.Repeat("x", maxPathsInfoBody-len(multipart))
	entries = pathsInfoOK(t, h, target, "application/x-www-form-urlencoded", capForm)
	if len(entries) != 1 || entries[0]["path"] != "README.md" {
		t.Errorf("form at cap %v", entries)
	}
	entries = pathsInfoOK(t, h, target, "multipart/form-data; boundary=b", capMultipart)
	if len(entries) != 1 || entries[0]["path"] != "sub/a.txt" {
		t.Errorf("multipart epilogue at cap %v", entries)
	}
	for _, tc := range []struct{ name, contentType, body string }{
		{"form over cap", "application/x-www-form-urlencoded", capForm + "a"},
		{"form bad escape", "application/x-www-form-urlencoded", "paths=README.md&expand=%zz"},
		{"multipart epilogue over cap", "multipart/form-data; boundary=b", capMultipart + "x"},
	} {
		before := len(opened)
		rec := postPathsInfo(h, target, tc.contentType, tc.body)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `"error"`) || len(opened) != before {
			t.Errorf("%s: %d %s opened %v", tc.name, rec.Code, rec.Body.String()[:min(rec.Body.Len(), 120)], opened[before:])
		}
	}

	entries = pathsInfoOK(t, h, "/api/models/alice/m1/paths-info/"+url.PathEscape("feature/x"), "application/json", `{"paths":["f.txt","README.md"]}`)
	if len(entries) != 1 || entries[0]["path"] != "f.txt" || !slices.Contains(checks, permCall{permission.OperationReadRepo, "alice/m1", "feature/x"}) {
		t.Errorf("slash rev %v checks %v", entries, checks)
	}
	entries = pathsInfoOK(t, h, "/api/models/alice/m1/paths-info/v1", "application/json", `{"paths":["sub/a.txt","model.bin"]}`)
	if len(entries) != 1 || entries[0]["path"] != "sub/a.txt" {
		t.Errorf("tag %v", entries)
	}
	entries = pathsInfoOK(t, h, "/api/models/alice/m1/paths-info/"+sha1, "application/json", `{"paths":["README.md","sub/a.txt"]}`)
	if len(entries) != 1 || entries[0]["path"] != "README.md" {
		t.Errorf("hash %v", entries)
	}
	entries = pathsInfoOK(t, h, "/api/datasets/alice/d1/paths-info/main", "application/json", `{"paths":["data.csv"]}`)
	if len(entries) != 1 || !slices.Contains(opened, "datasets/alice/d1") {
		t.Errorf("dataset %v opened %v", entries, opened)
	}

	tooMany, _ := json.Marshal(slices.Repeat([]string{"x"}, 1001))
	for _, tc := range []struct {
		target, body string
		status       int
	}{
		{"/api/models/alice/m1/paths-info/nope", `{"paths":["README.md"]}`, http.StatusNotFound},
		{"/api/models/alice/m1/paths-info/main~999", `{"paths":[]}`, http.StatusNotFound},
		{"/api/models/alice/missing/paths-info/main", `{"paths":["README.md"]}`, http.StatusNotFound},
		{"/api/models/alice/unborn/paths-info/main", `{"paths":["README.md"]}`, http.StatusNotFound},
		{"/api/models/alice/secret/paths-info/main", `{"paths":["README.md"]}`, http.StatusForbidden},
		{target, `{"paths":`, http.StatusBadRequest},
		{target, `{"paths":"README.md"}`, http.StatusBadRequest},
		{target, `{"paths":["../x"]}`, http.StatusBadRequest},
		{target, `{"paths":["sub/../../x"]}`, http.StatusBadRequest},
		{target, `{"paths":["/README.md"]}`, http.StatusBadRequest},
		{target, `{"paths":[""]}`, http.StatusBadRequest},
		{target, `{"paths":["."]}`, http.StatusBadRequest},
		{target, `{"paths":` + string(tooMany) + `}`, http.StatusBadRequest},
		{target, `{"paths":["` + strings.Repeat("a", 2<<20) + `"]}`, http.StatusBadRequest},
		{target, `{"paths":["README.md"]} garbage`, http.StatusBadRequest},
		{target, `{"paths":["README.md"]}{"paths":["sub"]}`, http.StatusBadRequest},
		{target, `{"paths":["README.md"]}` + strings.Repeat(" ", 1<<20), http.StatusBadRequest},
	} {
		before := len(opened)
		rec := postPathsInfo(h, tc.target, "application/json", tc.body)
		if rec.Code != tc.status || !strings.Contains(rec.Body.String(), `"error"`) {
			t.Errorf("%s %s: %d %s", tc.target, tc.body[:min(len(tc.body), 40)], rec.Code, rec.Body.String()[:min(rec.Body.Len(), 120)])
		}
		if tc.status == http.StatusBadRequest && len(opened) != before {
			t.Errorf("%s %s: rejected body opened %v", tc.target, tc.body[:min(len(tc.body), 40)], opened[before:])
		}
	}
	if slices.Contains(opened, "alice/secret") {
		t.Errorf("denied repository was opened: %v", opened)
	}
}

func TestHuggingFacePathsInfoDirectoryLastCommitFollowsTreeChanges(t *testing.T) {
	st := storage.NewStorage(storage.WithRootDir(t.TempDir()))
	h := NewHandler(WithStorage(st))
	sha1 := commitAt(t, st, "alice/m1", "main", "Add README.md", addOps(map[string]string{"README.md": "r"}), pathsInfoDay1)
	sha2 := commitAt(t, st, "alice/m1", "main", "Add sub and other", addOps(map[string]string{"sub/keep": "k", "sub/drop": "d", "other/f": "f"}), pathsInfoDay2)
	// The deletion is the newest commit yet carries an older author date than the addition.
	sha3 := commitAt(t, st, "alice/m1", "main", "Delete sub/drop", []repository.CommitOperation{{Type: repository.CommitOperationDelete, Path: "sub/drop"}}, pathsInfoDay1)
	sha4 := commitAt(t, st, "alice/m1", "main", "Add other/deep/g", addOps(map[string]string{"other/deep/g": "g"}), pathsInfoDay3)

	entries := pathsInfoOK(t, h, "/api/models/alice/m1/paths-info/main", "application/json", `{"paths":["sub","other","other/deep","README.md","sub/keep","sub/drop","other/f"],"expand":true}`)
	got := pathsInfoByPath(entries)
	if len(got) != 6 || got["sub/drop"] != nil {
		t.Fatalf("entries %v", entries)
	}
	for p, want := range map[string]string{"sub": sha3, "other": sha4, "other/deep": sha4, "README.md": sha1, "sub/keep": sha2, "other/f": sha2} {
		if id := lastCommitID(got[p]); id != want {
			t.Errorf("%s lastCommit %v, want %s", p, got[p]["lastCommit"], want)
		}
	}
	if lc := got["sub"]["lastCommit"].(map[string]any); lc["title"] != "Delete sub/drop" || lc["date"] != "2024-01-01T12:00:00.000Z" {
		t.Errorf("sub lastCommit %v", lc)
	}
	entries = pathsInfoOK(t, h, "/api/models/alice/m1/paths-info/"+sha2, "application/json", `{"paths":["sub"],"expand":true}`)
	if len(entries) != 1 || lastCommitID(entries[0]) != sha2 {
		t.Errorf("sub at %s: %v", sha2, entries)
	}
}

func TestHuggingFacePathsInfoStorageFailuresAreServerErrors(t *testing.T) {
	root := t.TempDir()
	st := storage.NewStorage(storage.WithRootDir(root))
	commitAt(t, st, "alice/m1", "main", "Add README.md, sub/keep", addOps(map[string]string{"README.md": "r", "sub/keep": "k"}), pathsInfoDay1)
	repo, err := repository.Open(st.RepositoriesFS(), repository.ResolvePath("alice/m1"))
	if err != nil {
		t.Fatal(err)
	}
	hashOf := func(dir, name string) string {
		entries, err := repo.Tree("main", dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		i := slices.IndexFunc(entries, func(e *repository.TreeEntry) bool { return e.Path() == name })
		if i < 0 {
			t.Fatalf("%q not in %q", name, dir)
		}
		return entries[i].Hash().String()
	}
	subTree, keepBlob := hashOf("", "sub"), hashOf("sub", "sub/keep")
	// A fresh Storage opens the repository again instead of serving it from caches.
	reopen := func() http.Handler { return NewHandler(WithStorage(storage.NewStorage(storage.WithRootDir(root)))) }
	removeObject := func(h string) {
		if err := st.RepositoriesFS().Remove(path.Join(repository.ResolvePath("alice/m1"), "objects", h[:2], h[2:])); err != nil {
			t.Fatal(err)
		}
	}

	const target = "/api/models/alice/m1/paths-info/main"
	entries := pathsInfoOK(t, reopen(), target, "application/json", `{"paths":["sub/keep","nope.txt","sub/nope","README.md/x","README.md/x/y","nope/x"],"expand":true}`)
	if len(entries) != 1 || entries[0]["path"] != "sub/keep" {
		t.Fatalf("intact store: %v", entries)
	}

	removeObject(keepBlob)
	if rec := postPathsInfo(reopen(), target, "application/json", `{"paths":["sub/keep"]}`); rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), `"error"`) {
		t.Errorf("missing blob: %d %s", rec.Code, rec.Body)
	}
	removeObject(subTree)
	if rec := postPathsInfo(reopen(), target, "application/json", `{"paths":["sub/keep"]}`); rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), `"error"`) {
		t.Errorf("missing subtree: %d %s", rec.Code, rec.Body)
	}
}
