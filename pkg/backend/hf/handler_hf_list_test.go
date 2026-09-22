package hf

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	iofs "io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/storage/filesystem"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// repoListItem is the decoded shape of one list entry for the tests reading it as a struct.
type repoListItem struct {
	RepoID      string   `json:"id"`
	Tags        []string `json:"tags"`
	PipelineTag string   `json:"pipeline_tag"`
	LibraryName string   `json:"library_name"`
	ModelID     string   `json:"modelId"`
}

var (
	day1 = time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	day2 = time.Date(2024, 2, 1, 12, 0, 0, 0, time.UTC)
	day3 = time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
)

func newListHandler(t *testing.T) (*Handler, *storage.Storage) {
	t.Helper()
	st := storage.NewStorage(storage.WithRootDir(t.TempDir()))
	return NewHandler(WithStorage(st)), st
}

// seedRepo commits files onto main of repoName (hfd storage name), creating the repository when needed; a non-zero when dates the commit.
func seedRepo(t *testing.T, st *storage.Storage, repoName string, files map[string]string, when time.Time) string {
	t.Helper()
	ctx := context.Background()
	fs := st.RepositoriesFS()
	repoPath := repository.ResolvePath(repoName)
	repo, err := repository.Open(fs, repoPath)
	if err != nil {
		if repo, err = repository.Init(ctx, fs, repoPath, "main"); err != nil {
			t.Fatalf("init %s: %v", repoName, err)
		}
	}
	var ops []repository.CommitOperation
	for _, p := range slices.Sorted(maps.Keys(files)) {
		ops = append(ops, repository.CommitOperation{Type: repository.CommitOperationAdd, Path: p, Content: []byte(files[p])})
	}
	hash, err := repo.CreateCommit(ctx, "main", "Seed "+repoName, "Alice", "alice@example.com", ops, "")
	if err != nil {
		t.Fatalf("commit %s: %v", repoName, err)
	}
	if when.IsZero() {
		return hash
	}
	return redate(t, fs, repoPath, "main", when)
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

func listDo(t *testing.T, h http.Handler, target string, hdr ...string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func listItems(t *testing.T, h http.Handler, target string) []map[string]any {
	t.Helper()
	rec := listDo(t, h, target)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: status %d: %s", target, rec.Code, rec.Body)
	}
	var items []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &items); err != nil {
		t.Fatalf("%s: decode %s: %v", target, rec.Body, err)
	}
	return items
}

func listIDs(t *testing.T, h http.Handler, target string) []string {
	t.Helper()
	var ids []string
	for _, item := range listItems(t, h, target) {
		ids = append(ids, item["id"].(string))
	}
	return ids
}

func keysOf(m map[string]any) []string {
	return slices.Sorted(maps.Keys(m))
}

const (
	modelCard = "---\nlicense: mit\nlanguage: en\npipeline_tag: text-generation\nlibrary_name: transformers\ntags: [chat]\ndatasets: [alice/d1]\n---\n# m1\n\nA chat model.\n"
	dataCard  = "---\ntask_categories: [text-classification]\ntask_ids: [sentiment-classification]\nlanguage: [en]\nlicense: other\nsize_categories: [1K<n<10K]\nlibrary_name: datasets\ntags: [qa]\npaperswithcode_id: imdb\n---\n# Dataset Card for d1\n\nLarge movie reviews.\nSecond line.\n\nMore text.\n"
	spaceCard = "---\nsdk: gradio\ntitle: S1\nmodels: [alice/m1]\n---\n# S1\n"
)

func seedShowcase(t *testing.T, st *storage.Storage) {
	t.Helper()
	seedRepo(t, st, "alice/m1", map[string]string{"README.md": modelCard, "config.json": `{"model_type":"llama","quantization_config":{"quant_method":"awq"}}`, "sub/dir/w.bin": "w"}, day1)
	seedRepo(t, st, "datasets/alice/d1", map[string]string{"README.md": dataCard}, day2)
	seedRepo(t, st, "spaces/alice/s1", map[string]string{"README.md": spaceCard, "app.py": "print(1)\n"}, day3)
}

func TestHandleListDefaultAndFullProjections(t *testing.T) {
	h, st := newListHandler(t)
	seedShowcase(t, st)

	m := listItems(t, h, "/api/models")[0]
	if got, want := keysOf(m), []string{"createdAt", "downloads", "id", "library_name", "likes", "modelId", "pipeline_tag", "private", "tags", "trendingScore"}; !slices.Equal(got, want) {
		t.Errorf("model default keys %v, want %v", got, want)
	}
	if m["modelId"] != "alice/m1" || m["pipeline_tag"] != "text-generation" || m["library_name"] != "transformers" || m["private"] != false || m["createdAt"] != "2024-01-01T12:00:00.000Z" {
		t.Errorf("model default values %v", m)
	}
	wantTags := []any{"chat", "en", "license:mit", "text-generation", "transformers", "dataset:alice/d1", "llama", "awq"}
	if got := m["tags"].([]any); !slices.Equal(got, wantTags) {
		t.Errorf("model tags %v, want %v", got, wantTags)
	}
	m = listItems(t, h, "/api/models?full=true")[0]
	if got, want := keysOf(m), []string{"author", "createdAt", "downloads", "gated", "id", "lastModified", "library_name", "likes", "modelId", "pipeline_tag", "private", "sha", "siblings", "tags", "trendingScore"}; !slices.Equal(got, want) {
		t.Errorf("model full keys %v, want %v", got, want)
	}
	if m["author"] != "alice" || m["lastModified"] != "2024-01-01T12:00:00.000Z" || len(m["sha"].(string)) != 40 {
		t.Errorf("model full values %v", m)
	}
	var files []string
	for _, s := range m["siblings"].([]any) {
		files = append(files, s.(map[string]any)["rfilename"].(string))
	}
	if want := []string{"README.md", "config.json", "sub/dir/w.bin"}; !slices.Equal(files, want) {
		t.Errorf("siblings %v, want %v", files, want)
	}

	d := listItems(t, h, "/api/datasets")[0]
	if got, want := keysOf(d), []string{"author", "createdAt", "description", "disabled", "downloads", "gated", "id", "lastModified", "likes", "paperswithcode_id", "private", "sha", "tags", "trendingScore"}; !slices.Equal(got, want) {
		t.Errorf("dataset default keys %v, want %v", got, want)
	}
	if d["description"] != "Large movie reviews.\nSecond line." || d["paperswithcode_id"] != "imdb" || d["author"] != "alice" || d["id"] != "alice/d1" {
		t.Errorf("dataset default values %v", d)
	}
	wantTags = []any{"task_categories:text-classification", "task_ids:sentiment-classification", "language:en", "license:other", "size_categories:1K<n<10K", "library:datasets", "qa"}
	if got := d["tags"].([]any); !slices.Equal(got, wantTags) {
		t.Errorf("dataset tags %v, want %v", got, wantTags)
	}
	d = listItems(t, h, "/api/datasets?full=1")[0]
	card, ok := d["cardData"].(map[string]any)
	if !ok || card["pretty_name"] != nil || card["paperswithcode_id"] != "imdb" || card["license"] != "other" {
		t.Errorf("dataset full cardData %v", d["cardData"])
	}

	s := listItems(t, h, "/api/spaces")[0]
	if got, want := keysOf(s), []string{"createdAt", "id", "likes", "private", "sdk", "tags", "trendingScore"}; !slices.Equal(got, want) {
		t.Errorf("space default keys %v, want %v", got, want)
	}
	if s["sdk"] != "gradio" || !slices.Equal(s["tags"].([]any), []any{"gradio"}) {
		t.Errorf("space default values %v", s)
	}
	s = listItems(t, h, "/api/spaces?full=true")[0]
	if got, want := keysOf(s), []string{"author", "cardData", "createdAt", "id", "lastModified", "likes", "private", "sdk", "sha", "siblings", "tags", "trendingScore"}; !slices.Equal(got, want) {
		t.Errorf("space full keys %v, want %v", got, want)
	}
}

func TestHandleListExpandProjection(t *testing.T) {
	h, st := newListHandler(t)
	seedShowcase(t, st)
	for target, want := range map[string][]string{
		"/api/models?expand[]=author&expand[]=lastModified":                                               {"author", "id", "lastModified", "trendingScore"},
		"/api/models?expand=createdAt&full=true":                                                          {"createdAt", "id", "trendingScore"},
		"/api/models?expand[]=safetensors":                                                                {"id", "trendingScore"},
		"/api/models?expand[]=private&expand[]=likes&expand[]=downloads&expand[]=gated&expand[]=disabled": {"disabled", "downloads", "gated", "id", "likes", "private", "trendingScore"},
		"/api/models?expand[]=cardData&expand[]=tags":                                                     {"cardData", "id", "tags", "trendingScore"},
		"/api/datasets?expand[]=description&expand[]=citation":                                            {"description", "id", "trendingScore"},
		"/api/spaces?expand[]=models&expand[]=sdk&expand[]=datasets":                                      {"id", "models", "sdk", "trendingScore"},
	} {
		items := listItems(t, h, target)
		if got := keysOf(items[0]); !slices.Equal(got, want) {
			t.Errorf("%s: keys %v, want %v", target, got, want)
		}
	}
	m := listItems(t, h, "/api/models?expand[]=private&expand[]=likes")[0]
	if m["private"] != false || m["likes"] != float64(0) {
		t.Errorf("requested zero values must stay: %v", m)
	}
	s := listItems(t, h, "/api/spaces?expand[]=models")[0]
	if !slices.Equal(s["models"].([]any), []any{"alice/m1"}) {
		t.Errorf("space models %v", s["models"])
	}
}

func TestHandleListSortsByDatesWithDirectionAndTieBreak(t *testing.T) {
	h, st := newListHandler(t)
	seedRepo(t, st, "alice/a", map[string]string{"README.md": "a\n"}, day1)
	seedRepo(t, st, "alice/a", map[string]string{"x.txt": "x\n"}, day3)
	seedRepo(t, st, "bob/b", map[string]string{"README.md": "b\n"}, day2)
	seedRepo(t, st, "carol/c", map[string]string{"README.md": "c\n"}, day2)
	if _, err := repository.Init(context.Background(), st.RepositoriesFS(), repository.ResolvePath("dave/empty"), "main"); err != nil {
		t.Fatal(err)
	}
	for target, want := range map[string][]string{
		"/api/models":                                  {"alice/a", "bob/b", "carol/c", "dave/empty"},
		"/api/models?sort=createdAt":                   {"bob/b", "carol/c", "alice/a", "dave/empty"},
		"/api/models?sort=createdAt&direction=-1":      {"bob/b", "carol/c", "alice/a", "dave/empty"},
		"/api/models?sort=createdAt&direction=1":       {"dave/empty", "alice/a", "bob/b", "carol/c"},
		"/api/models?sort=lastModified":                {"alice/a", "bob/b", "carol/c", "dave/empty"},
		"/api/models?sort=lastModified&direction=1":    {"dave/empty", "bob/b", "carol/c", "alice/a"},
		"/api/models?sort=likes":                       {"alice/a", "bob/b", "carol/c", "dave/empty"},
		"/api/models?sort=trendingScore&direction=1":   {"alice/a", "bob/b", "carol/c", "dave/empty"},
		"/api/models?sort=downloads&expand[]=author":   {"alice/a", "bob/b", "carol/c", "dave/empty"},
		"/api/models?sort=trending_score&direction=-1": {"alice/a", "bob/b", "carol/c", "dave/empty"},
	} {
		if got := listIDs(t, h, target); !slices.Equal(got, want) {
			t.Errorf("%s: ids %v, want %v", target, got, want)
		}
	}
	a := listItems(t, h, "/api/models?author=alice&expand[]=createdAt&expand[]=lastModified&expand[]=sha")[0]
	if a["createdAt"] != "2024-01-01T12:00:00.000Z" || a["lastModified"] != "2024-03-01T12:00:00.000Z" || len(a["sha"].(string)) != 40 {
		t.Errorf("alice dates %v", a)
	}
	empty := listItems(t, h, "/api/models?author=dave&full=true")[0]
	if _, has := empty["sha"]; has || empty["createdAt"] != nil || empty["lastModified"] != nil || len(empty["siblings"].([]any)) != 0 {
		t.Errorf("empty repository must carry no dates: %v", empty)
	}

	// An older-dated commit on carol/c moves its tip, so the cached earliest date must be recomputed.
	seedRepo(t, st, "carol/c", map[string]string{"old.txt": "o\n"}, day1.Add(-time.Hour))
	c := listItems(t, h, "/api/models?author=carol&expand[]=createdAt")[0]
	if c["createdAt"] != "2024-01-01T11:00:00.000Z" {
		t.Errorf("carol createdAt after new tip %v", c["createdAt"])
	}
	if got := listIDs(t, h, "/api/models?sort=createdAt&direction=1"); !slices.Equal(got, []string{"dave/empty", "carol/c", "alice/a", "bob/b"}) {
		t.Errorf("ids after tip move %v", got)
	}
}

func TestHandleListToleratesBrokenAndEmptyCards(t *testing.T) {
	h, st := newListHandler(t)
	seedRepo(t, st, "alice/good", map[string]string{"README.md": modelCard}, day1)
	seedRepo(t, st, "alice/nan", map[string]string{"README.md": "---\nscore: .nan\nlicense: mit\n---\n"}, day1)
	seedRepo(t, st, "alice/broken", map[string]string{"README.md": "---\n: : :\n  - [\n---\n# broken\n"}, day1)
	seedRepo(t, st, "alice/plain", map[string]string{"weights.bin": "x", "config.json": "{not json"}, day1)
	seedRepo(t, st, "datasets/alice/plain", map[string]string{"README.md": "# Title only\n\nJust prose here.\n"}, day1)
	if _, err := repository.Init(context.Background(), st.RepositoriesFS(), repository.ResolvePath("alice/empty"), "main"); err != nil {
		t.Fatal(err)
	}

	byID := map[string]map[string]any{}
	for _, item := range listItems(t, h, "/api/models?expand[]=cardData&expand[]=tags&expand[]=sha&expand[]=createdAt&expand[]=siblings") {
		byID[item["id"].(string)] = item
	}
	if len(byID) != 5 {
		t.Fatalf("items %v", byID)
	}
	if byID["alice/good"]["cardData"] == nil || byID["alice/nan"]["cardData"] != nil || !slices.Equal(byID["alice/nan"]["tags"].([]any), []any{"license:mit"}) {
		t.Errorf("NaN card must drop only its own cardData: good=%v nan=%v", byID["alice/good"]["cardData"], byID["alice/nan"])
	}
	if tags := byID["alice/broken"]["tags"].([]any); len(tags) != 0 {
		t.Errorf("broken card tags %v", tags)
	}
	if tags := byID["alice/plain"]["tags"].([]any); len(tags) != 0 || byID["alice/plain"]["cardData"] != nil {
		t.Errorf("plain repo %v", byID["alice/plain"])
	}
	empty := byID["alice/empty"]
	if _, has := empty["sha"]; has || empty["createdAt"] != nil || len(empty["tags"].([]any)) != 0 || len(empty["siblings"].([]any)) != 0 {
		t.Errorf("empty repo %v", empty)
	}
	// The default projection never reads cardData, so an unserializable card cannot touch it.
	if ids := listIDs(t, h, "/api/models"); !slices.Equal(ids, []string{"alice/broken", "alice/empty", "alice/good", "alice/nan", "alice/plain"}) {
		t.Errorf("default listing %v", ids)
	}
	d := listItems(t, h, "/api/datasets")[0]
	if d["description"] != "Just prose here." {
		t.Errorf("description without front matter %q", d["description"])
	}
}

type countingCard struct {
	calls int
	err   error
}

func (c *countingCard) MarshalJSON() ([]byte, error) {
	c.calls++
	return []byte(`{"license":"mit"}`), c.err
}

func TestProjectMarshalsCardOnlyWhenRequested(t *testing.T) {
	card := &countingCard{}
	it := &listItem{id: "alice/m1", meta: repoMetadata{cardData: card}}
	if out := it.project(map[string]bool{"tags": true, "author": true}); card.calls != 0 || out["cardData"] != nil {
		t.Errorf("unrequested cardData: %d marshal calls, %v", card.calls, out["cardData"])
	}
	if out := it.project(map[string]bool{"cardData": true}); card.calls != 1 || string(out["cardData"].(json.RawMessage)) != `{"license":"mit"}` {
		t.Errorf("requested cardData: %d marshal calls, %v", card.calls, out["cardData"])
	}
	card.err = errors.New("json: unsupported value: NaN")
	if out := it.project(map[string]bool{"cardData": true}); card.calls != 2 || out["cardData"] != nil {
		t.Errorf("unserializable cardData: %d marshal calls, %v", card.calls, out["cardData"])
	}
}

func TestHandleListFilters(t *testing.T) {
	h, st := newListHandler(t)
	seedShowcase(t, st)
	seedRepo(t, st, "bob/m3", map[string]string{"README.md": "---\npipeline_tag: fill-mask\ntags: [text-generation, chat]\n---\n"}, day1)
	for target, want := range map[string][]string{
		"/api/models?filter=license:mit&filter=chat":                         {"alice/m1"},
		"/api/models?filter=chat":                                            {"alice/m1", "bob/m3"},
		"/api/models?filter=license:mit&filter=nonexistent":                  nil,
		"/api/models?pipeline_tag=text-generation":                           {"alice/m1"},
		"/api/models?filter=text-generation":                                 {"alice/m1", "bob/m3"},
		"/api/models?pipeline_tag=fill-mask&pipeline_tag=zzz":                {"bob/m3"},
		"/api/models?pipeline_tag=fill-mask&filter=license:mit":              nil,
		"/api/datasets?pipeline_tag=text-classification":                     nil,
		"/api/datasets?filter=task_categories:text-classification&filter=qa": {"alice/d1"},
		"/api/spaces?filter=gradio":                                          {"alice/s1"},
		"/api/models?search=M1":                                              {"alice/m1"},
		"/api/models?author=alice&search=zzz":                                nil,
	} {
		if got := listIDs(t, h, target); !slices.Equal(got, want) {
			t.Errorf("%s: ids %v, want %v", target, got, want)
		}
	}
	if got := listItems(t, h, "/api/datasets?author=nobody"); got == nil {
		t.Error("empty list must be a JSON array")
	}
}

func TestHandleListPaginationCursorAndLink(t *testing.T) {
	h, st := newListHandler(t)
	for _, name := range []string{"r1", "r2", "r3", "r4", "r5"} {
		seedRepo(t, st, "alice/"+name, map[string]string{"README.md": name}, day1)
	}
	page := func(rec *httptest.ResponseRecorder) []string {
		var items []map[string]any
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &items) != nil {
			t.Fatalf("page %d %s", rec.Code, rec.Body)
		}
		var ids []string
		for _, item := range items {
			ids = append(ids, item["id"].(string))
		}
		return ids
	}
	rec := listDo(t, h, "/api/models?limit=2&search=r&expand[]=author", "X-Forwarded-Proto", "https")
	if got := page(rec); !slices.Equal(got, []string{"alice/r1", "alice/r2"}) {
		t.Fatalf("page 1 %v", got)
	}
	u, err := url.Parse(strings.TrimSuffix(strings.TrimPrefix(rec.Header().Get("Link"), "<"), `>; rel="next"`))
	if err != nil || u.Scheme != "https" || u.Host != "example.com" || u.Path != "/api/models" || u.Query().Get("search") != "r" || u.Query().Get("limit") != "2" || u.Query()["expand[]"][0] != "author" || u.Query().Get("cursor") == "" {
		t.Fatalf("link %q: %v", rec.Header().Get("Link"), err)
	}
	rec = listDo(t, h, u.RequestURI())
	if got := page(rec); !slices.Equal(got, []string{"alice/r3", "alice/r4"}) {
		t.Fatalf("page 2 %v", got)
	}
	u, err = url.Parse(strings.TrimSuffix(strings.TrimPrefix(rec.Header().Get("Link"), "<"), `>; rel="next"`))
	if err != nil || u.Scheme != "http" || u.Query().Get("cursor") == "" {
		t.Fatalf("link %q: %v", rec.Header().Get("Link"), err)
	}
	rec = listDo(t, h, u.RequestURI())
	if got := page(rec); !slices.Equal(got, []string{"alice/r5"}) || rec.Header().Get("Link") != "" {
		t.Errorf("page 3 %v link %q", got, rec.Header().Get("Link"))
	}
	cursor := u.Query().Get("cursor")
	if got := listIDs(t, h, "/api/models?cursor="+cursor+"&limit=1"); !slices.Equal(got, []string{"alice/r5"}) {
		t.Errorf("reused cursor %v", got)
	}
	// Cursors from earlier hfd releases were padded base64url; unpadded ones are accepted too.
	if got := listIDs(t, h, "/api/models?cursor="+strings.TrimRight(cursor, "=")); !slices.Equal(got, []string{"alice/r5"}) {
		t.Errorf("unpadded cursor %v", got)
	}
	if got := listIDs(t, h, "/api/models?limit=99999"); len(got) != 5 {
		t.Errorf("large limit %v", got)
	}
	far := listDo(t, h, "/api/models?cursor="+encodeCursorOffset(1<<40))
	if got := page(far); len(got) != 0 || far.Header().Get("Link") != "" {
		t.Errorf("far cursor %v %q", got, far.Header().Get("Link"))
	}
}

func TestHandleListStorageFailuresAreServerErrors(t *testing.T) {
	root := t.TempDir()
	st := storage.NewStorage(storage.WithRootDir(root))
	seedRepo(t, st, "alice/m1", map[string]string{"README.md": modelCard}, day1)
	seedRepo(t, st, "bob/m2", map[string]string{"README.md": "# m2\n"}, day1)
	// A new Storage opens repositories afresh instead of serving cached handles.
	reopen := func() *Handler { return NewHandler(WithStorage(storage.NewStorage(storage.WithRootDir(root)))) }

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := httptest.NewRecorder()
	reopen().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/models", nil).WithContext(ctx))
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), `"error"`) {
		t.Errorf("cancelled request: %d %s", rec.Code, rec.Body)
	}

	config, err := st.RepositoriesFS().Create("/alice/m1.git/config")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := config.Write([]byte("[malformed")); err != nil {
		t.Fatal(err)
	}
	if err := config.Close(); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"/api/models", "/api/models?author=alice"} {
		if rec := listDo(t, reopen(), target); rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), `"error"`) {
			t.Errorf("%s with a malformed config: %d %s", target, rec.Code, rec.Body)
		}
	}
	if got := listIDs(t, reopen(), "/api/models?author=bob"); !slices.Equal(got, []string{"bob/m2"}) {
		t.Errorf("intact repository alone: %v", got)
	}
}

// hookFS reports every Open, Stat and ReadDir path, including those of the chroots derived from it.
type hookFS struct {
	billy.Filesystem
	hook func(path string)
}

func (f *hookFS) Open(p string) (billy.File, error)    { f.hook(p); return f.Filesystem.Open(p) }
func (f *hookFS) Stat(p string) (iofs.FileInfo, error) { f.hook(p); return f.Filesystem.Stat(p) }
func (f *hookFS) ReadDir(p string) ([]iofs.DirEntry, error) {
	f.hook(p)
	return f.Filesystem.ReadDir(p)
}
func (f *hookFS) Chroot(p string) (billy.Filesystem, error) {
	sub, err := f.Filesystem.Chroot(p)
	if err != nil {
		return nil, err
	}
	return &hookFS{sub, f.hook}, nil
}

func TestHandleListCancellationWithoutStorageErrorIsServerError(t *testing.T) {
	// One namespace: Walk sorts a directory's entries, so alice/m2 is the last repository enumerated and projected.
	seeded := t.TempDir()
	st := storage.NewStorage(storage.WithRootDir(seeded))
	seedRepo(t, st, "alice/m1", map[string]string{"README.md": modelCard}, day1)
	seedRepo(t, st, "alice/m2", map[string]string{"README.md": "# m2\n"}, day1)
	empty := t.TempDir()
	if err := storage.NewStorage(storage.WithRootDir(empty)).FS().MkdirAll("/repositories", 0o755); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		root, cancelAt string
		inProjection   bool
	}{
		"PreCancelledMissingStorage":    {t.TempDir(), "", false},
		"PreCancelledEmptyStorage":      {empty, "", false},
		"CancelledInMissingReadDir":     {t.TempDir(), "/", false},
		"CancelledInLastRepositoryStat": {seeded, "/alice/m2.git/HEAD", false},
		"CancelledInProjection":         {seeded, "/alice/m2.git/objects/", true},
	} {
		t.Run(name, func(t *testing.T) {
			var calls int
			newCase := func() (context.Context, *Handler) {
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				if tc.cancelAt == "" {
					cancel()
				}
				calls = 0
				fs := &hookFS{osfs.New(tc.root), func(p string) {
					calls++
					if tc.cancelAt != "" && strings.HasPrefix(p, tc.cancelAt) {
						cancel()
					}
				}}
				return ctx, NewHandler(WithStorage(storage.NewStorage(storage.WithFilesystem(fs))))
			}
			if !tc.inProjection {
				ctx, h := newCase()
				f, _ := parseRepoListFilter(httptest.NewRequest(http.MethodGet, "/api/models", nil), "models")
				if items, err := h.buildRepoListItems(ctx, "models", f); !errors.Is(err, context.Canceled) {
					t.Errorf("buildRepoListItems: %d items, err %v", len(items), err)
				}
				if tc.cancelAt == "" && calls != 0 {
					t.Errorf("a cancelled request touched storage %d times", calls)
				}
			}
			ctx, h := newCase()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/models", nil).WithContext(ctx))
			if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), `"error"`) {
				t.Errorf("%d %s", rec.Code, rec.Body)
			}
		})
	}
}

func TestHandleListRejectsMalformedQuery(t *testing.T) {
	h, _ := newListHandler(t)
	for _, target := range []string{
		"/api/models?sort=bogus", "/api/models?sort=created_at", "/api/models?sort=createdAt&direction=abc", "/api/models?sort=createdAt&direction=0", "/api/models?direction=2",
		"/api/models?limit=0", "/api/models?limit=-1", "/api/models?limit=abc", "/api/models?limit=99999999999999999999",
		"/api/models?cursor=!!!", "/api/models?cursor=bm90anNvbg", "/api/models?cursor=" + encodeCursorOffset(-5), "/api/models?cursor=eyJvZmZzZXQiOjFlOTk5fQ",
		"/api/models?expand[]=bogus", "/api/models?expand[]=sdk", "/api/datasets?expand[]=pipeline_tag", "/api/spaces?expand[]=description", "/api/models?expand=downloads,likes",
	} {
		rec := listDo(t, h, target)
		var body map[string]string
		if rec.Code != http.StatusBadRequest || json.Unmarshal(rec.Body.Bytes(), &body) != nil || body["error"] == "" {
			t.Errorf("%s: %d %s", target, rec.Code, rec.Body)
		}
	}
	var body map[string]string
	rec := listDo(t, h, "/api/spaces?expand[]=safetensors")
	if json.Unmarshal(rec.Body.Bytes(), &body) != nil || !strings.Contains(body["error"], `Invalid option: expected one of "author"|`) || !strings.Contains(body["error"], `"subdomain"`) {
		t.Errorf("expand error must name the type's options: %s", rec.Body)
	}
}

func TestHandleListPermission(t *testing.T) {
	authors := []struct {
		name, query, want string
	}{
		{"NoAuthor", "", ""},
		{"Author", "?author=alice", "alice"},
		{"EncodedAuthor", "?author=my%20org%2Fteam&limit=1", "my org/team"},
	}
	seedRepo := map[string]string{"models": "alice/hidden-repo.git", "datasets": "datasets/alice/hidden-repo.git"}
	for _, repoType := range []string{"models", "datasets"} {
		for _, tc := range authors {
			t.Run(repoType+"/"+tc.name, func(t *testing.T) {
				dataDir := t.TempDir()
				if _, err := repository.Init(context.Background(), osfs.Default, filepath.Join(dataDir, "repositories", filepath.FromSlash(seedRepo[repoType])), "main"); err != nil {
					t.Fatalf("Init: %v", err)
				}
				var gotOp permission.Operation
				var gotRepo string
				var gotCtx permission.Context
				calls := 0
				hook := func(ctx context.Context, op permission.Operation, repoName string, opCtx permission.Context) (bool, error) {
					calls++
					gotOp, gotRepo, gotCtx = op, repoName, opCtx
					return false, nil
				}
				handler := NewHandler(
					WithStorage(storage.NewStorage(storage.WithRootDir(dataDir))),
					WithPermissionHookFunc(hook),
				)
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/"+repoType+tc.query, nil))
				if response.Code != http.StatusForbidden || strings.Contains(response.Body.String(), "hidden-repo") {
					t.Errorf("status=%d, want 403 with no repositories enumerated; body=%s", response.Code, response.Body.String())
				}
				if !strings.HasPrefix(response.Header().Get("Content-Type"), "application/json") || !json.Valid(response.Body.Bytes()) {
					t.Errorf("expected JSON response, got headers=%v body=%s", response.Header(), response.Body.String())
				}
				want := permission.Context{Author: tc.want}
				if calls != 1 || gotOp != permission.OperationListRepos || gotRepo != repoType || gotCtx != want {
					t.Errorf("hook calls=%d, op=%s, repo=%q, ctx=%+v; want 1, list_repos, %q, %+v", calls, gotOp, gotRepo, gotCtx, repoType, want)
				}
			})
		}
	}
}

func TestHandleListModelsEmpty(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	resp, err := http.Get(endpoint + "/api/models")
	if err != nil {
		t.Fatalf("Failed to list models: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("Expected 0 models, got %d", len(items))
	}
}

func TestHandleListModels(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create two model repos
	for _, body := range []string{
		`{"type":"model","name":"alpha-model","organization":"user-a"}`,
		`{"type":"model","name":"beta-model","organization":"user-b"}`,
	} {
		resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create repo: %v", err)
		}
		resp.Body.Close()
	}

	// Also create a dataset to ensure it's NOT listed
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"dataset","name":"my-dataset","organization":"user-a"}`))
	if err != nil {
		t.Fatalf("Failed to create dataset: %v", err)
	}
	resp.Body.Close()

	// List all models
	resp, err = http.Get(endpoint + "/api/models")
	if err != nil {
		t.Fatalf("Failed to list models: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("Expected 2 models, got %d", len(items))
	}

	// Verify modelId is set for models
	for _, item := range items {
		if item.ModelID == "" {
			t.Errorf("Expected modelId to be set for model %s", item.RepoID)
		}
	}
}

func TestHandleListModelsFilterByAuthor(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create repos under different authors
	for _, body := range []string{
		`{"type":"model","name":"model-1","organization":"alice"}`,
		`{"type":"model","name":"model-2","organization":"bob"}`,
	} {
		resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create repo: %v", err)
		}
		resp.Body.Close()
	}

	resp, err := http.Get(endpoint + "/api/models?author=alice")
	if err != nil {
		t.Fatalf("Failed to list models: %v", err)
	}
	defer resp.Body.Close()

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("Expected 1 model, got %d", len(items))
	}
	if items[0].RepoID != "alice/model-1" {
		t.Errorf("Expected alice/model-1, got %s", items[0].RepoID)
	}
}

func TestHandleListModelsSearch(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	for _, body := range []string{
		`{"type":"model","name":"llama-7b","organization":"meta"}`,
		`{"type":"model","name":"bert-base","organization":"google"}`,
	} {
		resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create repo: %v", err)
		}
		resp.Body.Close()
	}

	resp, err := http.Get(endpoint + "/api/models?search=llama")
	if err != nil {
		t.Fatalf("Failed to list models: %v", err)
	}
	defer resp.Body.Close()

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("Expected 1 model, got %d", len(items))
	}
	if items[0].RepoID != "meta/llama-7b" {
		t.Errorf("Expected meta/llama-7b, got %s", items[0].RepoID)
	}
}

func TestHandleListModelsLimit(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	for _, body := range []string{
		`{"type":"model","name":"model-a","organization":"org"}`,
		`{"type":"model","name":"model-b","organization":"org"}`,
		`{"type":"model","name":"model-c","organization":"org"}`,
	} {
		resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create repo: %v", err)
		}
		resp.Body.Close()
	}

	resp, err := http.Get(endpoint + "/api/models?limit=2")
	if err != nil {
		t.Fatalf("Failed to list models: %v", err)
	}
	defer resp.Body.Close()

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("Expected 2 models, got %d", len(items))
	}
}

func TestHandleListModelsResponseFormat(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create a model repo and commit a file with metadata
	createBody := `{"type":"model","name":"test-model","organization":"test-org"}`
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(createBody))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	// Commit a README with pipeline_tag and library_name
	ndjson := `{"key":"header","value":{"summary":"Initial commit"}}` + "\n" +
		`{"key":"file","value":{"content":"---\npipeline_tag: text-generation\nlibrary_name: transformers\n---\n# Test\n","path":"README.md","encoding":"utf-8"}}` + "\n"
	resp, err = http.Post(endpoint+"/api/models/test-org/test-model/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	resp.Body.Close()

	// List models and verify response format
	resp, err = http.Get(endpoint + "/api/models")
	if err != nil {
		t.Fatalf("Failed to list models: %v", err)
	}
	defer resp.Body.Close()

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("Expected 1 model, got %d", len(items))
	}

	item := items[0]
	if item.RepoID != "test-org/test-model" {
		t.Errorf("Expected id=test-org/test-model, got %s", item.RepoID)
	}
	if item.ModelID != "test-org/test-model" {
		t.Errorf("Expected modelId=test-org/test-model, got %s", item.ModelID)
	}
	if item.PipelineTag != "text-generation" {
		t.Errorf("Expected pipeline_tag=text-generation, got %s", item.PipelineTag)
	}
	if item.LibraryName != "transformers" {
		t.Errorf("Expected library_name=transformers, got %s", item.LibraryName)
	}
}

func TestHandleListDatasetsEmpty(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	resp, err := http.Get(endpoint + "/api/datasets")
	if err != nil {
		t.Fatalf("Failed to list datasets: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("Expected 0 datasets, got %d", len(items))
	}
}

func TestHandleListDatasets(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create datasets
	for _, body := range []string{
		`{"type":"dataset","name":"dataset-a","organization":"org-a"}`,
		`{"type":"dataset","name":"dataset-b","organization":"org-b"}`,
	} {
		resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create repo: %v", err)
		}
		resp.Body.Close()
	}

	// Also create a model to ensure it's NOT listed in datasets
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"my-model","organization":"org-a"}`))
	if err != nil {
		t.Fatalf("Failed to create model: %v", err)
	}
	resp.Body.Close()

	// List all datasets
	resp, err = http.Get(endpoint + "/api/datasets")
	if err != nil {
		t.Fatalf("Failed to list datasets: %v", err)
	}
	defer resp.Body.Close()

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("Expected 2 datasets, got %d", len(items))
	}

	// Verify modelId is NOT set for datasets
	for _, item := range items {
		if item.ModelID != "" {
			t.Errorf("Expected modelId to be empty for dataset %s, got %s", item.RepoID, item.ModelID)
		}
	}
}

func TestHandleListDatasetsFilterByAuthor(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	for _, body := range []string{
		`{"type":"dataset","name":"ds-1","organization":"alice"}`,
		`{"type":"dataset","name":"ds-2","organization":"bob"}`,
	} {
		resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create repo: %v", err)
		}
		resp.Body.Close()
	}

	resp, err := http.Get(endpoint + "/api/datasets?author=alice")
	if err != nil {
		t.Fatalf("Failed to list datasets: %v", err)
	}
	defer resp.Body.Close()

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("Expected 1 dataset, got %d", len(items))
	}
	if items[0].RepoID != "alice/ds-1" {
		t.Errorf("Expected alice/ds-1, got %s", items[0].RepoID)
	}
}

func TestHandleListSpacesEmpty(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	resp, err := http.Get(endpoint + "/api/spaces")
	if err != nil {
		t.Fatalf("Failed to list spaces: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("Expected 0 spaces, got %d", len(items))
	}
}

func TestHandleListSpaces(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create spaces
	for _, body := range []string{
		`{"type":"space","name":"space-a","organization":"org-a"}`,
		`{"type":"space","name":"space-b","organization":"org-b"}`,
	} {
		resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create repo: %v", err)
		}
		resp.Body.Close()
	}

	// Also create a model to ensure it's NOT listed in spaces
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"my-model","organization":"org-a"}`))
	if err != nil {
		t.Fatalf("Failed to create model: %v", err)
	}
	resp.Body.Close()

	// List all spaces
	resp, err = http.Get(endpoint + "/api/spaces")
	if err != nil {
		t.Fatalf("Failed to list spaces: %v", err)
	}
	defer resp.Body.Close()

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("Expected 2 spaces, got %d", len(items))
	}

	// Verify modelId is NOT set for spaces
	for _, item := range items {
		if item.ModelID != "" {
			t.Errorf("Expected modelId to be empty for space %s, got %s", item.RepoID, item.ModelID)
		}
	}
}

func TestHandleListSpacesSearch(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	for _, body := range []string{
		`{"type":"space","name":"chatbot-demo","organization":"dev"}`,
		`{"type":"space","name":"image-gen","organization":"dev"}`,
	} {
		resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create repo: %v", err)
		}
		resp.Body.Close()
	}

	resp, err := http.Get(endpoint + "/api/spaces?search=chatbot")
	if err != nil {
		t.Fatalf("Failed to list spaces: %v", err)
	}
	defer resp.Body.Close()

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("Expected 1 space, got %d", len(items))
	}
	if items[0].RepoID != "dev/chatbot-demo" {
		t.Errorf("Expected dev/chatbot-demo, got %s", items[0].RepoID)
	}
}

func TestHandleListModelsPagination(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create 5 models
	for _, name := range []string{"model-a", "model-b", "model-c", "model-d", "model-e"} {
		body := `{"type":"model","name":"` + name + `","organization":"org"}`
		resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create repo: %v", err)
		}
		resp.Body.Close()
	}

	// Page 1: limit=2, no cursor
	resp, err := http.Get(endpoint + "/api/models?limit=2")
	if err != nil {
		t.Fatalf("Failed to list models: %v", err)
	}

	var page1 []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&page1); err != nil {
		t.Fatalf("Failed to decode page 1: %v", err)
	}
	resp.Body.Close()

	if len(page1) != 2 {
		t.Fatalf("Page 1: expected 2 models, got %d", len(page1))
	}

	// Check Link header is present
	linkHeader := resp.Header.Get("Link")
	if linkHeader == "" {
		t.Fatal("Expected Link header on page 1, got none")
	}
	if !strings.Contains(linkHeader, `rel="next"`) {
		t.Errorf("Expected Link header to contain rel=\"next\", got: %s", linkHeader)
	}

	// Extract cursor URL from Link header
	nextURL := extractNextURL(linkHeader)
	if nextURL == "" {
		t.Fatalf("Could not extract next URL from Link header: %s", linkHeader)
	}

	// Page 2: follow the cursor
	resp, err = http.Get(nextURL)
	if err != nil {
		t.Fatalf("Failed to fetch page 2: %v", err)
	}

	var page2 []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&page2); err != nil {
		t.Fatalf("Failed to decode page 2: %v", err)
	}
	linkHeader2 := resp.Header.Get("Link")
	resp.Body.Close()

	if len(page2) != 2 {
		t.Fatalf("Page 2: expected 2 models, got %d", len(page2))
	}

	// Page 2 should have a Link header for page 3
	if linkHeader2 == "" {
		t.Fatal("Expected Link header on page 2, got none")
	}
	nextURL2 := extractNextURL(linkHeader2)

	// Page 3: last page (1 remaining item)
	resp, err = http.Get(nextURL2)
	if err != nil {
		t.Fatalf("Failed to fetch page 3: %v", err)
	}

	var page3 []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&page3); err != nil {
		t.Fatalf("Failed to decode page 3: %v", err)
	}
	linkHeader3 := resp.Header.Get("Link")
	resp.Body.Close()

	if len(page3) != 1 {
		t.Fatalf("Page 3: expected 1 model, got %d", len(page3))
	}

	// Last page should NOT have a Link header
	if linkHeader3 != "" {
		t.Errorf("Expected no Link header on last page, got: %s", linkHeader3)
	}

	// Verify no duplicates across pages
	allIDs := make(map[string]bool)
	for _, item := range page1 {
		allIDs[item.RepoID] = true
	}
	for _, item := range page2 {
		if allIDs[item.RepoID] {
			t.Errorf("Duplicate item across pages: %s", item.RepoID)
		}
		allIDs[item.RepoID] = true
	}
	for _, item := range page3 {
		if allIDs[item.RepoID] {
			t.Errorf("Duplicate item across pages: %s", item.RepoID)
		}
		allIDs[item.RepoID] = true
	}
	if len(allIDs) != 5 {
		t.Errorf("Expected 5 unique models across all pages, got %d", len(allIDs))
	}
}

func TestHandleListModelsNoLinkHeaderWhenAllResultsFit(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create 2 models
	for _, name := range []string{"model-x", "model-y"} {
		body := `{"type":"model","name":"` + name + `","organization":"org"}`
		resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create repo: %v", err)
		}
		resp.Body.Close()
	}

	// Request with limit=5 (more than available)
	resp, err := http.Get(endpoint + "/api/models?limit=5")
	if err != nil {
		t.Fatalf("Failed to list models: %v", err)
	}
	defer resp.Body.Close()

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("Expected 2 models, got %d", len(items))
	}

	// No Link header when all results fit
	linkHeader := resp.Header.Get("Link")
	if linkHeader != "" {
		t.Errorf("Expected no Link header when all results fit, got: %s", linkHeader)
	}
}

func TestHandleListModelsNoLinkHeaderWithoutLimit(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create 2 models
	for _, name := range []string{"model-x", "model-y"} {
		body := `{"type":"model","name":"` + name + `","organization":"org"}`
		resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create repo: %v", err)
		}
		resp.Body.Close()
	}

	// Request without limit
	resp, err := http.Get(endpoint + "/api/models")
	if err != nil {
		t.Fatalf("Failed to list models: %v", err)
	}
	defer resp.Body.Close()

	// No Link header when no limit is set
	linkHeader := resp.Header.Get("Link")
	if linkHeader != "" {
		t.Errorf("Expected no Link header without limit, got: %s", linkHeader)
	}
}

// extractNextURL parses the next URL from a Link header value like:
// <http://example.com/api/models?cursor=abc>; rel="next"
func extractNextURL(linkHeader string) string {
	for part := range strings.SplitSeq(linkHeader, ",") {
		part = strings.TrimSpace(part)
		if strings.Contains(part, `rel="next"`) {
			start := strings.Index(part, "<")
			end := strings.Index(part, ">")
			if start >= 0 && end > start {
				return part[start+1 : end]
			}
		}
	}
	return ""
}

func TestHandleListModelsFilterByTag(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create two models
	for _, body := range []string{
		`{"type":"model","name":"model-a","organization":"org"}`,
		`{"type":"model","name":"model-b","organization":"org"}`,
	} {
		resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create repo: %v", err)
		}
		resp.Body.Close()
	}

	// Commit a README with a tag to model-a
	ndjson := `{"key":"header","value":{"summary":"add readme"}}` + "\n" +
		`{"key":"file","value":{"content":"---\ntags:\n- text-classification\npipeline_tag: text-classification\n---\n# Model A\n","path":"README.md","encoding":"utf-8"}}` + "\n"
	resp, err := http.Post(endpoint+"/api/models/org/model-a/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	resp.Body.Close()

	// Filter by tag "text-classification" — should return only model-a
	resp, err = http.Get(endpoint + "/api/models?filter=text-classification")
	if err != nil {
		t.Fatalf("Failed to list models: %v", err)
	}
	defer resp.Body.Close()

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("Expected 1 model with tag text-classification, got %d", len(items))
	}
	if items[0].RepoID != "org/model-a" {
		t.Errorf("Expected org/model-a, got %s", items[0].RepoID)
	}
}

func TestHandleListModelsSortByCreatedAt(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create models in order
	for _, name := range []string{"model-first", "model-second"} {
		body := `{"type":"model","name":"` + name + `","organization":"org"}`
		resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create repo: %v", err)
		}
		resp.Body.Close()

		// Commit to give each model a commit date
		ndjson := `{"key":"header","value":{"summary":"init"}}` + "\n" +
			`{"key":"file","value":{"content":"# ` + name + `\n","path":"README.md","encoding":"utf-8"}}` + "\n"
		resp, err = http.Post(endpoint+"/api/models/org/"+name+"/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
		if err != nil {
			t.Fatalf("Failed to commit: %v", err)
		}
		resp.Body.Close()
	}

	// Sort by createdAt — most recently created first
	resp, err := http.Get(endpoint + "/api/models?sort=createdAt")
	if err != nil {
		t.Fatalf("Failed to list models: %v", err)
	}
	defer resp.Body.Close()

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("Expected 2 models, got %d", len(items))
	}
}

func TestHandleListModelsExpand(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create a model with metadata
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"model","name":"expand-test","organization":"org"}`))
	if err != nil {
		t.Fatalf("Failed to create repo: %v", err)
	}
	resp.Body.Close()

	// Commit a README with tags, pipeline_tag, library_name
	ndjson := `{"key":"header","value":{"summary":"init"}}` + "\n" +
		`{"key":"file","value":{"content":"---\ntags:\n- safetensors\npipeline_tag: text-generation\nlibrary_name: transformers\n---\n# Test\n","path":"README.md","encoding":"utf-8"}}` + "\n"
	resp, err = http.Post(endpoint+"/api/models/org/expand-test/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	resp.Body.Close()

	// Request with repeated expand parameters
	resp, err = http.Get(endpoint + "/api/models?expand[]=downloads&expand[]=likes&expand[]=tags&expand[]=pipeline_tag&expand[]=library_name&expand[]=createdAt&expand[]=trendingScore")
	if err != nil {
		t.Fatalf("Failed to list models: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("Expected 1 model, got %d", len(items))
	}

	item := items[0]
	if item.PipelineTag != "text-generation" {
		t.Errorf("Expected pipeline_tag=text-generation, got %s", item.PipelineTag)
	}
	if item.LibraryName != "transformers" {
		t.Errorf("Expected library_name=transformers, got %s", item.LibraryName)
	}
	if len(item.Tags) == 0 {
		t.Error("Expected tags to be populated")
	}
}

func TestHandleListModelsSortByLikes(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create models
	for _, name := range []string{"model-a", "model-b"} {
		body := `{"type":"model","name":"` + name + `","organization":"org"}`
		resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create repo: %v", err)
		}
		resp.Body.Close()
	}

	// Sort by likes (all zero, so should still return results ordered stably)
	resp, err := http.Get(endpoint + "/api/models?sort=likes")
	if err != nil {
		t.Fatalf("Failed to list models: %v", err)
	}
	defer resp.Body.Close()

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("Expected 2 models, got %d", len(items))
	}
}

func TestHandleListModelsSortByTrendingScore(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	// Create models
	for _, name := range []string{"model-a", "model-b"} {
		body := `{"type":"model","name":"` + name + `","organization":"org"}`
		resp, err := http.Post(endpoint+"/api/repos/create", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create repo: %v", err)
		}
		resp.Body.Close()
	}

	// Sort by trending_score
	resp, err := http.Get(endpoint + "/api/models?sort=trending_score")
	if err != nil {
		t.Fatalf("Failed to list models: %v", err)
	}
	defer resp.Body.Close()

	var items []repoListItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("Expected 2 models, got %d", len(items))
	}
}

// TestHandleListRepoIDIsPath pins that a listed id is the repository's path below its type's base
// directory with only the trailing .git removed, and that search and author apply to that path.
func TestHandleListRepoIDIsPath(t *testing.T) {
	server, dataDir := setupTestServer(t)
	endpoint := server.URL

	for _, p := range []string{"org/a/b.git", "ns.git/repo.git", "datasets/org/a/b.git", "datasets/alice/ds.git"} {
		if _, err := repository.Init(context.Background(), osfs.Default, filepath.Join(dataDir, "repositories", filepath.FromSlash(p)), "main"); err != nil {
			t.Fatalf("Init(%s): %v", p, err)
		}
	}

	tests := []struct {
		query string
		want  []string
	}{
		{"/api/models", []string{"ns.git/repo", "org/a/b"}},
		{"/api/models?search=a/b", []string{"org/a/b"}},
		{"/api/models?author=org", []string{"org/a/b"}},
		{"/api/datasets", []string{"alice/ds", "org/a/b"}},
		{"/api/datasets?author=org", []string{"org/a/b"}},
		// The author is a walk root: it must stay one element inside its own type.
		{"/api/models?author=datasets", nil},
		{"/api/models?author=datasets%2Forg", nil},
		{"/api/models?author=..", nil},
		{"/api/models?author=.", nil},
	}
	for _, tt := range tests {
		resp, err := http.Get(endpoint + tt.query)
		if err != nil {
			t.Fatalf("%s: %v", tt.query, err)
		}
		var items []repoListItem
		if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
			t.Fatalf("%s: decode: %v", tt.query, err)
		}
		resp.Body.Close()
		var got []string
		for _, item := range items {
			got = append(got, item.RepoID)
			if isModel := strings.HasPrefix(tt.query, "/api/models"); (item.ModelID != "") != isModel || (isModel && item.ModelID != item.RepoID) {
				t.Errorf("%s: %s has modelId %q", tt.query, item.RepoID, item.ModelID)
			}
		}
		if !slices.Equal(got, tt.want) {
			t.Errorf("%s: got %v, want %v", tt.query, got, tt.want)
		}
	}
}
