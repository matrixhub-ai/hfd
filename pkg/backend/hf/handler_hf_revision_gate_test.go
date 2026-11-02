package hf_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/backend/hf"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// The Hub API resolves a revision against the repository's own refs, whatever commit of another repository the shared pool holds.
func TestRevisionGate(t *testing.T) {
	st := newStorage(t, t.TempDir())
	commit := func(name, content string) string {
		t.Helper()
		repo, err := repository.Init(t.Context(), st.RepositoriesFS(), repository.ResolvePath(name), "main")
		if err != nil {
			t.Fatalf("init %s: %v", name, err)
		}
		hash, err := repo.CreateCommit(t.Context(), "main", "init", "Test", "test@test.com",
			[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: "README.md", Content: []byte(content)}}, "")
		if err != nil {
			t.Fatalf("commit to %s: %v", name, err)
		}
		return hash
	}
	own := commit("a/x", "# a\n")
	foreign := commit("b/y", "# b\n")
	h := hf.NewHandler(hf.WithStorage(st))

	for _, tc := range []struct {
		name, target string
		want         int
	}{
		{"ResolveOtherRepositoryCommit", "/a/x/resolve/" + foreign + "/README.md", http.StatusNotFound},
		{"TreeOtherRepositoryCommit", "/api/models/a/x/tree/" + foreign, http.StatusNotFound},
		{"ResolveShortHash", "/a/x/resolve/" + own[:7] + "/README.md", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.target, nil))
			if rec.Code != tc.want {
				t.Errorf("GET %s: status = %d, want %d: %s", tc.target, rec.Code, tc.want, rec.Body)
			}
		})
	}
}
