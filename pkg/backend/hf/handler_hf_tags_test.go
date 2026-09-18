package hf

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// seedRepoFiles creates a repository and commits the given files to main.
func seedRepoFiles(t *testing.T, endpoint, repoType, org, name string, files map[string]string) {
	t.Helper()
	resp, err := http.Post(endpoint+"/api/repos/create", "application/json",
		strings.NewReader(`{"type":"`+repoType+`","name":"`+name+`","organization":"`+org+`"}`))
	if err != nil {
		t.Fatalf("create %s/%s: %v", org, name, err)
	}
	resp.Body.Close()
	if len(files) == 0 {
		return
	}
	ndjson := `{"key":"header","value":{"summary":"seed"}}` + "\n"
	for path, content := range files {
		op, _ := json.Marshal(map[string]string{"content": content, "path": path, "encoding": "utf-8"})
		ndjson += `{"key":"file","value":` + string(op) + "}\n"
	}
	resp, err = http.Post(endpoint+"/api/"+repoType+"s/"+org+"/"+name+"/commit/main", "application/x-ndjson", strings.NewReader(ndjson))
	if err != nil {
		t.Fatalf("commit %s/%s: %v", org, name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("commit %s/%s: %d %s", org, name, resp.StatusCode, body)
	}
}

// canonicalJSON re-encodes JSON with sorted keys so two documents compare structurally.
func canonicalJSON(t *testing.T, data string) string {
	t.Helper()
	var v any
	if err := json.Unmarshal([]byte(data), &v); err != nil {
		t.Fatalf("invalid JSON %q: %v", data, err)
	}
	out, _ := json.Marshal(v)
	return string(out)
}

func getTagsByType(t *testing.T, endpoint, repoType string) (int, string) {
	t.Helper()
	resp, err := http.Get(endpoint + "/api/" + repoType + "-tags-by-type")
	if err != nil {
		t.Fatalf("GET %s-tags-by-type: %v", repoType, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if ct := resp.Header.Get("Content-Type"); resp.StatusCode == http.StatusOK && !strings.HasPrefix(ct, "application/json") {
		t.Errorf("%s: Content-Type=%q, want application/json", repoType, ct)
	}
	return resp.StatusCode, string(body)
}

// TestHandleTagsByType pins the HF category maps built from local card metadata: prefixed ids
// where HF uses them, taxonomy labels and subTypes, dedup across repositories, id ordering,
// unknown tags under other, and models/datasets staying distinct.
func TestHandleTagsByType(t *testing.T) {
	server, _ := setupTestServer(t)
	endpoint := server.URL

	seedRepoFiles(t, endpoint, "model", "org", "model-a", map[string]string{
		"README.md":   "---\nlanguage:\n- en\n- zh\nlicense: mit\npipeline_tag: text-classification\nlibrary_name: transformers\ndatasets:\n- imdb\ntags:\n- pytorch\n- safetensors\n- custom-thing\n- arxiv:1234.56789\n- dataset:org/ds\n- region:us\n- base_model:finetune:org/base\n---\n# A\n",
		"config.json": `{"model_type":"llama","quantization_config":{"quant_method":"fp8"}}`,
	})
	seedRepoFiles(t, endpoint, "model", "org", "model-b", map[string]string{
		"README.md": "---\nlanguage: en\nlicense: mit\npipeline_tag: my-pipeline\nlibrary_name: my-lib\ntags:\n- pytorch\n---\n# B\n",
	})
	seedRepoFiles(t, endpoint, "model", "org", "model-empty", nil)
	seedRepoFiles(t, endpoint, "dataset", "org", "ds", map[string]string{
		"README.md": "---\nlanguage: en\nlicense: apache-2.0\ntask_categories:\n- text-classification\n- custom-task\ntask_ids:\n- sentiment-classification\nsize_categories:\n- n<1K\n- 10K<n<100K\ntags:\n- synthetic\n- format:parquet\n- modality:text\n- library:datasets\n- arxiv:1234.56789\n---\n# DS\n",
	})

	wantModels := `{
		"region": [{"id":"region:us","label":"us","type":"region"}],
		"library": [
			{"id":"my-lib","label":"my-lib","type":"library"},
			{"id":"pytorch","label":"PyTorch","type":"library"},
			{"id":"safetensors","label":"Safetensors","type":"library"},
			{"id":"transformers","label":"Transformers","type":"library"}],
		"other": [
			{"id":"custom-thing","label":"custom-thing","type":"other","clickable":true},
			{"id":"fp8","label":"fp8","type":"other","clickable":true},
			{"id":"llama","label":"llama","type":"other","clickable":true}],
		"license": [{"id":"license:mit","label":"mit","type":"license"}],
		"language": [{"id":"en","label":"en","type":"language"},{"id":"zh","label":"zh","type":"language"}],
		"deploy": [],
		"dataset": [{"id":"dataset:imdb","label":"imdb","type":"dataset"},{"id":"dataset:org/ds","label":"org/ds","type":"dataset"}],
		"bucket": [],
		"pipeline_tag": [
			{"id":"my-pipeline","label":"my-pipeline","type":"pipeline_tag"},
			{"id":"text-classification","label":"Text Classification","type":"pipeline_tag","subType":"nlp"}]
	}`
	wantDatasets := `{
		"library": [{"id":"library:datasets","label":"datasets","type":"library"}],
		"license": [{"id":"license:apache-2.0","label":"apache-2.0","type":"license"}],
		"language": [{"id":"language:en","label":"en","type":"language"}],
		"other": [{"id":"synthetic","label":"synthetic","type":"other","clickable":true}],
		"task_ids": [{"id":"task_ids:sentiment-classification","label":"sentiment-classification","type":"task_ids"}],
		"task_categories": [
			{"id":"task_categories:custom-task","label":"custom-task","type":"task_categories"},
			{"id":"task_categories:text-classification","label":"text-classification","type":"task_categories","subType":"nlp"}],
		"size_categories": [
			{"id":"size_categories:10K<n<100K","label":"10K - 100K","type":"size_categories"},
			{"id":"size_categories:n<1K","label":"< 1K","type":"size_categories"}],
		"format": [{"id":"format:parquet","label":"parquet","type":"format"}],
		"modality": [{"id":"modality:text","label":"text","type":"modality"}],
		"benchmark": []
	}`

	for repoType, want := range map[string]string{"models": wantModels, "datasets": wantDatasets} {
		code, body := getTagsByType(t, endpoint, repoType)
		if code != http.StatusOK {
			t.Fatalf("%s: status=%d body=%s", repoType, code, body)
		}
		if got, want := canonicalJSON(t, body), canonicalJSON(t, want); got != want {
			t.Errorf("%s:\n got %s\nwant %s", repoType, got, want)
		}
	}

	// Items are encoded in HF field order with subType and clickable omitted when unset.
	_, body := getTagsByType(t, endpoint, "models")
	for _, item := range []string{
		`{"id":"pytorch","label":"PyTorch","type":"library"}`,
		`{"id":"text-classification","label":"Text Classification","type":"pipeline_tag","subType":"nlp"}`,
		`{"id":"custom-thing","label":"custom-thing","type":"other","clickable":true}`,
		`"deploy":[]`,
	} {
		if !strings.Contains(body, item) {
			t.Errorf("models body lacks %s: %s", item, body)
		}
	}
}

func TestHandleTagsByTypeEmpty(t *testing.T) {
	server, _ := setupTestServer(t)
	want := map[string]string{
		"models":   `{"region":[],"library":[],"other":[],"license":[],"language":[],"deploy":[],"dataset":[],"bucket":[],"pipeline_tag":[]}`,
		"datasets": `{"library":[],"license":[],"language":[],"other":[],"task_ids":[],"task_categories":[],"size_categories":[],"format":[],"modality":[],"benchmark":[]}`,
	}
	for repoType, want := range want {
		code, body := getTagsByType(t, server.URL, repoType)
		if code != http.StatusOK || canonicalJSON(t, body) != canonicalJSON(t, want) {
			t.Errorf("%s: status=%d body=%s, want 200 %s", repoType, code, body, want)
		}
	}
	if code, _ := getTagsByType(t, server.URL, "spaces"); code != http.StatusNotFound {
		t.Errorf("spaces-tags-by-type status=%d, want 404", code)
	}
}

// TestHandleTagsByTypePermission pins the list permission contract: OperationListRepos on the
// repo type, and a denial answers 403 before any repository metadata is read.
func TestHandleTagsByTypePermission(t *testing.T) {
	server, dataDir := setupTestServer(t)
	seedRepoFiles(t, server.URL, "model", "org", "m", map[string]string{"README.md": "---\ntags:\n- hidden-tag\nlicense: mit\n---\n"})
	seedRepoFiles(t, server.URL, "dataset", "org", "d", map[string]string{"README.md": "---\ntags:\n- hidden-tag\nlicense: mit\n---\n"})

	for _, repoType := range []string{"models", "datasets"} {
		for _, allow := range []bool{false, true} {
			t.Run(repoType+"/allow="+map[bool]string{false: "false", true: "true"}[allow], func(t *testing.T) {
				var gotOp permission.Operation
				var gotRepo string
				var gotCtx permission.Context
				calls := 0
				hook := func(ctx context.Context, op permission.Operation, repoName string, opCtx permission.Context) (bool, error) {
					calls++
					gotOp, gotRepo, gotCtx = op, repoName, opCtx
					return allow, nil
				}
				handler := NewHandler(
					WithStorage(storage.NewStorage(storage.WithRootDir(dataDir))),
					WithPermissionHookFunc(hook),
				)
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/"+repoType+"-tags-by-type", nil))
				body := response.Body.String()
				if calls != 1 || gotOp != permission.OperationListRepos || gotRepo != repoType || gotCtx != (permission.Context{}) {
					t.Errorf("hook calls=%d, op=%s, repo=%q, ctx=%+v; want 1, list_repos, %q, empty", calls, gotOp, gotRepo, gotCtx, repoType)
				}
				if !strings.HasPrefix(response.Header().Get("Content-Type"), "application/json") || !json.Valid(response.Body.Bytes()) {
					t.Errorf("expected JSON response, got headers=%v body=%s", response.Header(), body)
				}
				if allow {
					if response.Code != http.StatusOK || !strings.Contains(body, `"hidden-tag"`) || !strings.Contains(body, `"license:mit"`) {
						t.Errorf("status=%d, want 200 listing hidden-tag and license:mit; body=%s", response.Code, body)
					}
					return
				}
				if response.Code != http.StatusForbidden || strings.Contains(body, "hidden-tag") || strings.Contains(body, "license:mit") {
					t.Errorf("status=%d, want 403 with no tags disclosed; body=%s", response.Code, body)
				}
			})
		}
	}
}
