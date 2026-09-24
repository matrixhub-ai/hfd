package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// Fixture names the public repositories the cases are built around.
type Fixture struct {
	Model   string `json:"model"`
	Dataset string `json:"dataset"`
	Space   string `json:"space"`
	LFSFile string `json:"lfsFile"`
}

var defaultFixture = Fixture{
	Model:   "wzshiming/gpt2",
	Dataset: "wzshiming/fixtures_image_utils",
	Space:   "wzshiming/hello_world",
	LFSFile: "64-8bits.tflite",
}

// Request is the replayable identity of one recorded case.
type Request struct {
	Name    string            `json:"name"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body,omitempty"`
	File    bool              `json:"file"`
}

func splitRepo(kind, id string) (string, string, error) {
	ns, name, ok := strings.Cut(id, "/")
	if !ok || ns == "" || name == "" || strings.Contains(name, "/") {
		return "", "", fmt.Errorf("%s %q must be namespace/name", kind, id)
	}
	return ns, name, nil
}

func escapePath(segments ...string) string {
	var sb strings.Builder
	for _, seg := range segments {
		for _, part := range strings.Split(seg, "/") {
			sb.WriteByte('/')
			sb.WriteString(url.PathEscape(part))
		}
	}
	return sb.String()
}

func pathsInfoBody(paths ...string) string {
	body, _ := json.Marshal(struct {
		Paths  []string `json:"paths"`
		Expand bool     `json:"expand"`
	}{paths, true})
	return string(body)
}

// buildCases returns the case inventory for f sorted by name.
func buildCases(f Fixture) ([]Request, error) {
	if f.LFSFile == "" || strings.HasPrefix(f.LFSFile, "/") {
		return nil, fmt.Errorf("lfs file %q must be a relative path", f.LFSFile)
	}
	modelNS, modelName, err := splitRepo("model", f.Model)
	if err != nil {
		return nil, err
	}
	datasetNS, datasetName, err := splitRepo("dataset", f.Dataset)
	if err != nil {
		return nil, err
	}
	spaceNS, spaceName, err := splitRepo("space", f.Space)
	if err != nil {
		return nil, err
	}
	model := escapePath("api", "models", modelNS, modelName)
	dataset := escapePath("api", "datasets", datasetNS, datasetName)
	space := escapePath("api", "spaces", spaceNS, spaceName)
	modelFile := escapePath(modelNS, modelName, "resolve", "main")
	datasetFile := escapePath("datasets", datasetNS, datasetName, "resolve", "main")
	spaceFile := escapePath("spaces", spaceNS, spaceName, "resolve", "main")
	listQuery := url.Values{"author": {modelNS}, "search": {modelName}, "limit": {"5"}}

	var cases []Request
	api := func(name, path string) {
		cases = append(cases, Request{Name: name, Method: http.MethodGet, Path: path, Headers: map[string]string{"Accept": "application/json"}})
	}
	post := func(name, path, body string) {
		cases = append(cases, Request{Name: name, Method: http.MethodPost, Path: path, Body: body,
			Headers: map[string]string{"Accept": "application/json", "Content-Type": "application/json"}})
	}
	file := func(name, method, path string) {
		cases = append(cases, Request{Name: name, Method: method, Path: path, File: true})
	}

	api("agent-harnesses", "/api/agent-harnesses")
	api("whoami-v2", "/api/whoami-v2")

	api("models.info", model)
	api("models.list", "/api/models?"+listQuery.Encode())
	api("models.notfound", escapePath("api", "models", modelNS, "does-not-exist"))
	api("models.revision", model+"/revision/main")
	api("models.revision.notfound", model+"/revision/does-not-exist")
	api("models.tree", model+"/tree/main")
	api("models.tree.recursive", model+"/tree/main?"+url.Values{"recursive": {"true"}, "expand": {"true"}}.Encode())
	api("models.tree.notfound", model+"/tree/main/does-not-exist")
	api("models.treesize", model+"/treesize/main")
	api("models.refs", model+"/refs")
	api("models.commits", model+"/commits/main")
	api("models.commits.limit", model+"/commits/main?limit=2")
	post("models.paths-info", model+"/paths-info/main", pathsInfoBody("config.json", f.LFSFile, "does-not-exist.txt"))
	api("models.xet-read-token", model+"/xet-read-token/main")
	file("models.resolve.config", http.MethodGet, modelFile+"/config.json")
	file("models.resolve.config.head", http.MethodHead, modelFile+"/config.json")
	file("models.resolve.lfs", http.MethodGet, modelFile+escapePath(f.LFSFile))
	file("models.resolve.lfs.head", http.MethodHead, modelFile+escapePath(f.LFSFile))
	file("models.resolve.notfound", http.MethodGet, modelFile+"/does-not-exist.txt")

	api("datasets.info", dataset)
	api("datasets.tree", dataset+"/tree/main")
	api("datasets.tree.recursive", dataset+"/tree/main?"+url.Values{"recursive": {"true"}, "expand": {"true"}}.Encode())
	api("datasets.treesize", dataset+"/treesize/main")
	api("datasets.refs", dataset+"/refs")
	api("datasets.commits", dataset+"/commits/main")
	post("datasets.paths-info", dataset+"/paths-info/main", pathsInfoBody("README.md", "does-not-exist.txt"))
	file("datasets.resolve.readme.head", http.MethodHead, datasetFile+"/README.md")

	api("spaces.info", space)
	api("spaces.tree", space+"/tree/main")
	api("spaces.refs", space+"/refs")
	api("spaces.commits", space+"/commits/main")
	file("spaces.resolve.readme.head", http.MethodHead, spaceFile+"/README.md")

	sort.Slice(cases, func(i, j int) bool { return cases[i].Name < cases[j].Name })
	return cases, nil
}
