package hf_test

import (
	"strings"
	"testing"

	"github.com/wzshiming/hfd/pkg/hf"
)

func TestReadme_RepoTags_EvalResults(t *testing.T) {
	readme := "---\n" +
		"license: mit\n" +
		"library_name: transformers\n" +
		"model-index:\n" +
		"  - name: MyModel\n" +
		"    results:\n" +
		"      - task:\n" +
		"          type: text-generation\n" +
		"        dataset:\n" +
		"          type: benchmarks\n" +
		"          name: MMLU\n" +
		"        metrics:\n" +
		"          - type: acc\n" +
		"            value: 0.85\n" +
		"---\n" +
		"# Model\n"

	rm, err := hf.ParseReadme(strings.NewReader(readme))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tags := rm.Tags()
	found := false
	for _, tag := range tags {
		if tag == "eval-results" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'eval-results' tag, got %v", tags)
	}
}

func TestReadme_RepoTags_NoEvalResults(t *testing.T) {
	readme := "---\n" +
		"license: mit\n" +
		"library_name: transformers\n" +
		"---\n" +
		"# Model\n"

	rm, err := hf.ParseReadme(strings.NewReader(readme))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tags := rm.Tags()
	for _, tag := range tags {
		if tag == "eval-results" {
			t.Errorf("unexpected 'eval-results' tag when model-index is absent: %v", tags)
		}
	}
}

func TestReadme_RepoTags_AllSources(t *testing.T) {
	readme := "---\n" +
		"license: mit\n" +
		"language:\n  - en\n" +
		"library_name: transformers\n" +
		"pipeline_tag: text-generation\n" +
		"tags:\n  - pytorch\n" +
		"model-index:\n" +
		"  - name: MyModel\n" +
		"    results: []\n" +
		"---\n" +
		"# My Model\n\n" +
		"See https://arxiv.org/abs/2310.06825 for details.\n"

	rm, err := hf.ParseReadme(strings.NewReader(readme))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tags := rm.Tags()
	wantTags := map[string]bool{
		"pytorch":          true,
		"en":               true,
		"license:mit":      true,
		"text-generation":  true,
		"transformers":     true,
		"arxiv:2310.06825": true,
		"eval-results":     true,
	}
	if len(tags) != len(wantTags) {
		t.Fatalf("expected %d tags %v, got %d: %v", len(wantTags), wantTags, len(tags), tags)
	}
	for _, tag := range tags {
		if !wantTags[tag] {
			t.Errorf("unexpected tag %q", tag)
		}
	}
}
