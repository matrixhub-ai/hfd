// Package hf provides types and utilities for parsing HuggingFace repository card metadata.
//
// HuggingFace model, dataset, and space repos may include a README.md with a YAML
// front matter block (between "---" delimiters) that contains structured metadata
// known as card data. This package implements parsing of that metadata.
//
// References:
//   - https://huggingface.co/docs/hub/en/model-cards
//   - https://huggingface.co/docs/hub/en/datasets-cards
//   - https://github.com/huggingface/hub-docs/blob/main/modelcard.md
package hf

import (
	"bytes"
	"fmt"
	"regexp"

	"gopkg.in/yaml.v3"
)

// reArxiv matches arxiv.org abstract/PDF URLs and inline arXiv references,
// capturing the paper ID. Supports both modern IDs (e.g. "2310.06825") and
// legacy subject-class IDs (e.g. "math/0601001", "hep-th/9901001").
var reArxiv = regexp.MustCompile(`(?i)(?:arxiv\.org/(?:abs|pdf)/|arXiv:)((?:[a-zA-Z.-]+/\d{7}|\d{4}\.\d{4,5}))`)

// StringOrSlice is a helper type that can be unmarshalled from either a YAML
// scalar string or a sequence of strings, matching how HuggingFace card fields
// like "language", "license", and "base_model" are written in practice.
type StringOrSlice []string

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (s *StringOrSlice) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		*s = StringOrSlice{value.Value}
		return nil
	case yaml.SequenceNode:
		var ss []string
		if err := value.Decode(&ss); err != nil {
			return err
		}
		*s = ss
		return nil
	default:
		return fmt.Errorf("cannot unmarshal %v into StringOrSlice", value.Tag)
	}
}

// EvalResultMetric holds one metric entry inside a model-index result.
type EvalResultMetric struct {
	Type        string `yaml:"type"`
	Value       any    `yaml:"value"`
	Name        string `yaml:"name,omitempty"`
	Config      string `yaml:"config,omitempty"`
	VerifyToken string `yaml:"verifyToken,omitempty"`
}

// EvalResultDataset holds the dataset reference inside a model-index result.
type EvalResultDataset struct {
	Type     string         `yaml:"type"`
	Name     string         `yaml:"name"`
	Config   string         `yaml:"config,omitempty"`
	Split    string         `yaml:"split,omitempty"`
	Revision string         `yaml:"revision,omitempty"`
	Args     map[string]any `yaml:"args,omitempty"`
}

// EvalResultTask holds the task type inside a model-index result.
type EvalResultTask struct {
	Type string `yaml:"type"`
	Name string `yaml:"name,omitempty"`
}

// EvalResultSource holds the source attribution for evaluation results.
type EvalResultSource struct {
	Name string `yaml:"name,omitempty"`
	URL  string `yaml:"url,omitempty"`
}

// EvalResult holds one structured evaluation result entry from the model-index.
type EvalResult struct {
	Task    EvalResultTask     `yaml:"task"`
	Dataset EvalResultDataset  `yaml:"dataset"`
	Metrics []EvalResultMetric `yaml:"metrics,omitempty"`
	Source  *EvalResultSource  `yaml:"source,omitempty"`
}

// ModelIndex represents a single entry in the model-index section of a model card.
type ModelIndex struct {
	Name    string       `yaml:"name"`
	Results []EvalResult `yaml:"results,omitempty"`
}

// Card represents the parsed YAML front matter of a HuggingFace card (README.md).
//
// Fields marked as "common" apply to models, datasets, and spaces. Fields marked
// "model", "dataset", or "space" are specific to that repo type. Unknown fields
// from the front matter are preserved in Extra for round-trip fidelity.
//
// References:
//   - Model card spec: https://github.com/huggingface/hub-docs/blob/main/modelcard.md
//   - Dataset card spec: https://github.com/huggingface/hub-docs/blob/main/datasetcard.md
//   - Space card spec: https://huggingface.co/docs/hub/spaces-config-reference
type Card struct {
	// --- Common fields ---

	// Language(s) of the model/dataset. ISO 639-1/2/3 codes or special values
	// like "code" or "multilingual". May be a single string or a list.
	Language StringOrSlice `yaml:"language,omitempty"`

	// License identifier (e.g. "apache-2.0", "mit"). Use "other" for custom licenses.
	License StringOrSlice `yaml:"license,omitempty"`

	// LicenseName is the name of a custom license when License is "other".
	LicenseName string `yaml:"license_name,omitempty"`

	// LicenseLink is a URL or repo-relative path to the custom license text.
	LicenseLink string `yaml:"license_link,omitempty"`

	// Tags is the list of free-form tags used for filtering/discovery on the Hub.
	Tags []string `yaml:"tags,omitempty"`

	// Datasets lists datasets used to train/build this model or referenced by a space.
	Datasets []string `yaml:"datasets,omitempty"`

	// --- Model-specific fields ---

	// LibraryName identifies the ML framework/library (e.g. "transformers", "keras").
	LibraryName string `yaml:"library_name,omitempty"`

	// PipelineTag specifies the ML task type (e.g. "text-classification").
	PipelineTag string `yaml:"pipeline_tag,omitempty"`

	// BaseModel is the ID (or list of IDs) of the upstream model(s) this was derived from.
	BaseModel StringOrSlice `yaml:"base_model,omitempty"`

	// BaseModelRelation describes how this model relates to the base model.
	// One of: "adapter", "merge", "quantized", "finetune".
	BaseModelRelation string `yaml:"base_model_relation,omitempty"`

	// Metrics lists metric names used to evaluate the model (e.g. "wer", "accuracy").
	Metrics []string `yaml:"metrics,omitempty"`

	// NewVersion points to the Hub ID of a newer version of this model.
	NewVersion string `yaml:"new_version,omitempty"`

	// ModelIndex holds structured evaluation results (the model-index block).
	ModelIndex []ModelIndex `yaml:"model-index,omitempty"`

	// --- Dataset-specific fields ---

	// AnnotationsCreators describes how annotations were created.
	// Values: "found", "crowdsourced", "expert-generated", "machine-generated",
	// "no-annotation", "other".
	AnnotationsCreators StringOrSlice `yaml:"annotations_creators,omitempty"`

	// LanguageCreators describes how text data in the dataset was created.
	// Values: "found", "crowdsourced", "expert-generated", "machine-generated", "other".
	LanguageCreators StringOrSlice `yaml:"language_creators,omitempty"`

	// Multilinguality describes the multilingual nature of the dataset.
	// Values: "monolingual", "multilingual", "translation", "other".
	Multilinguality StringOrSlice `yaml:"multilinguality,omitempty"`

	// SizeCategories indicates the number of examples in the dataset.
	// Values: "n<1K", "1K<n<10K", "10K<n<100K", "100K<n<1M", etc.
	SizeCategories StringOrSlice `yaml:"size_categories,omitempty"`

	// SourceDatasets indicates whether the dataset is original or derived.
	// Values: "original", "extended".
	SourceDatasets []string `yaml:"source_datasets,omitempty"`

	// TaskCategories lists the broad task categories this dataset supports.
	TaskCategories StringOrSlice `yaml:"task_categories,omitempty"`

	// TaskIDs lists the specific tasks this dataset supports.
	TaskIDs StringOrSlice `yaml:"task_ids,omitempty"`

	// PrettyName is a human-readable display name for the dataset.
	PrettyName string `yaml:"pretty_name,omitempty"`

	// ConfigNames lists available dataset configuration names.
	ConfigNames StringOrSlice `yaml:"config_names,omitempty"`

	// PapersWithCodeID is the dataset identifier on PapersWithCode.
	PapersWithCodeID string `yaml:"paperswithcode_id,omitempty"`

	// --- Space-specific fields ---

	// Title is the display title of the Space.
	Title string `yaml:"title,omitempty"`

	// SDK specifies the Space SDK ("gradio", "streamlit", "docker", "static").
	SDK string `yaml:"sdk,omitempty"`

	// SDKVersion is the version of the Space SDK.
	SDKVersion string `yaml:"sdk_version,omitempty"`

	// AppFile is the path to the main application file within the Space repo.
	AppFile string `yaml:"app_file,omitempty"`

	// AppPort is the port number the Space application listens on (Docker spaces).
	AppPort int `yaml:"app_port,omitempty"`

	// DuplicatedFrom is the ID of the original Space this was duplicated from.
	DuplicatedFrom string `yaml:"duplicated_from,omitempty"`

	// Models lists model IDs related to this Space.
	Models []string `yaml:"models,omitempty"`

	// Extra holds any YAML front matter fields not explicitly mapped above, for
	// forward-compatibility with new fields added to the spec.
	Extra map[string]any `yaml:",inline"`
}

// extractFrontMatterAndBody splits a README into the YAML front matter bytes and
// the body bytes that follow. Either returned slice may be nil.
func extractFrontMatterAndBody(data []byte) (fm []byte, body []byte) {
	// Front matter must begin at the very start of the file.
	if !bytes.HasPrefix(data, []byte("---\n")) && !bytes.HasPrefix(data, []byte("---\r\n")) {
		return nil, data
	}

	// Advance past the opening "---" line entirely (skip "---" + the line ending).
	_, after, ok := bytes.Cut(data, []byte{'\n'})
	if !ok {
		return nil, data
	}

	// Find the closing "---" line (must appear as "\n---" at start of a line).
	end := bytes.Index(after, []byte("\n---"))
	if end < 0 {
		return nil, data
	}

	fm = after[:end+1]

	// Body starts after the closing "---" line.
	rest := after[end+1:] // starts at "\n---"
	_, after0, ok := bytes.Cut(rest, []byte{'\n'})
	if ok {
		body = after0
	}
	return fm, body
}

// extractArxivIDs scans text for arxiv.org URLs and inline arXiv references and
// returns the unique paper IDs found (e.g. ["2310.06825"]).
func extractArxivIDs(body []byte) []string {
	if len(body) == 0 {
		return nil
	}
	matches := reArxiv.FindAllSubmatch(body, -1)
	if len(matches) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(matches))
	var ids []string
	for _, m := range matches {
		id := string(m[1])
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	return ids
}
