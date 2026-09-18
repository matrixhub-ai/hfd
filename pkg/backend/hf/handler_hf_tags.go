package hf

import (
	"context"
	"maps"
	"net/http"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-git/go-billy/v6"
	"github.com/gorilla/mux"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// tagCategories lists the keys of the HF tags-by-type response for each repo type.
var tagCategories = map[string][]string{
	"models":   {"region", "library", "other", "license", "language", "deploy", "dataset", "bucket", "pipeline_tag"},
	"datasets": {"library", "license", "language", "other", "task_ids", "task_categories", "size_categories", "format", "modality", "benchmark"},
}

// pipelineTaxonomy is the HF task taxonomy shared by model pipeline tags and dataset task categories.
var pipelineTaxonomy = map[string]struct{ label, subType string }{
	"text-classification":            {"Text Classification", "nlp"},
	"token-classification":           {"Token Classification", "nlp"},
	"table-question-answering":       {"Table Question Answering", "nlp"},
	"question-answering":             {"Question Answering", "nlp"},
	"zero-shot-classification":       {"Zero-Shot Classification", "nlp"},
	"translation":                    {"Translation", "nlp"},
	"summarization":                  {"Summarization", "nlp"},
	"feature-extraction":             {"Feature Extraction", "nlp"},
	"text-generation":                {"Text Generation", "nlp"},
	"fill-mask":                      {"Fill-Mask", "nlp"},
	"sentence-similarity":            {"Sentence Similarity", "nlp"},
	"text-ranking":                   {"Text Ranking", "nlp"},
	"table-to-text":                  {"table-to-text", "nlp"},
	"multiple-choice":                {"multiple-choice", "nlp"},
	"text-retrieval":                 {"text-retrieval", "nlp"},
	"text-to-speech":                 {"Text-to-Speech", "audio"},
	"text-to-audio":                  {"Text-to-Audio", "audio"},
	"automatic-speech-recognition":   {"Automatic Speech Recognition", "audio"},
	"audio-to-audio":                 {"Audio-to-Audio", "audio"},
	"audio-classification":           {"Audio Classification", "audio"},
	"voice-activity-detection":       {"Voice Activity Detection", "audio"},
	"depth-estimation":               {"Depth Estimation", "cv"},
	"image-classification":           {"Image Classification", "cv"},
	"object-detection":               {"Object Detection", "cv"},
	"image-segmentation":             {"Image Segmentation", "cv"},
	"text-to-image":                  {"Text-to-Image", "cv"},
	"image-to-text":                  {"Image-to-Text", "cv"},
	"image-to-image":                 {"Image-to-Image", "cv"},
	"image-to-video":                 {"Image-to-Video", "cv"},
	"unconditional-image-generation": {"Unconditional Image Generation", "cv"},
	"video-classification":           {"Video Classification", "cv"},
	"text-to-video":                  {"Text-to-Video", "cv"},
	"zero-shot-image-classification": {"Zero-Shot Image Classification", "cv"},
	"mask-generation":                {"Mask Generation", "cv"},
	"zero-shot-object-detection":     {"Zero-Shot Object Detection", "cv"},
	"text-to-3d":                     {"Text-to-3D", "cv"},
	"image-to-3d":                    {"Image-to-3D", "cv"},
	"image-feature-extraction":       {"Image Feature Extraction", "cv"},
	"keypoint-detection":             {"Keypoint Detection", "cv"},
	"video-to-video":                 {"Video-to-Video", "cv"},
	"audio-text-to-text":             {"Audio-Text-to-Text", "multimodal"},
	"image-text-to-text":             {"Image-Text-to-Text", "multimodal"},
	"image-text-to-image":            {"Image-Text-to-Image", "multimodal"},
	"image-text-to-video":            {"Image-Text-to-Video", "multimodal"},
	"visual-question-answering":      {"Visual Question Answering", "multimodal"},
	"document-question-answering":    {"Document Question Answering", "multimodal"},
	"video-text-to-text":             {"Video-Text-to-Text", "multimodal"},
	"visual-document-retrieval":      {"Visual Document Retrieval", "multimodal"},
	"any-to-any":                     {"Any-to-Any", "multimodal"},
	"reinforcement-learning":         {"Reinforcement Learning", "rl"},
	"robotics":                       {"Robotics", "rl"},
	"tabular-classification":         {"Tabular Classification", "tabular"},
	"tabular-regression":             {"Tabular Regression", "tabular"},
	"time-series-forecasting":        {"Time Series Forecasting", "tabular"},
	"tabular-to-text":                {"tabular-to-text", "tabular"},
	"graph-ml":                       {"Graph Machine Learning", "other"},
}

// libraryLabels maps the HF model library tags to their display labels.
var libraryLabels = map[string]string{
	"pytorch": "PyTorch", "tf": "TensorFlow", "jax": "JAX", "safetensors": "Safetensors",
	"transformers": "Transformers", "peft": "PEFT", "gguf": "GGUF", "tensorboard": "TensorBoard",
	"diffusers": "Diffusers", "onnx": "ONNX", "stable-baselines3": "stable-baselines3",
	"sentence-transformers": "sentence-transformers", "mlx": "MLX", "ml-agents": "ml-agents",
	"keras": "Keras", "tf-keras": "TF-Keras", "joblib": "Joblib", "transformers.js": "Transformers.js",
	"adapter-transformers": "Adapters", "timm": "timm", "openvino": "OpenVINO", "setfit": "setfit",
	"sample-factory": "sample-factory", "coreml": "Core ML", "tflite": "LiteRT", "nemo": "NeMo",
	"flair": "Flair", "fastai": "fastai", "espnet": "ESPnet", "rust": "Rust", "sklearn": "Scikit-learn",
	"bertopic": "BERTopic", "spacy": "spaCy", "fasttext": "fastText", "open_clip": "OpenCLIP",
	"executorch": "ExecuTorch", "keras-hub": "KerasHub", "asteroid": "Asteroid", "speechbrain": "speechbrain",
	"allennlp": "AllenNLP", "llamafile": "llamafile", "paddlepaddle": "PaddlePaddle", "PaddleOCR": "PaddleOCR",
	"fairseq": "Fairseq", "stanza": "Stanza", "pyannote-audio": "pyannote.audio", "optimum_habana": "Habana",
	"span-marker": "SpanMarker", "optimum_graphcore": "Graphcore", "paddlenlp": "paddlenlp",
	"unity-sentis": "unity-sentis", "dduf": "DDUF", "univa": "univa",
}

// handleTagsByType serves the models/datasets-tags-by-type category maps from local repository metadata.
func (h *Handler) handleTagsByType(w http.ResponseWriter, r *http.Request) {
	repoType := mux.Vars(r)["repoType"]
	if !h.checkPermission(w, r, permission.OperationListRepos, repoType, permission.Context{}) {
		return
	}
	responseJSON(w, buildTagsByType(r.Context(), h.storage.RepositoriesFS(), repoType), http.StatusOK)
}

// buildTagsByType collects the facets of every repository of repoType, deduplicated by id and
// sorted by id within each category; every category key is present, possibly empty.
func buildTagsByType(ctx context.Context, fs billy.Filesystem, repoType string) map[string][]tagFacet {
	isModel := repoType == "models"
	baseDir := "/"
	if !isModel {
		baseDir = filepath.Join("/", repoType)
	}
	facets := map[string]map[string]tagFacet{}
	for _, root := range repoRoots(fs, baseDir, isModel, "") {
		_ = repository.Walk(ctx, fs, root, func(path string) error {
			repo, err := repository.Open(fs, path)
			if err != nil {
				return nil
			}
			classifyRepoTags(repoType, collectRepoMetadata(repo, repo.DefaultBranch()), func(category, value string) {
				if value == "" {
					return
				}
				f := tagFacetFor(repoType, category, value)
				if facets[category] == nil {
					facets[category] = map[string]tagFacet{}
				}
				facets[category][f.ID] = f
			})
			return nil
		})
	}
	result := make(map[string][]tagFacet, len(tagCategories[repoType]))
	for _, category := range tagCategories[repoType] {
		items := slices.AppendSeq(make([]tagFacet, 0, len(facets[category])), maps.Values(facets[category]))
		slices.SortFunc(items, func(a, b tagFacet) int { return strings.Compare(a.ID, b.ID) })
		result[category] = items
	}
	return result
}

// classifyRepoTags reports the (category, value) facets one repository contributes. Card fields
// give a tag its provenance; a prefixed tag counts only when the category really prefixes its ids.
func classifyRepoTags(repoType string, meta repoMetadata, add func(category, value string)) {
	isModel := repoType == "models"
	card := meta.card
	for _, tag := range meta.tags {
		_, isPipeline := pipelineTaxonomy[tag]
		_, isLibrary := libraryLabels[tag]
		prefix, value, prefixed := strings.Cut(tag, ":")
		switch {
		case slices.Contains(card.Language, tag):
			add("language", tag)
		case isModel && (tag == card.PipelineTag || isPipeline):
			add("pipeline_tag", tag)
		case tag == card.LibraryName || (isModel && isLibrary):
			add("library", tag)
		case isModel && tag == "endpoints_compatible":
			add("deploy", tag)
		case prefixed:
			if slices.Contains(tagCategories[repoType], prefix) && tagFacetFor(repoType, prefix, value).ID == tag {
				add(prefix, value)
			}
		default:
			add("other", tag)
		}
	}
	if isModel {
		for _, d := range card.Datasets {
			add("dataset", d)
		}
		return
	}
	for _, t := range card.TaskCategories {
		add("task_categories", t)
	}
	for _, t := range card.TaskIDs {
		add("task_ids", t)
	}
	for _, s := range card.SizeCategories {
		add("size_categories", s)
	}
}

// tagFacetFor builds the HF-shaped facet: ids are "category:value" except for other, endpoints_compatible
// and, on models, library, pipeline_tag and language, which HF keeps bare.
func tagFacetFor(repoType, category, value string) tagFacet {
	f := tagFacet{ID: category + ":" + value, Label: value, Type: category}
	switch category {
	case "other":
		f.ID, f.Clickable = value, true
	case "deploy":
		if value == "endpoints_compatible" {
			f.ID, f.Label, f.Clickable = value, "Inference Endpoints", true
		}
	case "pipeline_tag":
		f.ID = value
		if p, ok := pipelineTaxonomy[value]; ok {
			f.Label, f.SubType = p.label, p.subType
		}
	case "task_categories":
		f.SubType = pipelineTaxonomy[value].subType
	case "library":
		if repoType == "models" {
			f.ID = value
			if label, ok := libraryLabels[value]; ok {
				f.Label = label
			}
		}
	case "language":
		if repoType == "models" {
			f.ID = value
		}
	case "size_categories":
		switch {
		case strings.HasPrefix(value, "n<"):
			f.Label = "< " + value[2:]
		case strings.HasPrefix(value, "n>"):
			f.Label = "> " + value[2:]
		default:
			f.Label = strings.Replace(value, "<n<", " - ", 1)
		}
	}
	return f
}
