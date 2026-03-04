package hf

import (
	"encoding/json"
	"io"
)

// QuantizationConfig holds the quantization-related fields from config.json that
// contribute to the Hub's computed tag list.
type QuantizationConfig struct {
	// QuantMethod is the quantization type identifier (e.g. "fp8", "int4", "awq").
	// It is appended as a plain tag to the repo's tag list when non-empty.
	QuantMethod string `json:"quant_method"`
}

// ConfigData holds fields extracted from a model's config.json that contribute
// to the Hub's computed tag list.
//
// The HuggingFace Hub reads config.json to derive additional tags for a repo.
// Currently the fields used are "model_type", "quantization_config.quant_method",
// and "trust_remote_code", all of which are added as plain tags.
//
// Reference: https://huggingface.co/docs/hub/en/model-cards#model-card-metadata
type ConfigData struct {
	// ModelType is the model architecture identifier (e.g. "bert", "gpt2", "llama").
	// It is appended as a plain tag to the repo's tag list.
	ModelType string `json:"model_type"`

	// QuantizationConfig holds quantization-related metadata. When present and
	// its QuantType is non-empty, the quant_type value is appended as a plain tag
	// (e.g. "fp8").
	QuantizationConfig *QuantizationConfig `json:"quantization_config"`
}

// ParseConfigData reads a config.json and extracts tag-relevant fields.
// Fields that are absent or empty result in a zero-value *ConfigData with no error.
// JSON parse errors are returned as errors.
func ParseConfigData(r io.Reader) (*ConfigData, error) {
	var cfg ConfigData
	if err := json.NewDecoder(r).Decode(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Tags returns the list of tags derived from this config.json.
// Includes model_type (if non-empty), quantization_config.quant_type (if present and non-empty),
// and "custom_code" when trust_remote_code is true.
func (c *ConfigData) Tags() []string {
	var tags []string
	if c.ModelType != "" {
		tags = append(tags, c.ModelType)
	}
	if c.QuantizationConfig != nil && c.QuantizationConfig.QuantMethod != "" {
		tags = append(tags, c.QuantizationConfig.QuantMethod)
	}
	return tags
}
