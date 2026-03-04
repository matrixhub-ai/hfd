package hf_test

import (
	"strings"
	"testing"

	"github.com/wzshiming/hfd/pkg/hf"
)

func TestParseConfigData_ModelType(t *testing.T) {
	cfg, err := hf.ParseConfigData(strings.NewReader(`{"model_type": "bert"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ModelType != "bert" {
		t.Errorf("expected ModelType %q, got %q", "bert", cfg.ModelType)
	}
}

func TestParseConfigData_UnknownFieldsIgnored(t *testing.T) {
	cfg, err := hf.ParseConfigData(strings.NewReader(`{"model_type":"gpt2","hidden_size":768,"vocab_size":50257}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ModelType != "gpt2" {
		t.Errorf("expected ModelType %q, got %q", "gpt2", cfg.ModelType)
	}
}

func TestParseConfigData_NoModelType(t *testing.T) {
	cfg, err := hf.ParseConfigData(strings.NewReader(`{"hidden_size":768}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ModelType != "" {
		t.Errorf("expected empty ModelType, got %q", cfg.ModelType)
	}
	if tags := cfg.Tags(); len(tags) != 0 {
		t.Errorf("expected no tags, got %v", tags)
	}
}

func TestConfigData_Tags_Single(t *testing.T) {
	cfg := &hf.ConfigData{ModelType: "llama"}
	tags := cfg.Tags()
	if len(tags) != 1 || tags[0] != "llama" {
		t.Errorf("expected [llama], got %v", tags)
	}
}

func TestConfigData_Tags_Empty(t *testing.T) {
	cfg := &hf.ConfigData{}
	if tags := cfg.Tags(); len(tags) != 0 {
		t.Errorf("expected no tags for empty config, got %v", tags)
	}
}

func TestParseConfigData_NoQuantizationConfig(t *testing.T) {
	cfg, err := hf.ParseConfigData(strings.NewReader(`{"model_type":"bert"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.QuantizationConfig != nil {
		t.Errorf("expected nil QuantizationConfig, got %v", cfg.QuantizationConfig)
	}
	tags := cfg.Tags()
	if len(tags) != 1 || tags[0] != "bert" {
		t.Errorf("expected [bert], got %v", tags)
	}
}

func TestParseConfigData_EmptyQuantType(t *testing.T) {
	cfg, err := hf.ParseConfigData(strings.NewReader(`{"model_type":"bert","quantization_config":{}}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tags := cfg.Tags()
	// quant_type is empty, so only model_type should appear
	if len(tags) != 1 || tags[0] != "bert" {
		t.Errorf("expected [bert], got %v", tags)
	}
}

func TestParseConfigData_InvalidJSON(t *testing.T) {
	_, err := hf.ParseConfigData(strings.NewReader(`not json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestParseConfigData_TrustRemoteCodeFalse(t *testing.T) {
	cfg, err := hf.ParseConfigData(strings.NewReader(`{"model_type":"bert","trust_remote_code":false}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tags := cfg.Tags()
	for _, tag := range tags {
		if tag == "custom_code" {
			t.Errorf("unexpected 'custom_code' tag when trust_remote_code is false")
		}
	}
}

func TestParseConfigData_TrustRemoteCodeAbsent(t *testing.T) {
	cfg, err := hf.ParseConfigData(strings.NewReader(`{"model_type":"bert"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tags := cfg.Tags()
	for _, tag := range tags {
		if tag == "custom_code" {
			t.Errorf("unexpected 'custom_code' tag when trust_remote_code is absent")
		}
	}
}
