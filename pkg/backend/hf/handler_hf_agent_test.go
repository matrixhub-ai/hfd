package hf

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestHuggingFaceAgentHarnesses(t *testing.T) {
	server, _ := setupTestServer(t)

	// huggingface_hub >=1.29 fetches this anonymously during xet bootstrap.
	resp, err := http.Get(server.URL + "/api/agent-harnesses")
	if err != nil {
		t.Fatalf("Failed to request agent-harnesses: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, respBody)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Expected Content-Type application/json, got %q", ct)
	}

	var result struct {
		StandardEnvVars []string `json:"standardEnvVars"`
		Harnesses       map[string]struct {
			EnvVars map[string]string `json:"envVars"`
		} `json:"harnesses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(result.StandardEnvVars) == 0 {
		t.Error("Expected non-empty standardEnvVars")
	}
	for i, v := range result.StandardEnvVars {
		if v == "" {
			t.Errorf("standardEnvVars[%d] is empty", i)
		}
	}
	if len(result.Harnesses) == 0 {
		t.Fatal("Expected non-empty harnesses")
	}

	hasEnvVars := false
	for _, h := range result.Harnesses {
		if len(h.EnvVars) > 0 {
			hasEnvVars = true
			break
		}
	}
	if !hasEnvVars {
		t.Error("Expected at least one harness with non-empty envVars")
	}
}
