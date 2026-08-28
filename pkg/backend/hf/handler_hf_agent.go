package hf

import (
	"bytes"
	_ "embed"
	"net/http"
	"time"
)

// agentHarnessesJSON is a snapshot of https://huggingface.co/api/agent-harnesses,
// the agent harness registry consumed by huggingface_hub >=1.29 agent detection.
// Refresh: curl -sS https://huggingface.co/api/agent-harnesses > agent_harnesses.json
//
//go:embed agent_harnesses.json
var agentHarnessesJSON []byte

// handleAgentHarnesses serves the embedded registry. Public upstream, so no permission check.
func (h *Handler) handleAgentHarnesses(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(agentHarnessesJSON))
}
