package netutil

import (
	"net/http"
)

type fixHFMirrorRoundTripper struct {
	base http.RoundTripper
}

func newFixHFMirrorRoundTripper(base http.RoundTripper) *fixHFMirrorRoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &fixHFMirrorRoundTripper{base: base}
}

func (h *fixHFMirrorRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// TODO(@wzshiming): This is a hack to workaround the issue in hf-mirror.org.
	// hf-mirror.org is a mirror of huggingface.co and hf.co.
	// It has bug about content replacement, which causes issues when git-lfs tries to download objects from huggingface.co through the mirror.
	// To workaround this issue, we directly send requests to hf.co when the host is cas-bridge.xethub.hf-mirror.org.
	// This is a hack and should be removed once the issue in hf-mirror.org is fixed.
	if req.URL.Host == "cas-bridge.xethub.hf-mirror.org" {
		req.Host = "cas-bridge.xethub.hf.co"
		req.URL.Host = req.Host

	}

	resp, err := h.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
