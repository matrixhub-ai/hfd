

.PHONY: run
run:
	go run ./cmd/hfd

.PHONY: update-hf-api-status
update-hf-api-status:
	go run ./hack/update-hf-api-status

HFD_URL ?= http://127.0.0.1:8080
# The default fixture's LFS file is 125 MB, above the tool's 64MiB/2m defaults.
HF_API_MAX_BODY ?= 128MiB
HF_API_TIMEOUT ?= 30m

.PHONY: hf-api-record
hf-api-record:
	go run ./hack/hf-api-diff record -max-body "$(HF_API_MAX_BODY)" -timeout "$(HF_API_TIMEOUT)"

# Target for hf-api-diff: go run ./cmd/hfd --addr 127.0.0.1:8080 --ssh-addr= --pull-mirror=https://huggingface.co --data $(mktemp -d)
.PHONY: hf-api-diff
hf-api-diff:
	go run ./hack/hf-api-diff compare -hfd-url "$(HFD_URL)" -max-body "$(HF_API_MAX_BODY)" -timeout "$(HF_API_TIMEOUT)"

# Same comparison as CI: builds hfd from this checkout, seeds the pinned fixtures and replays against it.
.PHONY: hf-api-diff-ci
hf-api-diff-ci:
	hack/hf-api-diff/ci.sh
