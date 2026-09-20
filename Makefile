GO_DIRS := cmd hack internal pkg test
TEST_PKGS ?= ./...
TEST_ARGS ?=

.PHONY: run
run:
	go run ./cmd/hfd

.PHONY: build
build:
	go build ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: fmt
fmt:
	gofmt -w $(GO_DIRS)

.PHONY: fmt-check
fmt-check:
	@out=$$(gofmt -l $(GO_DIRS)) || exit $$?; [ -z "$$out" ] || { echo "$$out"; exit 1; }

.PHONY: test
test:
	go test -timeout 30m $(TEST_ARGS) $(TEST_PKGS)

.PHONY: update-hf-api-status
update-hf-api-status:
	go run ./hack/update-hf-api-status
