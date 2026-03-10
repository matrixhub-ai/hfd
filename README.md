# hfd

This is a git server and implements part of the [huggingface openapi](https://huggingface.co/spaces/huggingface/openapi).

## Testing

- Run unit tests (excluding e2e): `go test $(go list ./... | grep -v '/test/e2e$') -count=1`
- Run e2e suites with `E2E_SUITE` set to one of `git-http`, `git-ssh`, `git-lfs`, `hf-cli`, or `hf-python`, for example: `E2E_SUITE=git-http go test ./test/e2e -count=1`
