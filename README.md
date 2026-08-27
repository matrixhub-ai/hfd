# hfd

Self-hostable Hugging Face style hub written in Go: a git server that speaks the
Hugging Face Hub API, git smart HTTP / SSH protocols, Git LFS, and the XET
content-addressable storage protocol.

This project tracks:

- [Hugging Face Hub OpenAPI](https://huggingface.co/.well-known/openapi.json)
  ([interactive viewer](https://huggingface.co/spaces/huggingface/openapi)),
  per-endpoint coverage is documented in [hf-api-status.md](hf-api-status.md)

## Current Scope

Implemented in this repository:

- Git smart protocol over HTTP and SSH (clone / fetch / push), with HTTP basic
  auth, bearer / signed tokens, SSH public key and password authentication
- Git LFS server backed by XET CAS storage (chunk-level deduplication,
  xorb / shard reconstruction)
- Hugging Face Hub API for models, datasets, and spaces: repo management,
  branches, tags, commits, refs, tree / treesize, compare, preupload, commit,
  super-squash, settings, `resolve` downloads, and xet write tokens
- Compatible with real clients: `git`, `git-lfs`, the `hf` CLI, and the Python
  `huggingface_hub` library (upload / download / snapshot, xet-enabled transfers)
- Mirror modes: pull-through cache of an upstream hub (e.g. huggingface.co)
  and push mirror to a remote hub, with TTL cache and per-file concurrency control
- Storage backends: local filesystem or S3-compatible object stores (MinIO
  etc.), with optional presigned download URLs
- Permission and receive hooks for customizable access control
- End-to-end test matrix covering git / LFS / hf CLI / Python clients over
  both filesystem and S3 backends

## Compatibility Notes

- This project aims to be a drop-in `HF_ENDPOINT` replacement for the
  supported subset of the Hub API; endpoint-by-endpoint status is tracked in
  [hf-api-status.md](hf-api-status.md).
- Behavior is validated end-to-end against official clients (`git`,
  `git-lfs`, `hf` CLI, `huggingface_hub`) in [test/e2e](test/e2e).
- Coverage is evolving with upstream API and client changes.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.
