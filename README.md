# hfd

Privately Headless HuggingFace Daemon

hfd is a Go daemon that serves Hugging Face Hub protocols from your own
infrastructure. Point `HF_ENDPOINT` at it or add it as a Git remote, and the
Hub API, Git, Git LFS, and Xet clients work against repositories you host for
supported operations.

## Why Headless

Deployment and storage stay under your control. Repositories are
operator-managed bare Git repositories, and large files are chunk-deduplicated
Xet content, kept in a local data directory or in S3-compatible object
storage, optionally with pull-through or push mirroring of another hub.

There is no web UI, and no external database service is required. Identity
comes from HTTP basic auth, bearer or signed tokens, and SSH keys;
authorization and push handling go through pluggable permission and receive
hooks.

## Protocols and Clients

| Protocol             | Clients                     |
| -------------------- | --------------------------- |
| Hugging Face Hub API | `hf` CLI, `huggingface_hub` |
| Git smart HTTP       | `git`                       |
| Git over SSH         | `git`                       |
| Git LFS              | `git-lfs`                   |
| Xet CAS              | `git-xet`, `hf_xet`         |

## Compatibility

- hfd implements a subset of the Hub API. Coverage is tracked per endpoint in
  [hf-api-status.md](hf-api-status.md) against the
  [Hugging Face Hub OpenAPI](https://huggingface.co/.well-known/openapi.json)
  ([interactive viewer](https://huggingface.co/spaces/huggingface/openapi)).
- [test/e2e](test/e2e) exercises `git`, `git-lfs`, `hf`, `huggingface_hub`, and
  `hf_xet` against local and S3 backends.
- Filesystem-backed repositories use the system `git` binary when one is
  available; otherwise, and for S3, hfd falls back to go-git.
- Coverage evolves with upstream API and client changes.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.
