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

## LFS Policy

As on huggingface.co, files larger than 10 MiB and binary files larger than
100000 bytes must be stored with Git LFS or Xet, not as regular Git blobs:

- The Hub API's preupload routes them to LFS, so `hf` and `huggingface_hub`
  upload them there; the commit API refuses them inline with 400 and the
  Hub's message.
- A commit that adds an LFS file not yet tracked in `.gitattributes` appends
  `<path> filter=lfs diff=lfs merge=lfs -text` to it, as the Hub does.
- `git push` of such a regular blob is refused with the Hub's banner and
  `pre-receive hook declined` for every ref, through the system `git` and
  go-git alike. The system `git` runs the policy as hfd's own pre-receive
  hook through `core.hooksPath`, so hooks in a repository's `hooks/`
  directory do not run while the policy is on. That hook is written under
  the system temporary directory, which must allow executables (a `noexec`
  `TMPDIR` makes every push through the system `git` fail closed) and must
  not be cleaned while hfd runs.

`-max-regular-file-size` and `-max-binary-file-size` set the limits in bytes
(defaults 10485760 and 100000); 0 disables a rule. Embedders set
`server.Options.LFSPolicy`: nil applies the Hub's limits, `&lfs.Policy{}`
disables both rules.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.
