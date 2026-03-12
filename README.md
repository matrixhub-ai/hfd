# hfd

Lightweight Git server that implements a large subset of the [Hugging Face Hub API](https://huggingface.co/spaces/huggingface/openapi). It serves Git over HTTP and SSH, speaks LFS, and can mirror or proxy Hugging Face repositories.

## Quick start

```bash
# run a local instance
go run ./cmd/hfd -data ./data

# push via Git (HTTP)
git clone http://localhost:8080/example/my-model
cd my-model
echo "hello" > README.md
git add README.md && git commit -m "init"
git push origin main

# push via HF CLI (requires hf installed)
HF_ENDPOINT=http://localhost:8080 HF_HUB_DISABLE_TELEMETRY=1 HF_TOKEN=dummy-token \
hf repo create example/hf-cli-model --type model --yes
HF_ENDPOINT=http://localhost:8080 HF_HUB_DISABLE_TELEMETRY=1 HF_TOKEN=dummy-token \
hf upload example/hf-cli-model path/to/file.txt
```

## Features

- Hugging Face compatible endpoints for models, datasets, and spaces (coverage tracked in `hf-api-status.md`).
- Git over HTTP and SSH with Large File Storage support.
- Optional proxy/mirror mode to pull from an upstream Hugging Face instance (`--proxy` with `--mirror-ttl` throttling).
- Pluggable storage: local filesystem by default or S3 for repositories/LFS (`--s3-*` flags, `--s3-repositories` to mount repos on S3).
- Authentication helpers: basic auth, static tokens, signed tokens, and SSH public key validation.

## Configuration

Common flags (see `cmd/hfd/main.go` for the full list):

- `--addr` and `--ssh-addr`: HTTP/SSH listen addresses (default `:8080` / `:2222`).
- `--data`: root data directory for repositories and LFS objects.
- `--proxy`: upstream source URL for on-demand mirroring (e.g. `https://huggingface.co`).
- `--mirror-ttl`: minimum duration between mirror syncs (`0` to sync every fetch).
- `--s3-endpoint`, `--s3-bucket`, `--s3-access-key`, `--s3-secret-key`, `--s3-use-path-style`: S3 storage settings; add `--s3-repositories` to mount git repos on S3.
- `--username`/`--password`/`--token`/`--sign-key`: HTTP/SSH authentication options.
- `--ssh-authorized-key`: authorized_keys file for SSH public key auth; `--ssh-host-key` to load or generate the host key.

## Development

```bash
go test ./...
```
