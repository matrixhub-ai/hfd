# hfd Agent Instructions

hfd is a headless Hugging Face daemon written in Go. It serves the HF Hub API,
Git smart HTTP, Git over SSH, Git LFS and the Xet CAS from bare Git repositories
plus chunk-deduplicated Xet content, stored on a local directory or S3, and can
mirror another hub. No web UI, no database. Other projects embed the exported
`pkg/` API, so extend it with functional options instead of changing signatures.

## Layout

- `cmd/hfd`: the binary; flag parsing and hand-written wiring.
- `internal/server`: assembles the HTTP chain and the SSH server from one `Options`.
- `pkg/backend/{hf,http,lfs,ssh,internalapi}`: protocol frontends; the HTTP ones
  are gorilla/mux routers that fall through to the next handler on unmatched paths.
- `pkg/authenticate`, `pkg/permission`, `pkg/receive`: identity, permission hooks,
  receive hooks.
- `pkg/repository`, `pkg/mirror`, `pkg/lfs`, `pkg/gc`, `pkg/storage`: go-git
  repositories (system `git` when available), mirroring and the xet data plane,
  LFS pointers and locks, garbage collection, storage roots.
- `test/e2e`: client matrices driving real `git`, `git-lfs`, `git-xet`, `hf` and
  `huggingface_hub` against local and S3 storage.
- `data/`: gitignored runtime state.

## Commands

```sh
go build ./... && go vet ./...              # what CI runs before the tests
gofmt -l cmd hack internal pkg test         # must print nothing
go test ./cmd/... ./internal/... ./pkg/...  # unit tests; pkg/repository needs git
go test -timeout 30m ./test/e2e/            # e2e; every test runs twice, local then fake S3
make update-hf-api-status                   # regenerate hf-api-status.md after HF route changes (network)
```

e2e tests skip when a client tool is missing locally and fail when `CI` is set;
narrow with `-run` and report skips instead of claiming full coverage.

## Invariants

- HTTP chain (`internal/server/server.go`), outermost first: access log,
  `/internal` API (only with `-internal`, unauthenticated), xet CAS server,
  authentication, Git smart HTTP, Git LFS, HF Hub API, 404. CAS routes are
  answered before user authentication and never yield a user identity.
- Authentication only establishes identity; anonymous requests pass through.
  Enforcement is the `permission.PermissionHookFunc` each backend calls, so every
  new route must call it with the right `Operation`.
- HTTP and SSH are built from the same `Options`; hooks, policy and validators
  stay symmetric.
- Local and S3 storage are both first-class; keep both e2e passes green.

## Conventions

- Stdlib `testing` only, table-driven under `t.Run`; new e2e coverage goes into
  the existing matrix for its area on top of `newE2EServer`.
- Match the surrounding code: functional options, `log/slog`, sentinel errors
  with `errors.Is`, one-line doc comments.
- Base branch `master`, one squash-merged change per PR; imperative subject,
  body explains why.
