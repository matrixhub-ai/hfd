ARG ALPINE_VERSION=3.23
ARG GOLANG_VERSION=1.26

ARG IMAGE_PREFIX=docker.io/
ARG GOPROXY=https://proxy.golang.org,direct

##########################################

FROM ${IMAGE_PREFIX}library/golang:${GOLANG_VERSION}-alpine${ALPINE_VERSION} AS builder

ARG GOPROXY
WORKDIR /src
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=bind,source=./go.mod,target=/src/go.mod \
    --mount=type=bind,source=./go.sum,target=/src/go.sum \
    go mod download

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=bind,source=./,target=/src/ \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

##########################################

FROM ${IMAGE_PREFIX}library/alpine:${ALPINE_VERSION} AS hfd

COPY --from=builder /out/hfd /usr/local/bin/hfd

EXPOSE 8080
EXPOSE 2222

ENTRYPOINT ["/usr/local/bin/hfd"]
