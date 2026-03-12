ARG ALPINE_VERSION=3.23
ARG GOLANG_VERSION=1.25

ARG IMAGE_PREFIX=docker.io/
ARG GOPROXY=https://proxy.golang.org,direct

##########################################

FROM ${IMAGE_PREFIX}library/golang:${GOLANG_VERSION}-alpine${ALPINE_VERSION} AS builder

WORKDIR /app

ARG GOPROXY
ENV GOPROXY=${GOPROXY}
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=bind,source=./go.mod,target=/app/go.mod \
    --mount=type=bind,source=./go.sum,target=/app/go.sum \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -o /hfd ./cmd/hfd

##########################################

FROM ${IMAGE_PREFIX}library/alpine:${ALPINE_VERSION} AS hfd

RUN --mount=type=cache,target=/var/cache/apk \
    apk add ca-certificates git s3fs-fuse && \
    update-ca-certificates

COPY --from=builder /hfd /usr/local/bin/hfd

EXPOSE 8080
EXPOSE 2222

ENTRYPOINT ["/usr/local/bin/hfd"]
