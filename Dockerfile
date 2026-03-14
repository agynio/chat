# syntax=docker/dockerfile:1.8
ARG GO_VERSION=1.24
ARG BUF_VERSION=1.64.0

# Stage 1: Download buf binary
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS buf
ARG BUF_VERSION
RUN curl -sSL \
      "https://github.com/bufbuild/buf/releases/download/v${BUF_VERSION}/buf-$(uname -s)-$(uname -m)" \
      -o /usr/local/bin/buf && \
    chmod +x /usr/local/bin/buf

# Stage 2: Generate protos + compile
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src
ENV CGO_ENABLED=0 GO111MODULE=on

COPY --from=buf /usr/local/bin/buf /usr/local/bin/buf

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/root/go/pkg/mod \
    go mod download && go mod verify

COPY buf.gen.yaml buf.yaml ./
RUN buf generate buf.build/agynio/api --path agynio/api/chat/v1 --path agynio/api/threads/v1

COPY . .

ARG TARGETOS TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/root/go/pkg/mod \
    GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
      -trimpath -ldflags="-s -w" -o /out/chat ./cmd/chat

# Stage 3: Runtime
FROM gcr.io/distroless/base-debian12 AS runtime

WORKDIR /app

LABEL org.opencontainers.image.source="https://github.com/agynio/chat"

COPY --from=build /out/chat /usr/local/bin/chat

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/chat"]
