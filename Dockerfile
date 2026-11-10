# syntax=docker/dockerfile:1

# ---- Build stage ----------------------------------------------------------
# Runs on the builder's own platform and cross-compiles for the target one,
# so a multi-arch build (amd64 + arm64 for Apple Silicon) needs no emulation.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src

# Dependencies first: this layer stays cached while only the code changes.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
# CGO_ENABLED=0 makes the Go linker produce a fully static binary: no libc,
# no dynamic loader, so it runs on an empty filesystem. timetzdata embeds the
# time zone database, which scratch does not have.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -tags timetzdata -ldflags="-s -w" -o /out/quotes ./cmd/quotes

# ---- Migrations image (target "migrate") ----------------------------------
# golang-migrate CLI with the SQL files baked in, so nothing is mounted from
# the host, and the same image can run as a Kubernetes Job before a rollout.
# The DSN comes from DATABASE_URL; arguments are migrate commands, "up" by
# default: `docker compose run --rm migrate down 1`.
FROM migrate/migrate:v4.19.1 AS migrate
COPY migrations /migrations
ENTRYPOINT ["/bin/sh", "-c", "exec migrate -path=/migrations -database \"$DATABASE_URL\" \"$@\"", "migrate"]
CMD ["up"]

# ---- Runtime stage (default target) ---------------------------------------
# Empty image: no shell, no package manager, nothing but the binary.
FROM scratch
# Root certificates for HTTPS calls to the rates API. The golang alpine image
# ships them, and they are the same for every architecture.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/quotes /usr/local/bin/quotes
# scratch has no /etc/passwd, so the user is set by number. 65532 is the
# conventional "nonroot" UID, and a numeric UID also satisfies Kubernetes
# runAsNonRoot.
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/quotes"]
CMD ["api"]
