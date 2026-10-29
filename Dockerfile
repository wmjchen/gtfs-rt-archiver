# syntax=docker/dockerfile:1.7
FROM golang:1.26.5-alpine3.24 AS build

WORKDIR /src
ENV GOMAXPROCS=2 GOGC=50
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -p 2 -buildvcs=false -trimpath \
    -ldflags "-s -w -X gtfs-rt-archiver/internal/version.Version=${VERSION} -X gtfs-rt-archiver/internal/version.Commit=${COMMIT} -X gtfs-rt-archiver/internal/version.BuildTime=${BUILD_TIME}" \
    -o /out/gtfs-rt-archiver ./cmd/gtfs-rt-archiver

FROM rclone/rclone:1.74.2 AS rclone

FROM alpine:3.24
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S -g 10001 archiver \
    && adduser -S -D -H -u 10001 -G archiver archiver \
    && mkdir -p /data /config /tmp \
    && chown -R 10001:10001 /data /tmp

COPY --from=build /out/gtfs-rt-archiver /usr/local/bin/gtfs-rt-archiver
COPY --from=rclone /usr/local/bin/rclone /usr/local/bin/rclone

USER 10001:10001
ENV HOME=/data \
    TMPDIR=/tmp
VOLUME ["/data"]
EXPOSE 8080
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -q -O /dev/null http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/gtfs-rt-archiver"]
CMD ["run", "--config", "/config/config.yaml"]
