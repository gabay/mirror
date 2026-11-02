# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS builder

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mirror ./cmd/mirror

FROM alpine:3.21

RUN mkdir -p /data /var/lib/mirror

COPY --from=builder /out/mirror /usr/local/bin/mirror

ENV MIRROR_PATH=/data \
    MIRROR_STATE_DIR=/var/lib/mirror \
    MIRROR_LISTEN_ADDR=:22001 \
    MIRROR_DISCOVERY_LAN=true \
    MIRROR_DISCOVERY_PORT=22002 \
    MIRROR_CONFLICT_STRATEGY=lww \
    MIRROR_DEBOUNCE_DELAY=500ms \
    MIRROR_RESCAN_INTERVAL=60s \
    MIRROR_LOG_LEVEL=info

VOLUME ["/data", "/var/lib/mirror"]

EXPOSE 22001/tcp 22002/udp

ENTRYPOINT ["/usr/local/bin/mirror"]
