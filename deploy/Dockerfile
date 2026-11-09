# syntax=docker/dockerfile:1
# ──────────────────────────────────────────────
# Stage 1 – build the statically linked binary
# ──────────────────────────────────────────────
FROM golang:1.23-alpine AS builder

# Install ca-certificates for TLS (not strictly needed here but good practice).
RUN apk add --no-cache ca-certificates git

WORKDIR /src

# Download dependencies first so this layer is cached when only source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build a fully static binary (CGO disabled) with all debug info stripped.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" \
    -o /beacon-broker ./cmd/broker

# ──────────────────────────────────────────────
# Stage 2 – minimal non-root final image
# ──────────────────────────────────────────────
FROM gcr.io/distroless/static:nonroot

# Copy the CA bundle so the broker can establish TLS connections if needed.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the statically compiled broker binary.
COPY --from=builder /beacon-broker /beacon-broker

# 9001 – gRPC (client-facing & inter-broker internal RPCs)
# 9091 – Prometheus metrics
EXPOSE 9001 9091

# distroless/static:nonroot runs as uid=65532 (nonroot) automatically.
# No USER instruction needed – it is baked into the base image.

ENTRYPOINT ["/beacon-broker"]
