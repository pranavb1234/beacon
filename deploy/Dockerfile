# syntax=docker/dockerfile:1
FROM golang:1.22-alpine AS builder

WORKDIR /src

# Download deps first so this layer is cached when only source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /beacon-broker ./cmd/broker

# Distroless: no shell, no package manager, minimal attack surface.
FROM gcr.io/distroless/static:nonroot
COPY --from=builder /beacon-broker /beacon-broker
EXPOSE 9001 9091
ENTRYPOINT ["/beacon-broker"]
