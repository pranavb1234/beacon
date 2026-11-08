.PHONY: build test proto lint run-broker clean

# Build all binaries into bin/
build:
	@mkdir -p bin
	go build -o bin/broker ./cmd/broker
	go build -o bin/publisher ./cmd/publisher
	go build -o bin/subscriber ./cmd/subscriber
	go build -o bin/bench ./cmd/bench

# Run all tests with race detection
test:
	go test -race ./...

# Generate protobuf and gRPC Go code into internal/pb
proto:
	@mkdir -p internal/pb
	protoc --proto_path=proto \
		--go_out=internal/pb --go_opt=paths=source_relative \
		--go-grpc_out=internal/pb --go-grpc_opt=paths=source_relative \
		proto/pubsub.proto
	@echo "✓ Proto generated into internal/pb"

# Lint code using go vet and static analysis tools if present
lint:
	go vet ./...
	@if command -v staticcheck >/dev/null 2>&1; then staticcheck ./...; fi
	@if command -v golangci-lint >/dev/null 2>&1; then golangci-lint run; fi

# Run broker locally
run-broker:
	go run ./cmd/broker

# Clean binaries and caches
clean:
	rm -rf bin/ internal/pb/ proto/gen/ proto/pubsubv1/
	go clean -cache
