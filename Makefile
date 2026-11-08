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

# Generate protobuf code
proto:
	@if [ -d "proto" ] && command -v buf >/dev/null 2>&1; then \
		cd proto && buf generate && echo "Proto generated with buf"; \
	elif [ -d "proto" ] && command -v protoc >/dev/null 2>&1; then \
		protoc --go_out=. --go-grpc_out=. proto/*.proto && echo "Proto generated with protoc"; \
	else \
		echo "Skipping proto generation: buf/protoc tool or proto directory not found"; \
	fi

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
	rm -rf bin/ proto/pubsubv1/
	go clean -cache
