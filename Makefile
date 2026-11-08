.PHONY: proto build test lint docker-up docker-down clean help

# ── Variables ──────────────────────────────────────────────────────────────────
MODULE      := github.com/pranavb1234/beacon
GO          := go
GOFLAGS     := -race
TIMEOUT     := 120s
DOCKER_COMP := docker compose -f deploy/docker-compose.yml

# ── Default target ─────────────────────────────────────────────────────────────
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

# ── Proto generation ───────────────────────────────────────────────────────────
proto: ## Generate Go code from proto/pubsub.proto using buf
	cd proto && buf generate
	@echo "✓ proto generated"

proto-lint: ## Lint the proto files
	cd proto && buf lint

# ── Go targets ─────────────────────────────────────────────────────────────────
build: ## Build all binaries
	$(GO) build ./...

test: ## Run all tests with race detector
	$(GO) test $(GOFLAGS) -timeout $(TIMEOUT) -count=1 ./...

test-short: ## Run only short tests (skips slow distribution tests)
	$(GO) test $(GOFLAGS) -short -timeout $(TIMEOUT) ./...

lint: ## Run go vet (add staticcheck / golangci-lint as desired)
	$(GO) vet ./...
	@if command -v staticcheck >/dev/null 2>&1; then staticcheck ./...; fi

tidy: ## Tidy and verify go.mod
	$(GO) mod tidy
	$(GO) mod verify

# ── Docker targets ─────────────────────────────────────────────────────────────
docker-up: ## Build images and start the 3-node cluster + Prometheus + Grafana
	$(DOCKER_COMP) up --build -d

docker-down: ## Stop and remove containers
	$(DOCKER_COMP) down

docker-logs: ## Stream logs from all containers
	$(DOCKER_COMP) logs -f

# ── Cleanup ────────────────────────────────────────────────────────────────────
clean: ## Remove generated proto files and binaries
	rm -rf proto/pubsubv1/
	$(GO) clean -cache
