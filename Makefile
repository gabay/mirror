BINARY_NAME ?= mirror
BIN_DIR     ?= bin
BINARY      := $(BIN_DIR)/$(BINARY_NAME)
PKG         := ./...
CMD_PKG     := ./cmd/mirror

DOCKER_IMAGE ?= mirror
DOCKER_TAG   ?= latest
STACK_NAME   ?= mirror

GO      ?= go
GOFLAGS ?= -trimpath
LDFLAGS ?= -s -w

.DEFAULT_GOAL := help

.PHONY: all help build install run test test-race test-short cover bench fmt vet check docker docker-run swarm-deploy swarm-rm clean

all: check test-race build ## Run checks, race tests, and build the binary

help: ## Show available Makefile targets
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

build: ## Build static mirror binary into ./bin/mirror
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build $(GOFLAGS) -ldflags="$(LDFLAGS)" -o $(BINARY) $(CMD_PKG)

install: ## Install mirror binary into GOPATH/bin
	CGO_ENABLED=0 $(GO) install $(GOFLAGS) -ldflags="$(LDFLAGS)" $(CMD_PKG)

run: build ## Build and run mirror locally (uses ./tmp/data and ./tmp/state by default)
	@mkdir -p ./tmp/data ./tmp/state
	MIRROR_PATH=$${MIRROR_PATH:-./tmp/data} \
	MIRROR_STATE_DIR=$${MIRROR_STATE_DIR:-./tmp/state} \
	MIRROR_LISTEN_ADDR=$${MIRROR_LISTEN_ADDR:-:22001} \
	MIRROR_DISCOVERY_PORT=$${MIRROR_DISCOVERY_PORT:-22002} \
	./$(BINARY)

test: ## Run all unit and integration tests
	$(GO) test -count=1 $(PKG)

test-race: ## Run all tests with the Go race detector enabled
	$(GO) test -v -count=1 -race $(PKG)

test-short: ## Run fast unit tests only
	$(GO) test -short -count=1 $(PKG)

cover: ## Run tests and generate coverage report (coverage.out & coverage.html)
	$(GO) test -count=1 -race -coverprofile=coverage.out $(PKG)
	$(GO) tool cover -func=coverage.out
	$(GO) tool cover -html=coverage.out -o coverage.html

bench: ## Run all benchmarks with memory allocation stats
	$(GO) test -run=^$$ -bench=. -benchmem $(PKG)

fmt: ## Format all Go source files
	$(GO) fmt $(PKG)

vet: ## Run go vet static analysis
	$(GO) vet $(PKG)

check: fmt vet ## Format code and run static analysis

docker: ## Build Docker image (mirror:latest)
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

docker-run: ## Run mirror container mounting ./tmp/data to /data
	@mkdir -p ./tmp/data ./tmp/state
	docker run --rm -it \
		-p 22001:22001/tcp \
		-p 22002:22002/udp \
		-v "$(CURDIR)/tmp/data:/data" \
		-v "$(CURDIR)/tmp/state:/var/lib/mirror" \
		-e MIRROR_SECRET="$${MIRROR_SECRET:-}" \
		$(DOCKER_IMAGE):$(DOCKER_TAG)

swarm-deploy: ## Deploy mirror stack to Docker Swarm using docker-compose.swarm.yml
	docker stack deploy -c docker-compose.swarm.yml $(STACK_NAME)

swarm-rm: ## Remove mirror stack from Docker Swarm
	docker stack rm $(STACK_NAME)

clean: ## Remove build artifacts, coverage reports, and temporary local data
	rm -rf $(BIN_DIR) coverage.out coverage.html ./tmp $(BINARY_NAME)
