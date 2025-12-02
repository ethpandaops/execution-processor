# Colors for terminal output
CYAN := \033[36m
GREEN := \033[32m
RESET := \033[0m

# Version info
VERSION ?= dev
GIT_COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DIRTY_SUFFIX := $(shell git diff --quiet 2>/dev/null || echo "-dirty")

# Build flags
LDFLAGS := -s -w \
	-X github.com/ethpandaops/execution-processor/internal/version.Release=$(VERSION)-$(GIT_COMMIT) \
	-X github.com/ethpandaops/execution-processor/internal/version.GitCommit=$(GIT_COMMIT)

.PHONY: build-binary
build-binary:
	@printf "$(CYAN)==> Building execution-processor...$(RESET)\n"
	@printf "$(CYAN)    Version: $(VERSION)-$(GIT_COMMIT)$(DIRTY_SUFFIX)$(RESET)\n"
	@go build -ldflags "$(LDFLAGS)" -o bin/execution-processor ./main.go
	@printf "$(GREEN)✓ Built: bin/execution-processor$(RESET)\n"
