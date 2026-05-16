.PHONY: all build test lint clean docker-build docker-up docker-down proto help

# Variables
SERVICES := collector-service analytics-service prediction-service websocket-gateway query-service auth-service alert-service
DOCKER_COMPOSE := docker compose -f infra/docker/docker-compose.yml

# Default target
all: build

# Build all services
build:
	@for service in $(SERVICES); do \
		echo "Building $$service..."; \
		cd services/$$service && go build -o bin/server ./cmd/server && cd ../..; \
	done

# Build specific service
build-%:
	@echo "Building $*..."
	@cd services/$* && go build -o bin/server ./cmd/server

# Run tests for all services
test:
	@for service in $(SERVICES); do \
		echo "Testing $$service..."; \
		cd services/$$service && go test -v ./... && cd ../..; \
	done

# Run tests for specific service
test-%:
	@echo "Testing $*..."
	@cd services/$* && go test -v ./...

# Run linter
lint:
	@golangci-lint run ./...

# Clean build artifacts
clean:
	@for service in $(SERVICES); do \
		rm -rf services/$$service/bin; \
	done

# Download dependencies
deps:
	@go work sync
	@for service in $(SERVICES); do \
		cd services/$$service && go mod tidy && cd ../..; \
	done
	@cd shared/libs && go mod tidy

# Generate proto files
proto:
	@protoc --go_out=. --go-grpc_out=. shared/proto/*.proto

# Docker commands
docker-build:
	$(DOCKER_COMPOSE) build

docker-up:
	$(DOCKER_COMPOSE) up -d

docker-down:
	$(DOCKER_COMPOSE) down

docker-logs:
	$(DOCKER_COMPOSE) logs -f

docker-ps:
	$(DOCKER_COMPOSE) ps

# Infrastructure only
infra-up:
	$(DOCKER_COMPOSE) up -d postgres redis mongodb zookeeper kafka

infra-down:
	$(DOCKER_COMPOSE) down postgres redis mongodb zookeeper kafka

# Database migrations
migrate-up:
	@migrate -path infra/postgres/migrations -database "postgres://postgres:postgres@localhost:5432/event_platform?sslmode=disable" up

migrate-down:
	@migrate -path infra/postgres/migrations -database "postgres://postgres:postgres@localhost:5432/event_platform?sslmode=disable" down

migrate-create:
	@migrate create -ext sql -dir infra/postgres/migrations -seq $(name)

# Run specific service locally
run-%:
	@cd services/$* && go run ./cmd/server

# Help
help:
	@echo "Available targets:"
	@echo "  build          - Build all services"
	@echo "  build-<svc>    - Build specific service"
	@echo "  test           - Run tests for all services"
	@echo "  test-<svc>     - Run tests for specific service"
	@echo "  lint           - Run linter"
	@echo "  clean          - Clean build artifacts"
	@echo "  deps           - Download and sync dependencies"
	@echo "  proto          - Generate proto files"
	@echo "  docker-build   - Build Docker images"
	@echo "  docker-up      - Start all containers"
	@echo "  docker-down    - Stop all containers"
	@echo "  docker-logs    - View container logs"
	@echo "  infra-up       - Start infrastructure only"
	@echo "  infra-down     - Stop infrastructure"
	@echo "  migrate-up     - Run database migrations"
	@echo "  migrate-down   - Rollback database migrations"
	@echo "  run-<svc>      - Run specific service locally"
