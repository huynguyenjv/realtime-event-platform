#!/bin/bash

set -e

echo "Setting up realtime-event-platform..."

# Check Go version
if ! command -v go &> /dev/null; then
    echo "Go is not installed. Please install Go 1.26+"
    exit 1
fi

GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
echo "Go version: $GO_VERSION"

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker"
    exit 1
fi

echo "Docker version: $(docker --version)"

# Install Go tools
echo "Installing Go tools..."
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest

# Sync Go workspace
echo "Syncing Go workspace..."
go work sync

# Download dependencies for all services
echo "Downloading dependencies..."
for service in collector-service analytics-service prediction-service websocket-gateway query-service auth-service alert-service; do
    echo "  - $service"
    (cd services/$service && go mod tidy)
done

echo "  - shared/libs"
(cd shared/libs && go mod tidy)

echo ""
echo "Setup complete!"
echo ""
echo "Next steps:"
echo "  1. Start infrastructure: make infra-up"
echo "  2. Run migrations: make migrate-up"
echo "  3. Start all services: make docker-up"
echo ""
