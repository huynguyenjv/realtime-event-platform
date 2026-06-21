# Build stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Copy go workspace files
COPY go.work go.work.sum* ./
COPY services/analytics-service/go.mod services/analytics-service/go.sum ./services/analytics-service/
COPY shared/ ./shared/

RUN cd services/analytics-service && go mod download

COPY services/analytics-service/ ./services/analytics-service/

RUN cd services/analytics-service && CGO_ENABLED=0 GOOS=linux go build -o /analytics-service ./cmd/server/

# Run stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

WORKDIR /app
COPY --from=builder /analytics-service .

EXPOSE 8080

CMD ["./analytics-service"]
