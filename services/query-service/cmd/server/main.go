package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	log.Println("query-service starting...")

	// TODO: Initialize config
	// TODO: Initialize dependencies (TimescaleDB, Redis cache, Elasticsearch)
	// TODO: Start HTTP/gRPC server
	// TODO: Implement query endpoints for historical data

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("query-service shutting down...")
}
