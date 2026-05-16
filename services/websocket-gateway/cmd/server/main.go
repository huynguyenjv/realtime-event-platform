package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	log.Println("websocket-gateway starting...")

	// TODO: Initialize config
	// TODO: Initialize dependencies (Redis pub/sub, Kafka consumer)
	// TODO: Start WebSocket server
	// TODO: Handle client connections and subscriptions

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("websocket-gateway shutting down...")
}
