package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	log.Println("alert-service starting...")

	// TODO: Initialize config
	// TODO: Initialize dependencies (Kafka consumer, notification providers)
	// TODO: Start HTTP server for alert management
	// TODO: Start Kafka consumer for threshold monitoring

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("alert-service shutting down...")
}
