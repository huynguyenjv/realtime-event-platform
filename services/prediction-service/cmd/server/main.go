package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	log.Println("prediction-service starting...")

	// TODO: Initialize config
	// TODO: Initialize dependencies (ML model, Kafka, Redis)
	// TODO: Start gRPC server for predictions
	// TODO: Start Kafka consumer for real-time prediction triggers

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("prediction-service shutting down...")
}
