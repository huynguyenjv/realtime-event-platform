package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	log.Println("auth-service starting...")

	// TODO: Initialize config
	// TODO: Initialize dependencies (PostgreSQL, Redis, JWT)
	// TODO: Start HTTP server
	// TODO: Implement auth endpoints (login, register, refresh, validate)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("auth-service shutting down...")
}
