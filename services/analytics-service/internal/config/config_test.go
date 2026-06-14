package config

import (
	"os"
	"testing"
)

func TestLoad_Defaults(t *testing.T) {
	os.Unsetenv("PORT")
	os.Unsetenv("APP_ENV")
	os.Unsetenv("KAFKA_BROKERS")

	cfg := Load()

	if cfg.Port != "8080" {
		t.Errorf("Port = %q, want %q", cfg.Port, "8080")
	}
	if cfg.AppEnv != "development" {
		t.Errorf("AppEnv = %q, want %q", cfg.AppEnv, "development")
	}
	if len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "localhost:9092" {
		t.Errorf("KafkaBrokers = %v, want [localhost:9092]", cfg.KafkaBrokers)
	}
	if cfg.KafkaGroupID != "analytics-service" {
		t.Errorf("KafkaGroupID = %q, want %q", cfg.KafkaGroupID, "analytics-service")
	}
	if cfg.ConsumeTopic != "events.raw" {
		t.Errorf("ConsumeTopic = %q, want %q", cfg.ConsumeTopic, "events.raw")
	}
	if cfg.DatabaseURL != "postgres://postgres:postgres@localhost:5433/eventdb?sslmode=disable" {
		t.Errorf("DatabaseURL = %q", cfg.DatabaseURL)
	}
	if cfg.RedisAddr != "localhost:6379" {
		t.Errorf("RedisAddr = %q, want %q", cfg.RedisAddr, "localhost:6379")
	}
	if cfg.WorkerCount != 4 {
		t.Errorf("WorkerCount = %d, want 4", cfg.WorkerCount)
	}
	if cfg.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want 100", cfg.BatchSize)
	}
}

func TestLoad_EnvOverride(t *testing.T) {
	os.Setenv("PORT", "9999")
	os.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	defer func() {
		os.Unsetenv("PORT")
		os.Unsetenv("KAFKA_BROKERS")
	}()

	cfg := Load()

	if cfg.Port != "9999" {
		t.Errorf("Port = %q, want %q", cfg.Port, "9999")
	}
	if len(cfg.KafkaBrokers) != 2 {
		t.Errorf("KafkaBrokers len = %d, want 2", len(cfg.KafkaBrokers))
	}
}
