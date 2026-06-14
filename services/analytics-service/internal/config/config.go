package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port   string
	AppEnv string

	// Kafka
	KafkaBrokers []string
	KafkaGroupID string
	ConsumeTopic string

	// TimescaleDB
	DatabaseURL string

	// Redis
	RedisAddr     string
	RedisPassword string

	// Pipeline
	WorkerCount        int
	ChannelBuffer      int
	BatchSize          int
	BatchFlushInterval time.Duration

	// Aggregation
	FlushInterval time.Duration
	MaxEventAge   time.Duration
}

func Load() *Config {
	return &Config{
		Port:   getEnv("PORT", "8080"),
		AppEnv: getEnv("APP_ENV", "development"),

		KafkaBrokers: strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaGroupID: getEnv("KAFKA_GROUP_ID", "analytics-service"),
		ConsumeTopic: getEnv("KAFKA_CONSUME_TOPIC", "events.raw"),

		DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5433/eventdb?sslmode=disable"),

		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),

		WorkerCount:        getEnvInt("WORKER_COUNT", 4),
		ChannelBuffer:      getEnvInt("CHANNEL_BUFFER", 1000),
		BatchSize:          getEnvInt("BATCH_SIZE", 100),
		BatchFlushInterval: getEnvDuration("BATCH_FLUSH_INTERVAL", 5*time.Second),

		FlushInterval: getEnvDuration("FLUSH_INTERVAL", 10*time.Second),
		MaxEventAge:   getEnvDuration("MAX_EVENT_AGE", 24*time.Hour),
	}
}

func (c *Config) IsDevelopment() bool {
	return c.AppEnv == "development"
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return defaultValue
}
