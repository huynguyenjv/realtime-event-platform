package config

import (
	"os"
	_ "os"
	"strings"
)

type Config struct {
	Port   string
	AppEnv string

	KafkaBrokers []string
	KafkaTopic   string

	RedisURL string
}

func Load() *Config {
	return &Config{
		Port:         getEnv("PORT", "8088"),
		AppEnv:       getEnv("APP_ENV", "development"),
		KafkaBrokers: strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "events.raw"),
		RedisURL:     getEnv("REDIS_URL", "redis://localhost:6379"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func (c *Config) IsDevelopment() bool {
	return c.AppEnv == "development"
}
