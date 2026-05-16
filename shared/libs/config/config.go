package config

import (
	"os"
	"strconv"
	"time"
)

func GetEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func GetEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func GetEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return defaultValue
}

func GetEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

type BaseConfig struct {
	AppEnv      string
	ServiceName string
	HTTPPort    int
	GRPCPort    int
}

func LoadBaseConfig(serviceName string) BaseConfig {
	return BaseConfig{
		AppEnv:      GetEnv("APP_ENV", "development"),
		ServiceName: serviceName,
		HTTPPort:    GetEnvInt("HTTP_PORT", 8080),
		GRPCPort:    GetEnvInt("GRPC_PORT", 9090),
	}
}
