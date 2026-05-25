// Package config provides a single source of truth for all runtime configuration of the ingestion service.
package config

import "os"

// Config holds all runtime configuration for the ingestion service.
// Values are read from environment variables so nothing is hardcoded.
type Config struct {
	// Environment
	Environment string // e.g. "local", "dev", "prod"
	LogLevel    string // debug, info, warn, error

	// Networking
	ListenAddr string // address the webhook HTTP server binds to, e.g. ":8080"

	// MinIO / S3
	S3Endpoint       string // e.g. "http://localhost:9000"
	S3AccessKey      string
	S3SecretKey      string
	LandingBucket    string // raw uploads land here
	ProcessedBucket  string // normalized Parquet written here
	QuarantineBucket string // invalid files moved here

	// Postgres (manifest)
	DatabaseURL string // e.g. "postgres://postgres:password@localhost:5432/pipeline"
}

// ConfigFromEnv reads configuration from environment variables.
// Any missing required variable causes an immediate exit with a clear message.
func ConfigFromEnv() Config {
	return Config{
		Environment:      getEnv("ENVIRONMENT", "local"),
		LogLevel:         getEnv("LOG_LEVEL", "info"),
		ListenAddr:       getEnv("LISTEN_ADDR", ":8080"),
		S3Endpoint:       requireEnv("S3_ENDPOINT"),
		S3AccessKey:      requireEnv("S3_ACCESS_KEY"),
		S3SecretKey:      requireEnv("S3_SECRET_KEY"),
		LandingBucket:    getEnv("LANDING_BUCKET", "landing"),
		ProcessedBucket:  getEnv("PROCESSED_BUCKET", "processed"),
		QuarantineBucket: getEnv("QUARANTINE_BUCKET", "quarantine"),
		DatabaseURL:      requireEnv("DATABASE_URL"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		// Fail loudly at startup rather than mysteriously at runtime
		panic("required environment variable not set: " + key)
	}
	return v
}
