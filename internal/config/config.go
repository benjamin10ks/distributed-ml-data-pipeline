package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Environment string
	ServiceName string
	LogLevel    string
	TraceID     string

	HTTPPort    int
	MetricsPort int

	KafkaBrokers             []string
	SchemaRegistryURL        string
	KafkaTopicFilesRaw       string
	KafkaTopicFilesProcessed string
	KafkaTopicCdcEvents      string
	KafkaTopicDLQ            string

	PostgresHost     string
	PostgresPort     int
	PostgresUser     string
	PostgresPassword string
	PostgresDB       string

	MinioEndpoint         string
	MinioAccessKey        string
	MinioSecretKey        string
	MinioUseSSL           bool
	MinioBucketLanding    string
	MinioBucketProcessed  string
	MinioBucketQuarantine string
}

func Load() (Config, error) {
	httpPort, err := getEnvInt("HTTP_PORT", 8080)
	if err != nil {
		return Config{}, err
	}
	metricsPort, err := getEnvInt("METRICS_PORT", 9090)
	if err != nil {
		return Config{}, err
	}
	postgresPort, err := getEnvInt("POSTGRES_PORT", 5432)
	if err != nil {
		return Config{}, err
	}
	minioUseSSL, err := getEnvBool("MINIO_USE_SSL", false)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		Environment:              getEnv("ENVIRONMENT", "local"),
		ServiceName:              os.Getenv("SERVICE_NAME"),
		LogLevel:                 getEnv("LOG_LEVEL", "info"),
		TraceID:                  os.Getenv("TRACE_ID"),
		HTTPPort:                 httpPort,
		MetricsPort:              metricsPort,
		KafkaBrokers:             splitCSV(os.Getenv("KAFKA_BROKERS")),
		SchemaRegistryURL:        os.Getenv("SCHEMA_REGISTRY_URL"),
		KafkaTopicFilesRaw:       getEnv("KAFKA_TOPIC_FILES_RAW", "files.raw"),
		KafkaTopicFilesProcessed: getEnv("KAFKA_TOPIC_FILES_PROCESSED", "files.processed"),
		KafkaTopicCdcEvents:      getEnv("KAFKA_TOPIC_CDC_EVENTS", "cdc.events"),
		KafkaTopicDLQ:            getEnv("KAFKA_TOPIC_DLQ", "dead-letter"),
		PostgresHost:             os.Getenv("POSTGRES_HOST"),
		PostgresPort:             postgresPort,
		PostgresUser:             os.Getenv("POSTGRES_USER"),
		PostgresPassword:         os.Getenv("POSTGRES_PASSWORD"),
		PostgresDB:               os.Getenv("POSTGRES_DB"),
		MinioEndpoint:            os.Getenv("MINIO_ENDPOINT"),
		MinioAccessKey:           os.Getenv("MINIO_ACCESS_KEY"),
		MinioSecretKey:           os.Getenv("MINIO_SECRET_KEY"),
		MinioUseSSL:              minioUseSSL,
		MinioBucketLanding:       getEnv("MINIO_BUCKET_LANDING", "landing"),
		MinioBucketProcessed:     getEnv("MINIO_BUCKET_PROCESSED", "processed"),
		MinioBucketQuarantine:    getEnv("MINIO_BUCKET_QUARANTINE", "quarantine"),
	}

	missing := []string{}
	if cfg.ServiceName == "" {
		missing = append(missing, "SERVICE_NAME")
	}
	if len(cfg.KafkaBrokers) == 0 {
		missing = append(missing, "KAFKA_BROKERS")
	}
	if cfg.SchemaRegistryURL == "" {
		missing = append(missing, "SCHEMA_REGISTRY_URL")
	}
	if cfg.PostgresHost == "" {
		missing = append(missing, "POSTGRES_HOST")
	}
	if cfg.PostgresUser == "" {
		missing = append(missing, "POSTGRES_USER")
	}
	if cfg.PostgresPassword == "" {
		missing = append(missing, "POSTGRES_PASSWORD")
	}
	if cfg.PostgresDB == "" {
		missing = append(missing, "POSTGRES_DB")
	}
	if cfg.MinioEndpoint == "" {
		missing = append(missing, "MINIO_ENDPOINT")
	}
	if cfg.MinioAccessKey == "" {
		missing = append(missing, "MINIO_ACCESS_KEY")
	}
	if cfg.MinioSecretKey == "" {
		missing = append(missing, "MINIO_SECRET_KEY")
	}

	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return parsed, nil
}

func getEnvBool(key string, fallback bool) (bool, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("invalid %s: %w", key, err)
	}
	return parsed, nil
}

func splitCSV(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
