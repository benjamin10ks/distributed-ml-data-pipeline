package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/benjamin10ks/distributed-ml-data-pipeline/ingestion/file"
	"github.com/benjamin10ks/distributed-ml-data-pipeline/internal/logging"
	streamingkafka "github.com/benjamin10ks/distributed-ml-data-pipeline/streaming/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// processed-consumer is a smoke-test consumer for files.processed: it logs
// every processed-file event it reads. It exists to prove the generic
// Consumer in streaming/kafka actually works end-to-end against a real
// topic; Phase 5 (feature engineering) is the intended long-term consumer
// of this topic.
func main() {
	logger := logging.NewLogger("processed-consumer", getEnv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	seeds := strings.Split(getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ",")
	topic := getEnv("KAFKA_TOPIC_FILES_PROCESSED", "files.processed")
	groupID := getEnv("KAFKA_CONSUMER_GROUP", "processed-consumer")

	consumer, err := streamingkafka.NewConsumer(streamingkafka.ConsumerConfig{
		SeedBrokers: seeds,
		GroupID:     groupID,
		Topics:      []string{topic},
		Logger:      logger,
	})
	if err != nil {
		logger.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	logger.Info("processed-consumer starting", "seeds", seeds, "topic", topic, "group_id", groupID)

	err = consumer.Run(ctx, func(ctx context.Context, record *kgo.Record) error {
		var event file.ProcessedEvent
		if err := json.Unmarshal(record.Value, &event); err != nil {
			logger.Error("failed to decode processed event, skipping",
				"partition", record.Partition, "offset", record.Offset, "error", err)
			return nil
		}
		logger.Info("processed event received",
			"source", event.Source,
			"path", event.Path,
			"processed_key", event.ProcessedKey,
			"partition", record.Partition,
			"offset", record.Offset,
		)
		return nil
	})
	if err != nil {
		logger.Error("consumer stopped with error", "error", err)
		os.Exit(1)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
