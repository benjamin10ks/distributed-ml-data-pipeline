package file

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	processedTopicEnv     = "KAFKA_TOPIC_FILES_PROCESSED"
	defaultProcessedTopic = "files.processed"
)

type ProcessedEvent struct {
	Source          string            `json:"source"`
	Path            string            `json:"path"`
	ContentHash     string            `json:"content_hash"`
	Format          string            `json:"format"`
	Size            int64             `json:"size"`
	ReceivedAt      time.Time         `json:"received_at"`
	ProcessedAt     time.Time         `json:"processed_at"`
	ProcessedBucket string            `json:"processed_bucket"`
	ProcessedKey    string            `json:"processed_key"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

func processedTopic() string {
	if topic := os.Getenv(processedTopicEnv); topic != "" {
		return topic
	}
	return defaultProcessedTopic
}

func (w *Worker) publishProcessed(ctx context.Context, event RawEvent, processedKey string) error {
	if w.cfg.KafkaClient == nil {
		return fmt.Errorf("kafka client is not configured")
	}

	processedAt := time.Now().UTC()
	msg := ProcessedEvent{
		Source:          event.Source,
		Path:            event.Path,
		ContentHash:     event.ContentHash,
		Format:          event.Format,
		Size:            event.Size,
		ReceivedAt:      event.ReceivedAt.UTC(),
		ProcessedAt:     processedAt,
		ProcessedBucket: w.cfg.ProcessedBucket,
		ProcessedKey:    processedKey,
		Metadata:        event.Metadata,
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal processed event: %w", err)
	}

	key := event.ContentHash
	if key == "" {
		key = processedKey
	}

	record := &kgo.Record{
		Topic:     processedTopic(),
		Key:       []byte(key),
		Value:     payload,
		Timestamp: processedAt,
	}

	result := w.cfg.KafkaClient.ProduceSync(ctx, record)
	if err := result.FirstErr(); err != nil {
		return fmt.Errorf("failed to publish processed event: %w", err)
	}

	return nil
}
