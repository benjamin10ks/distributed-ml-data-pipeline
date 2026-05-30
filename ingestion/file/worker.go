package file

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/benjamin10ks/distributed-ml-data-pipeline/ingestion/file/parser"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type WorkerConfig struct {
	Adapters         []IngestionsAdapter
	ProcessedBucket  string
	QuarantineBucket string
	DB               *pgxpool.Pool
	S3Client         *s3.Client
	KafkaClient      *kgo.Client
	Logger           *slog.Logger
}

type Worker struct {
	cfg      WorkerConfig
	manifest *Manifest
}

func NewWorker(cfg WorkerConfig) (*Worker, error) {
	return &Worker{
		cfg:      cfg,
		manifest: NewManifest(cfg.DB),
	}, nil
}

func (w *Worker) Adapters() []IngestionsAdapter {
	return w.cfg.Adapters
}

func (w *Worker) Run(ctx context.Context) error {
	merged := make(chan RawEvent, 512)

	for _, adapter := range w.Adapters() {
		adapter := adapter
		go func() {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-adapter.Events():
				if !ok {
					return
				}
				merged <- event
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-merged:
			if err := w.process(ctx, event); err != nil {
				w.cfg.Logger.Error("failed to process event", "path", event.Path, "source", event.Source, "error", err)
			}
		}
	}
}

// Process runs RawEvent through the ingestion sequence
func (w *Worker) process(ctx context.Context, event RawEvent) error {
	log := w.cfg.Logger.With("path", event.Path, "source", event.Source, "hash", event.ContentHash)

	// Check if file has been processed
	existing, err := w.manifest.GetByHash(ctx, event.ContentHash)
	if err != nil {
		return fmt.Errorf("failed to check manifest: %w", err)
	}
	if existing != nil {
		log.Info("skipping already processed file")
		return nil
	}

	// Record in manifest as pending
	if err := w.manifest.Insert(ctx, ManifestEntry{
		Path:        event.Path,
		ContentHash: event.ContentHash,
		Source:      event.Source,
	}); err != nil {
		return fmt.Errorf("failed to insert manifest entry: %w", err)
	}

	err = w.manifest.UpdateStatus(ctx, event.ContentHash, StatusProcessing)
	if err != nil {
		log.Error("failed to update manifest status to processing", "error", err)
	}

	// Validate file
	if err := validate(event); err != nil {
		log.Warn("validation failed, moving to quarantine", "reason", err)
		if qErr := w.quarantine(ctx, event); qErr != nil {
			log.Error("failed to quarantine file", "error", qErr)
		}
		_ = w.manifest.UpdateStatus(ctx, event.ContentHash, StatusQuarantined)
		return nil
	}

	// Parse file
	records, err := parse(event)
	if err != nil {
		log.Error("parsing failed", "reason", err)
		_ = w.manifest.UpdateStatus(ctx, event.ContentHash, StatusFailed)
		return fmt.Errorf("failed to parse file: %w", err)
	}
	log.Info("file parsed successfully", "record_count", len(records))

	// Write to Processed bucket
	processedKey, err := w.writeProcessed(ctx, event, records)
	if err != nil {
		_ = w.manifest.UpdateStatus(ctx, event.ContentHash, StatusFailed)
		return fmt.Errorf("failed to write processed file: %w", err)
	}

	// Mark done
	if err := w.manifest.UpdateStatus(ctx, event.ContentHash, StatusDone); err != nil {
		return fmt.Errorf("failed to update manifest status to done: %w", err)
	}

	if err := w.publishProcessed(ctx, event, processedKey); err != nil {
		log.Error("failed to publish processed event", "error", err)
	}

	log.Info("file processed successfully")
	return nil
}

func (w *Worker) quarantine(ctx context.Context, event RawEvent) error {
	_, err := w.cfg.S3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &w.cfg.QuarantineBucket,
		Key:    &event.Path,
		Body:   bytes.NewReader(event.Payload),
	})
	return err
}

type Record map[string]any

func (w *Worker) writeProcessed(ctx context.Context, event RawEvent, records []parser.Record) (string, error) {
	var buf bytes.Buffer

	for _, record := range records {
		if _, err := fmt.Fprintf(&buf, "%v\n", record); err != nil {
			return "", fmt.Errorf("failed to write record to buffer: %w", err)
		}
	}

	_, err := w.cfg.S3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &w.cfg.ProcessedBucket,
		Key:    &event.Path,
		Body:   bytes.NewReader(buf.Bytes()),
	})
	return event.Path, err
}

// TODO: publish event to Kafka
func (w *Worker) publishProcessed(ctx context.Context, event RawEvent, processedKey string) error {
	return nil
}
