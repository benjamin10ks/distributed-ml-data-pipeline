package file

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type WorkerConfig struct {
	Adapters         []IngestionsAdapter
	LandingBucket    string
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
		manifest: NewManifest(cfg.DB, cfg.Logger),
	}, nil
}

func (w *Worker) Adapters() []IngestionsAdapter {
	return w.cfg.Adapters
}

func (w *Worker) Run(ctx context.Context) error {
	stuckThreshold := 31 * time.Minute
	if err := w.recoverStuck(ctx, stuckThreshold); err != nil {
		w.cfg.Logger.Error("failed to recover stuck entries", "error", err)
	}

	merged := make(chan RawEvent, 512)

	for _, adapter := range w.Adapters() {
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

func (w *Worker) recoverStuck(ctx context.Context, olderThan time.Duration) error {
	stuckEntries, err := w.manifest.GetStuck(ctx, olderThan)
	if err != nil {
		return fmt.Errorf("failed to query stuck manifest entries: %w", err)
	}

	if len(stuckEntries) == 0 {
		return nil
	}

	w.cfg.Logger.Info("re-queueing stuck entries", "count", len(stuckEntries))

	for _, entry := range stuckEntries {
		event := RawEvent{
			Path:        entry.Path,
			ContentHash: entry.ContentHash,
			Source:      entry.Source,
			Payload:     nil,
		}

		payload, err := w.fetchPayload(ctx, event)
		if err != nil {
			w.cfg.Logger.Error("failed to fetch payload for stuck entry, marking as failed", "content_hash", entry.ContentHash, "error", err)
			_ = w.manifest.UpdateStatus(ctx, entry.ContentHash, StatusFailed)
			continue
		}
		event.Payload = payload

		w.cfg.Logger.Info("re-queueing stuck entry", "path", event.Path, "source", event.Source, "content_hash", event.ContentHash)

		if err := w.manifest.UpdateStatus(ctx, entry.ContentHash, StatusPending); err != nil {
			w.cfg.Logger.Error("failed to mark stuck entry as failed", "content_hash", entry.ContentHash, "error", err)
			continue
		}

		if err := w.process(ctx, event); err != nil {
			w.cfg.Logger.Error("failed to re-process stuck entry", "path", event.Path, "source", event.Source, "content_hash", event.ContentHash, "error", err)
		}
	}
	return nil
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
		switch existing.Status {
		case StatusDone:
			log.Info("file already processed successfully, skipping")
			return nil
		case StatusProcessing:
			log.Info("file is currently being processed by another worker, skipping")
			return nil
		case StatusFailed, StatusQuarantined:
			log.Warn("file was previously processed but failed or quarantined, skipping", "previous_status", existing.Status)
			return nil
		}
	}

	// Record in manifest as pending
	if err := w.manifest.Insert(ctx, ManifestEntry{
		Path:        event.Path,
		ContentHash: event.ContentHash,
		Source:      event.Source,
		Status:      StatusPending,
	}); err != nil {
		return fmt.Errorf("failed to insert manifest entry: %w", err)
	}
	log.Info("manifest entry created, starting processing")

	err = w.manifest.UpdateStatus(ctx, event.ContentHash, StatusProcessing)
	if err != nil {
		log.Error("failed to update manifest status to processing", "error", err)
	}
	log.Info("file marked as processing")

	// Validate file
	if err := validate(event); err != nil {
		log.Warn("validation failed, moving to quarantine", "reason", err)
		if qErr := w.quarantine(ctx, event); qErr != nil {
			log.Error("failed to quarantine file", "error", qErr)
		}
		if mErr := w.manifest.UpdateStatus(ctx, event.ContentHash, StatusQuarantined); mErr != nil {
			return fmt.Errorf("failed to update manifest status to quarantined: %w", mErr)
		}
		return nil
	}
	log.Info("file validated successfully")

	// Parse file
	records, err := parse(event)
	if err != nil {
		log.Error("parsing failed", "reason", err)
		if mErr := w.manifest.UpdateStatus(ctx, event.ContentHash, StatusFailed); mErr != nil {
			return fmt.Errorf("failed to update manifest status to failed: %w", mErr)
		}
		return fmt.Errorf("failed to parse file: %w", err)
	}
	log.Info("file parsed successfully", "record_count", len(records))

	// Write to Processed bucket
	processedKey, err := w.writeProcessed(ctx, event, records)
	if err != nil {
		_ = w.manifest.UpdateStatus(ctx, event.ContentHash, StatusFailed)
		return fmt.Errorf("failed to write processed file: %w", err)
	}
	log.Info("file written to processed bucket", "processed_key", processedKey)

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

func (w *Worker) fetchPayload(ctx context.Context, event RawEvent) ([]byte, error) {
	output, err := w.cfg.S3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &w.cfg.LandingBucket,
		Key:    &event.Path,
	})
	if err != nil {
		return nil, err
	}

	defer output.Body.Close()

	payload, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, err
	}
	return payload, nil
}
