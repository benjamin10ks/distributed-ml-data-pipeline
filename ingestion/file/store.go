package file

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/benjamin10ks/distributed-ml-data-pipeline/ingestion/file/parser"
	"github.com/parquet-go/parquet-go"
)

const (
	processedMinSizeBytes = 128 * 1024 * 1024
	processedMaxSizeBytes = 256 * 1024 * 1024
)

type processedManifest struct {
	BaseKey     string    `json:"base_key"`
	Parts       []string  `json:"parts"`
	RecordCount int       `json:"record_count"`
	CreatedAt   time.Time `json:"created_at"`
}

func (w *Worker) writeProcessed(ctx context.Context, event RawEvent, records []parser.Record) (string, error) {
	if w.cfg.S3Client == nil {
		return "", fmt.Errorf("s3 client is not configured")
	}
	if w.cfg.ProcessedBucket == "" {
		return "", fmt.Errorf("processed bucket is not configured")
	}

	baseKey := processedBaseKey(event)
	chunkSize := recordsPerChunk(event.Size, len(records))
	if chunkSize <= 0 {
		chunkSize = len(records)
	}

	if len(records) == 0 {
		return "", fmt.Errorf("no records to write")
	}

	var keys []string
	partNum := 1

	for start := 0; start < len(records); start += chunkSize {
		end := min(start+chunkSize, len(records))
		payload, err := encodeParquet(records[start:end])
		if err != nil {
			return "", fmt.Errorf("failed to encode parquet chunk %d: %w", partNum, err)
		}

		key := baseKey
		// If total records exceed a single chunk size, it's a multi-part upload
		if len(records) > chunkSize {
			key = processedPartKey(baseKey, partNum)
		}

		if err := w.putProcessedObject(ctx, key, payload); err != nil {
			return "", err
		}
		keys = append(keys, key)
		partNum++
	}

	if len(keys) == 1 {
		return keys[0], nil
	}

	manifestKey := processedManifestKey(baseKey)
	manifest := processedManifest{
		BaseKey:     baseKey,
		Parts:       keys,
		RecordCount: len(records),
		CreatedAt:   time.Now().UTC(),
	}
	if err := w.putProcessedManifest(ctx, manifestKey, manifest); err != nil {
		return "", err
	}

	return manifestKey, nil
}

func (w *Worker) putProcessedObject(ctx context.Context, key string, payload []byte) error {
	_, err := w.cfg.S3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &w.cfg.ProcessedBucket,
		Key:    &key,
		Body:   bytes.NewReader(payload),
	})
	if err != nil {
		return fmt.Errorf("failed to upload processed object %s: %w", key, err)
	}
	return nil
}

func (w *Worker) putProcessedManifest(ctx context.Context, key string, manifest processedManifest) error {
	payload, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("failed to marshal processed manifest: %w", err)
	}
	_, err = w.cfg.S3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &w.cfg.ProcessedBucket,
		Key:    &key,
		Body:   bytes.NewReader(payload),
	})
	if err != nil {
		return fmt.Errorf("failed to upload processed manifest %s: %w", key, err)
	}
	return nil
}

func encodeParquet(records []parser.Record) ([]byte, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("cannot encode empty records")
	}

	// Union all keys across all records so sparse rows don't lose columns
	seen := make(map[string]struct{})
	for _, rec := range records {
		for k := range rec {
			seen[k] = struct{}{}
		}
	}

	// Build schema from discovered keys
	group := make(parquet.Group)
	for k := range seen {
		group[k] = parquet.Optional(parquet.String())
	}
	schema := parquet.NewSchema("record", group)

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[map[string]any](
		&buf,
		schema,
		parquet.Compression(&parquet.Snappy),
	)

	if _, err := writer.Write(records); err != nil {
		return nil, fmt.Errorf("write parquet records: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close parquet writer: %w", err)
	}
	return buf.Bytes(), nil
}

func processedBaseKey(event RawEvent) string {
	key := event.Metadata["key"]
	if key == "" {
		key = event.Path
		if trimmed, found := strings.CutPrefix(key, "s3://"); found {
			if _, remainder, foundCut := strings.Cut(trimmed, "/"); foundCut {
				key = remainder
			} else {
				key = trimmed
			}
		}
	}

	key = strings.TrimPrefix(key, "/")
	if ext := path.Ext(key); ext != "" {
		key = strings.TrimSuffix(key, ext)
	}
	return key + ".parquet"
}

func processedPartKey(baseKey string, part int) string {
	base := strings.TrimSuffix(baseKey, path.Ext(baseKey))
	return fmt.Sprintf("%s.part-%05d.parquet", base, part)
}

func processedManifestKey(baseKey string) string {
	base := strings.TrimSuffix(baseKey, path.Ext(baseKey))
	return base + ".manifest.json"
}

func recordsPerChunk(totalBytes int64, recordCount int) int {
	if recordCount <= 0 {
		return 0
	}
	if totalBytes <= 0 {
		return recordCount
	}

	bytesPerRecord := max(1, totalBytes/int64(recordCount))

	targetBytes := int64((processedMinSizeBytes + processedMaxSizeBytes) / 2)
	targetRecords := max(1, int(targetBytes/bytesPerRecord))
	maxRecords := max(1, int(int64(processedMaxSizeBytes)/bytesPerRecord))

	return min(targetRecords, maxRecords)
}
