package file_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/benjamin10ks/distributed-ml-data-pipeline/ingestion/file"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestUploadFileEndToEnd(t *testing.T) {
	cfg := integrationConfigFromEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db := mustPool(t, ctx, cfg.databaseURL)
	defer db.Close()

	if err := file.RunMigrations(cfg.databaseURL); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	s3Client := mustS3Client(t, cfg)
	ensureBucket(t, ctx, s3Client, cfg.landingBucket)
	ensureBucket(t, ctx, s3Client, cfg.processedBucket)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	s3Adapter, err := file.NewS3Adapter(cfg.landingBucket, cfg.s3Endpoint, cfg.s3AccessKey, cfg.s3SecretKey, logger)
	if err != nil {
		t.Fatalf("create S3 adapter: %v", err)
	}

	mux := http.NewServeMux()
	s3Adapter.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	worker, err := file.NewWorker(file.WorkerConfig{
		Adapters:         []file.IngestionsAdapter{s3Adapter},
		ProcessedBucket:  cfg.processedBucket,
		QuarantineBucket: cfg.quarantineBucket,
		DB:               db,
		S3Client:         s3Client,
		Logger:           logger,
	})
	if err != nil {
		t.Fatalf("create worker: %v", err)
	}

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	workerErr := make(chan error, 1)
	go func() {
		workerErr <- worker.Run(workerCtx)
	}()

	payload := []byte("name,score\nalice,42\n")
	source := "integration"
	filename := "sample.csv"
	day := time.Now().UTC().Format("2006-01-02")
	landingKey := fmt.Sprintf("source=%s/date=%s/%s", source, day, filename)
	expectedHash := sha256.Sum256(payload)
	expectedHashHex := hex.EncodeToString(expectedHash[:])
	expectedProcessedKey := fmt.Sprintf("source=%s/date=%s/%s", source, day, strings.TrimSuffix(filename, filepath.Ext(filename))+".parquet")

	if _, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &cfg.landingBucket,
		Key:    &landingKey,
		Body:   bytes.NewReader(payload),
	}); err != nil {
		t.Fatalf("upload landing object: %v", err)
	}

	notification := map[string]any{
		"Records": []map[string]any{
			{
				"s3": map[string]any{
					"bucket": map[string]any{"name": cfg.landingBucket},
					"object": map[string]any{"key": landingKey, "eTag": "ignored"},
				},
			},
		},
	}
	body, err := json.Marshal(notification)
	if err != nil {
		t.Fatalf("marshal notification: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/minio/events", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build notification request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post notification: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("notification status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	entry := waitForManifestDone(t, ctx, file.NewManifest(db, logger), expectedHashHex, workerErr)
	if entry.Status != file.StatusDone {
		t.Fatalf("manifest status = %s, want %s", entry.Status, file.StatusDone)
	}
	if entry.ContentHash != expectedHashHex {
		t.Fatalf("manifest hash = %s, want %s", entry.ContentHash, expectedHashHex)
	}

	if _, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &cfg.processedBucket,
		Key:    &expectedProcessedKey,
	}); err != nil {
		t.Fatalf("processed object missing: %v", err)
	}

	workerCancel()
	select {
	case err := <-workerErr:
		if err != nil {
			t.Fatalf("worker stopped with error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop")
	}

	t.Cleanup(func() {
		db.Exec(context.Background(),
			"DELETE FROM file_manifest WHERE content_hash = $1", expectedHashHex)
		s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &cfg.processedBucket,
			Key:    &expectedProcessedKey,
		})
	})
}

type integrationConfig struct {
	databaseURL      string
	s3Endpoint       string
	s3AccessKey      string
	s3SecretKey      string
	landingBucket    string
	processedBucket  string
	quarantineBucket string
}

func integrationConfigFromEnv(t *testing.T) integrationConfig {
	t.Helper()

	cfg := integrationConfig{
		databaseURL:      os.Getenv("DATABASE_URL"),
		s3Endpoint:       os.Getenv("S3_ENDPOINT"),
		s3AccessKey:      os.Getenv("S3_ACCESS_KEY"),
		s3SecretKey:      os.Getenv("S3_SECRET_KEY"),
		landingBucket:    getenvDefault("LANDING_BUCKET", "landing"),
		processedBucket:  getenvDefault("PROCESSED_BUCKET", "processed"),
		quarantineBucket: getenvDefault("QUARANTINE_BUCKET", "quarantine"),
	}

	if cfg.databaseURL == "" || cfg.s3Endpoint == "" || cfg.s3AccessKey == "" || cfg.s3SecretKey == "" {
		t.Skip("integration test requires DATABASE_URL, S3_ENDPOINT, S3_ACCESS_KEY, and S3_SECRET_KEY")
	}

	return cfg
}

func mustPool(t *testing.T, ctx context.Context, databaseURL string) *pgxpool.Pool {
	t.Helper()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create pg pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping pg: %v", err)
	}
	return pool
}

func mustS3Client(t *testing.T, cfg integrationConfig) *s3.Client {
	t.Helper()

	awsCfg, err := awscfg.LoadDefaultConfig(
		context.Background(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.s3AccessKey, cfg.s3SecretKey, "")),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = &cfg.s3Endpoint
		o.UsePathStyle = true
	})
}

func ensureBucket(t *testing.T, ctx context.Context, client *s3.Client, bucket string) {
	t.Helper()

	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &bucket})
	if err == nil {
		return
	}
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("create bucket %s: %v", bucket, err)
	}
}

func waitForManifestDone(t *testing.T, ctx context.Context, manifest *file.Manifest, hash string, workerErr <-chan error) *file.ManifestEntry {
	t.Helper()

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(30 * time.Second)
	var lastErr error

	// Trigger an immediate check before the first ticker fires
	entry, err := manifest.GetByHash(ctx, hash)
	if err == nil && entry != nil && entry.Status == file.StatusDone {
		return entry
	}
	lastErr = err

	for {
		select {
		case err := <-workerErr:
			// If the worker returns anything (error or nil) before we finish
			// polling, it stopped prematurely. Fail immediately.
			if err != nil {
				t.Fatalf("worker crashed unexpectedly during polling: %v", err)
			}
			t.Fatal("worker stopped unexpectedly without an error during polling")

		case <-timeout:
			t.Fatalf("manifest did not reach done for %s: last error: %v", hash, lastErr)

		case <-ticker.C:
			entry, err := manifest.GetByHash(ctx, hash)
			if err == nil && entry.Status == file.StatusDone {
				return entry
			}
			lastErr = err
		}
	}
}

func getenvDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
