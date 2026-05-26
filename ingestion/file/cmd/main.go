package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/benjamin10ks/distributed-ml-data-pipeline/ingestion/file"
	"github.com/benjamin10ks/distributed-ml-data-pipeline/internal/config"
	"github.com/benjamin10ks/distributed-ml-data-pipeline/internal/logging"
)

func main() {
	cfg := config.ConfigFromEnv()

	logger := logging.NewLogger("ingestion-file", cfg.LogLevel)
	slog.SetDefault(logger)

	mux := http.NewServeMux()

	s3Adapter, err := file.NewS3Adapter(
		cfg.LandingBucket,
		cfg.S3Endpoint,
		cfg.S3AccessKey,
		cfg.S3SecretKey,
		logger,
	)
	if err != nil {
		logger.Error("failed to create S3 adapter", "error", err)
		os.Exit(1)
	}

	s3Adapter.Register(mux)

	httpAdapter, err := file.NewHTTPAdapter(logger)
	if err != nil {
		logger.Error("failed to create HTTP adapter", "error", err)
		os.Exit(1)
	}
	httpAdapter.Register(mux)

	srv := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: mux,
	}

	worker, err := file.NewWorker(file.WorkerConfig{
		Adapters:         []file.IngestionsAdapter{s3Adapter, httpAdapter},
		ProcessedBucket:  cfg.ProcessedBucket,
		QuarantineBucket: cfg.QuarantineBucket,
		DB:               nil, // mustOpenDB(cfg.DatabaseURL),
		Logger:           logger,
	})
	if err != nil {
		logger.Error("failed to create worker", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("http server starting", "listen_addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil {
			logger.Error("HTTP server error", "error", err)
			stop()
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	if err := worker.Run(ctx); err != nil {
		logger.Error("worker error", "error", err)
		os.Exit(1)
	}

	logger.InfoContext(
		ctx, "ingestion file service starting",
		"environment", cfg.Environment,
		"listen_addr", cfg.ListenAddr,
	)
	<-ctx.Done()
	logger.Info("ingestion file service stopping")
}
