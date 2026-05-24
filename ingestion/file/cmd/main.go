package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/benjamin10ks/distributed-ml-data-pipeline/internal/config"
	"github.com/benjamin10ks/distributed-ml-data-pipeline/internal/logging"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("failed to load config", "error", err)
		os.Exit(1)
	}

	logger := logging.NewLogger(cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if cfg.TraceID != "" {
		ctx = logging.WithTraceID(ctx, cfg.TraceID)
	}

	logger.InfoContext(ctx, "ingestion file service starting", "environment", cfg.Environment)
	<-ctx.Done()
	logger.Info("ingestion file service stopping")
}
