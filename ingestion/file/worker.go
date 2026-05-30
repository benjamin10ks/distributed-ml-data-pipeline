package file

import (
	"context"
	"database/sql"
	"log/slog"
)

type Worker struct {
	adapters []IngestionsAdapter
	logger   *slog.Logger
}

type WorkerConfig struct {
	Adapters         []IngestionsAdapter
	ProcessedBucket  string
	QuarantineBucket string
	manifest         *Manifest
	DB               *sql.DB
	Logger           *slog.Logger
}

func NewWorker(cfg WorkerConfig) (*Worker, error) {
	return &Worker{
		adapters: cfg.Adapters,
		logger:   cfg.Logger,
	}, nil
}

func (w *Worker) Adapters() []IngestionsAdapter {
	if w == nil {
		return nil
	}
	return w.adapters
}

func (w *Worker) Run(ctx context.Context) error {
	return nil
}
