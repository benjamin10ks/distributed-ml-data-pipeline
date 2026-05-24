package file

import (
	"context"
	"time"
)

type IngestionsAdapter interface {
	Start(ctx context.Context) error
	Events() <-chan RawEvent
}

type RawEvent struct {
	Source      string
	Payload     []byte
	Format      string
	Path        string
	ContentHash string
	Size        int64
	ReceivedAt  time.Time
	Metadata    map[string]string
}
