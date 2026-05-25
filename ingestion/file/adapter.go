package file

import (
	"context"
	"net/http"
	"time"
)

type IngestionsAdapter interface {
	Register(mux *http.ServeMux)
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

type Runner interface {
	Run(ctx context.Context) error
}
