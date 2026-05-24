package file

import "context"

type IngestionsAdapter interface {
	Start(ctx context.Context) error
	Events() <-chan RawEvent
}

type RawEvent struct {
	Source     string
	Payload    []byte
	Format     string
	ReceivedAt int64
	Metadata   map[string]string
}
