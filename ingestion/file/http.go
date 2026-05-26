package file

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type httpAdapter struct {
	client        *s3.Client
	landingBucket string
	events        chan RawEvent
	logger        *slog.Logger
}

func NewHTTPAdapter(logger *slog.Logger) (*httpAdapter, error) {
	return &httpAdapter{
		logger: logger,
		events: make(chan RawEvent, 256), // Buffered channel to hold events
	}, nil
}

func (h *httpAdapter) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /ingest/events/{source}", h.handleIngest)
}

func (h *httpAdapter) Events() <-chan RawEvent {
	return h.events
}

func (h *httpAdapter) handleIngest(w http.ResponseWriter, r *http.Request) {
	source := r.PathValue("source")
	filename := r.Header.Get("X-Filename")
	if filename == "" {
		filename = fmt.Sprintf("%d.json", time.Now().UnixNano())
	}
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Error("failed to read request body", "error", err)
		return
	}
	err = h.writeToLanding(r.Context(), source, filename, body)
	if err != nil {
		h.logger.Error("failed to write to landing", "error", err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *httpAdapter) writeToLanding(ctx context.Context, source, filename string, body []byte) error {
	key := landingKey(source, filename, time.Now())

	_, err := h.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(h.landingBucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	return err
}

func landingKey(source, filename string, t time.Time) string {
	// source=crm/date=2026-05-17/orders.csv
	return fmt.Sprintf("source=%s/date=%s/%s", source, t.UTC().Format("2007-01-02"), filename)
}
