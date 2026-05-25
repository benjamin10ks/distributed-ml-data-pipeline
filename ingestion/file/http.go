package file

import (
	"log/slog"
	"net/http"
)

type httpAdapter struct {
	addr   string
	events chan RawEvent
	logger *slog.Logger
}

func NewHTTPAdapter(addr string, logger *slog.Logger) (*httpAdapter, error) {
	return &httpAdapter{
		addr:   addr,
		logger: logger,
		events: make(chan RawEvent, 256), // Buffered channel to hold events
	}, nil
}

func (h *httpAdapter) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /ingest/events", h.handleIngest)
}

func (h *httpAdapter) Events() <-chan RawEvent {
	return h.events
}

func (h *httpAdapter) handleIngest(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
