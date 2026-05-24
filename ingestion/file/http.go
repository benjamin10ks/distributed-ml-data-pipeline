package file

import "net/http"

type httpAdapter struct {
	addr   string
	events chan RawEvent
}

func NewHTTPAdapter(addr string) *httpAdapter {
	return &httpAdapter{
		addr:   addr,
		events: make(chan RawEvent, 100), // Buffered channel to hold events
	}
}

func (h *httpAdapter) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /ingest/events", h.handleIngest)
	return http.ListenAndServe(h.addr, mux)
}

func (h *httpAdapter) Events() <-chan RawEvent {
	return h.events
}

func (h *httpAdapter) handleIngest(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
