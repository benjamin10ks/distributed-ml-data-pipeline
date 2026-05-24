package file

import "net/http"

type httpAdapter struct {
	addr   string
	events chan RawEvent
}

func (h *httpAdapter) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /ingest/events", h.handleIngest)
	return http.ListenAndServe(h.addr, mux)
}

func (h *httpAdapter) Events() <-chan RawEvent {
	return h.events
}
