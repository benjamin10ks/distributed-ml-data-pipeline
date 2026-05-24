package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type traceKey struct{}

func WithTraceID(ctx context.Context, traceID string) context.Context {
	if traceID == "" {
		return ctx
	}
	return context.WithValue(ctx, traceKey{}, traceID)
}

func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if value, ok := ctx.Value(traceKey{}).(string); ok {
		return value
	}
	return ""
}

type contextHandler struct {
	handler slog.Handler
}

func (h contextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}

func (h contextHandler) Handle(ctx context.Context, record slog.Record) error {
	if traceID := TraceIDFromContext(ctx); traceID != "" {
		record.AddAttrs(slog.String("trace_id", traceID))
	}
	return h.handler.Handle(ctx, record)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{handler: h.handler.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{handler: h.handler.WithGroup(name)}
}

func NewLogger(serviceName, level string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLevel(level),
	})
	logger := slog.New(contextHandler{handler: handler})
	if serviceName != "" {
		logger = logger.With("service", serviceName)
	}
	return logger
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
