package logger

import (
	"context"
	"log/slog"
	"os"
)

var defaultLogger *slog.Logger

func init() {
	// Default to JSON handler for structured logs
	defaultLogger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}

// SetLogger sets the global logger instance.
func SetLogger(l *slog.Logger) {
	defaultLogger = l
}

// SetTextLogger configures the logger to use text output instead of JSON.
func SetTextLogger() {
	defaultLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}

// SetLevel sets the log level.
func SetLevel(level slog.Level) {
	defaultLogger = slog.New(defaultLogger.Handler().WithOptions(slog.HandlerOptions{
		Level: level,
	}))
}

// Logger returns the default logger.
func Logger() *slog.Logger {
	return defaultLogger
}

// WithContext returns a logger with context values attached.
func WithContext(ctx context.Context) *slog.Logger {
	if ctx == nil {
		return defaultLogger
	}
	
	// Extract trace ID from context if available
	if traceID := ctx.Value("trace_id"); traceID != nil {
		return defaultLogger.With("trace_id", traceID)
	}
	
	return defaultLogger
}

// Info logs at Info level.
func Info(msg string, args ...any) {
	defaultLogger.Info(msg, args...)
}

// InfoContext logs at Info level with context.
func InfoContext(ctx context.Context, msg string, args ...any) {
	WithContext(ctx).Info(msg, args...)
}

// Error logs at Error level.
func Error(msg string, args ...any) {
	defaultLogger.Error(msg, args...)
}

// ErrorContext logs at Error level with context.
func ErrorContext(ctx context.Context, msg string, args ...any) {
	WithContext(ctx).Error(msg, args...)
}

// Warn logs at Warn level.
func Warn(msg string, args ...any) {
	defaultLogger.Warn(msg, args...)
}

// WarnContext logs at Warn level with context.
func WarnContext(ctx context.Context, msg string, args ...any) {
	WithContext(ctx).Warn(msg, args...)
}

// Debug logs at Debug level.
func Debug(msg string, args ...any) {
	defaultLogger.Debug(msg, args...)
}

// DebugContext logs at Debug level with context.
func DebugContext(ctx context.Context, msg string, args ...any) {
	WithContext(ctx).Debug(msg, args...)
}

