package sink

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"k8s-log-etl/internal/config"
)

// Build constructs a sink based on config.
func Build(ctx context.Context, cfg config.Config) (Writer, error) {
	switch strings.ToLower(cfg.OutputType) {
	case "", "stdout":
		return NewJSONLSink(nopCloser{os.Stdout}), nil
	case "file":
		if cfg.OutputPath == "" {
			return nil, fmt.Errorf("%w: output path required for file sink", ErrOpenSink)
		}
		f, err := os.Create(cfg.OutputPath)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrOpenSink, err)
		}
		return NewJSONLSink(f), nil
	case "rotate", "rotating":
		if cfg.OutputPath == "" {
			return nil, fmt.Errorf("%w: output path required for rotating sink", ErrOpenSink)
		}
		maxBytes := cfg.OutputMaxB
		if maxBytes <= 0 {
			maxBytes = 10 * 1024 * 1024 // fallback
		}
		maxFiles := cfg.OutputMaxFiles
		if maxFiles <= 0 {
			maxFiles = 5
		}
		return NewRotatingJSONLSink(cfg.OutputPath, maxBytes, maxFiles)
	case "http", "webhook":
		if cfg.OutputPath == "" {
			return nil, fmt.Errorf("%w: output URL required for http sink", ErrOpenSink)
		}
		return NewHTTPSink(ctx, cfg.OutputPath, cfg.SinkMaxRetries, time.Duration(cfg.SinkBackoffBaseMS)*time.Millisecond)
	case "s3":
		// S3 sink would require AWS SDK - placeholder for now
		return nil, fmt.Errorf("%w: S3 sink not yet implemented (requires AWS SDK)", ErrOpenSink)
	case "kafka":
		// Kafka sink would require Kafka client - placeholder for now
		return nil, fmt.Errorf("%w: Kafka sink not yet implemented (requires Kafka client library)", ErrOpenSink)
	default:
		return nil, fmt.Errorf("%w: unknown output type %q", ErrOpenSink, cfg.OutputType)
	}
}

type nopCloser struct {
	w *os.File
}

func (n nopCloser) Write(p []byte) (int, error) { return n.w.Write(p) }
func (n nopCloser) Close() error                { return nil }
