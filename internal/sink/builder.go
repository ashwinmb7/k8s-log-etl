package sink

import (
	"fmt"
	"os"
	"strings"

	"k8s-log-etl/internal/config"
)

// Build constructs a sink based on config.
func Build(cfg config.Config) (Writer, error) {
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
	default:
		return nil, fmt.Errorf("%w: unknown output type %q", ErrOpenSink, cfg.OutputType)
	}
}

type nopCloser struct {
	w *os.File
}

func (n nopCloser) Write(p []byte) (int, error) { return n.w.Write(p) }
func (n nopCloser) Close() error                { return nil }
