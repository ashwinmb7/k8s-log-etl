package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s-log-etl/internal/config"
	"k8s-log-etl/internal/report"
	"k8s-log-etl/internal/sink"
)

func TestRunPipeline_Basic(t *testing.T) {
	input := `{"ts":"2024-01-01T12:00:00Z","level":"ERROR","msg":"test message","service":"test-service"}
{"ts":"2024-01-01T12:00:01Z","level":"INFO","msg":"info message","service":"test-service"}
`
	cfg := config.Default()
	cfg.OutputType = "stdout"
	cfg.FilterLevels = []string{"ERROR"}

	rep := report.NewReport()
	ctx := context.Background()

	err := runPipeline(ctx, strings.NewReader(input), cfg, rep)
	if err != nil {
		t.Fatalf("runPipeline: %v", err)
	}

	if rep.TotalLines != 2 {
		t.Errorf("expected 2 total lines, got %d", rep.TotalLines)
	}
	if rep.JSONParsed != 2 {
		t.Errorf("expected 2 parsed, got %d", rep.JSONParsed)
	}
	if rep.NormalizedOK != 2 {
		t.Errorf("expected 2 normalized, got %d", rep.NormalizedOK)
	}
	if rep.WrittenOK != 1 {
		t.Errorf("expected 1 written (filtered 1), got %d", rep.WrittenOK)
	}
}

func TestRunPipeline_ContextCancellation(t *testing.T) {
	// Create a large input
	var input strings.Builder
	for i := 0; i < 1000; i++ {
		input.WriteString(`{"ts":"2024-01-01T12:00:00Z","level":"ERROR","msg":"test","service":"test"}`)
		input.WriteString("\n")
	}

	cfg := config.Default()
	cfg.OutputType = "stdout"

	rep := report.NewReport()
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := runPipeline(ctx, strings.NewReader(input.String()), cfg, rep)
	if err == nil {
		t.Error("expected error due to context cancellation")
	}
	if err != context.Canceled {
		t.Logf("got error (may be timeout): %v", err)
	}
}

func TestRunPipeline_WithBatching(t *testing.T) {
	var input strings.Builder
	for i := 0; i < 10; i++ {
		input.WriteString(`{"ts":"2024-01-01T12:00:00Z","level":"ERROR","msg":"test","service":"test"}`)
		input.WriteString("\n")
	}

	cfg := config.Default()
	cfg.OutputType = "stdout"
	cfg.BatchSize = 5
	cfg.BatchFlushInterval = 100

	rep := report.NewReport()
	ctx := context.Background()

	err := runPipeline(ctx, strings.NewReader(input.String()), cfg, rep)
	if err != nil {
		t.Fatalf("runPipeline: %v", err)
	}

	if rep.WrittenOK != 10 {
		t.Errorf("expected 10 written, got %d", rep.WrittenOK)
	}
}

func TestWriteWithRetry_ContextCancellation(t *testing.T) {
	cfg := config.Default()
	rep := report.NewReport()

	// Create a sink that always fails
	failingSink := &failingWriter{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := writeWithRetry(ctx, failingSink, "test", cfg, rep)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

type failingWriter struct{}

func (fw *failingWriter) Write(interface{}) error {
	return sink.ErrWriteSink
}

func (fw *failingWriter) Close() error {
	return nil
}

