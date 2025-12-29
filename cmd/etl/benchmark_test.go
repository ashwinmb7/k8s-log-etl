package main

import (
	"context"
	"strings"
	"testing"

	"k8s-log-etl/internal/config"
	"k8s-log-etl/internal/report"
)

func BenchmarkPipeline_NoBatching(b *testing.B) {
	// Create test input
	var input strings.Builder
	for i := 0; i < 1000; i++ {
		input.WriteString(`{"ts":"2024-01-01T12:00:00Z","level":"ERROR","msg":"test message","service":"test-service"}`)
		input.WriteString("\n")
	}

	cfg := config.Default()
	cfg.OutputType = "stdout"
	cfg.BatchSize = 1 // No batching

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rep := report.NewReport()
		ctx := context.Background()
		_ = runPipeline(ctx, strings.NewReader(input.String()), cfg, rep)
	}
}

func BenchmarkPipeline_WithBatching(b *testing.B) {
	// Create test input
	var input strings.Builder
	for i := 0; i < 1000; i++ {
		input.WriteString(`{"ts":"2024-01-01T12:00:00Z","level":"ERROR","msg":"test message","service":"test-service"}`)
		input.WriteString("\n")
	}

	cfg := config.Default()
	cfg.OutputType = "stdout"
	cfg.BatchSize = 100
	cfg.BatchFlushInterval = 100

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rep := report.NewReport()
		ctx := context.Background()
		_ = runPipeline(ctx, strings.NewReader(input.String()), cfg, rep)
	}
}

func BenchmarkPipeline_MultipleWorkers(b *testing.B) {
	// Create test input
	var input strings.Builder
	for i := 0; i < 1000; i++ {
		input.WriteString(`{"ts":"2024-01-01T12:00:00Z","level":"ERROR","msg":"test message","service":"test-service"}`)
		input.WriteString("\n")
	}

	cfg := config.Default()
	cfg.OutputType = "stdout"
	cfg.MaxWorkers = 4
	cfg.QueueSize = 128

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rep := report.NewReport()
		ctx := context.Background()
		_ = runPipeline(ctx, strings.NewReader(input.String()), cfg, rep)
	}
}

