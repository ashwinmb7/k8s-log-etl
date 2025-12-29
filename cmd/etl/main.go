package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"k8s-log-etl/internal/config"
	"k8s-log-etl/internal/logger"
	"k8s-log-etl/internal/model"
	"k8s-log-etl/internal/plugins"
	"k8s-log-etl/internal/report"
	"k8s-log-etl/internal/sink"
	"k8s-log-etl/internal/stages"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	// Flags with env + config file override support.
	flagConfig := flag.String("config", "", "path to YAML or JSON config file")
	flagInput := flag.String("input", "", "input JSONL path (use '-' for stdin)")
	flagOutput := flag.String("output", "", "output path (use '-' for stdout)")
	flagOutputType := flag.String("output-type", "", "sink type: stdout|file|rotate (default stdout)")
	flagOutputMaxBytes := flag.Int64("output-max-bytes", 0, "max bytes before rotation when using rotate sink")
	flagOutputMaxFiles := flag.Int("output-max-files", 0, "max rotated files to keep when using rotate sink")
	flagReport := flag.String("report", "", "report output path")
	flagMaxWorkers := flag.Int("max-workers", 0, "number of sink workers")
	flagQueueSize := flag.Int("queue-size", 0, "bounded queue size between normalize and sink")
	flagSinkRetries := flag.Int("sink-max-retries", 0, "max retries for sink writes")
	flagBackoffBase := flag.Int("sink-backoff-base-ms", 0, "base backoff in ms for sink retries")
	flagBackoffMax := flag.Int("sink-backoff-max-ms", 0, "max backoff in ms for sink retries")
	flagBackoffJitter := flag.Float64("sink-backoff-jitter-pct", 0, "jitter pct (0.2 = 20%) for sink retries")
	flagDLQ := flag.String("dlq", "", "dead-letter path for failed records (jsonl). 's3://...' not supported.")
	flagFilterLevels := flag.String("filter-levels", "", "comma-separated levels to emit (e.g. WARN,ERROR)")
	flagFilterServices := flag.String("filter-services", "", "comma-separated services to emit (case-insensitive)")
	flagRedactKeys := flag.String("redact-keys", "", "comma-separated field keys to redact from extra fields")
	flagBatchSize := flag.Int("batch-size", 0, "batch size for sink writes (0 = no batching)")
	flagBatchFlushInterval := flag.Int("batch-flush-interval-ms", 0, "batch flush interval in milliseconds")
	flagShutdownTimeout := flag.Int("shutdown-timeout-seconds", 0, "graceful shutdown timeout in seconds")
	flagLogLevel := flag.String("log-level", "", "log level: debug, info, warn, error")
	flagLogFormat := flag.String("log-format", "", "log format: json, text")
	flag.Parse()

	cfg := config.Default()

	// Load config file if provided by flag or env.
	cfgPath := *flagConfig
	if cfgPath == "" {
		cfgPath = os.Getenv("ETL_CONFIG")
	}
	if cfgPath != "" {
		fileCfg, err := config.Load(cfgPath)
		if err != nil {
			log.Fatalf("load config: %v", err)
		}
		cfg = config.Merge(cfg, fileCfg)
	}

	// Env overrides.
	cfg = config.FromEnv(cfg)

	// Flag overrides (highest precedence).
	override := config.Config{}
	if *flagInput != "" {
		override.InputPath = *flagInput
	}
	if *flagOutput != "" {
		override.OutputPath = *flagOutput
	}
	if *flagOutputType != "" {
		override.OutputType = *flagOutputType
	}
	if *flagOutputMaxBytes != 0 {
		override.OutputMaxB = *flagOutputMaxBytes
	}
	if *flagOutputMaxFiles != 0 {
		override.OutputMaxFiles = *flagOutputMaxFiles
	}
	if *flagReport != "" {
		override.ReportPath = *flagReport
	}
	if *flagMaxWorkers != 0 {
		override.MaxWorkers = *flagMaxWorkers
	}
	if *flagQueueSize != 0 {
		override.QueueSize = *flagQueueSize
	}
	if *flagSinkRetries != 0 {
		override.SinkMaxRetries = *flagSinkRetries
	}
	if *flagBackoffBase != 0 {
		override.SinkBackoffBaseMS = *flagBackoffBase
	}
	if *flagBackoffMax != 0 {
		override.SinkBackoffMaxMS = *flagBackoffMax
	}
	if *flagBackoffJitter != 0 {
		override.SinkBackoffJitter = *flagBackoffJitter
	}
	if *flagDLQ != "" {
		override.DLQPath = *flagDLQ
	}
	if *flagFilterLevels != "" {
		override.FilterLevels = parseList(*flagFilterLevels)
	}
	if *flagFilterServices != "" {
		override.FilterSvcs = parseList(*flagFilterServices)
	}
	if *flagRedactKeys != "" {
		override.RedactKeys = parseList(*flagRedactKeys)
	}
	if *flagBatchSize != 0 {
		override.BatchSize = *flagBatchSize
	}
	if *flagBatchFlushInterval != 0 {
		override.BatchFlushInterval = *flagBatchFlushInterval
	}
	if *flagShutdownTimeout != 0 {
		override.ShutdownTimeoutSeconds = *flagShutdownTimeout
	}
	if *flagLogLevel != "" {
		override.LogLevel = *flagLogLevel
	}
	if *flagLogFormat != "" {
		override.LogFormat = *flagLogFormat
	}
	cfg = config.Merge(cfg, override)

	// Validate configuration before proceeding
	if err := config.Validate(cfg); err != nil {
		log.Fatalf("configuration validation failed: %v", err)
	}

	// Initialize structured logging
	initLogger(cfg)

	// Create context with signal handling for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	rep := report.NewReport()
	in, closeFn, err := inputReader(cfg.InputPath)
	if err != nil {
		log.Fatalf("open input: %v", err)
	}
	if closeFn != nil {
		defer closeFn()
	}

	// Run pipeline with context for graceful shutdown
	if err := runPipeline(ctx, in, cfg, rep); err != nil {
		logger.ErrorContext(ctx, "pipeline failed", "error", err)
		os.Exit(1)
	}

	fmt.Printf(
		"Total Lines: %d, JSON Parsed: %d, JSON Failed: %d, Normalized OK: %d, Normalized Failed: %d, Written OK: %d\n",
		rep.TotalLines,
		rep.JSONParsed,
		rep.JSONFailed,
		rep.NormalizedOK,
		rep.NormalizedFailed,
		rep.WrittenOK,
	)

	// Print operational metrics
	if rep.StageTimings.ParsingSeconds > 0 || rep.StageTimings.NormalizationSeconds > 0 || rep.StageTimings.FilteringSeconds > 0 || rep.StageTimings.WritingSeconds > 0 {
		fmt.Printf(
			"Stage Timings (seconds): Parsing: %.3f, Normalization: %.3f, Filtering: %.3f, Writing: %.3f\n",
			rep.StageTimings.ParsingSeconds,
			rep.StageTimings.NormalizationSeconds,
			rep.StageTimings.FilteringSeconds,
			rep.StageTimings.WritingSeconds,
		)
	}

	if rep.RetryStats.TotalRetries > 0 {
		fmt.Printf(
			"Retry Stats: Total Retries: %d, Writes with Retries: %d, Max Retries per Write: %d\n",
			rep.RetryStats.TotalRetries,
			rep.RetryStats.WritesWithRetries,
			rep.RetryStats.MaxRetriesPerWrite,
		)
	}

	if rep.DLQWritten > 0 {
		fmt.Printf("DLQ Written: %d", rep.DLQWritten)
		if len(rep.DLQReasons) > 0 {
			fmt.Print(" (Reasons: ")
			reasons := make([]string, 0, len(rep.DLQReasons))
			for reason, count := range rep.DLQReasons {
				reasons = append(reasons, fmt.Sprintf("%s=%d", reason, count))
			}
			fmt.Print(strings.Join(reasons, ", "))
			fmt.Print(")")
		}
		fmt.Println()
	}
}

func initLogger(cfg config.Config) {
	// Set log format
	if strings.ToLower(cfg.LogFormat) == "text" {
		logger.SetTextLogger()
	}

	// Set log level
	var level slog.Level
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	logger.SetLevel(level)
}

func runPipeline(ctx context.Context, in io.Reader, cfg config.Config, rep *report.Report) error {
	logger.InfoContext(ctx, "starting pipeline", "workers", cfg.MaxWorkers, "queue_size", cfg.QueueSize)
	transforms, err := plugins.BuildTransforms(cfg)
	if err != nil {
		return fmt.Errorf("load transforms: %w", err)
	}
	
	// Build sink with batching support
	sinkWriter, err := sink.Build(ctx, cfg)
	if err != nil {
		return fmt.Errorf("open sink: %w", err)
	}
	defer func() {
		if err := sinkWriter.Close(); err != nil {
			logger.ErrorContext(ctx, "error closing sink", "error", err)
		}
	}()
	
	// Wrap sink with batching if configured
	var finalSink sink.Writer = sinkWriter
	if cfg.BatchSize > 1 {
		batchedSink, err := sink.NewBatchedSink(sinkWriter, cfg.BatchSize, time.Duration(cfg.BatchFlushInterval)*time.Millisecond)
		if err != nil {
			return fmt.Errorf("create batched sink: %w", err)
		}
		finalSink = batchedSink
		defer func() {
			if err := batchedSink.Close(); err != nil {
				logger.ErrorContext(ctx, "error closing batched sink", "error", err)
			}
		}()
	}
	
	lockedSink := &lockedWriter{w: finalSink}

	var dlqWriter *lockedWriter
	if cfg.DLQPath != "" {
		dlq, err := openDLQ(cfg.DLQPath)
		if err != nil {
			return fmt.Errorf("open dlq: %w", err)
		}
		dlqWriter = &lockedWriter{w: dlq}
		defer func() {
			if err := dlqWriter.Close(); err != nil {
				logger.ErrorContext(ctx, "error closing DLQ", "error", err)
			}
		}()
	}

	start := time.Now()
	scanner := bufio.NewScanner(in)

	workerCount := cfg.MaxWorkers
	if workerCount <= 0 {
		workerCount = 1
	}
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = 128
	}

	queue := make(chan workItem, queueSize)
	var wg sync.WaitGroup
	wg.Add(workerCount)
	rand.Seed(time.Now().UnixNano())

	// Start workers with context-aware shutdown
	for i := 0; i < workerCount; i++ {
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					logger.DebugContext(ctx, "worker shutting down", "worker_id", workerID)
					return
				case item, ok := <-queue:
					if !ok {
						return
					}
					writeStart := time.Now()
					retries, err := writeWithRetry(ctx, lockedSink, item.record, cfg, rep)
					rep.AddStageTiming("writing", time.Since(writeStart))
					if err != nil {
						rep.AddWriteFailed()
						logger.WarnContext(ctx, "write failed", "error", err, "retries", retries)
						if dlqWriter != nil {
							reason := err.Error()
							if writeErr := dlqWriter.Write(dlqRecord{Record: item.record, Reason: reason}); writeErr != nil {
								logger.ErrorContext(ctx, "failed to write to DLQ", "error", writeErr)
							}
							rep.AddDLQWithReason(reason)
						}
						continue
					}
					rep.AddWriteOK()
					if retries > 0 {
						logger.DebugContext(ctx, "write succeeded after retries", "retries", retries)
					}
				}
			}
		}(i)
	}

	// Main processing loop with context cancellation
	lineNum := 0
	shutdownRequested := false
	for scanner.Scan() {
		// Check for shutdown signal
		select {
		case <-ctx.Done():
			logger.InfoContext(ctx, "shutdown signal received, finishing in-flight records")
			shutdownRequested = true
		default:
		}
		
		if shutdownRequested {
			break
		}

		line := scanner.Text()
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}

		lineNum++
		rep.TotalLines++
		
		// Create context with trace ID for this record
		recordCtx := context.WithValue(ctx, "trace_id", fmt.Sprintf("line-%d", lineNum))

		// Track parsing time
		parseStart := time.Now()
		var js map[string]interface{}
		if err := json.Unmarshal([]byte(line), &js); err != nil {
			rep.JSONFailed++
			rep.AddStageTiming("parsing", time.Since(parseStart))
			logger.DebugContext(recordCtx, "JSON parse failed", "error", err, "line", lineNum)
			continue
		}
		rep.AddStageTiming("parsing", time.Since(parseStart))
		rep.JSONParsed++

		// Track normalization time
		normStart := time.Now()
		normalized, normerr := stages.Normalize(js)
		rep.AddStageTiming("normalization", time.Since(normStart))
		if normerr != nil {
			rep.NormalizedFailed++
			logger.WarnContext(recordCtx, "normalization failed", "error", normerr, "line", lineNum)
			continue
		}

		rep.NormalizedOK++
		rep.AddLevel(normalized.Level)
		rep.AddService(normalized.Service)

		// Track filtering time
		filterStart := time.Now()
		skipped := false
		for _, tf := range transforms {
			nn, drop, reason, err := tf(normalized)
			if err != nil {
				rep.NormalizedFailed++
				logger.WarnContext(recordCtx, "transform error", "error", err, "line", lineNum)
				skipped = true
				break
			}
			if drop {
				rep.AddFiltered(reason)
				skipped = true
				break
			}
			normalized = nn
		}
		rep.AddStageTiming("filtering", time.Since(filterStart))
		if skipped {
			continue
		}

		queue <- workItem{record: normalized}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	// Close queue and wait for workers with timeout
	logger.InfoContext(ctx, "input exhausted, waiting for workers to finish")
	close(queue)
	
	// Wait for workers with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	
	shutdownTimeout := time.Duration(cfg.ShutdownTimeoutSeconds) * time.Second
	if shutdownTimeout <= 0 {
		shutdownTimeout = 30 * time.Second
	}
	
	select {
	case <-done:
		logger.InfoContext(ctx, "all workers finished")
	case <-time.After(shutdownTimeout):
		logger.WarnContext(ctx, "shutdown timeout exceeded, some records may not have been processed", "timeout", shutdownTimeout)
		return fmt.Errorf("shutdown timeout exceeded after %v", shutdownTimeout)
	case <-ctx.Done():
		logger.WarnContext(ctx, "context cancelled during shutdown wait")
		return ctx.Err()
	}

	rep.SetDuration(time.Since(start))
	logger.InfoContext(ctx, "pipeline completed", "duration_seconds", rep.DurationSeconds, "throughput", rep.Throughput)
	
	if err := rep.WriteJSON(cfg.ReportPath); err != nil {
		return fmt.Errorf("write report: %w", err)
	}

	return nil
}

// parseList is a small helper for comma/semicolon-separated values.
func parseList(s string) []string {
	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ';'
	})
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

type workItem struct {
	record model.Normalized
}

type dlqRecord struct {
	Record model.Normalized `json:"record"`
	Reason string           `json:"reason"`
}

func writeWithRetry(ctx context.Context, w sink.Writer, record any, cfg config.Config, rep *report.Report) (int, error) {
	maxRetries := cfg.SinkMaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	base := time.Duration(cfg.SinkBackoffBaseMS) * time.Millisecond
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	max := time.Duration(cfg.SinkBackoffMaxMS) * time.Millisecond
	if max <= 0 {
		max = 2 * time.Second
	}
	jitterPct := cfg.SinkBackoffJitter
	if jitterPct <= 0 {
		jitterPct = 0.2
	}

	var err error
	retries := 0
	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return retries, ctx.Err()
		default:
		}
		
		if err = w.Write(record); err == nil {
			if retries > 0 && rep != nil {
				rep.AddRetry(retries)
			}
			return retries, nil
		}

		if attempt == maxRetries {
			break
		}

		retries++
		sleep := base << attempt
		if sleep > max {
			sleep = max
		}
		jitter := time.Duration(rand.Float64() * float64(sleep) * jitterPct)
		
		// Sleep with context cancellation support
		select {
		case <-ctx.Done():
			return retries, ctx.Err()
		case <-time.After(sleep + jitter):
		}
	}
	if retries > 0 && rep != nil {
		rep.AddRetry(retries)
	}
	return retries, err
}

type lockedWriter struct {
	mu sync.Mutex
	w  sink.Writer
}

func (l *lockedWriter) Write(record any) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(record)
}

func (l *lockedWriter) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Close()
}

func openDLQ(path string) (sink.Writer, error) {
	if strings.HasPrefix(path, "s3://") {
		return nil, fmt.Errorf("DLQ s3 target not supported in this build: %s", path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return sink.NewJSONLSink(f), nil
}

func inputReader(path string) (io.Reader, func(), error) {
	if path == "" || path == "-" {
		return os.Stdin, nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	return f, func() { f.Close() }, nil
}
