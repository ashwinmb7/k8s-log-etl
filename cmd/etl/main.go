package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"k8s-log-etl/internal/config"
	"k8s-log-etl/internal/model"
	"k8s-log-etl/internal/plugins"
	"k8s-log-etl/internal/report"
	"k8s-log-etl/internal/sink"
	"k8s-log-etl/internal/stages"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	cfg = config.Merge(cfg, override)

	rep := report.NewReport()
	in, closeFn, err := inputReader(cfg.InputPath)
	if err != nil {
		log.Fatalf("open input: %v", err)
	}
	if closeFn != nil {
		defer closeFn()
	}

	if err := runPipeline(in, cfg, rep); err != nil {
		log.Fatal(err)
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
}

func runPipeline(in io.Reader, cfg config.Config, rep *report.Report) error {
	transforms, err := plugins.BuildTransforms(cfg)
	if err != nil {
		return fmt.Errorf("load transforms: %w", err)
	}
	sinkWriter, err := sink.Build(cfg)
	if err != nil {
		return fmt.Errorf("open sink: %w", err)
	}
	defer sinkWriter.Close()
	lockedSink := &lockedWriter{w: sinkWriter}

	var dlqWriter *lockedWriter
	if cfg.DLQPath != "" {
		dlq, err := openDLQ(cfg.DLQPath)
		if err != nil {
			return fmt.Errorf("open dlq: %w", err)
		}
		dlqWriter = &lockedWriter{w: dlq}
		defer dlqWriter.Close()
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

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for item := range queue {
				if err := writeWithRetry(lockedSink, item.record, cfg); err != nil {
					rep.AddWriteFailed()
					if dlqWriter != nil {
						_ = dlqWriter.Write(dlqRecord{Record: item.record, Reason: err.Error()})
						rep.AddDLQ()
					}
					continue
				}
				rep.AddWriteOK()
			}
		}()
	}

	for scanner.Scan() {
		line := scanner.Text()
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}

		rep.TotalLines++

		var js map[string]interface{}
		if err := json.Unmarshal([]byte(line), &js); err != nil {
			rep.JSONFailed++
			continue
		}

		rep.JSONParsed++
		normalized, normerr := stages.Normalize(js)
		if normerr != nil {
			rep.NormalizedFailed++
			fmt.Fprintf(os.Stderr, "Normalization error: %v\n", normerr)
			continue
		}

		rep.NormalizedOK++
		rep.AddLevel(normalized.Level)
		rep.AddService(normalized.Service)

		skipped := false
		for _, tf := range transforms {
			nn, drop, reason, err := tf(normalized)
			if err != nil {
				rep.NormalizedFailed++
				fmt.Fprintf(os.Stderr, "Transform error: %v\n", err)
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
		if skipped {
			continue
		}

		queue <- workItem{record: normalized}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	close(queue)
	wg.Wait()

	rep.SetDuration(time.Since(start))
	if err := rep.WriteJSON(cfg.ReportPath); err != nil {
		return err
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

func writeWithRetry(w sink.Writer, record any, cfg config.Config) error {
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
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err = w.Write(record); err == nil {
			return nil
		}

		if attempt == maxRetries {
			break
		}

		sleep := base << attempt
		if sleep > max {
			sleep = max
		}
		jitter := time.Duration(rand.Float64() * float64(sleep) * jitterPct)
		time.Sleep(sleep + jitter)
	}
	return err
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
