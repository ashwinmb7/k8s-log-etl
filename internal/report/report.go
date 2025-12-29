package report

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// Report aggregates ETL processing statistics.
type Report struct {
	TotalLines       int            `json:"total_lines"`
	JSONFailed       int            `json:"json_failed"`
	JSONParsed       int            `json:"json_parsed"`
	NormalizedOK     int            `json:"normalized_ok"`
	NormalizedFailed int            `json:"normalized_failed"`
	WrittenOK        int            `json:"written_ok"`
	WriteFailed      int            `json:"written_failed"`
	ByLevel          map[string]int `json:"by_level"`
	ByService        map[string]int `json:"by_service"`
	Filtered         FilterStats    `json:"filtered"`
	DLQWritten       int            `json:"dlq_written"`
	DurationSeconds  float64        `json:"duration_seconds"`
	Throughput       float64        `json:"throughput_lines_per_sec"`
	JSONErrorRate    float64        `json:"json_error_rate"`
	NormalizeErrRate float64        `json:"normalize_error_rate"`
	WriteErrorRate   float64        `json:"write_error_rate"`
	// Per-stage timings in seconds
	StageTimings StageTimings `json:"stage_timings"`
	// Retry statistics
	RetryStats RetryStats `json:"retry_stats"`
	// DLQ reasons breakdown
	DLQReasons map[string]int `json:"dlq_reasons"`
	mu         sync.Mutex     `json:"-"`
}

type FilterStats struct {
	Level   int `json:"by_level"`
	Service int `json:"by_service"`
	Other   int `json:"other"`
}

// StageTimings tracks time spent in each pipeline stage.
type StageTimings struct {
	ParsingSeconds      float64 `json:"parsing_seconds"`
	NormalizationSeconds float64 `json:"normalization_seconds"`
	FilteringSeconds    float64 `json:"filtering_seconds"`
	WritingSeconds      float64 `json:"writing_seconds"`
}

// RetryStats tracks retry attempts for sink writes.
type RetryStats struct {
	TotalRetries      int `json:"total_retries"`
	WritesWithRetries int `json:"writes_with_retries"`
	MaxRetriesPerWrite int `json:"max_retries_per_write"`
}

// NewReport initializes a Report with maps ready to use.
func NewReport() *Report {
	return &Report{
		ByLevel:   make(map[string]int),
		ByService: make(map[string]int),
		DLQReasons: make(map[string]int),
	}
}

// AddLevel increments the count for a log level.
func (r *Report) AddLevel(level string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if level == "" {
		return
	}
	r.ByLevel[level]++
}

// AddService increments the count for a service.
func (r *Report) AddService(service string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if service == "" {
		return
	}
	r.ByService[service]++
}

// AddFiltered increments filter stats by reason.
func (r *Report) AddFiltered(reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch reason {
	case "level":
		r.Filtered.Level++
	case "service":
		r.Filtered.Service++
	default:
		r.Filtered.Other++
	}
}

// AddWriteOK increments successful writes.
func (r *Report) AddWriteOK() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.WrittenOK++
}

// AddWriteFailed increments failed writes.
func (r *Report) AddWriteFailed() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.WriteFailed++
}

// AddDLQ increments DLQ count.
func (r *Report) AddDLQ() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.DLQWritten++
}

// AddDLQWithReason increments DLQ count and tracks the reason.
func (r *Report) AddDLQWithReason(reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.DLQWritten++
	if reason == "" {
		reason = "unknown"
	}
	r.DLQReasons[reason]++
}

// AddRetry increments retry statistics.
func (r *Report) AddRetry(retries int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.RetryStats.TotalRetries += retries
	if retries > 0 {
		r.RetryStats.WritesWithRetries++
		if retries > r.RetryStats.MaxRetriesPerWrite {
			r.RetryStats.MaxRetriesPerWrite = retries
		}
	}
}

// AddStageTiming adds time to a specific stage.
func (r *Report) AddStageTiming(stage string, duration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	seconds := duration.Seconds()
	switch stage {
	case "parsing":
		r.StageTimings.ParsingSeconds += seconds
	case "normalization":
		r.StageTimings.NormalizationSeconds += seconds
	case "filtering":
		r.StageTimings.FilteringSeconds += seconds
	case "writing":
		r.StageTimings.WritingSeconds += seconds
	}
}

// SetDuration computes derived metrics based on runtime.
func (r *Report) SetDuration(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d <= 0 && r.TotalLines > 0 {
		d = time.Nanosecond
	}
	r.DurationSeconds = d.Seconds()
	if d.Seconds() > 0 {
		r.Throughput = float64(r.TotalLines) / d.Seconds()
	}
	if r.TotalLines > 0 {
		r.JSONErrorRate = float64(r.JSONFailed) / float64(r.TotalLines)
		r.NormalizeErrRate = float64(r.NormalizedFailed) / float64(r.TotalLines)
		writes := r.WrittenOK + r.WriteFailed
		if writes > 0 {
			r.WriteErrorRate = float64(r.WriteFailed) / float64(writes)
		}
	}
}

// WriteJSON writes the report to a JSON file at the given path.
func (r *Report) WriteJSON(path string) error {
	var closer io.Closer
	var w io.Writer
	if path == "" || path == "-" {
		w = os.Stdout
	} else {
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		closer = f
		w = f
	}
	defer func() {
		if closer != nil {
			closer.Close()
		}
	}()

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// Prometheus renders counters/gauges for metrics scraping.
func (r *Report) Prometheus() string {
	sb := &strings.Builder{}
	fmt.Fprintf(sb, "etl_total_lines %d\n", r.TotalLines)
	fmt.Fprintf(sb, "etl_json_failed %d\n", r.JSONFailed)
	fmt.Fprintf(sb, "etl_json_parsed %d\n", r.JSONParsed)
	fmt.Fprintf(sb, "etl_normalized_ok %d\n", r.NormalizedOK)
	fmt.Fprintf(sb, "etl_normalized_failed %d\n", r.NormalizedFailed)
	fmt.Fprintf(sb, "etl_written_ok %d\n", r.WrittenOK)
	fmt.Fprintf(sb, "etl_written_failed %d\n", r.WriteFailed)
	fmt.Fprintf(sb, "etl_dlq_written %d\n", r.DLQWritten)
	fmt.Fprintf(sb, "etl_duration_seconds %.6f\n", r.DurationSeconds)
	fmt.Fprintf(sb, "etl_throughput_lines_per_sec %.6f\n", r.Throughput)
	fmt.Fprintf(sb, "etl_json_error_rate %.6f\n", r.JSONErrorRate)
	fmt.Fprintf(sb, "etl_normalize_error_rate %.6f\n", r.NormalizeErrRate)
	fmt.Fprintf(sb, "etl_write_error_rate %.6f\n", r.WriteErrorRate)
	fmt.Fprintf(sb, "etl_filtered_level %d\n", r.Filtered.Level)
	fmt.Fprintf(sb, "etl_filtered_service %d\n", r.Filtered.Service)
	fmt.Fprintf(sb, "etl_filtered_other %d\n", r.Filtered.Other)
	for k, v := range r.ByLevel {
		fmt.Fprintf(sb, "etl_level_total{level=%q} %d\n", k, v)
	}
	for k, v := range r.ByService {
		fmt.Fprintf(sb, "etl_service_total{service=%q} %d\n", k, v)
	}
	fmt.Fprintf(sb, "etl_stage_timing_seconds{stage=\"parsing\"} %.6f\n", r.StageTimings.ParsingSeconds)
	fmt.Fprintf(sb, "etl_stage_timing_seconds{stage=\"normalization\"} %.6f\n", r.StageTimings.NormalizationSeconds)
	fmt.Fprintf(sb, "etl_stage_timing_seconds{stage=\"filtering\"} %.6f\n", r.StageTimings.FilteringSeconds)
	fmt.Fprintf(sb, "etl_stage_timing_seconds{stage=\"writing\"} %.6f\n", r.StageTimings.WritingSeconds)
	fmt.Fprintf(sb, "etl_retry_total %d\n", r.RetryStats.TotalRetries)
	fmt.Fprintf(sb, "etl_retry_writes_with_retries %d\n", r.RetryStats.WritesWithRetries)
	fmt.Fprintf(sb, "etl_retry_max_per_write %d\n", r.RetryStats.MaxRetriesPerWrite)
	for reason, count := range r.DLQReasons {
		fmt.Fprintf(sb, "etl_dlq_reason_total{reason=%q} %d\n", reason, count)
	}
	return sb.String()
}
