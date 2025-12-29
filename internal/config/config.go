package config

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Config holds ETL runtime options.
type Config struct {
	InputPath         string   `json:"input,omitempty" yaml:"input,omitempty"`
	OutputPath        string   `json:"output,omitempty" yaml:"output,omitempty"`
	ReportPath        string   `json:"report,omitempty" yaml:"report,omitempty"`
	OutputType        string   `json:"output_type,omitempty" yaml:"output_type,omitempty"` // stdout|file|rotate
	OutputMaxB        int64    `json:"output_max_bytes,omitempty" yaml:"output_max_bytes,omitempty"`
	OutputMaxFiles    int      `json:"output_max_files,omitempty" yaml:"output_max_files,omitempty"`
	FilterLevels      []string `json:"filter_levels,omitempty" yaml:"filter_levels,omitempty"`
	FilterSvcs        []string `json:"filter_services,omitempty" yaml:"filter_services,omitempty"`
	RedactKeys        []string `json:"redact_keys,omitempty" yaml:"redact_keys,omitempty"`
	Transforms        []string `json:"transforms,omitempty" yaml:"transforms,omitempty"`
	MaxWorkers        int      `json:"max_workers,omitempty" yaml:"max_workers,omitempty"`
	QueueSize         int      `json:"queue_size,omitempty" yaml:"queue_size,omitempty"`
	SinkMaxRetries    int      `json:"sink_max_retries,omitempty" yaml:"sink_max_retries,omitempty"`
	SinkBackoffBaseMS int      `json:"sink_backoff_base_ms,omitempty" yaml:"sink_backoff_base_ms,omitempty"`
	SinkBackoffMaxMS  int      `json:"sink_backoff_max_ms,omitempty" yaml:"sink_backoff_max_ms,omitempty"`
	SinkBackoffJitter float64  `json:"sink_backoff_jitter_pct,omitempty" yaml:"sink_backoff_jitter_pct,omitempty"`
	DLQPath           string   `json:"dlq,omitempty" yaml:"dlq,omitempty"`
	// Batching configuration
	BatchSize         int      `json:"batch_size,omitempty" yaml:"batch_size,omitempty"`
	BatchFlushInterval int      `json:"batch_flush_interval_ms,omitempty" yaml:"batch_flush_interval_ms,omitempty"`
	// Shutdown configuration
	ShutdownTimeoutSeconds int `json:"shutdown_timeout_seconds,omitempty" yaml:"shutdown_timeout_seconds,omitempty"`
	// Logging configuration
	LogLevel            string `json:"log_level,omitempty" yaml:"log_level,omitempty"` // debug, info, warn, error
	LogFormat           string `json:"log_format,omitempty" yaml:"log_format,omitempty"` // json, text
}

// Default returns a Config with sensible defaults.
func Default() Config {
	return Config{
		// Maintain legacy behavior of reading bundled sample logs.
		InputPath:         "examples/k8s_logs.jsonl",
		ReportPath:        "report.json",
		OutputType:        "stdout",
		OutputMaxB:        10 * 1024 * 1024, // 10 MiB default rotation threshold
		OutputMaxFiles:    5,
		FilterLevels:      []string{"WARN", "ERROR"},
		Transforms:        []string{"filter_redact"},
		MaxWorkers:        4,
		QueueSize:         128,
		SinkMaxRetries:    3,
		SinkBackoffBaseMS: 100,
		SinkBackoffMaxMS:  2000,
		SinkBackoffJitter: 0.2,
		BatchSize:         100,
		BatchFlushInterval: 1000, // 1 second
		ShutdownTimeoutSeconds: 30,
		LogLevel:           "info",
		LogFormat:          "json",
	}
}

// Merge overlays non-zero values from override onto base.
func Merge(base, override Config) Config {
	result := base

	if override.InputPath != "" {
		result.InputPath = override.InputPath
	}
	if override.OutputPath != "" {
		result.OutputPath = override.OutputPath
	}
	if override.OutputType != "" {
		result.OutputType = override.OutputType
	}
	if override.OutputMaxB != 0 {
		result.OutputMaxB = override.OutputMaxB
	}
	if override.OutputMaxFiles != 0 {
		result.OutputMaxFiles = override.OutputMaxFiles
	}
	if override.ReportPath != "" {
		result.ReportPath = override.ReportPath
	}
	if len(override.FilterLevels) > 0 {
		result.FilterLevels = override.FilterLevels
	}
	if len(override.FilterSvcs) > 0 {
		result.FilterSvcs = override.FilterSvcs
	}
	if len(override.RedactKeys) > 0 {
		result.RedactKeys = override.RedactKeys
	}
	if len(override.Transforms) > 0 {
		result.Transforms = override.Transforms
	}
	if override.MaxWorkers > 0 {
		result.MaxWorkers = override.MaxWorkers
	}
	if override.QueueSize > 0 {
		result.QueueSize = override.QueueSize
	}
	if override.SinkMaxRetries > 0 {
		result.SinkMaxRetries = override.SinkMaxRetries
	}
	if override.SinkBackoffBaseMS > 0 {
		result.SinkBackoffBaseMS = override.SinkBackoffBaseMS
	}
	if override.SinkBackoffMaxMS > 0 {
		result.SinkBackoffMaxMS = override.SinkBackoffMaxMS
	}
	if override.SinkBackoffJitter > 0 {
		result.SinkBackoffJitter = override.SinkBackoffJitter
	}
	if override.DLQPath != "" {
		result.DLQPath = override.DLQPath
	}
	if override.BatchSize > 0 {
		result.BatchSize = override.BatchSize
	}
	if override.BatchFlushInterval > 0 {
		result.BatchFlushInterval = override.BatchFlushInterval
	}
	if override.ShutdownTimeoutSeconds > 0 {
		result.ShutdownTimeoutSeconds = override.ShutdownTimeoutSeconds
	}
	if override.LogLevel != "" {
		result.LogLevel = override.LogLevel
	}
	if override.LogFormat != "" {
		result.LogFormat = override.LogFormat
	}

	return result
}

// FromEnv applies environment overrides to the provided config.
func FromEnv(base Config) Config {
	result := base

	if v := os.Getenv("ETL_INPUT"); v != "" {
		result.InputPath = v
	}
	if v := os.Getenv("ETL_OUTPUT"); v != "" {
		result.OutputPath = v
	}
	if v := os.Getenv("ETL_OUTPUT_TYPE"); v != "" {
		result.OutputType = v
	}
	if v := os.Getenv("ETL_OUTPUT_MAX_BYTES"); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			result.OutputMaxB = parsed
		}
	}
	if v := os.Getenv("ETL_OUTPUT_MAX_FILES"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			result.OutputMaxFiles = parsed
		}
	}
	if v := os.Getenv("ETL_MAX_WORKERS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			result.MaxWorkers = parsed
		}
	}
	if v := os.Getenv("ETL_QUEUE_SIZE"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			result.QueueSize = parsed
		}
	}
	if v := os.Getenv("ETL_SINK_MAX_RETRIES"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			result.SinkMaxRetries = parsed
		}
	}
	if v := os.Getenv("ETL_SINK_BACKOFF_BASE_MS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			result.SinkBackoffBaseMS = parsed
		}
	}
	if v := os.Getenv("ETL_SINK_BACKOFF_MAX_MS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			result.SinkBackoffMaxMS = parsed
		}
	}
	if v := os.Getenv("ETL_SINK_BACKOFF_JITTER_PCT"); v != "" {
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
			result.SinkBackoffJitter = parsed
		}
	}
	if v := os.Getenv("ETL_DLQ"); v != "" {
		result.DLQPath = v
	}
	if v := os.Getenv("ETL_REPORT"); v != "" {
		result.ReportPath = v
	}
	if v := os.Getenv("ETL_FILTER_LEVELS"); v != "" {
		result.FilterLevels = parseList(v)
	}
	if v := os.Getenv("ETL_FILTER_SERVICES"); v != "" {
		result.FilterSvcs = parseList(v)
	}
	if v := os.Getenv("ETL_REDACT_KEYS"); v != "" {
		result.RedactKeys = parseList(v)
	}
	if v := os.Getenv("ETL_TRANSFORMS"); v != "" {
		result.Transforms = parseList(v)
	}
	if v := os.Getenv("ETL_BATCH_SIZE"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			result.BatchSize = parsed
		}
	}
	if v := os.Getenv("ETL_BATCH_FLUSH_INTERVAL_MS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			result.BatchFlushInterval = parsed
		}
	}
	if v := os.Getenv("ETL_SHUTDOWN_TIMEOUT_SECONDS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			result.ShutdownTimeoutSeconds = parsed
		}
	}
	if v := os.Getenv("ETL_LOG_LEVEL"); v != "" {
		result.LogLevel = v
	}
	if v := os.Getenv("ETL_LOG_FORMAT"); v != "" {
		result.LogFormat = v
	}

	return result
}

// Load reads a JSON or YAML config file into Config.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}

	var cfg Config
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".yaml", ".yml":
		if err := unmarshalYAML(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("parse yaml: %w", err)
		}
	default:
		if err := json.Unmarshal(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("parse json: %w", err)
		}
	}

	return cfg, nil
}

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

// unmarshalYAML is a tiny, limited YAML reader that supports top-level key/value
// pairs and simple lists (e.g., "filter_levels:\n  - WARN\n  - ERROR").
// It intentionally avoids third-party dependencies.
func unmarshalYAML(data []byte, out any) error {
	lines := splitLines(data)
	raw := make(map[string]any)

	for i := 0; i < len(lines); {
		line := strings.TrimSpace(lines[i])
		if line == "" || strings.HasPrefix(line, "#") {
			i++
			continue
		}

		if strings.HasPrefix(line, "-") {
			return errors.New("top-level lists are not supported")
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid line %q", line)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Collect indented list items if the value is empty.
		if value == "" {
			i++
			list := []any{}
			for i < len(lines) {
				next := strings.TrimSpace(lines[i])
				if next == "" {
					i++
					continue
				}
				if strings.HasPrefix(next, "-") {
					item := strings.TrimSpace(strings.TrimPrefix(next, "-"))
					list = append(list, parseScalar(item))
					i++
					continue
				}
				break
			}
			raw[key] = list
			continue
		}

		raw[key] = parseScalar(value)
		i++
	}

	jsonBytes, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonBytes, out)
}

func parseScalar(val string) any {
	unquoted := strings.Trim(val, `"'`)

	if i, err := strconv.ParseInt(unquoted, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(unquoted, 64); err == nil {
		return f
	}
	if b, err := strconv.ParseBool(unquoted); err == nil {
		return b
	}

	return unquoted
}

func splitLines(data []byte) []string {
	var lines []string
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines
}

// Validate checks the configuration for common misconfigurations and returns
// an error describing all issues found.
func Validate(cfg Config) error {
	var errs []string

	// Validate output type
	if cfg.OutputType != "" && cfg.OutputType != "stdout" && cfg.OutputType != "file" && cfg.OutputType != "rotate" && cfg.OutputType != "rotating" {
		errs = append(errs, fmt.Sprintf("invalid output_type %q: must be stdout, file, or rotate", cfg.OutputType))
	}

	// Validate output path requirements
	if (cfg.OutputType == "file" || cfg.OutputType == "rotate" || cfg.OutputType == "rotating") && cfg.OutputPath == "" {
		errs = append(errs, "output_path is required when output_type is file or rotate")
	}

	// Validate numeric limits (must be non-negative)
	if cfg.MaxWorkers < 0 {
		errs = append(errs, fmt.Sprintf("max_workers cannot be negative: %d", cfg.MaxWorkers))
	}
	if cfg.QueueSize < 0 {
		errs = append(errs, fmt.Sprintf("queue_size cannot be negative: %d", cfg.QueueSize))
	}
	if cfg.SinkMaxRetries < 0 {
		errs = append(errs, fmt.Sprintf("sink_max_retries cannot be negative: %d", cfg.SinkMaxRetries))
	}
	if cfg.SinkBackoffBaseMS < 0 {
		errs = append(errs, fmt.Sprintf("sink_backoff_base_ms cannot be negative: %d", cfg.SinkBackoffBaseMS))
	}
	if cfg.SinkBackoffMaxMS < 0 {
		errs = append(errs, fmt.Sprintf("sink_backoff_max_ms cannot be negative: %d", cfg.SinkBackoffMaxMS))
	}
	if cfg.SinkBackoffJitter < 0 {
		errs = append(errs, fmt.Sprintf("sink_backoff_jitter_pct cannot be negative: %.2f", cfg.SinkBackoffJitter))
	}
	if cfg.OutputMaxB < 0 {
		errs = append(errs, fmt.Sprintf("output_max_bytes cannot be negative: %d", cfg.OutputMaxB))
	}
	if cfg.OutputMaxFiles < 0 {
		errs = append(errs, fmt.Sprintf("output_max_files cannot be negative: %d", cfg.OutputMaxFiles))
	}

	// Validate DLQ path
	if cfg.DLQPath != "" {
		if strings.HasPrefix(cfg.DLQPath, "s3://") {
			errs = append(errs, fmt.Sprintf("DLQ path with s3:// scheme is not supported: %s", cfg.DLQPath))
		}
		// Check if DLQ path is empty after trimming
		if strings.TrimSpace(cfg.DLQPath) == "" {
			errs = append(errs, "DLQ path cannot be empty or whitespace-only")
		}
	}

	// Validate backoff configuration consistency
	if cfg.SinkBackoffMaxMS > 0 && cfg.SinkBackoffBaseMS > 0 && cfg.SinkBackoffMaxMS < cfg.SinkBackoffBaseMS {
		errs = append(errs, fmt.Sprintf("sink_backoff_max_ms (%d) must be >= sink_backoff_base_ms (%d)", cfg.SinkBackoffMaxMS, cfg.SinkBackoffBaseMS))
	}

	// Validate jitter percentage
	if cfg.SinkBackoffJitter > 1.0 {
		errs = append(errs, fmt.Sprintf("sink_backoff_jitter_pct should be between 0.0 and 1.0, got: %.2f", cfg.SinkBackoffJitter))
	}

	// Validate batching configuration
	if cfg.BatchSize < 0 {
		errs = append(errs, fmt.Sprintf("batch_size cannot be negative: %d", cfg.BatchSize))
	}
	if cfg.BatchFlushInterval < 0 {
		errs = append(errs, fmt.Sprintf("batch_flush_interval_ms cannot be negative: %d", cfg.BatchFlushInterval))
	}

	// Validate shutdown timeout
	if cfg.ShutdownTimeoutSeconds < 0 {
		errs = append(errs, fmt.Sprintf("shutdown_timeout_seconds cannot be negative: %d", cfg.ShutdownTimeoutSeconds))
	}

	// Validate log level
	validLogLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if cfg.LogLevel != "" && !validLogLevels[strings.ToLower(cfg.LogLevel)] {
		errs = append(errs, fmt.Sprintf("invalid log_level %q: must be debug, info, warn, or error", cfg.LogLevel))
	}

	// Validate log format
	validLogFormats := map[string]bool{"json": true, "text": true}
	if cfg.LogFormat != "" && !validLogFormats[strings.ToLower(cfg.LogFormat)] {
		errs = append(errs, fmt.Sprintf("invalid log_format %q: must be json or text", cfg.LogFormat))
	}

	if len(errs) > 0 {
		return fmt.Errorf("configuration validation failed:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}
