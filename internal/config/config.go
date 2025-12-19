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
	InputPath    string   `json:"input,omitempty" yaml:"input,omitempty"`
	OutputPath   string   `json:"output,omitempty" yaml:"output,omitempty"`
	ReportPath   string   `json:"report,omitempty" yaml:"report,omitempty"`
	FilterLevels []string `json:"filter_levels,omitempty" yaml:"filter_levels,omitempty"`
	FilterSvcs   []string `json:"filter_services,omitempty" yaml:"filter_services,omitempty"`
	RedactKeys   []string `json:"redact_keys,omitempty" yaml:"redact_keys,omitempty"`
}

// Default returns a Config with sensible defaults.
func Default() Config {
	return Config{
		// Maintain legacy behavior of reading bundled sample logs.
		InputPath:    "examples/k8s_logs.jsonl",
		ReportPath:   "report.json",
		FilterLevels: []string{"WARN", "ERROR"},
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
