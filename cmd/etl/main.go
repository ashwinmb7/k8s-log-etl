package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"k8s-log-etl/internal/config"
	"k8s-log-etl/internal/report"
	"k8s-log-etl/internal/stages"
	"log"
	"os"
	"strings"
)

func main() {
	// Flags with env + config file override support.
	flagConfig := flag.String("config", "", "path to YAML or JSON config file")
	flagInput := flag.String("input", "", "input JSONL path (use '-' for stdin)")
	flagOutput := flag.String("output", "", "output path (use '-' for stdout)")
	flagReport := flag.String("report", "", "report output path")
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
	if *flagReport != "" {
		override.ReportPath = *flagReport
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

	// Prepare IO based on config.
	in, err := openInput(cfg.InputPath)
	if err != nil {
		log.Fatalf("open input: %v", err)
	}
	defer in.Close()

	out, err := openOutput(cfg.OutputPath)
	if err != nil {
		log.Fatalf("open output: %v", err)
	}
	defer out.Close()

	rep := report.NewReport()
	scanner := bufio.NewScanner(in)
	enc := json.NewEncoder(out)
	filterStage := stages.NewFilterStage(cfg)

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

		if !filterStage.Apply(&normalized) {
			continue
		}

		if err := enc.Encode(normalized); err != nil {
			fmt.Fprintf(os.Stderr, "write output error: %v\n", err)
		} else {
			rep.WrittenOK++
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	if err := rep.WriteJSON(cfg.ReportPath); err != nil {
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

func openInput(path string) (io.ReadCloser, error) {
	if path == "" || path == "-" {
		return io.NopCloser(os.Stdin), nil
	}
	return os.Open(path)
}

func openOutput(path string) (io.WriteCloser, error) {
	if path == "" || path == "-" {
		return nopWriteCloser{w: os.Stdout}, nil
	}
	return os.Create(path)
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

// nopWriteCloser wraps an io.Writer to satisfy io.WriteCloser.
type nopWriteCloser struct {
	w io.Writer
}

func (n nopWriteCloser) Write(p []byte) (int, error) {
	return n.w.Write(p)
}

func (n nopWriteCloser) Close() error { return nil }
