package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"k8s-log-etl/internal/report"
)

func TestCLIProcessesExampleFixture(t *testing.T) {
	tmp := t.TempDir()
	outPath := filepath.Join(tmp, "out.jsonl")
	reportPath := filepath.Join(tmp, "report.json")
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("abs repo root: %v", err)
	}
	inputPath := filepath.Join(repoRoot, "examples", "k8s_logs.jsonl")

	cmd := exec.Command("go", "run", "./cmd/etl",
		"--input", inputPath,
		"--output-type", "file",
		"--output", outPath,
		"--report", reportPath,
		"--filter-levels", "WARN,ERROR",
		"--redact-keys", "user_email,token",
	)
	cmd.Dir = repoRoot
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Env = append(os.Environ(), "ETL_CONFIG=")

	if err := cmd.Run(); err != nil {
		t.Fatalf("cli run failed: %v\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
	}

	outFile, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	defer outFile.Close()

	var records []map[string]any
	scanner := bufio.NewScanner(outFile)
	for scanner.Scan() {
		var m map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
			t.Fatalf("unmarshal output: %v", err)
		}
		records = append(records, m)
		if fields, ok := m["Fields"].(map[string]any); ok {
			for k := range fields {
				if k == "user_email" || k == "token" {
					t.Fatalf("expected field %s to be redacted", k)
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan output: %v", err)
	}
	if got := len(records); got != 3 {
		t.Fatalf("expected 3 emitted records, got %d", got)
	}

	reportBytes, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var rep report.Report
	if err := json.Unmarshal(reportBytes, &rep); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}

	if rep.TotalLines != 6 || rep.JSONParsed != 6 || rep.JSONFailed != 0 {
		t.Fatalf("unexpected totals: %+v", rep)
	}
	if rep.WrittenOK != 3 || rep.WriteFailed != 0 {
		t.Fatalf("unexpected write stats: %+v", rep)
	}
	if rep.Filtered.Level != 3 || rep.Filtered.Service != 0 {
		t.Fatalf("unexpected filter stats: %+v", rep.Filtered)
	}
	if rep.DurationSeconds <= 0 || rep.Throughput <= 0 {
		t.Fatalf("expected duration/throughput to be set: duration=%f throughput=%f", rep.DurationSeconds, rep.Throughput)
	}

	summary := stdout.String()
	if !strings.Contains(summary, "Total Lines") {
		t.Fatalf("expected stdout summary, got: %q", summary)
	}
}
