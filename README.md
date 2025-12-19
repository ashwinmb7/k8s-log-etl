# Kubernetes Log ETL Pipeline (Go)

A streaming ETL pipeline written in Go that ingests Kubernetes-style
application logs (JSONL), normalizes schema, filters errors,
redacts PII, and emits clean structured logs with reports.

## Features
- Streaming JSONL processing
- Schema normalization
- Error filtering
- PII redaction
- Backpressure-aware pipeline

## Usage
```bash
# From repo root
go run ./cmd/etl ^
  --config config.yaml ^
  --input examples/k8s_logs.jsonl ^
  --output - ^
  --report report.json ^
  --filter-levels WARN,ERROR ^
  --filter-services orders,payments ^
  --redact-keys user_email,token
```

### Flags
- `--config` path to YAML or JSON config file (env: `ETL_CONFIG`).
- `--input` JSONL input path or `-` for stdin (env: `ETL_INPUT`; default `examples/k8s_logs.jsonl`).
- `--output` output path or `-` for stdout (env: `ETL_OUTPUT`; default stdout).
- `--report` report output path (env: `ETL_REPORT`; default `report.json`).
- `--filter-levels` comma/semicolon list of levels to emit (env: `ETL_FILTER_LEVELS`; default `WARN,ERROR`).
- `--filter-services` comma/semicolon list of services to emit (env: `ETL_FILTER_SERVICES`; default allow all).
- `--redact-keys` comma/semicolon list of extra-field keys to strip (env: `ETL_REDACT_KEYS`).

### Config file example (YAML)
```yaml
input: examples/k8s_logs.jsonl
output: "-"
report: report.json
filter_levels:
  - WARN
  - ERROR
filter_services:
  - orders
  - payments
redact_keys:
  - user_email
  - token
```

### Schema
Normalized field definitions live in `docs/schema.md`.
