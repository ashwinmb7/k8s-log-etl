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
# From repo root (Windows caret for line continuation)
go run ./cmd/etl ^
  --config config.yaml ^
  --input examples/k8s_logs.jsonl ^
  --output output.jsonl ^
  --output-type rotate ^
  --output-max-bytes 5242880 ^
  --output-max-files 5 ^
  --report report.json ^
  --filter-levels WARN,ERROR ^
  --filter-services orders,payments ^
  --redact-keys user_email,token

# After building: etl run with config
go build -o bin/etl ./cmd/etl
./bin/etl --config config.yaml
```

### Flags
- `--config` path to YAML or JSON config file (env: `ETL_CONFIG`).
- `--input` JSONL input path or `-` for stdin (env: `ETL_INPUT`; default `examples/k8s_logs.jsonl`).
- `--output` output path or `-` for stdout (env: `ETL_OUTPUT`; default stdout).
- `--output-type` `stdout|file|rotate` (env: `ETL_OUTPUT_TYPE`; default stdout).
- `--output-max-bytes` rotate threshold in bytes (env: `ETL_OUTPUT_MAX_BYTES`; default 10MiB).
- `--output-max-files` max rotated files to keep (env: `ETL_OUTPUT_MAX_FILES`; default 5).
- `--report` report output path or `-` for stdout (env: `ETL_REPORT`; default `report.json`).
- `--filter-levels` comma/semicolon list of levels to emit (env: `ETL_FILTER_LEVELS`; default `WARN,ERROR`).
- `--filter-services` comma/semicolon list of services to emit (env: `ETL_FILTER_SERVICES`; default allow all).
- `--redact-keys` comma/semicolon list of extra-field keys to strip (env: `ETL_REDACT_KEYS`).

### Config file example (YAML)
```yaml
input: examples/k8s_logs.jsonl
output: "-"
output_type: rotate
output_max_bytes: 5242880
output_max_files: 5
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

### Expected outputs
- The bundled `examples/k8s_logs.jsonl` yields 3 emitted records (WARN/ERROR) with `user_email`/`token` redacted when run with defaults.
- Summary is printed to stdout; detailed report is written to the configured path (or stdout with `--report -`).
- Report JSON includes throughput, error rates, filtered counts, and per-level/service tallies.

### Development / CI
- Format: `gofmt -w ./...`
- Lint/vet: `go vet ./...`
- Tests (includes CLI integration against fixtures): `go test ./...`
- Dependency hygiene: `go mod tidy`
- CI: see `.github/workflows/ci.yml` (fmt, vet, test, tidy check).

### Containerization
- Build a static binary and copy into a small image:
```Dockerfile
FROM golang:1.25 AS build
WORKDIR /app
COPY . .
RUN go build -o /app/bin/etl ./cmd/etl

FROM gcr.io/distroless/base-debian12
COPY --from=build /app/bin/etl /usr/local/bin/etl
ENTRYPOINT ["/usr/local/bin/etl"]
```
- Run: `docker run --rm -v $(pwd)/examples:/data etl --input /data/k8s_logs.jsonl --report -`

### Schema
Normalized field definitions live in `docs/schema.md`.
