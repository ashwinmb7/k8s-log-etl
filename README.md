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
- **Structured logging** with context support and configurable levels
- **Graceful shutdown** with signal handling (SIGINT/SIGTERM)
- **Batched writing** for improved performance
- **Multiple sink types**: stdout, file, rotate, HTTP/webhook
- **Comprehensive metrics**: per-stage timings, retry stats, DLQ reasons

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
- `--output-type` `stdout|file|rotate|http` (env: `ETL_OUTPUT_TYPE`; default stdout).
  - `stdout`: write to standard output
  - `file`: write to a single file
  - `rotate`: rotate files when size limit is reached
  - `http`: POST records to HTTP endpoint (requires `--output` to be a URL)
- `--output-max-bytes` rotate threshold in bytes (env: `ETL_OUTPUT_MAX_BYTES`; default 10MiB).
- `--output-max-files` max rotated files to keep (env: `ETL_OUTPUT_MAX_FILES`; default 5).
- `--report` report output path or `-` for stdout (env: `ETL_REPORT`; default `report.json`).
- `--filter-levels` comma/semicolon list of levels to emit (env: `ETL_FILTER_LEVELS`; default `WARN,ERROR`).
- `--filter-services` comma/semicolon list of services to emit (env: `ETL_FILTER_SERVICES`; default allow all).
- `--redact-keys` comma/semicolon list of extra-field keys to strip (env: `ETL_REDACT_KEYS`).
- `--batch-size` batch size for sink writes, 0 = no batching (env: `ETL_BATCH_SIZE`; default 100).
- `--batch-flush-interval-ms` batch flush interval in milliseconds (env: `ETL_BATCH_FLUSH_INTERVAL_MS`; default 1000).
- `--shutdown-timeout-seconds` graceful shutdown timeout in seconds (env: `ETL_SHUTDOWN_TIMEOUT_SECONDS`; default 30).
- `--log-level` log level: debug, info, warn, error (env: `ETL_LOG_LEVEL`; default info).
- `--log-format` log format: json, text (env: `ETL_LOG_FORMAT`; default json).

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
- Report JSON includes throughput, error rates, filtered counts, per-level/service tallies, **per-stage timings**, **retry statistics**, and **DLQ reason breakdowns**.
- Structured logs (JSON or text format) are written to stderr with context information.

### New Features

#### Structured Logging
The pipeline now uses structured logging with context support:
```bash
# Use text format for human-readable logs
./bin/etl --log-format text --log-level debug --input examples/k8s_logs.jsonl

# Use JSON format for log aggregation (default)
./bin/etl --log-format json --log-level info --input examples/k8s_logs.jsonl
```

#### Batched Writing
Improve performance by batching writes:
```bash
# Batch 1000 records or flush every 2 seconds
./bin/etl --batch-size 1000 --batch-flush-interval-ms 2000 --input large_file.jsonl
```

#### HTTP/Webhook Sink
Send records to HTTP endpoints:
```bash
# POST records to a webhook
./bin/etl --output-type http --output https://api.example.com/logs --input examples/k8s_logs.jsonl
```

#### Graceful Shutdown
The pipeline handles SIGINT/SIGTERM gracefully:
- Finishes processing in-flight records
- Flushes all buffers
- Writes final report
- Configurable timeout (default 30 seconds)

### Development / CI
- Format: `gofmt -w ./...`
- Lint/vet: `go vet ./...`
- Tests (includes CLI integration, unit tests, and benchmarks): `go test ./...`
- Benchmarks: `go test -bench=. ./...`
- Dependency hygiene: `go mod tidy`
- CI: see `.github/workflows/ci.yml` (fmt, vet, test, tidy check).

## Troubleshooting

### CI Failures

#### `gofmt` failures
**Symptom**: CI fails with "files are not gofmt'd" or similar formatting errors.

**Solution**:
```bash
# Format all Go files
gofmt -w ./...

# Verify formatting
gofmt -d ./...
```

**Prevention**: Configure your editor to run `gofmt` on save, or use a pre-commit hook.

#### `go vet` failures
**Symptom**: CI fails with vet errors like unused variables, unreachable code, or suspicious constructs.

**Solution**:
```bash
# Run vet to see all issues
go vet ./...

# Fix reported issues (e.g., remove unused variables, fix shadowing)
```

**Common issues**:
- Unused variables: Remove or use `_ = variable` if intentionally unused
- Shadowed variables: Rename inner variables to avoid shadowing
- Suspicious nil checks: Ensure proper nil handling

#### Test failures
**Symptom**: CI fails with test errors or panics.

**Solution**:
```bash
# Run tests with verbose output
go test -v ./...

# Run specific test
go test -v ./internal/stages -run TestNormalize

# Run with race detector
go test -race ./...
```

**Common issues**:
- Flaky tests: Check for race conditions or timing dependencies
- Missing test fixtures: Ensure `examples/k8s_logs.jsonl` exists
- Environment-specific failures: Check for hardcoded paths or OS-specific code

#### `go mod tidy` failures
**Symptom**: CI fails with "go.mod and go.sum are out of sync" or dependency errors.

**Solution**:
```bash
# Update dependencies
go mod tidy

# Verify go.sum is up to date
go mod verify

# If issues persist, try cleaning module cache
go clean -modcache
go mod download
go mod tidy
```

**Prevention**: Always run `go mod tidy` before committing changes that modify imports.

### Runtime Issues

#### DLQ Path Permissions
**Symptom**: Error message like `open dlq: permission denied` or `open dlq: no such file or directory`.

**Solution**:
- Ensure the directory containing the DLQ file exists and is writable:
  ```bash
  # Create directory with proper permissions
  mkdir -p /path/to/dlq/directory
  chmod 755 /path/to/dlq/directory
  ```
- Check that the process has write permissions to the parent directory
- On Windows, ensure the path doesn't contain invalid characters and the user has write access
- If using relative paths, ensure the working directory is correct

**Prevention**: Validate DLQ path permissions during startup (now done automatically via config validation).

#### Rotation Limits
**Symptom**: Unexpected file rotation behavior, files not being cleaned up, or "too many open files" errors.

**Solution**:
- Check `output_max_files` setting: if set to 0, old files are never deleted
- Ensure `output_max_bytes` is reasonable (e.g., not too small causing excessive rotation)
- Verify disk space is available for rotated files
- Check file descriptor limits:
  ```bash
  # Linux
  ulimit -n
  
  # Increase if needed
  ulimit -n 4096
  ```

**Common misconfigurations**:
- `output_max_files: 0` with large `output_max_bytes` can fill disk
- Negative values are now caught by validation and will fail at startup
- Very small `output_max_bytes` values cause excessive rotation overhead

#### Configuration Validation Errors
**Symptom**: Startup fails with "configuration validation failed" errors.

**Common issues and fixes**:
- **Negative limits**: All numeric limits (workers, queue size, retries, backoff values, max files/bytes) must be non-negative
  ```yaml
  # ❌ Invalid
  max_workers: -1
  
  # ✅ Valid
  max_workers: 4
  ```
- **Invalid output type**: Must be `stdout`, `file`, or `rotate`
  ```yaml
  # ❌ Invalid
  output_type: invalid
  
  # ✅ Valid
  output_type: rotate
  ```
- **Missing output path**: Required when using `file` or `rotate` output types
  ```yaml
  # ❌ Invalid
  output_type: file
  # output: missing
  
  # ✅ Valid
  output_type: file
  output: output.jsonl
  ```
- **S3 DLQ paths**: S3 URLs are not supported for DLQ
  ```yaml
  # ❌ Invalid
  dlq: s3://bucket/dlq.jsonl
  
  # ✅ Valid
  dlq: /local/path/dlq.jsonl
  ```
- **Backoff configuration**: `sink_backoff_max_ms` must be >= `sink_backoff_base_ms`
  ```yaml
  # ❌ Invalid
  sink_backoff_base_ms: 1000
  sink_backoff_max_ms: 500
  
  # ✅ Valid
  sink_backoff_base_ms: 100
  sink_backoff_max_ms: 2000
  ```

#### Performance Issues
**Symptom**: Slow processing, high memory usage, or pipeline stalls.

**Diagnosis**: Check the operational metrics in the summary output:
- **Stage timings**: Identify bottlenecks (parsing, normalization, filtering, writing)
- **Retry stats**: High retry counts indicate sink write issues
- **DLQ reasons**: Frequent DLQ writes suggest downstream problems

**Solutions**:
- Increase `max_workers` if writing is the bottleneck
- Increase `queue_size` if normalization is faster than writing
- Check disk I/O performance for file-based sinks
- Review DLQ reasons to identify root causes of write failures
- Adjust backoff parameters if retries are excessive

#### Empty or Missing Input
**Symptom**: No output produced, or "open input: no such file or directory".

**Solution**:
- Verify input path is correct (use `-` for stdin)
- Check file permissions for input file
- Ensure input file exists and is readable
- For stdin, verify data is being piped correctly:
  ```bash
  # Test stdin input
  cat examples/k8s_logs.jsonl | ./bin/etl --input -
  ```

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
