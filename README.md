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
etl run --config config.yaml
---

## 7️⃣ Define the normalized schema (VERY IMPORTANT)
In `docs/schema.md`:
```md
## NormalizedLog

- ts (RFC3339)
- level (DEBUG | INFO | WARN | ERROR)
- service
- namespace
- pod
- node (optional)
- message
- trace_id (optional)
- fields (map of remaining attributes)