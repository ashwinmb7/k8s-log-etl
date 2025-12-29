## Normalized Log Schema

- **Timestamp (`ts`)**  
  Type: string (RFC3339). Required. Missing timestamp is an error. Primary key `ts`; accepts alias `time`.  
  Constraint: must parse as RFC3339.

- **Level (`level`)**  
  Type: string. Required. Missing level is an error. Primary key `level`; accepts alias `severity`.

- **Message (`msg`)**  
  Type: string. Required. Missing message is an error. Primary key `msg`; accepts alias `message`.

- **Service (`service`)**  
  Type: string. Optional. Primary key `service`; accepts alias `app`.

- **Kubernetes metadata (`kubernetes` block or top-level keys)**  
  - `namespace` (`namespace_name` inside `kubernetes`, or 
  top-level `namespace`) – string. Not hard-required by validation.  
  - `pod` (`pod_name` inside `kubernetes`, or top-level `pod`) – string. Not hard-required by validation.  
  - `node` (`node_name` inside `kubernetes`, or top-level `node`) – string. Optional per schema note.

- **Trace ID (`trace_id`)**  
  Type: string. Optional. Primary key `trace_id`; accepts alias `trace`.

- **Fields map (`fields`)**  
  Type: `map[string]any`. Captures all remaining attributes not matched to the canonical keys or their aliases: `ts`/`time`, `level`/`severity`, `msg`/`message`, `service`/`app`, `kubernetes` block, `trace_id`/`trace`, and top-level `namespace`/`pod`/`node`.

### Alias Expectations & Constraints
- Timestamp must be present and RFC3339 formatted; `ts` preferred, `time` used as fallback.
- Level and message are mandatory; use `level`/`severity` and `msg`/`message`.
- Service may be provided as `service` or `app`.
- Kubernetes metadata can arrive under `kubernetes.namespace_name`/`pod_name`/`node_name` or the top-level aliases `namespace`/`pod`/`node`.
- Trace ID can be provided as `trace_id` or `trace`.
- Any other keys flow into `fields`.

### Example Normalized Log Line
```json
{"ts":"2025-12-14T19:25:12.345Z","level":"INFO","msg":"request started","service":"orders","namespace":"prod","pod":"orders-api-6f4c9b7c8d-xp9k2","node":"ip-10-0-2-15","trace_id":"a1","path":"/checkout","status":200}
```
