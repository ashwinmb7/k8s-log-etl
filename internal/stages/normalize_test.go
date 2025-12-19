package stages

import (
	"testing"
	"time"
)

func TestNormalizeAliasesAndUppercasesLevel(t *testing.T) {
	raw := map[string]any{
		"time":      "2025-12-14T19:25:14Z",
		"severity":  "warn",
		"message":   "hello",
		"component": "payments",
		"hostname":  "node-1",
		"extra":     "keep",
	}

	got, err := Normalize(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got.Level != "WARN" {
		t.Fatalf("expected level WARN, got %q", got.Level)
	}
	if got.Service != "payments" {
		t.Fatalf("expected service from component alias, got %q", got.Service)
	}
	if got.Node != "node-1" {
		t.Fatalf("expected node from hostname alias, got %q", got.Node)
	}
	if _, ok := got.Fields["extra"]; !ok {
		t.Fatalf("expected extra field to remain")
	}
	if _, ok := got.Fields["hostname"]; ok {
		t.Fatalf("expected hostname alias to be consumed")
	}
}

func TestNormalizeRejectsInvalidTimestamp(t *testing.T) {
	raw := map[string]any{
		"ts":      "not-a-time",
		"level":   "INFO",
		"msg":     "ok",
		"service": "svc",
	}

	_, err := Normalize(raw)
	if err == nil {
		t.Fatalf("expected error for invalid timestamp")
	}
	if got := err.Error(); got != `invalid timestamp "not-a-time": expected RFC3339` {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNormalizeMissingFieldsErrors(t *testing.T) {
	tests := []struct {
		name string
		raw  map[string]any
		want string
	}{
		{
			name: "missing ts",
			raw: map[string]any{
				"level": "INFO", "msg": "hi",
			},
			want: "missing timestamp: expected ts/time in RFC3339",
		},
		{
			name: "missing level",
			raw: map[string]any{
				"ts":  time.Now().UTC().Format(time.RFC3339),
				"msg": "hi",
			},
			want: "missing level: expected level/severity",
		},
		{
			name: "missing message",
			raw: map[string]any{
				"ts":    time.Now().UTC().Format(time.RFC3339),
				"level": "info",
			},
			want: "missing message: expected msg/message",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Normalize(tt.raw)
			if err == nil {
				t.Fatalf("expected error")
			}
			if err.Error() != tt.want {
				t.Fatalf("want %q, got %q", tt.want, err.Error())
			}
		})
	}
}

func TestNormalizeFormatsTimestampRFC3339Nano(t *testing.T) {
	raw := map[string]any{
		"ts":    "2025-12-14T19:25:12.3456789Z",
		"level": "error",
		"msg":   "x",
	}

	got, err := Normalize(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got.TS != "2025-12-14T19:25:12.3456789Z" {
		t.Fatalf("expected nano-format timestamp, got %q", got.TS)
	}
	if got.Level != "ERROR" {
		t.Fatalf("expected uppercased level, got %q", got.Level)
	}
}
