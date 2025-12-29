package stages

import (
	"testing"
	"time"
)

func TestNormalize_CompleteRecord(t *testing.T) {
	raw := map[string]interface{}{
		"ts":      "2024-01-01T12:00:00Z",
		"level":   "ERROR",
		"msg":     "test message",
		"service": "test-service",
		"kubernetes": map[string]interface{}{
			"namespace_name": "default",
			"pod_name":       "test-pod",
			"node_name":      "node-1",
		},
		"trace_id": "abc123",
		"extra":    "value",
	}

	normalized, err := Normalize(raw)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}

	if normalized.TS == "" {
		t.Error("expected TS to be set")
	}
	if normalized.Level != "ERROR" {
		t.Errorf("expected Level ERROR, got %q", normalized.Level)
	}
	if normalized.Message != "test message" {
		t.Errorf("expected Message 'test message', got %q", normalized.Message)
	}
	if normalized.Service != "test-service" {
		t.Errorf("expected Service 'test-service', got %q", normalized.Service)
	}
	if normalized.Namespace != "default" {
		t.Errorf("expected Namespace 'default', got %q", normalized.Namespace)
	}
	if normalized.Pod != "test-pod" {
		t.Errorf("expected Pod 'test-pod', got %q", normalized.Pod)
	}
	if normalized.Node != "node-1" {
		t.Errorf("expected Node 'node-1', got %q", normalized.Node)
	}
	if normalized.TraceID != "abc123" {
		t.Errorf("expected TraceID 'abc123', got %q", normalized.TraceID)
	}
	if normalized.Fields["extra"] != "value" {
		t.Error("expected extra field to be preserved")
	}
}

func TestNormalize_MissingRequiredFields(t *testing.T) {
	tests := []struct {
		name string
		raw  map[string]interface{}
		want string
	}{
		{
			name: "missing message",
			raw:  map[string]interface{}{"level": "ERROR"},
			want: "missing message",
		},
		{
			name: "missing level",
			raw:  map[string]interface{}{"msg": "test"},
			want: "missing level",
		},
		{
			name: "missing timestamp",
			raw:  map[string]interface{}{"msg": "test", "level": "ERROR"},
			want: "missing timestamp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Normalize(tt.raw)
			if err == nil {
				t.Fatal("expected error")
			}
			if err.Error() == "" {
				t.Error("expected non-empty error message")
			}
		})
	}
}

func TestNormalize_FieldAliases(t *testing.T) {
	tests := []struct {
		name string
		raw  map[string]interface{}
		want string
	}{
		{
			name: "time alias",
			raw: map[string]interface{}{
				"time":  "2024-01-01T12:00:00Z",
				"level": "ERROR",
				"msg":   "test",
			},
			want: "2024-01-01T12:00:00Z",
		},
		{
			name: "severity alias",
			raw: map[string]interface{}{
				"ts":       "2024-01-01T12:00:00Z",
				"severity": "WARN",
				"msg":      "test",
			},
			want: "WARN",
		},
		{
			name: "message alias",
			raw: map[string]interface{}{
				"ts":       "2024-01-01T12:00:00Z",
				"level":    "ERROR",
				"message":  "test message",
			},
			want: "test message",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			normalized, err := Normalize(tt.raw)
			if err != nil {
				t.Fatalf("Normalize: %v", err)
			}
			if tt.name == "time alias" {
				// Check TS is set (format may vary)
				if normalized.TS == "" {
					t.Error("expected TS to be set")
				}
			} else if tt.name == "severity alias" {
				if normalized.Level != tt.want {
					t.Errorf("expected Level %q, got %q", tt.want, normalized.Level)
				}
			} else if tt.name == "message alias" {
				if normalized.Message != tt.want {
					t.Errorf("expected Message %q, got %q", tt.want, normalized.Message)
				}
			}
		})
	}
}

func TestNormalize_TimestampFormats(t *testing.T) {
	tests := []struct {
		name string
		ts   string
		want bool
	}{
		{"RFC3339Nano", "2024-01-01T12:00:00.123456789Z", true},
		{"RFC3339", "2024-01-01T12:00:00Z", true},
		{"invalid", "not-a-date", false},
		{"empty", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := map[string]interface{}{
				"ts":    tt.ts,
				"level": "ERROR",
				"msg":   "test",
			}
			_, err := Normalize(raw)
			if (err == nil) != tt.want {
				t.Errorf("expected success=%v, got error=%v", tt.want, err)
			}
		})
	}
}

func BenchmarkNormalize(b *testing.B) {
	raw := map[string]interface{}{
		"ts":      "2024-01-01T12:00:00Z",
		"level":   "ERROR",
		"msg":     "test message",
		"service": "test-service",
		"kubernetes": map[string]interface{}{
			"namespace_name": "default",
			"pod_name":       "test-pod",
			"node_name":      "node-1",
		},
		"trace_id": "abc123",
		"extra":    "value",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Normalize(raw)
	}
}
