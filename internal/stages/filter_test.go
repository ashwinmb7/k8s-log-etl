package stages

import (
	"testing"

	"k8s-log-etl/internal/config"
	"k8s-log-etl/internal/model"
)

func TestFilterAllowsLevelAndRedacts(t *testing.T) {
	stage := NewFilterStage(config.Config{
		FilterLevels: []string{"WARN", "ERROR"},
		RedactKeys:   []string{"user_email", "token"},
	})

	rec := model.Normalized{
		Level:  "warn",
		Fields: map[string]any{"user_email": "a", "token": "b", "keep": "ok"},
	}

	if ok, _ := stage.Apply(&rec); !ok {
		t.Fatalf("expected record to pass filter")
	}

	if _, ok := rec.Fields["user_email"]; ok {
		t.Fatalf("expected user_email to be redacted")
	}
	if _, ok := rec.Fields["token"]; ok {
		t.Fatalf("expected token to be redacted")
	}
	if rec.Fields["keep"] != "ok" {
		t.Fatalf("expected keep field to remain")
	}
}

func TestFilterBlocksLevelWithoutRedaction(t *testing.T) {
	stage := NewFilterStage(config.Config{
		FilterLevels: []string{"ERROR"},
		RedactKeys:   []string{"user_email"},
	})

	rec := model.Normalized{
		Level:  "info",
		Fields: map[string]any{"user_email": "a", "other": "b"},
	}

	if ok, _ := stage.Apply(&rec); ok {
		t.Fatalf("expected record to be blocked by level filter")
	}
	if rec.Fields["user_email"] != "a" || rec.Fields["other"] != "b" {
		t.Fatalf("expected fields to remain untouched when blocked")
	}
}

func TestFilterByServiceCaseInsensitive(t *testing.T) {
	stage := NewFilterStage(config.Config{
		FilterSvcs: []string{"Payments"},
	})

	recBlocked := model.Normalized{Service: "orders"}
	if ok, _ := stage.Apply(&recBlocked); ok {
		t.Fatalf("expected orders service to be blocked")
	}

	recAllowed := model.Normalized{Service: "PAYMENTS"}
	if ok, _ := stage.Apply(&recAllowed); !ok {
		t.Fatalf("expected payments service to be allowed")
	}
}

func TestFilterAllowsWhenNoRules(t *testing.T) {
	stage := NewFilterStage(config.Config{})
	rec := model.Normalized{Level: "debug", Service: "any"}
	if ok, _ := stage.Apply(&rec); !ok {
		t.Fatalf("expected record to pass when no filters configured")
	}
}
