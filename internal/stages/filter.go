package stages

import (
	"strings"

	"k8s-log-etl/internal/config"
	"k8s-log-etl/internal/model"
)

// FilterStage applies level/service allowlists and redacts PII fields.
type FilterStage struct {
	levels   map[string]struct{}
	services map[string]struct{}
	redact   map[string]struct{}
}

// NewFilterStage constructs a FilterStage from config.
func NewFilterStage(cfg config.Config) *FilterStage {
	fs := &FilterStage{
		levels:   buildUpperSet(cfg.FilterLevels),
		services: buildLowerSet(cfg.FilterSvcs),
		redact:   buildExactSet(cfg.RedactKeys),
	}
	return fs
}

// Apply returns true when the record should be written, mutating Fields for redaction.
func (f *FilterStage) Apply(n *model.Normalized) bool {
	if len(f.levels) > 0 && !containsUpper(f.levels, n.Level) {
		return false
	}
	if len(f.services) > 0 && !containsLower(f.services, n.Service) {
		return false
	}

	if len(f.redact) > 0 && len(n.Fields) > 0 {
		for key := range f.redact {
			delete(n.Fields, key)
		}
	}
	return true
}

func buildUpperSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, v := range values {
		if v == "" {
			continue
		}
		set[strings.ToUpper(v)] = struct{}{}
	}
	return set
}

func buildLowerSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, v := range values {
		if v == "" {
			continue
		}
		set[strings.ToLower(v)] = struct{}{}
	}
	return set
}

func buildExactSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, v := range values {
		if v == "" {
			continue
		}
		set[v] = struct{}{}
	}
	return set
}

func containsUpper(set map[string]struct{}, v string) bool {
	_, ok := set[strings.ToUpper(v)]
	return ok
}

func containsLower(set map[string]struct{}, v string) bool {
	_, ok := set[strings.ToLower(v)]
	return ok
}
