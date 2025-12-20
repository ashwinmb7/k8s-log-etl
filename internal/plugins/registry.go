package plugins

import (
	"fmt"
	"strings"

	"k8s-log-etl/internal/config"
	"k8s-log-etl/internal/model"
	"k8s-log-etl/internal/stages"
)

// Transform applies a mutation to a record and can drop it with a reason.
// Returned record replaces the input.
type Transform func(model.Normalized) (model.Normalized, bool, string, error)

var transformRegistry = map[string]func(config.Config) Transform{}

// RegisterTransform registers a transform factory by name.
func RegisterTransform(name string, builder func(config.Config) Transform) {
	transformRegistry[strings.ToLower(name)] = builder
}

// BuildTransforms constructs the transforms specified in config.Transforms.
func BuildTransforms(cfg config.Config) ([]Transform, error) {
	names := cfg.Transforms
	if len(names) == 0 {
		names = []string{"filter_redact"}
	}
	var result []Transform
	for _, name := range names {
		builder, ok := transformRegistry[strings.ToLower(name)]
		if !ok {
			return nil, fmt.Errorf("unknown transform %q", name)
		}
		result = append(result, builder(cfg))
	}
	return result, nil
}

func init() {
	// Built-in filter+redact plugin using existing FilterStage.
	RegisterTransform("filter_redact", func(cfg config.Config) Transform {
		fs := stages.NewFilterStage(cfg)
		return func(n model.Normalized) (model.Normalized, bool, string, error) {
			if ok, reason := fs.Apply(&n); !ok {
				return n, true, reason, nil
			}
			return n, false, "", nil
		}
	})
}
