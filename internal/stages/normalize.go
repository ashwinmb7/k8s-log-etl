package stages

import (
	"errors"
	"k8s-log-etl/internal/model"
)

func Normalize(raw map[string]any) (model.Normalized, error) {
	//output of formatted normalized log
	var output model.Normalized

	// extract timestamp

	// try raw["ts"]
	if v, ok := raw["ts"]; ok {
		if s, ok := v.(string); ok {
			output.TS = s
		}
	}

	// fallback to raw["time"] only if TS not set yet
	if output.TS == "" {
		if v, ok := raw["time"]; ok {
			if s, ok := v.(string); ok {
				output.TS = s
			}
		}
	}
	// extract level

	if v, ok := raw["level"]; ok {
		if s, ok := v.(string); ok {
			output.Level = s
		}
	}

	if output.Level == "" {
		if v, ok := raw["severity"]; ok {
			if s, ok := v.(string); ok {
				output.Level = s
			}
		}
	}
	// extract message

	if v, ok := raw["msg"]; ok {
		if s, ok := v.(string); ok {
			output.Message = s
		}
	}

	if output.Message == "" {
		if v, ok := raw["message"]; ok {
			if s, ok := v.(string); ok {
				output.Message = s
			}
		}
	}
	// extract service

	if v, ok := raw["service"]; ok {
		if s, ok := v.(string); ok {
			output.Service = s
		}
	}

	if output.Service == "" {
		if v, ok := raw["app"]; ok {
			if s, ok := v.(string); ok {
				output.Service = s
			}
		}
	}
	// extract namespace / pod / node

	if v, ok := raw["kubernetes"]; ok {
		if m, ok := v.(map[string]any); ok {

			if ns, ok := m["namespace_name"]; ok {
				if s, ok := ns.(string); ok {
					output.Namespace = s
				}
			}

			if pod, ok := m["pod_name"]; ok {
				if s, ok := pod.(string); ok {
					output.Pod = s
				}
			}

			if node, ok := m["node_name"]; ok {
				if s, ok := node.(string); ok {
					output.Node = s
				}
			}
		}
	}

	if v, ok := raw["namespace"]; ok {
		if s, ok := v.(string); ok {
			output.Namespace = s
		}
	}

	if v, ok := raw["pod"]; ok {
		if s, ok := v.(string); ok {
			output.Pod = s
		}
	}

	if v, ok := raw["node"]; ok {
		if s, ok := v.(string); ok {
			output.Node = s
		}
	}
	// extract trace id

	if v, ok := raw["trace_id"]; ok {
		if s, ok := v.(string); ok {
			output.TraceID = s
		}
	}

	if output.TraceID == "" {
		if v, ok := raw["trace"]; ok {
			if s, ok := v.(string); ok {
				output.TraceID = s
			}
		}
	}
	// collect remaining fields
	output.Fields = make(map[string]any)

	for k, v := range raw {
		if k != "ts" &&
			k != "time" &&
			k != "level" &&
			k != "severity" &&
			k != "msg" &&
			k != "message" &&
			k != "service" &&
			k != "app" &&
			k != "kubernetes" &&
			k != "trace_id" &&
			k != "trace" &&
			k != "namespace" &&
			k != "pod" &&
			k != "node" {
			output.Fields[k] = v
		}
	}

	if output.TS == "" {
		return output, errors.New("missing timestamp")
	}

	if output.Message == "" {
		return output, errors.New("missing message")
	}

	if output.Level == "" {
		return output, errors.New("missing level")
	}

	return output, nil
}
