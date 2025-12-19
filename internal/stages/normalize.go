package stages

import (
	"errors"
	"fmt"
	"k8s-log-etl/internal/model"
	"strings"
	"time"
)

func Normalize(raw map[string]any) (model.Normalized, error) {
	//output of formatted normalized log
	var output model.Normalized

	// extract timestamp

	// try raw["ts"]
	if v, ok := raw["ts"]; ok {
		if s, ok := v.(string); ok {
			output.TS = strings.TrimSpace(s)
		}
	}

	// fallback to raw["time"] only if TS not set yet
	if output.TS == "" {
		if v, ok := raw["time"]; ok {
			if s, ok := v.(string); ok {
				output.TS = strings.TrimSpace(s)
			}
		}
	}
	// extract level

	if v, ok := raw["level"]; ok {
		if s, ok := v.(string); ok {
			output.Level = strings.TrimSpace(s)
		}
	}

	if output.Level == "" {
		if v, ok := raw["severity"]; ok {
			if s, ok := v.(string); ok {
				output.Level = strings.TrimSpace(s)
			}
		}
	}
	// extract message

	if v, ok := raw["msg"]; ok {
		if s, ok := v.(string); ok {
			output.Message = strings.TrimSpace(s)
		}
	}

	if output.Message == "" {
		if v, ok := raw["message"]; ok {
			if s, ok := v.(string); ok {
				output.Message = strings.TrimSpace(s)
			}
		}
	}
	// extract service

	if v, ok := raw["service"]; ok {
		if s, ok := v.(string); ok {
			output.Service = strings.TrimSpace(s)
		}
	}

	if output.Service == "" {
		if v, ok := raw["app"]; ok {
			if s, ok := v.(string); ok {
				output.Service = strings.TrimSpace(s)
			}
		}
	}

	if output.Service == "" {
		if v, ok := raw["component"]; ok {
			if s, ok := v.(string); ok {
				output.Service = strings.TrimSpace(s)
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
			output.Node = strings.TrimSpace(s)
		}
	}

	if output.Node == "" {
		if v, ok := raw["hostname"]; ok {
			if s, ok := v.(string); ok {
				output.Node = strings.TrimSpace(s)
			}
		}
	}
	// extract trace id

	if v, ok := raw["trace_id"]; ok {
		if s, ok := v.(string); ok {
			output.TraceID = strings.TrimSpace(s)
		}
	}

	if output.TraceID == "" {
		if v, ok := raw["trace"]; ok {
			if s, ok := v.(string); ok {
				output.TraceID = strings.TrimSpace(s)
			}
		}
	}
	// collect remaining fields
	output.Fields = make(map[string]any)

	for k, v := range raw {
		if k != "ts" &&
			k != "time" &&
			k != "hostname" &&
			k != "level" &&
			k != "severity" &&
			k != "msg" &&
			k != "message" &&
			k != "service" &&
			k != "app" &&
			k != "component" &&
			k != "kubernetes" &&
			k != "trace_id" &&
			k != "trace" &&
			k != "namespace" &&
			k != "pod" &&
			k != "node" {
			output.Fields[k] = v
		}
	}

	parsedTime, err := parseTimestamp(output.TS)
	if err != nil {
		return output, err
	}
	output.TS = parsedTime.Format(time.RFC3339Nano)

	if output.Message == "" {
		return output, errors.New("missing message: expected msg/message")
	}

	if output.Level == "" {
		return output, errors.New("missing level: expected level/severity")
	}
	output.Level = strings.ToUpper(output.Level)

	return output, nil
}

func parseTimestamp(ts string) (time.Time, error) {
	if ts == "" {
		return time.Time{}, errors.New("missing timestamp: expected ts/time in RFC3339")
	}

	parsed, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		// attempt without nano for better message
		if parsed2, err2 := time.Parse(time.RFC3339, ts); err2 == nil {
			return parsed2, nil
		}
		return time.Time{}, fmt.Errorf("invalid timestamp %q: expected RFC3339", ts)
	}
	return parsed, nil
}
