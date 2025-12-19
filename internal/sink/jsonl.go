package sink

import (
	"encoding/json"
	"fmt"
	"io"
)

// Writer writes normalized records.
type Writer interface {
	Write(record any) error
	Close() error
}

// JSONLSink writes records as JSON lines.
type JSONLSink struct {
	enc    *json.Encoder
	closer io.Closer
}

// NewJSONLSink wraps a WriteCloser into a JSONL writer.
func NewJSONLSink(w io.WriteCloser) *JSONLSink {
	return &JSONLSink{
		enc:    json.NewEncoder(w),
		closer: w,
	}
}

func (s *JSONLSink) Write(record any) error {
	if err := s.enc.Encode(record); err != nil {
		return fmt.Errorf("%w: %v", ErrWriteSink, err)
	}
	return nil
}

func (s *JSONLSink) Close() error {
	return s.closer.Close()
}
