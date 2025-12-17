package sink

import (
	"encoding/json"
	"io"
)

type JSONLSource struct {
	enc *json.Encoder
}

func New(w io.Writer) *JSONLSource {
	return &JSONLSource{
		enc: json.NewEncoder(w),
	}
}

func (s *JSONLSource) Write(record any) error {
	return s.enc.Encode(record)
}
