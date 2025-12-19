package sink

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// RotatingJSONLSink writes JSONL and rotates files when maxBytes is exceeded.
type RotatingJSONLSink struct {
	basePath string
	maxBytes int64
	maxFiles int

	current     *os.File
	currentSize int64
	index       int
}

func NewRotatingJSONLSink(path string, maxBytes int64, maxFiles int) (*RotatingJSONLSink, error) {
	s := &RotatingJSONLSink{
		basePath: path,
		maxBytes: maxBytes,
		maxFiles: maxFiles,
		index:    0,
	}
	if err := s.openNew(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *RotatingJSONLSink) Write(record any) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrWriteSink, err)
	}
	data = append(data, '\n')

	if s.currentSize+int64(len(data)) > s.maxBytes {
		if err := s.rotate(); err != nil {
			return err
		}
	}

	n, err := s.current.Write(data)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrWriteSink, err)
	}
	s.currentSize += int64(n)
	return nil
}

func (s *RotatingJSONLSink) Close() error {
	if s.current != nil {
		return s.current.Close()
	}
	return nil
}

func (s *RotatingJSONLSink) rotate() error {
	if err := s.current.Close(); err != nil {
		return fmt.Errorf("%w: %v", ErrRotateSink, err)
	}
	s.index++
	if s.maxFiles > 0 && s.index > s.maxFiles {
		oldIdx := s.index - s.maxFiles
		os.Remove(s.rotatedPath(oldIdx))
	}
	return s.openNew()
}

func (s *RotatingJSONLSink) openNew() error {
	target := s.basePath
	if s.index > 0 {
		target = s.rotatedPath(s.index)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return fmt.Errorf("%w: %v", ErrOpenSink, err)
	}
	f, err := os.Create(target)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrOpenSink, err)
	}
	s.current = f
	s.currentSize = 0
	return nil
}

func (s *RotatingJSONLSink) rotatedPath(idx int) string {
	return fmt.Sprintf("%s.%d", s.basePath, idx)
}
