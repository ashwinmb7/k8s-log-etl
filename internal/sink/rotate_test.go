package sink

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRotatingSinkRotatesAndKeepsMaxFiles(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "out.log")

	sink, err := NewRotatingJSONLSink(base, 50, 2)
	if err != nil {
		t.Fatalf("init sink: %v", err)
	}
	defer sink.Close()

	// Write enough records to trigger rotation.
	for i := 0; i < 5; i++ {
		if err := sink.Write(map[string]any{"i": i}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("list dir: %v", err)
	}
	if len(entries) > 2 {
		t.Fatalf("expected at most 2 files, got %d", len(entries))
	}
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "out.log") {
			t.Fatalf("unexpected file %s", e.Name())
		}
	}
}
