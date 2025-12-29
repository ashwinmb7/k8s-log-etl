package sink

import (
	"sync"
	"testing"
	"time"
)

type testWriter struct {
	records []interface{}
	mu      sync.Mutex
}

func (tw *testWriter) Write(record interface{}) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.records = append(tw.records, record)
	return nil
}

func (tw *testWriter) Close() error {
	return nil
}

func TestBatchedSink_Write(t *testing.T) {
	tw := &testWriter{}
	bs, err := NewBatchedSink(tw, 3, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewBatchedSink: %v", err)
	}
	defer bs.Close()

	// Write 2 records (should not flush yet)
	if err := bs.Write("record1"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := bs.Write("record2"); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Give it a moment to ensure no premature flush
	time.Sleep(10 * time.Millisecond)
	if len(tw.records) != 0 {
		t.Errorf("expected 0 records, got %d", len(tw.records))
	}

	// Write 3rd record (should trigger flush)
	if err := bs.Write("record3"); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Wait for flush
	time.Sleep(50 * time.Millisecond)
	if len(tw.records) != 3 {
		t.Errorf("expected 3 records after flush, got %d", len(tw.records))
	}
}

func TestBatchedSink_FlushInterval(t *testing.T) {
	tw := &testWriter{}
	bs, err := NewBatchedSink(tw, 10, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewBatchedSink: %v", err)
	}
	defer bs.Close()

	// Write 1 record
	if err := bs.Write("record1"); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Wait for flush interval
	time.Sleep(100 * time.Millisecond)
	if len(tw.records) != 1 {
		t.Errorf("expected 1 record after flush interval, got %d", len(tw.records))
	}
}

func TestBatchedSink_Close(t *testing.T) {
	tw := &testWriter{}
	bs, err := NewBatchedSink(tw, 10, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewBatchedSink: %v", err)
	}

	// Write records that won't trigger auto-flush
	if err := bs.Write("record1"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := bs.Write("record2"); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Close should flush remaining
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if len(tw.records) != 2 {
		t.Errorf("expected 2 records after close, got %d", len(tw.records))
	}
}

