package sink

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPSink_Write(t *testing.T) {
	var receivedRecords []interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var record interface{}
		if err := json.NewDecoder(r.Body).Decode(&record); err != nil {
			t.Errorf("decode request: %v", err)
		}
		receivedRecords = append(receivedRecords, record)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx := context.Background()
	hs, err := NewHTTPSink(ctx, server.URL, 3, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("NewHTTPSink: %v", err)
	}
	defer hs.Close()

	record := map[string]interface{}{"test": "value"}
	if err := hs.Write(record); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Give server time to process
	time.Sleep(50 * time.Millisecond)

	if len(receivedRecords) != 1 {
		t.Errorf("expected 1 record, got %d", len(receivedRecords))
	}
}

func TestHTTPSink_Retry(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	ctx := context.Background()
	hs, err := NewHTTPSink(ctx, server.URL, 3, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("NewHTTPSink: %v", err)
	}
	defer hs.Close()

	record := map[string]interface{}{"test": "value"}
	if err := hs.Write(record); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestHTTPSink_MaxRetriesExceeded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	ctx := context.Background()
	hs, err := NewHTTPSink(ctx, server.URL, 2, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("NewHTTPSink: %v", err)
	}
	defer hs.Close()

	record := map[string]interface{}{"test": "value"}
	if err := hs.Write(record); err == nil {
		t.Error("expected error after max retries")
	}
}

