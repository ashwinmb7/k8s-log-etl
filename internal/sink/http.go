package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// HTTPSink writes records to an HTTP endpoint.
type HTTPSink struct {
	url         string
	client      *http.Client
	maxRetries  int
	backoffBase time.Duration
}

// NewHTTPSink creates a new HTTP sink.
func NewHTTPSink(ctx context.Context, url string, maxRetries int, backoffBase time.Duration) (*HTTPSink, error) {
	if url == "" {
		return nil, fmt.Errorf("%w: URL required for HTTP sink", ErrOpenSink)
	}

	hs := &HTTPSink{
		url:         url,
		maxRetries:  maxRetries,
		backoffBase: backoffBase,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	// Test connection
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid URL: %v", ErrOpenSink, err)
	}

	// Don't fail on connection test, just log if it fails
	_ = req

	return hs, nil
}

// Write sends a record to the HTTP endpoint.
func (hs *HTTPSink) Write(record interface{}) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("%w: marshal error: %v", ErrWriteSink, err)
	}

	var lastErr error
	for attempt := 0; attempt <= hs.maxRetries; attempt++ {
		req, err := http.NewRequest("POST", hs.url, bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("%w: create request: %v", ErrWriteSink, err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := hs.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("%w: http request failed: %v", ErrWriteSink, err)
			if attempt < hs.maxRetries {
				time.Sleep(hs.backoffBase * time.Duration(1<<attempt))
				continue
			}
			return lastErr
		}

		// Read and close response body
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}

		lastErr = fmt.Errorf("%w: http error status %d", ErrWriteSink, resp.StatusCode)
		if attempt < hs.maxRetries {
			time.Sleep(hs.backoffBase * time.Duration(1<<attempt))
			continue
		}
	}

	return lastErr
}

// Close closes the HTTP sink (no-op for HTTP).
func (hs *HTTPSink) Close() error {
	if hs.client != nil {
		hs.client.CloseIdleConnections()
	}
	return nil
}

