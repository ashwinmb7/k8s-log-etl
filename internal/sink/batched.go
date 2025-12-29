package sink

import (
	"context"
	"sync"
	"time"
)

// BatchedSink wraps a Writer to batch writes for better performance.
type BatchedSink struct {
	wrapped      Writer
	batchSize    int
	flushInterval time.Duration
	buffer       []interface{}
	mu           sync.Mutex
	flushTicker  *time.Ticker
	done         chan struct{}
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewBatchedSink creates a new batched sink wrapper.
func NewBatchedSink(wrapped Writer, batchSize int, flushInterval time.Duration) (*BatchedSink, error) {
	if batchSize <= 0 {
		return nil, ErrOpenSink
	}
	if flushInterval <= 0 {
		flushInterval = time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	bs := &BatchedSink{
		wrapped:      wrapped,
		batchSize:    batchSize,
		flushInterval: flushInterval,
		buffer:       make([]interface{}, 0, batchSize),
		done:         make(chan struct{}),
		ctx:          ctx,
		cancel:       cancel,
	}

	// Start flush ticker
	bs.flushTicker = time.NewTicker(flushInterval)
	bs.wg.Add(1)
	go bs.flushLoop()

	return bs, nil
}

// Write adds a record to the batch. Flushes automatically when batch is full.
func (bs *BatchedSink) Write(record interface{}) error {
	bs.mu.Lock()
	bs.buffer = append(bs.buffer, record)
	shouldFlush := len(bs.buffer) >= bs.batchSize
	bs.mu.Unlock()

	if shouldFlush {
		return bs.flush()
	}
	return nil
}

// flush writes all buffered records to the wrapped sink.
func (bs *BatchedSink) flush() error {
	bs.mu.Lock()
	if len(bs.buffer) == 0 {
		bs.mu.Unlock()
		return nil
	}
	batch := make([]interface{}, len(bs.buffer))
	copy(batch, bs.buffer)
	bs.buffer = bs.buffer[:0]
	bs.mu.Unlock()

	// Write all records in the batch
	for _, record := range batch {
		if err := bs.wrapped.Write(record); err != nil {
			return err
		}
	}
	return nil
}

// flushLoop periodically flushes the buffer.
func (bs *BatchedSink) flushLoop() {
	defer bs.wg.Done()
	for {
		select {
		case <-bs.ctx.Done():
			return
		case <-bs.flushTicker.C:
			if err := bs.flush(); err != nil {
				// Log error but continue
				continue
			}
		}
	}
}

// Close flushes remaining records and closes the wrapped sink.
func (bs *BatchedSink) Close() error {
	// Stop ticker and flush loop
	bs.cancel()
	bs.flushTicker.Stop()
	close(bs.done)
	bs.wg.Wait()

	// Flush remaining records
	if err := bs.flush(); err != nil {
		return err
	}

	return bs.wrapped.Close()
}

