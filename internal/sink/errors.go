package sink

import "errors"

var (
	// ErrOpenSink indicates a failure to open or initialize a sink.
	ErrOpenSink = errors.New("open sink")
	// ErrWriteSink indicates a failure while writing a record.
	ErrWriteSink = errors.New("write sink")
	// ErrRotateSink indicates a failure while rotating an output file.
	ErrRotateSink = errors.New("rotate sink")
)
