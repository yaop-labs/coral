package pipeline

import "context"

// Signal is a batch of one telemetry signal — trace spans, metric points, or
// log records. Len reports the number of items carried; a batch with Len() == 0
// is empty and is never enqueued or exported.
type Signal interface {
	Len() int
}

// Receiver generates batches and pushes them into the pipeline via emit.
// Start blocks until ctx is canceled or a fatal error occurs.
// Stop stops the receiver.
type Receiver[T Signal] interface {
	Start(ctx context.Context, emit func(context.Context, T) error) error
	Stop(ctx context.Context) error
}

// Processor transforms or filters a batch.
// Process returns an empty batch to drop everything.
// Close releases processor resources.
type Processor[T Signal] interface {
	Process(ctx context.Context, b T) (T, error)
	Close() error
}

// Exporter receives batches that have passed the full processor chain.
// Export may be called concurrently.
// Close waits for in-flight exports.
type Exporter[T Signal] interface {
	Export(ctx context.Context, b T) error
	Close() error
}
